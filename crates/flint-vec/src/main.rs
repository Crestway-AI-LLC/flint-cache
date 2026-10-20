// SPDX-License-Identifier: Elastic-2.0
//! The vector-memory co-processor binary (ADR-0017). It accepts `FLINTFAM`
//! frames from the proxy, serves `VEC.*` against a shared [`Store`], and for a
//! write performs the durable side over a `PROXYCHAN` channel to the proxy edge
//! BEFORE committing the index change — so the in-memory index is never ahead
//! of the durable copy (ADR-0017 D2).
//!
//! Cold-start rebuild (D3): the index lives only in memory, so a restarted
//! co-processor must reload it from the durable rows in the tenant namespace.
//! It cannot scan proactively (it holds no token until a command arrives), so
//! rebuild is LAZY, per namespace, on FIRST TOUCH: the first `FLINTFAM` for a
//! namespace marks it `Loading`, spawns a background rebuild that reuses that
//! command's channel to SCAN + GET the reserved-prefix rows, and replies
//! `-LOADING`; the client retries and is served once the index is warm.
//!
//! Mesh TLS (ADR-0010 D5) is opt-in and matches the rest of the fleet's flag
//! convention. With `--internal-ca/--internal-cert/--internal-key` the inbound
//! FLINTFAM listener is a mutual-TLS server that presents the co-processor's
//! **serverAuth-only** leaf (`coproc.crt`, minted by flintctl) and verifies the
//! proxy's mesh client leaf — the co-processor holds no clientAuth credential,
//! so it can never dial the mesh as a member (that absence IS the isolation
//! boundary). With `--client-tls` the outbound PROXYCHAN dial-back to the proxy
//! edge is an edge client that verifies the edge's server cert against the same
//! CA and presents nothing (edge auth is the channel token, not a cert). Omit
//! the flags and every hop is plaintext, exactly like the ADR-0010 stand-ins.
//!
//! Rebuild is RESUMABLE across channels (D3): each channel is bounded by the
//! proxy's `--family-budget` (a per-channel data-command count), so a set larger
//! than one channel is loaded over several — each `FLINTFAM` retry donates a
//! fresh channel, the co-processor GETs as many rows as that budget allows, and
//! the namespace is marked warm only when the LAST row is in. A partially-loaded
//! index is never served (a partial k-NN is a wrong answer, not a slow one). The
//! one bound is that a set's KEY LIST must fit a single channel's SCAN — keys
//! are tiny next to the vectors, and a set that overflows even that surfaces a
//! raise-the-budget error rather than a silent partial.

use flint_resp::{Decoded, Value, decode, encode};
use flint_vec::filter::Field;
use flint_vec::ftstore;
use flint_vec::{
    Apply, IndexKind, LeftOut, Metric, Persist, Plan, Quant, Store, decode_config, decode_vec_row,
    parse_durable_key,
};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod ftchan;

/// Per-namespace load state: absent = never touched; `Loading` = a rebuild is
/// in flight or paused between channels (commands get `-LOADING`); `Loaded` = the
/// index reflects every durable row and serves normally.
///
/// A large set can exceed one channel's data-command budget (D3), so the rebuild
/// resumes across channels: each `FLINTFAM` retry donates a fresh budget, and the
/// namespace is marked `Loaded` ONLY when the last row is in — a partially-loaded
/// index is never served (a partial k-NN is a wrong answer, not a slow one).
enum LoadState {
    Loading {
        /// A chunk is actively draining a channel right now (guards against two
        /// chunks racing on one namespace).
        running: bool,
        /// How far the rebuild has got: still enumerating the indexes, or
        /// fetching the rows they named.
        phase: Phase,
    },
    Loaded,
}

/// Where a rebuild is up to. BOTH halves are resumable across channels, which
/// discovery was not when it first landed: enumerating a set costs one command
/// per bucket, so `1 + INDEX_BUCKETS` commands for a single set already exceeds
/// a channel's 256-command budget (D3) and the rebuild could never finish.
#[derive(Clone)]
enum Phase {
    /// Enumerating the co-processor's own indexes.
    Discover {
        /// Set names still to walk; `None` until the set index itself is read.
        /// The front element is the set currently being enumerated.
        sets: Option<Vec<Vec<u8>>>,
        /// Next bucket to read for the front set.
        bucket: u16,
        /// Keys found so far — configs first, then vectors, so a set exists
        /// before its Inserts commit.
        configs: Vec<Vec<u8>>,
        vectors: Vec<Vec<u8>>,
    },
    /// Enumeration finished; fetching rows from `next` onward.
    Fetch { keys: Vec<Vec<u8>>, next: usize },
    /// The vectors are in; loading the `FT.*` indexes (ADR-0060).
    Ft(ftchan::FtLoad),
}

impl Phase {
    fn start() -> Self {
        Phase::Discover {
            sets: None,
            bucket: 0,
            configs: Vec::new(),
            vectors: Vec::new(),
        }
    }
}
type Loads = Arc<Mutex<HashMap<Vec<u8>, LoadState>>>;

/// The result of one rebuild chunk (one channel's worth of budget).
enum Chunk {
    /// Every row is loaded; the set is warm. Carries the total vector count.
    Done(usize),
    /// The channel's budget ran out; carries the resume point for the next
    /// channel, plus how many rows this chunk installed.
    More { phase: Phase, installed: usize },
}

/// A decoded config row awaiting install: `(set, dim, metric, index, quant,
/// fields)`.
type LoadedConfig = (Vec<u8>, usize, Metric, IndexKind, Quant, Vec<Field>);
/// A decoded vector row awaiting install: `(set, id, vector, meta, expires_at)`.
type LoadedVector = (Vec<u8>, Vec<u8>, Vec<f32>, Option<Vec<u8>>, Option<u64>);

/// The co-processor's mesh identity (ADR-0010 D5). Cheaply cloned (Arcs) into
/// every connection and rebuild thread. Both fields `None` = all-plaintext.
#[derive(Clone, Default)]
struct Tls {
    /// Inbound FLINTFAM listener: a mutual-TLS server presenting the co-proc's
    /// serverAuth-only leaf and verifying the proxy's mesh client leaf. Hot-
    /// reloadable so `rotate-certs` swaps the leaf without a restart.
    inbound: Option<Arc<flint_tls::ReloadableServerConfig>>,
    /// Outbound PROXYCHAN dial-back to the proxy edge: verify the edge server
    /// cert against the internal CA, present no client cert. Built once — the CA
    /// is stable across leaf rotations, and this is the only thing the edge dial
    /// needs.
    edge: Option<Arc<flint_tls::ClientConfig>>,
}

/// `FLINTINFO`'s body: `field:value` lines, as a node's, so flint-exporter
/// turns each number into a gauge and `build` into `build_info`.
fn flintinfo(s: &flint_vec::StoreStats, loading: usize) -> String {
    format!(
        "build:{}\r\nrole:coproc\r\nindex_bytes:{}\r\nindex_cap_ns_bytes:{}\r\n\
         index_cap_total_bytes:{}\r\nnamespaces:{}\r\nnamespaces_loading:{loading}\r\n\
         sets:{}\r\nvectors:{}\r\nvecfull_ns_total:{}\r\nvecfull_server_total:{}\r\n\
         packed_rows:{}\r\nvec_filters:{}\r\nft:{}\r\nft_indexes:{}\r\nft_docs:{}\r\n",
        build_version(),
        s.index_bytes,
        s.index_cap,
        s.total_cap,
        s.namespaces,
        s.sets,
        s.vectors,
        s.refused_ns,
        s.refused_total,
        u8::from(s.packed_rows),
        u8::from(s.filters),
        u8::from(s.ft),
        s.ft_indexes,
        s.ft_docs,
    )
}

fn build_version() -> String {
    flint_build::version(env!("CARGO_PKG_VERSION"))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    // Ask the binary what it is, WITHOUT starting it — the same flag every
    // other fleet binary answers (#111: the launcher stamps builds by asking
    // each one). A co-processor that ignored it did not just miss a version
    // string: the flag fell through to the serving path, so the caller got a
    // process that bound a port and never returned. `flintctl upgrade`'s
    // post-roll check and the drill warm-up both hang on exactly that.
    if args.iter().any(|a| a == "--build-version") {
        println!("{}", build_version());
        return;
    }
    let port: u16 = arg(&args, "--port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(6700);
    // BUG-0203: listen where `flintctl` says (`--bind`, the host of the
    // inventory's `coproc` address), on loopback by default as flint-server
    // and the control plane do. This built `0.0.0.0` whatever it was told, so
    // a seat asked for loopback answered FLINTFAM on every interface.
    let bind_host = arg(&args, "--bind").unwrap_or_else(|| "127.0.0.1".into());
    // D4: per-namespace index-memory cap in bytes; 0 (default) = unlimited. On
    // a co-processor shared by many tenants, set this so one tenant's set cannot
    // exhaust the process RAM and starve every other tenant's search.
    let index_cap: usize = arg(&args, "--index-mem-bytes")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // ADR-0050 D2: a cap over every namespace's index together; 0 (default) =
    // none. Set it below what the host's seats need, so vector indexes can never
    // take the cache's RAM however many tenants hold their per-namespace cap.
    let total_cap: usize = arg(&args, "--index-mem-total-bytes")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut initial = Store::new();
    initial.set_index_cap(index_cap);
    initial.set_index_total_cap(total_cap);
    // ADR-0050 D5, the write half: durable vector rows are written packed, a
    // third of text's bytes, which is a third of what the tenant is billed
    // for them. One release after the read half (`499e5ea`), so the release a
    // fleet rolls back to reads every row this one writes. `--write-text-rows`
    // writes what the releases before this one wrote: for a fleet that must
    // stay readable two releases back, and for the drill that stands in for
    // the previous release. `--write-packed-rows`, the read half's opt-in, is
    // still accepted and now changes nothing. This binary reads both either way.
    let packed_rows = !args.iter().any(|a| a == "--write-text-rows");
    initial.set_packed_rows(packed_rows);
    // ADR-0059, the second of two releases: serve `VEC.CREATE ... FIELD` and
    // `VEC.SEARCH ... FILTER` unless started with `--no-vec-filters`. The
    // release before this one (v0.1.0-rc.84) knows them, behind its opt-in
    // `--vec-filters`, and refuses every `VEC.SEARCH` option it does not
    // know, so a rollback to it serves a filter or refuses it and never
    // answers it as if there were none. The releases before that one do, so
    // a fleet whose tenants filter must not roll back below rc.84.
    // `--vec-filters` is still accepted and now changes nothing.
    let filters = !args.iter().any(|a| a == "--no-vec-filters");
    initial.set_filters(filters);
    // ADR-0060, the first of two releases: an `FT.*` index is made only with
    // `--ft`. A release before this one does not route writes under an
    // index's prefix through the co-processor, so after a rollback they would
    // reach the pairs and the index would miss them. One made while it was
    // on is served without it, so it can still be dropped.
    let ft = args.iter().any(|a| a == "--ft");
    initial.set_ft(ft);
    // ADR-0049 D2: where a quantized set keeps its full vectors. The files are
    // derived like the index, so every one found here is deleted before
    // serving and the rebuild writes them again. The lock is what makes that
    // safe: a second co-processor on the same directory would otherwise delete
    // the first one's files from under it. Held until the process exits.
    let vec_dir = arg(&args, "--vec-dir").map(std::path::PathBuf::from);
    let _vec_lock = vec_dir.as_ref().map(|dir| {
        let lock =
            claim_vec_dir(dir).unwrap_or_else(|e| panic!("--vec-dir {}: {e}", dir.display()));
        initial.set_vec_dir(dir.clone());
        lock
    });
    let store: Arc<Mutex<Store>> = Arc::new(Mutex::new(initial));
    let loads: Loads = Arc::new(Mutex::new(HashMap::new()));
    let tls = build_tls(&args);
    let bind = format!("{bind_host}:{port}");
    let listener = TcpListener::bind((bind_host.as_str(), port))
        .unwrap_or_else(|e| panic!("bind {bind}: {e}"));
    let cap_note = match (index_cap, total_cap) {
        (0, 0) => "index mem unlimited".to_string(),
        (ns, 0) => format!("index mem cap {ns} B/ns"),
        (0, all) => format!("index mem cap {all} B in all"),
        (ns, all) => format!("index mem cap {ns} B/ns, {all} B in all"),
    };
    let vec_note = match &vec_dir {
        Some(d) => format!("quantized vectors in {}", d.display()),
        None => "quantized vectors in RAM".to_string(),
    };
    let tls_note = match (tls.inbound.is_some(), tls.edge.is_some()) {
        (false, false) => "plaintext",
        (true, false) => "mesh mTLS",
        (true, true) => "mesh mTLS + edge TLS",
        (false, true) => "edge TLS only",
    };
    let rows_note = if packed_rows {
        "packed rows"
    } else {
        "text rows"
    };
    let filters_note = if filters { "filters on" } else { "filters off" };
    let ft_note = if ft { "FT.* on" } else { "FT.* off" };
    eprintln!(
        "flint-vec co-processor on {bind} ({tls_note}, v0.2 flat+hnsw, {cap_note}, {vec_note}, {rows_note}, {filters_note}, {ft_note})"
    );

    // Background reclamation of expired vectors. Reads/searches already mask an
    // expired id the instant its deadline passes (so cadence is not a correctness
    // knob — only how promptly the RAM and the D4 accounting are freed), and the
    // durable rows carry their own PX and expire independently. 0 disables it.
    let sweep_ms: u64 = arg(&args, "--sweep-ms")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    if sweep_ms > 0 {
        let store = store.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(sweep_ms));
                let swept = store.lock().expect("store lock").sweep_expired(now_ms());
                if swept > 0 {
                    eprintln!("flint-vec: swept {swept} expired vector(s)");
                }
            }
        });
    }

    for conn in listener.incoming().flatten() {
        let (store, loads, tls) = (store.clone(), loads.clone(), tls.clone());
        std::thread::spawn(move || serve(conn, store, loads, tls));
    }
}

/// Take `--vec-dir` for this process: create it, lock it, and delete the
/// vector files a previous process left. Returns the lock, which must live as
/// long as the process does.
fn claim_vec_dir(dir: &std::path::Path) -> std::io::Result<std::fs::File> {
    std::fs::create_dir_all(dir)?;
    let lock = std::fs::File::create(dir.join("LOCK"))?;
    lock.try_lock()
        .map_err(|e| std::io::Error::other(format!("held by another flint-vec ({e})")))?;
    let mut swept = 0;
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path
            .extension()
            .is_some_and(|x| x == flint_vec::vecfile::EXTENSION)
        {
            std::fs::remove_file(&path)?;
            swept += 1;
        }
    }
    if swept > 0 {
        eprintln!(
            "flint-vec: removed {swept} vector file(s) from {} (derived; the rebuild writes them again)",
            dir.display()
        );
    }
    Ok(lock)
}

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

/// Build the mesh-TLS identity from the flags (ADR-0010 D5). The three
/// `--internal-*` flags are all-or-nothing (matching flint-server/flint-proxy);
/// `--client-tls` additionally TLS-wraps the PROXYCHAN dial-back, verifying
/// the edge against `--edge-ca`: the bundle the fleet's edge certificate
/// chains to, which flintctl takes from the inventory's `edge-trust`. Without
/// it the internal CA, which signs the edge certificate `bootstrap` mints.
///
/// BUG-0226: this used the internal CA always, so on a fleet whose edge
/// certificate is publicly issued every rebuild's dial-back failed
/// `UnknownIssuer` and every vector set answered LOADING for ever.
fn build_tls(args: &[String]) -> Tls {
    let inbound = match (
        arg(args, "--internal-ca"),
        arg(args, "--internal-cert"),
        arg(args, "--internal-key"),
    ) {
        (Some(ca), Some(cert), Some(key)) => Some(
            flint_tls::ReloadableServerConfig::watch(&ca, &cert, &key)
                .unwrap_or_else(|e| panic!("internal TLS config: {e}")),
        ),
        (None, None, None) => None,
        _ => panic!("--internal-ca, --internal-cert, --internal-key must be given together"),
    };
    let edge = if args.iter().any(|a| a == "--client-tls") {
        let ca = arg(args, "--edge-ca")
            .or_else(|| arg(args, "--internal-ca"))
            .unwrap_or_else(|| {
                panic!("--client-tls needs --edge-ca or --internal-ca to verify the edge")
            });
        Some(flint_tls::edge_client_config(&ca).unwrap_or_else(|e| panic!("edge TLS config: {e}")))
    } else {
        None
    };
    Tls { inbound, edge }
}

/// One FLINTFAM connection from the proxy (its co-proc pool keeps the
/// connection warm and sends one command at a time over it).
fn serve(tcp: TcpStream, store: Arc<Mutex<Store>>, loads: Loads, tls: Tls) {
    // Drive the server-side handshake when mesh TLS is on: present the co-proc's
    // serverAuth-only leaf and REQUIRE + verify the proxy's mesh client leaf. A
    // plaintext prober or any dialer without a CA-signed client cert fails the
    // handshake on first read and drops — the isolation boundary at the transport.
    let cfg = tls.inbound.as_ref().and_then(|r| r.current());
    let mut stream = match flint_tls::accept(tcp, &cfg) {
        Ok(s) => s,
        Err(_) => return,
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(60)));
    let mut buf = Vec::new();
    let mut chunk = [0u8; 16 * 1024];
    loop {
        let frame = match decode(&buf) {
            Ok(Decoded::Complete(v, used)) => {
                buf.drain(..used);
                v
            }
            Ok(Decoded::NeedMore) => match stream.read(&mut chunk) {
                Ok(0) | Err(_) => return, // peer closed / timeout
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    continue;
                }
            },
            Err(_) => return, // protocol error: drop the connection
        };
        let reply = handle_flintfam(&frame, &store, &loads, &tls);
        let mut out = Vec::new();
        encode(&reply, &mut out);
        if stream.write_all(&out).is_err() {
            return;
        }
    }
}

/// Parse `FLINTFAM <token> <callback> <ns> <cmd...>`. Gate on the namespace's
/// load state (rebuild-on-first-touch, D3), then plan the command; for a write
/// perform the durable side over the channel before committing.
fn handle_flintfam(frame: &Value, store: &Arc<Mutex<Store>>, loads: &Loads, tls: &Tls) -> Value {
    let Value::Array(Some(parts)) = frame else {
        return err("ERR expected FLINTFAM array");
    };
    let bulks: Option<Vec<&[u8]>> = parts
        .iter()
        .map(|p| match p {
            Value::Bulk(Some(b)) => Some(b.as_slice()),
            _ => None,
        })
        .collect();
    let Some(bulks) = bulks else {
        return err("ERR FLINTFAM parts must be bulk strings");
    };
    // The operator's view (ADR-0050 D2), read by flint-exporter. It is totals
    // over every tenant, so it is not a family command: tenants' commands
    // arrive inside FLINTFAM, and only the mesh reaches this listener (mutual
    // TLS, or loopback since BUG-0203).
    if bulks.len() == 1 && bulks[0].eq_ignore_ascii_case(b"FLINTINFO") {
        let stats = store.lock().expect("store lock").stats();
        let loading = loads
            .lock()
            .expect("loads lock")
            .values()
            .filter(|l| !matches!(l, LoadState::Loaded))
            .count();
        return Value::Bulk(Some(flintinfo(&stats, loading).into_bytes()));
    }
    // BUG-0255: forget a namespace whose tenant has gone, so a tenant given
    // the same namespace later cannot read the old one's vectors from memory.
    // flintctl sends it on `tenant remove` and on `tenant add`. Like FLINTINFO
    // it is not a family command, so no tenant can send it.
    if bulks.len() == 2 && bulks[0].eq_ignore_ascii_case(b"FLINTNSDROP") {
        let ns = bulks[1];
        // Loads, then the store: the order every path that holds both takes.
        let mut lk = loads.lock().expect("loads lock");
        lk.remove(ns);
        let dropped = store.lock().expect("store lock").drop_namespace(ns);
        ftchan::forget(ns);
        drop(lk);
        eprintln!(
            "flint-vec: dropped ns {:?} ({dropped} set(s)); its next touch rebuilds from durable rows",
            String::from_utf8_lossy(ns)
        );
        return Value::Integer(dropped as i64);
    }
    if bulks.len() < 5 || !bulks[0].eq_ignore_ascii_case(b"FLINTFAM") {
        return err("ERR FLINTFAM <token> <callback> <ns> <cmd...>");
    }
    let (token, callback, ns) = (bulks[1], bulks[2], bulks[3]);
    let cmd_args: Vec<Vec<u8>> = bulks[4..].iter().map(|b| b.to_vec()).collect();

    // Load gate (D3): serve only a fully-warm namespace. A cold or paused
    // namespace claims THIS command's channel to run one rebuild chunk (resuming
    // where the previous channel's budget ran out) and answers -LOADING; the
    // client retries, donating another channel, until the whole set is in. `keys`
    // being cloned into both the state and the thread lets a concurrent retry see
    // `running` without waiting on the chunk.
    {
        let mut lk = loads.lock().expect("loads lock");
        let resume = match lk.get(ns) {
            Some(LoadState::Loaded) => None,
            Some(LoadState::Loading { running: true, .. }) => {
                return err("LOADING vector index is warming, retry");
            }
            Some(LoadState::Loading {
                running: false,
                phase,
            }) => Some(phase.clone()),
            None => Some(Phase::start()),
        };
        if let Some(phase) = resume {
            lk.insert(
                ns.to_vec(),
                LoadState::Loading {
                    running: true,
                    phase: phase.clone(),
                },
            );
            // Read under the loads lock, so a FLINTNSDROP either came before
            // this chunk or makes it install nothing (BUG-0255).
            let generation = store.lock().expect("store lock").generation(ns);
            drop(lk);
            let (ns, token, callback) = (ns.to_vec(), token.to_vec(), callback.to_vec());
            let (store, loads, edge) = (store.clone(), loads.clone(), tls.edge.clone());
            std::thread::spawn(move || {
                rebuild_chunk(ns, token, callback, store, loads, edge, phase, generation)
            });
            return err("LOADING vector index is warming, retry");
        }
    }

    // ADR-0060: FT.* and the writes a proxy sends for an indexed key.
    if ftstore::is_ft_command(&cmd_args[0]) {
        return ftchan::handle(ns, token, callback, &cmd_args, store, &tls.edge);
    }
    let plan = store
        .lock()
        .expect("store lock")
        .plan(ns, &cmd_args, now_ms());
    match plan {
        Plan::Reply(v) => v,
        Plan::Write { persist, apply, ok } => {
            match perform_persist(callback, token, &persist, &tls.edge) {
                Ok(()) => {
                    store.lock().expect("store lock").commit(ns, apply);
                    ok
                }
                // A shed or failed durable write (e.g. -QUOTA on an over-quota
                // tenant) is relayed; the index is left untouched.
                Err(e) => e,
            }
        }
    }
}

/// Run ONE rebuild chunk on `token`'s channel: continue the SCAN (first chunk)
/// and GET as many rows as the channel's data-command budget allows, installing
/// them. Marks the namespace `Loaded` only when the LAST row is in; otherwise
/// saves the resume point and stays `Loading` for the next channel. On a
/// transient channel failure it clears `running` (keeping prior progress) so the
/// next touch retries — a partially-loaded index is NEVER marked warm (D3: a
/// partial k-NN is a wrong answer, not a slow one).
///
/// `generation` is the namespace's [`Store::generation`] when the chunk began.
/// A FLINTNSDROP since then (BUG-0255) means the rows this chunk read may be
/// the old tenant's: it installs none of them and leaves the load state as the
/// drop left it.
#[allow(clippy::too_many_arguments)]
fn rebuild_chunk(
    ns: Vec<u8>,
    token: Vec<u8>,
    callback: Vec<u8>,
    store: Arc<Mutex<Store>>,
    loads: Loads,
    edge: Option<Arc<flint_tls::ClientConfig>>,
    phase: Phase,
    generation: u64,
) {
    let name = String::from_utf8_lossy(&ns).into_owned();
    let result = rebuild_chunk_inner(&ns, &token, &callback, &store, &edge, phase, generation);
    let mut lk = loads.lock().expect("loads lock");
    if store.lock().expect("store lock").generation(&ns) != generation {
        eprintln!(
            "flint-vec: ns {name:?} was dropped during a rebuild chunk; it installed nothing"
        );
        return;
    }
    match result {
        Ok(Chunk::Done(total)) => {
            eprintln!("flint-vec: rebuilt ns {name:?} ({total} vectors) from durable rows");
            lk.insert(ns, LoadState::Loaded);
        }
        Ok(Chunk::More { phase, installed }) => {
            let where_ = match &phase {
                Phase::Discover {
                    sets,
                    bucket,
                    vectors,
                    ..
                } => format!(
                    "still enumerating ({} set(s) left, bucket {bucket}, {} vectors found)",
                    sets.as_ref().map_or(0, |s| s.len()),
                    vectors.len()
                ),
                Phase::Fetch { keys, next } => format!("{next}/{} keys fetched", keys.len()),
                Phase::Ft(_) => "vectors in, loading FT.* indexes".to_string(),
            };
            eprintln!(
                "flint-vec: ns {name:?} rebuild chunk loaded {installed}; {where_}; resuming on the next channel"
            );
            lk.insert(
                ns,
                LoadState::Loading {
                    running: false,
                    phase,
                },
            );
        }
        Err(e) => {
            eprintln!(
                "flint-vec: ns {name:?} rebuild chunk failed: {e} (will retry on next touch; index not served partial)"
            );
            if let Some(LoadState::Loading { running, .. }) = lk.get_mut(&ns) {
                *running = false;
            }
        }
    }
}

/// The channel side of one rebuild chunk. Returns [`Chunk::Done`] when the last
/// row lands, [`Chunk::More`] when the budget runs out mid-set (resume point
/// attached), or `Err` for a transient failure the caller should retry.
fn rebuild_chunk_inner(
    ns: &[u8],
    token: &[u8],
    callback: &[u8],
    store: &Arc<Mutex<Store>>,
    edge: &Option<Arc<flint_tls::ClientConfig>>,
    phase: Phase,
    generation: u64,
) -> Result<Chunk, String> {
    let addr = std::str::from_utf8(callback).map_err(|_| "bad callback".to_string())?;
    let mut ch = flint_tls::connect_edge(addr, edge).map_err(|e| format!("channel dial: {e}"))?;
    let _ = ch.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = ch.set_write_timeout(Some(Duration::from_secs(5)));
    // Closed with a reset when it drops, on every path out (BUG-0269).
    let _ = ch.set_abortive_close();
    send_cmd(&mut ch, &[b"PROXYCHAN", token]).map_err(|e| format!("channel open: {e}"))?;
    if let Value::Error(e) = read_reply(&mut ch).map_err(|e| format!("channel open: {e}"))? {
        return Err(format!("channel refused: {e}"));
    }

    // Phase 1: build the key list from the co-processor's OWN durable indexes,
    // configs before vectors so a set exists before its Inserts commit.
    //
    // This USED TO SCAN THE WHOLE NAMESPACE and filter for the reserved prefix,
    // which made recovery cost the size of the TENANT rather than the number of
    // vectors. Beside a 500 GB data plane that is ~500M keys: the seat sat in
    // -LOADING until it tripped the per-channel SCAN budget, so a co-processor
    // could never come back on any fleet big enough to want one — and restart is
    // every upgrade, every host replacement, every OOM (#194). A prefix seek
    // cannot fix it either: keys encode as <ns><slot><user key> and the slot is
    // a hash, so rows sharing a user-key prefix share no physical range.
    //
    // ENUMERATION IS ITSELF CHUNKED, which the first version of this got wrong.
    // Walking a set costs one command per bucket, so `1 + INDEX_BUCKETS` already
    // exceeds a channel's 256-command budget for a SINGLE set: discovery never
    // finished and the seat rebuilt 0 vectors forever. Rather than hardcode the
    // proxy's budget here — two constants that would drift apart silently — the
    // budget is discovered the same way Phase 2 discovers it: an exhausted
    // channel answers an error naming it, which is the EXPECTED end of a chunk.
    let (keys, next) = match phase {
        Phase::Ft(at) => return Ok(ft_chunk(&mut ch, ns, store, at, generation, 0)),
        Phase::Fetch { keys, next } => (keys, next),
        Phase::Discover {
            mut sets,
            mut bucket,
            mut configs,
            mut vectors,
        } => {
            // The set index itself, once. Its cost is one command, and a fleet
            // has a handful of vector sets, so it is never the bottleneck.
            if sets.is_none() {
                match smembers(&mut ch, &flint_vec::sets_index_key())? {
                    Some(names) => {
                        configs.extend(names.iter().map(|n| flint_vec::config_key(n)));
                        sets = Some(names);
                    }
                    None => {
                        return Ok(Chunk::More {
                            phase: Phase::Discover {
                                sets: None,
                                bucket: 0,
                                configs,
                                vectors,
                            },
                            installed: 0,
                        });
                    }
                }
            }
            let mut remaining = sets.unwrap_or_default();
            // Walk buckets of the front set until the channel is spent.
            while let Some(set) = remaining.first().cloned() {
                while bucket < flint_vec::INDEX_BUCKETS {
                    match smembers(&mut ch, &flint_vec::index_key(&set, bucket))? {
                        Some(ids) => {
                            vectors.extend(ids.iter().map(|id| flint_vec::vector_key(&set, id)));
                            bucket += 1;
                        }
                        None => {
                            return Ok(Chunk::More {
                                phase: Phase::Discover {
                                    sets: Some(remaining),
                                    bucket,
                                    configs,
                                    vectors,
                                },
                                installed: 0,
                            });
                        }
                    }
                }
                remaining.remove(0);
                bucket = 0;
            }
            configs.extend(vectors);
            (configs, 0)
        }
    };

    // Phase 2: GET from `next` onward, installing each row, until the channel's
    // data-command budget is exhausted (resume next chunk) or the set is done.
    let mut configs: Vec<LoadedConfig> = Vec::new();
    let mut vectors: Vec<LoadedVector> = Vec::new();
    let mut i = next;
    let mut budget_done = false;
    // Rows this binary cannot read: a damaged row, or one a later binary wrote
    // in a format this one predates (ADR-0050 D5 is the first). Each is left
    // out of the index, so the count is said out loud rather than leaving a
    // set quietly smaller than its rows.
    let mut unreadable = 0usize;
    let mut first_unreadable = String::new();
    while i < keys.len() {
        let Some((kind, set, id)) = parse_durable_key(&keys[i]) else {
            i += 1;
            continue;
        };
        send_cmd(&mut ch, &[b"GET", &keys[i]]).map_err(|e| format!("GET send: {e}"))?;
        match read_reply(&mut ch).map_err(|e| format!("GET read: {e}"))? {
            Value::Bulk(Some(val)) if kind == b'c' => {
                if let Some((dim, metric, index, quant, fields)) = decode_config(&val) {
                    configs.push((set, dim, metric, index, quant, fields));
                }
                i += 1;
            }
            Value::Bulk(Some(val)) if kind == b'v' => {
                // The expiry rides the durable row, so a rebuilt index restores
                // each id's TTL for free — no separate expiry store to replay.
                match decode_vec_row(&val) {
                    Ok((vec, meta, expires_at)) => vectors.push((set, id, vec, meta, expires_at)),
                    Err(e) => {
                        if unreadable == 0 {
                            first_unreadable = format!(
                                "set {:?} id {:?}: {e}",
                                String::from_utf8_lossy(&set),
                                String::from_utf8_lossy(&id)
                            );
                        }
                        unreadable += 1;
                    }
                }
                i += 1;
            }
            // A budget-exhausted channel is the EXPECTED end of a chunk, not a
            // failure: install what this chunk got and resume from `i` next time.
            Value::Error(e) if e.contains("budget") => {
                budget_done = true;
                break;
            }
            Value::Error(e) => return Err(format!("channel error mid-rebuild: {e}")),
            _ => i += 1, // null / wrong shape: skip
        }
    }
    install(store, ns, generation, configs, vectors);
    if unreadable > 0 {
        eprintln!(
            "flint-vec: rebuild of ns {:?} left out {unreadable} vector row(s) it cannot read; first: {first_unreadable}",
            String::from_utf8_lossy(ns)
        );
    }
    let installed = i - next;
    if i >= keys.len() && !budget_done {
        let total = keys
            .iter()
            .filter(|k| matches!(parse_durable_key(k), Some((b'v', _, _))))
            .count();
        // With `--ft` or without: an index made while it was on is loaded and
        // served either way (ADR-0060).
        Ok(
            match ft_chunk(
                &mut ch,
                ns,
                store,
                ftchan::FtLoad::Defs,
                generation,
                installed,
            ) {
                Chunk::Done(_) => Chunk::Done(total),
                more => more,
            },
        )
    } else {
        Ok(Chunk::More {
            phase: Phase::Fetch { keys, next: i },
            installed,
        })
    }
}

/// One chunk of the `FT.*` half of a rebuild (ADR-0060), from `at`. A failed
/// chunk resumes from the index list on the next channel: reading a document
/// again gives the same index.
fn ft_chunk(
    ch: &mut flint_tls::Stream,
    ns: &[u8],
    store: &Arc<Mutex<Store>>,
    at: ftchan::FtLoad,
    generation: u64,
    installed: usize,
) -> Chunk {
    match ftchan::load_chunk(ch, ns, store, at, generation) {
        Ok(None) => Chunk::Done(0),
        Ok(Some(at)) => Chunk::More {
            phase: Phase::Ft(at),
            installed,
        },
        Err(e) => {
            eprintln!(
                "flint-vec: ns {:?} FT.* rebuild chunk failed: {e}; resuming from the index list",
                String::from_utf8_lossy(ns)
            );
            Chunk::More {
                phase: Phase::Ft(ftchan::FtLoad::Defs),
                installed,
            }
        }
    }
}

/// Install decoded configs then vectors into the store under one lock. Returns
/// the vector count.
fn install(
    store: &Arc<Mutex<Store>>,
    ns: &[u8],
    generation: u64,
    configs: Vec<LoadedConfig>,
    vectors: Vec<LoadedVector>,
) -> usize {
    let mut st = store.lock().expect("store lock");
    // Dropped since the chunk began (BUG-0255): these rows may be the old
    // tenant's. Checked under the same lock as the commits below.
    if st.generation(ns) != generation {
        return 0;
    }
    for (set, dim, metric, index, quant, fields) in configs {
        st.commit(
            ns,
            Apply::CreateSet {
                set,
                dim,
                metric,
                index,
                quant,
                fields,
            },
        );
    }
    // Each row is held to the shape VEC.SET would have held it to (BUG-0265):
    // the tenant can write these rows itself. What is left out is said, by
    // reason, as an unreadable row is. The caps govern new writes only
    // (ADR-0017 D3), so they are not checked here.
    let mut n = 0usize;
    let mut left: Vec<(LeftOut, usize, String)> = Vec::new();
    for (set, id, vec, meta, expires_at) in vectors {
        match st.check_vector(ns, &set, &id, vec.len()) {
            Ok(()) => {
                st.commit(
                    ns,
                    Apply::Insert {
                        set,
                        id,
                        vec,
                        meta,
                        expires_at,
                    },
                );
                n += 1;
            }
            Err(why) => match left.iter_mut().find(|(w, _, _)| *w == why) {
                Some((_, count, _)) => *count += 1,
                None => left.push((
                    why,
                    1,
                    format!(
                        "set {:?} id {:?}",
                        String::from_utf8_lossy(&set),
                        String::from_utf8_lossy(&id)
                    ),
                )),
            },
        }
    }
    for (why, count, first) in left {
        eprintln!(
            "flint-vec: rebuild of ns {:?} left out {count} vector row(s) {}; first: {first}",
            String::from_utf8_lossy(ns),
            why.reason()
        );
    }
    n
}

/// Every member of a durable index key. A MISSING key is an empty set, not an
/// error: a set with no vectors in a given bucket is the ordinary case, and
/// there are INDEX_BUCKETS of them per set.
/// Every member of a durable index key, or `None` when the channel's budget is
/// spent — which is the EXPECTED end of a chunk, not a fault, so the caller
/// saves its place and resumes on the next channel. A MISSING key is an empty
/// set: most of a set's INDEX_BUCKETS buckets are empty for a small corpus.
fn smembers(ch: &mut flint_tls::Stream, key: &[u8]) -> Result<Option<Vec<Vec<u8>>>, String> {
    send_cmd(ch, &[b"SMEMBERS", key]).map_err(|e| format!("SMEMBERS: {e}"))?;
    match read_reply(ch).map_err(|e| format!("SMEMBERS: {e}"))? {
        Value::Array(Some(items)) => Ok(Some(
            items
                .into_iter()
                .filter_map(|v| match v {
                    Value::Bulk(Some(b)) => Some(b),
                    _ => None,
                })
                .collect(),
        )),
        Value::Array(None) => Ok(Some(Vec::new())),
        Value::Error(e) if e.contains("budget") => Ok(None),
        Value::Error(e) => Err(format!("SMEMBERS rejected: {e}")),
        _ => Err("SMEMBERS unexpected reply".into()),
    }
}

/// Open a single-use `PROXYCHAN` channel to the proxy edge and perform one
/// durable data command. `Ok(())` on a non-error reply; `Err(reply)` carries
/// the channel's error for the co-processor to relay to the client.
fn perform_persist(
    callback: &[u8],
    token: &[u8],
    persist: &[Persist],
    edge: &Option<Arc<flint_tls::ClientConfig>>,
) -> Result<(), Value> {
    let addr = std::str::from_utf8(callback).map_err(|_| err("ERR bad callback address"))?;
    let mut ch = flint_tls::connect_edge(addr, edge)
        .map_err(|e| err(&format!("COPROCUNAVAIL channel dial failed: {e}")))?;
    let _ = ch.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = ch.set_write_timeout(Some(Duration::from_secs(5)));
    // A channel per write, closed by this side, left a TIME_WAIT socket per
    // write here: a bulk load ran out of ephemeral ports and its writes
    // failed COPROCUNAVAIL. Every reply is read before it drops, so a reset
    // loses nothing (BUG-0269).
    let _ = ch.set_abortive_close();

    send_cmd(&mut ch, &[b"PROXYCHAN", token])
        .map_err(|e| err(&format!("COPROCUNAVAIL channel open failed: {e}")))?;
    if let Value::Error(e) =
        read_reply(&mut ch).map_err(|e| err(&format!("COPROCUNAVAIL channel open failed: {e}")))?
    {
        return Err(err(&format!("COPROCUNAVAIL channel refused: {e}")));
    }
    run_persist(&mut ch, persist)
}

/// Perform `persist` in order on an open channel, stopping at the first
/// error, which is relayed.
fn run_persist(ch: &mut flint_tls::Stream, persist: &[Persist]) -> Result<(), Value> {
    // A TTL'd row is written `SET key val PX <ms>`, so the durable copy expires on
    // the same deadline the index does — the data-plane key's own metering and
    // quota reclamation then come for free. `ttl_buf` outlives the borrow in the
    // command vector.
    // A write is a SEQUENCE now (the id index plus the row), and every command
    // must land before the caller commits to the index. Executed in order and
    // aborted on the first error: the orders are chosen so that stopping part
    // way leaves a recoverable state (see Plan::Write).
    for step in persist {
        let ttl_buf;
        let dcmd: Vec<&[u8]> = match step {
            Persist::Put {
                key,
                val,
                ttl_ms: Some(ms),
            } => {
                ttl_buf = ms.to_string().into_bytes();
                vec![b"SET", key, val, b"PX", &ttl_buf]
            }
            Persist::Put { key, val, .. } => vec![b"SET", key, val],
            Persist::Del { key } => vec![b"DEL", key],
            Persist::SAdd { key, member } => vec![b"SADD", key, member],
            Persist::SRem { key, member } => vec![b"SREM", key, member],
        };
        send_cmd(ch, &dcmd)
            .map_err(|e| err(&format!("COPROCUNAVAIL channel write failed: {e}")))?;
        // Relay the channel's own error verbatim — a -QUOTA (over storage quota)
        // is a truer, more actionable answer than a blanket COPROCUNAVAIL.
        if let r @ Value::Error(_) =
            read_reply(ch).map_err(|e| err(&format!("COPROCUNAVAIL channel write failed: {e}")))?
        {
            return Err(r);
        }
    }
    Ok(())
}

fn send_cmd(ch: &mut flint_tls::Stream, parts: &[&[u8]]) -> std::io::Result<()> {
    let arr = Value::Array(Some(
        parts
            .iter()
            .map(|p| Value::Bulk(Some(p.to_vec())))
            .collect(),
    ));
    let mut out = Vec::new();
    encode(&arr, &mut out);
    ch.write_all(&out)
}

fn read_reply(ch: &mut flint_tls::Stream) -> std::io::Result<Value> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match decode(&buf) {
            Ok(Decoded::Complete(v, _)) => return Ok(v),
            Ok(Decoded::NeedMore) => {
                let n = ch.read(&mut chunk)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "channel closed",
                    ));
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "bad channel reply",
                ));
            }
        }
    }
}

fn err(msg: &str) -> Value {
    Value::Error(msg.to_string())
}

/// Wall-clock ms since the Unix epoch — the clock every TTL is measured against,
/// and the same one the data plane stamps native keys with, so a vector's index
/// expiry and its durable key's PX agree.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(parts: &[&[u8]]) -> Value {
        Value::Array(Some(
            parts
                .iter()
                .map(|p| Value::Bulk(Some(p.to_vec())))
                .collect(),
        ))
    }

    fn loaded_set(store: &Arc<Mutex<Store>>, ns: &[u8]) {
        let configs = vec![(
            b"s".to_vec(),
            2,
            Metric::L2,
            IndexKind::Flat,
            Quant::None,
            vec![],
        )];
        let vectors = vec![(b"s".to_vec(), b"id".to_vec(), vec![1.0, 2.0], None, None)];
        assert_eq!(install(store, ns, 0, configs, vectors), 1);
    }

    /// BUG-0255: FLINTNSDROP forgets the namespace's sets and its load state,
    /// so the next touch rebuilds from durable rows, and leaves the rest.
    #[test]
    fn flintnsdrop_forgets_a_namespace_and_only_it() {
        let store = Arc::new(Mutex::new(Store::new()));
        let loads: Loads = Arc::new(Mutex::new(HashMap::new()));
        for ns in [b"gone".as_slice(), b"kept"] {
            loaded_set(&store, ns);
            loads
                .lock()
                .expect("loads lock")
                .insert(ns.to_vec(), LoadState::Loaded);
        }
        let tls = Tls::default();
        let reply = handle_flintfam(&frame(&[b"FLINTNSDROP", b"gone"]), &store, &loads, &tls);
        assert_eq!(reply, Value::Integer(1));
        assert!(
            !loads
                .lock()
                .expect("loads lock")
                .contains_key(b"gone".as_slice())
        );
        assert!(
            loads
                .lock()
                .expect("loads lock")
                .contains_key(b"kept".as_slice())
        );
        let st = store.lock().expect("store lock");
        assert_eq!(st.set_count(), 1);
        assert_eq!(st.generation(b"gone"), 1);
        assert_eq!(st.generation(b"kept"), 0);
        drop(st);
        let again = handle_flintfam(&frame(&[b"flintnsdrop", b"gone"]), &store, &loads, &tls);
        assert_eq!(
            again,
            Value::Integer(0),
            "dropping it twice drops nothing more"
        );
    }

    /// BUG-0255: rows a rebuild read before a drop may be the old tenant's,
    /// so a chunk that began before the drop installs none of them.
    #[test]
    fn a_rebuild_begun_before_a_drop_installs_nothing() {
        let store = Arc::new(Mutex::new(Store::new()));
        let before = store.lock().expect("store lock").generation(b"ns");
        store.lock().expect("store lock").drop_namespace(b"ns");
        let configs = vec![(
            b"s".to_vec(),
            2,
            Metric::L2,
            IndexKind::Flat,
            Quant::None,
            vec![],
        )];
        let vectors = vec![(b"s".to_vec(), b"id".to_vec(), vec![1.0, 2.0], None, None)];
        assert_eq!(install(&store, b"ns", before, configs, vectors), 0);
        assert_eq!(store.lock().expect("store lock").set_count(), 0);
    }

    /// BUG-0265: a rebuild installs a row only when VEC.SET would have taken
    /// it, and counts the rest out.
    #[test]
    fn a_rebuild_leaves_out_a_row_of_the_wrong_length() {
        let store: Arc<Mutex<Store>> = Arc::new(Mutex::new(Store::new()));
        let configs = vec![(
            b"s".to_vec(),
            2,
            Metric::L2,
            IndexKind::Hnsw,
            Quant::Bin,
            vec![],
        )];
        let vectors = vec![
            (b"s".to_vec(), b"ok".to_vec(), vec![1.0, 2.0], None, None),
            (
                b"s".to_vec(),
                b"long".to_vec(),
                vec![1.0, 2.0, 3.0, 4.0, 5.0],
                None,
                None,
            ),
            (b"s".to_vec(), b"".to_vec(), vec![1.0, 2.0], None, None),
            (b"x".to_vec(), b"noset".to_vec(), vec![1.0, 2.0], None, None),
        ];
        assert_eq!(install(&store, b"ns", 0, configs, vectors), 1);
        let st = store.lock().expect("store lock");
        assert_eq!(st.stats().vectors, 1);
    }
}
