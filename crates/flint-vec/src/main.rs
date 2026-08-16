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
use flint_vec::{
    Apply, IndexKind, Metric, Persist, Plan, Store, decode_config, decode_vec_row,
    parse_durable_key,
};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

/// A decoded config row awaiting install: `(set, dim, metric, index)`.
type LoadedConfig = (Vec<u8>, usize, Metric, IndexKind);
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
    let bind = format!("0.0.0.0:{port}");
    // D4: per-namespace index-memory cap in bytes; 0 (default) = unlimited. On
    // a co-processor shared by many tenants, set this so one tenant's set cannot
    // exhaust the process RAM and starve every other tenant's search.
    let index_cap: usize = arg(&args, "--index-mem-bytes")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut initial = Store::new();
    initial.set_index_cap(index_cap);
    let store: Arc<Mutex<Store>> = Arc::new(Mutex::new(initial));
    let loads: Loads = Arc::new(Mutex::new(HashMap::new()));
    let tls = build_tls(&args);
    let listener = TcpListener::bind(&bind).unwrap_or_else(|e| panic!("bind {bind}: {e}"));
    let cap_note = if index_cap == 0 {
        "index mem unlimited".to_string()
    } else {
        format!("index mem cap {index_cap} B/ns")
    };
    let tls_note = match (tls.inbound.is_some(), tls.edge.is_some()) {
        (false, false) => "plaintext",
        (true, false) => "mesh mTLS",
        (true, true) => "mesh mTLS + edge TLS",
        (false, true) => "edge TLS only",
    };
    eprintln!("flint-vec co-processor on {bind} ({tls_note}, v0.2 flat+hnsw, {cap_note})");

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

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

/// Build the mesh-TLS identity from the flags (ADR-0010 D5). The three
/// `--internal-*` flags are all-or-nothing (matching flint-server/flint-proxy);
/// `--client-tls` additionally TLS-wraps the PROXYCHAN dial-back and reuses the
/// internal CA to verify the edge.
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
        let ca = arg(args, "--internal-ca").unwrap_or_else(|| {
            panic!(
                "--client-tls needs --internal-ca (the edge is verified against the internal CA)"
            )
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
            drop(lk);
            let (ns, token, callback) = (ns.to_vec(), token.to_vec(), callback.to_vec());
            let (store, loads, edge) = (store.clone(), loads.clone(), tls.edge.clone());
            std::thread::spawn(move || {
                rebuild_chunk(ns, token, callback, store, loads, edge, phase)
            });
            return err("LOADING vector index is warming, retry");
        }
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
#[allow(clippy::too_many_arguments)]
fn rebuild_chunk(
    ns: Vec<u8>,
    token: Vec<u8>,
    callback: Vec<u8>,
    store: Arc<Mutex<Store>>,
    loads: Loads,
    edge: Option<Arc<flint_tls::ClientConfig>>,
    phase: Phase,
) {
    let name = String::from_utf8_lossy(&ns).into_owned();
    match rebuild_chunk_inner(&ns, &token, &callback, &store, &edge, phase) {
        Ok(Chunk::Done(total)) => {
            eprintln!("flint-vec: rebuilt ns {name:?} ({total} vectors) from durable rows");
            loads
                .lock()
                .expect("loads lock")
                .insert(ns, LoadState::Loaded);
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
            };
            eprintln!(
                "flint-vec: ns {name:?} rebuild chunk loaded {installed}; {where_}; resuming on the next channel"
            );
            loads.lock().expect("loads lock").insert(
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
            if let Some(LoadState::Loading { running, .. }) =
                loads.lock().expect("loads lock").get_mut(&ns)
            {
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
) -> Result<Chunk, String> {
    let addr = std::str::from_utf8(callback).map_err(|_| "bad callback".to_string())?;
    let mut ch = flint_tls::connect_edge(addr, edge).map_err(|e| format!("channel dial: {e}"))?;
    let _ = ch.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = ch.set_write_timeout(Some(Duration::from_secs(5)));
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
    while i < keys.len() {
        let Some((kind, set, id)) = parse_durable_key(&keys[i]) else {
            i += 1;
            continue;
        };
        send_cmd(&mut ch, &[b"GET", &keys[i]]).map_err(|e| format!("GET send: {e}"))?;
        match read_reply(&mut ch).map_err(|e| format!("GET read: {e}"))? {
            Value::Bulk(Some(val)) if kind == b'c' => {
                if let Some((dim, metric, index)) = decode_config(&val) {
                    configs.push((set, dim, metric, index));
                }
                i += 1;
            }
            Value::Bulk(Some(val)) if kind == b'v' => {
                // The expiry rides the durable row, so a rebuilt index restores
                // each id's TTL for free — no separate expiry store to replay.
                if let Ok((vec, meta, expires_at)) = decode_vec_row(&val) {
                    vectors.push((set, id, vec, meta, expires_at));
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
    install(store, ns, configs, vectors);
    let installed = i - next;
    if i >= keys.len() && !budget_done {
        let total = keys
            .iter()
            .filter(|k| matches!(parse_durable_key(k), Some((b'v', _, _))))
            .count();
        Ok(Chunk::Done(total))
    } else {
        Ok(Chunk::More {
            phase: Phase::Fetch { keys, next: i },
            installed,
        })
    }
}

/// Install decoded configs then vectors into the store under one lock. Returns
/// the vector count.
fn install(
    store: &Arc<Mutex<Store>>,
    ns: &[u8],
    configs: Vec<LoadedConfig>,
    vectors: Vec<LoadedVector>,
) -> usize {
    let mut st = store.lock().expect("store lock");
    for (set, dim, metric, index) in configs {
        st.commit(
            ns,
            Apply::CreateSet {
                set,
                dim,
                metric,
                index,
            },
        );
    }
    let n = vectors.len();
    for (set, id, vec, meta, expires_at) in vectors {
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

    send_cmd(&mut ch, &[b"PROXYCHAN", token])
        .map_err(|e| err(&format!("COPROCUNAVAIL channel open failed: {e}")))?;
    if let Value::Error(e) =
        read_reply(&mut ch).map_err(|e| err(&format!("COPROCUNAVAIL channel open failed: {e}")))?
    {
        return Err(err(&format!("COPROCUNAVAIL channel refused: {e}")));
    }

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
        send_cmd(&mut ch, &dcmd)
            .map_err(|e| err(&format!("COPROCUNAVAIL channel write failed: {e}")))?;
        // Relay the channel's own error verbatim — a -QUOTA (over storage quota)
        // is a truer, more actionable answer than a blanket COPROCUNAVAIL.
        if let r @ Value::Error(_) = read_reply(&mut ch)
            .map_err(|e| err(&format!("COPROCUNAVAIL channel write failed: {e}")))?
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
