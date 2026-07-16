//! flint-server: the data-plane node binary.
//!
//! v0: blocking TCP, thread per connection, RESP2 (+ inline commands).
//! Engines: `--engine mem` (default) or `--engine rocks --data-dir DIR`
//! (build with `--features rocks`).
//!
//! Replication (rocks only): a master serves `FLINTSYNC <seq>` by turning
//! the connection into a push stream of WAL batches; `--replica-of HOST:PORT`
//! starts a replica that tails the master, applies batches atomically, and
//! rejects mutating commands with -READONLY.

mod commands;
mod repl_hub;

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use flint_resp::{Decoded, Value, decode, encode};
use flint_storage::{Kv, MemKv};

use crate::commands::Dispatcher;
use crate::repl_hub::ReplHub;

#[cfg(feature = "rocks")]
use flint_storage::rocks::RocksKv;

#[cfg(not(feature = "rocks"))]
type RocksHandle = ();
#[cfg(feature = "rocks")]
type RocksHandle = Arc<RocksKv>;

fn arg(name: &str) -> Option<String> {
    std::env::args().skip_while(|a| a != name).nth(1)
}

/// Internal-mesh mutual-TLS client config (the --internal-* triple in the
/// client role), set once at startup. Every node→node dial — replication
/// tail, full-sync download, migrate-in, cutover orchestration — goes
/// through [`internal_connect`], so the whole data plane speaks mutual TLS
/// when configured and plaintext when not, with one dial path.
static INTERNAL_CLIENT: std::sync::OnceLock<Option<Arc<flint_tls::ClientConfig>>> =
    std::sync::OnceLock::new();

// Every node→node dialer is rocks-gated (replication/migration), so the
// mem-only build never dials out.
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
/// The build stamp surfaced in FLINTINFO — what canary rollouts gate on.
/// FLINT_BUILD_VERSION overrides (deploy artifacts stamp themselves);
/// otherwise the crate version.
fn build_version() -> String {
    std::env::var("FLINT_BUILD_VERSION").unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string())
}

// Callers are all rocks-gated dial sites (replication/migration/cutover);
// the mem-only build still parses --internal-* for its listener.
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
fn internal_connect(addr: &str) -> std::io::Result<flint_tls::Stream> {
    flint_tls::connect(addr, INTERNAL_CLIENT.get().unwrap_or(&None))
}

/// Fleet-journal target (--journal <cp-addr>) and this node's own address,
/// for role-transition events. Reporting is best-effort and detached — a
/// transition never waits on (or fails because of) the journal.
static JOURNAL_TARGET: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
static SELF_ADDR: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn journal_event(kind: flint_journal::EventKind, epoch: Option<String>, cause: &str) {
    let Some(Some(target)) = JOURNAL_TARGET.get().cloned() else {
        return;
    };
    let me = SELF_ADDR.get().cloned().unwrap_or_default();
    flint_journal::emit_detached(
        target,
        INTERNAL_CLIENT.get().cloned().unwrap_or(None),
        flint_journal::Event {
            at_ms: flint_journal::now_ms(),
            actor: format!("node:{me}"),
            kind,
            subject: me.clone(),
            epoch,
            cause: Some(cause.to_string()),
            detail: None,
        },
    );
}

fn main() -> std::io::Result<()> {
    let port = arg("--port")
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(6380);
    let _ = SELF_ADDR.set(format!("127.0.0.1:{port}"));
    let _ = JOURNAL_TARGET.set(arg("--journal"));
    let engine = arg("--engine").unwrap_or_else(|| "mem".into());
    let replica_of = arg("--replica-of");

    // Internal-mesh mutual TLS: one --internal-* triple, used in BOTH roles —
    // server config on the data-port listener (peers must present a cert
    // chaining to the CA), client config on every node→node dial (replication
    // tail, full sync, migrate, cutover). Parsed before anything dials out:
    // a fresh replica's checkpoint download happens during startup, below.
    let internal_tls: Option<Arc<flint_tls::ServerConfig>> = match (
        arg("--internal-ca"),
        arg("--internal-cert"),
        arg("--internal-key"),
    ) {
        (Some(ca), Some(cert), Some(key)) => {
            let _ = INTERNAL_CLIENT.set(Some(
                flint_tls::client_config(&ca, &cert, &key)
                    .expect("build internal TLS client config"),
            ));
            Some(
                flint_tls::server_config(&ca, &cert, &key)
                    .expect("build internal TLS server config"),
            )
        }
        (None, None, None) => {
            let _ = INTERNAL_CLIENT.set(None);
            None
        }
        _ => panic!("--internal-ca, --internal-cert, --internal-key must be given together"),
    };
    // Role is dynamic: FLINTPROMOTE flips a replica to master at runtime.
    let read_only = Arc::new(AtomicBool::new(replica_of.is_some()));
    let tailer_stop = Arc::new(AtomicBool::new(false));
    // Lease deadline (unix-ms). 0 = unmanaged (standalone: never self-fences).
    // Once a controller sends FLINTLEASE, the node is lease-managed and
    // self-fences to read-only if the deadline passes without renewal — so a
    // master partitioned from ALL controllers stops accepting writes on its
    // own, closing the split-brain window without anyone reaching it.
    let lease_deadline = Arc::new(std::sync::atomic::AtomicU64::new(0));

    #[allow(unused_mut)]
    let mut rocks: Option<RocksHandle> = None;
    let store: Arc<dyn Kv> = match engine.as_str() {
        "mem" => {
            if replica_of.is_some() {
                eprintln!("--replica-of requires --engine rocks");
                std::process::exit(2);
            }
            Arc::new(MemKv::new())
        }
        #[cfg(feature = "rocks")]
        "rocks" => {
            let dir = arg("--data-dir").unwrap_or_else(|| "./flint-data".into());
            let fresh = !std::path::Path::new(&dir).join("CURRENT").exists();
            // A fresh replica seeds from a checkpoint (the spare-seeding
            // path), then tails the WAL from the copied DB's own sequence.
            if fresh && let Some(target) = &replica_of {
                replica::full_sync_download(target, std::path::Path::new(&dir))?;
            }
            // Spare restore (whole-pair loss): seed from the latest durable
            // snapshot instead of a live master. Only meaningful on a fresh
            // node; a node with data has a lineage and must not be papered
            // over by a snapshot.
            let restore_from = arg("--restore-from");
            assert!(
                !(restore_from.is_some() && replica_of.is_some()),
                "--restore-from and --replica-of are mutually exclusive"
            );
            let mut restored_from_id: Option<String> = None;
            if fresh && let Some(root) = &restore_from {
                let root = std::path::Path::new(root);
                let id = std::fs::read_to_string(root.join("LATEST"))
                    .map_err(|e| std::io::Error::other(format!("read LATEST: {e}")))?;
                let id = id.trim().to_string();
                let src = root.join(&id);
                let dst = std::path::Path::new(&dir);
                std::fs::create_dir_all(dst)?;
                for entry in std::fs::read_dir(&src)? {
                    let entry = entry?;
                    // Checkpoints are flat (SSTs, MANIFEST, CURRENT, OPTIONS).
                    if entry.file_type()?.is_file() {
                        std::fs::copy(entry.path(), dst.join(entry.file_name()))?;
                    }
                }
                eprintln!("restored data dir from snapshot {id}");
                restored_from_id = Some(id);
            }
            let kv = RocksKv::open(std::path::Path::new(&dir))
                .map_err(|e| std::io::Error::other(format!("rocksdb open: {e}")))?;
            if fresh && replica_of.is_some() {
                // The checkpoint copies the SOURCE's system rows — including
                // its replication cursor, which is the source's OLD upstream
                // position (frozen at its promotion), not ours. Like the
                // role row, the cursor is node-local identity and must be
                // reasserted at seed time: our true cursor is the copied
                // DB's own latest sequence. (Found by chaos: the inherited
                // marker caused a permanent SequenceGap loop against
                // promoted masters, leaving pairs unreplicated.)
                let cursor = kv.latest_seq();
                kv.set_last_applied(cursor)
                    .map_err(|e| std::io::Error::other(format!("cursor init: {e:?}")))?;
                eprintln!("full sync complete; tailing from seq {cursor}");
            }
            // Initialize the manifest on first boot: default slot claim
            // and a role claim at epoch (0,1).
            use flint_storage::manifest::{self, Epoch, Role, RoleClaim, SlotClaim};
            if manifest::read_claim(&kv, b"0").is_none() {
                manifest::set_claim(
                    &kv,
                    &SlotClaim {
                        ns: b"0".to_vec(),
                        start: 0,
                        end: 16383,
                        epoch: Epoch {
                            generation: 0,
                            counter: 1,
                        },
                    },
                )
                .map_err(|e| std::io::Error::other(format!("manifest claim: {e:?}")))?;
            }
            if manifest::read_role(&kv).is_none() {
                let role = if replica_of.is_some() {
                    Role::Replica
                } else {
                    Role::Master
                };
                manifest::set_role(
                    &kv,
                    RoleClaim {
                        role,
                        epoch: Epoch {
                            generation: 0,
                            counter: 1,
                        },
                    },
                )
                .map_err(|e| std::io::Error::other(format!("manifest role: {e:?}")))?;
            }
            // A restored spare asserts mastership in a NEW GENERATION:
            // (g,c) -> (g+1, 1). The whole dead lineage is thereby fenced —
            // any old node that limps back carries generation g and loses to
            // the restored line no matter how high its counter climbed.
            // force_role (unfenced) is correct here: the snapshot's copied
            // role row is the OLD lineage's identity, not this node's.
            if let Some(id) = &restored_from_id {
                let old = manifest::read_role(&kv).map(|c| c.epoch).unwrap_or(Epoch {
                    generation: 0,
                    counter: 1,
                });
                let bumped = Epoch {
                    generation: old.generation + 1,
                    counter: 1,
                };
                manifest::force_role(
                    &kv,
                    RoleClaim {
                        role: Role::Master,
                        epoch: bumped,
                    },
                );
                // Restart replication bookkeeping at the copied DB's own seq
                // (same reassertion the full-sync seed path does).
                let cursor = kv.latest_seq();
                kv.set_last_applied(cursor)
                    .map_err(|e| std::io::Error::other(format!("cursor init: {e:?}")))?;
                eprintln!(
                    "spare restored from {id}: MASTER at epoch {bumped} (generation bump fences the old lineage)"
                );
                journal_event(
                    flint_journal::EventKind::SpareRestored,
                    Some(bumped.to_string()),
                    "whole-pair loss: restored from latest snapshot; generation bump fences the old lineage",
                );
            }
            // A checkpoint full sync copied the MASTER's manifest; the
            // seeded replica reasserts its own identity (same epoch, so
            // promotion fencing still measures against the copied history).
            if fresh && replica_of.is_some() {
                let epoch = manifest::read_role(&kv).map(|c| c.epoch).unwrap_or(Epoch {
                    generation: 0,
                    counter: 1,
                });
                manifest::force_role(
                    &kv,
                    RoleClaim {
                        role: Role::Replica,
                        epoch,
                    },
                );
            }
            // The DURABLE role is authoritative over CLI flags: a promoted
            // master restarted with a stale --replica-of must stay master
            // (and must not tail its old peer).
            match manifest::read_role(&kv).map(|c| c.role) {
                Some(Role::Master) => {
                    if replica_of.is_some() {
                        eprintln!("manifest role is master (durable); ignoring --replica-of");
                        tailer_stop.store(true, Ordering::Relaxed);
                    }
                    // ZOMBIE HAZARD (until the trio's leases land): if this
                    // node was replaced while down, it will serve writes
                    // alongside its successor until FLINTDEMOTE fences it.
                    // The trio will demote returning masters automatically.
                    eprintln!(
                        "booting as MASTER from durable role; if a successor was promoted while this node was down, fence it with FLINTDEMOTE"
                    );
                    read_only.store(false, Ordering::Relaxed);
                }
                Some(Role::Replica) => read_only.store(true, Ordering::Relaxed),
                None => {}
            }
            let kv = Arc::new(kv);
            rocks = Some(Arc::clone(&kv));
            eprintln!("engine=rocks data-dir={dir}");
            kv
        }
        other => {
            eprintln!(
                "unknown --engine '{other}' (built-in: mem{})",
                if cfg!(feature = "rocks") {
                    ", rocks"
                } else {
                    "; build with --features rocks for rocks"
                }
            );
            std::process::exit(2);
        }
    };

    #[cfg(feature = "rocks")]
    if let (Some(target), Some(kv)) = (replica_of.clone(), rocks.clone()) {
        eprintln!("replica-of={target} (writes rejected with -READONLY)");
        let stop = Arc::clone(&tailer_stop);
        std::thread::spawn(move || replica::run(&target, &kv, &stop));
    }

    // RPO knobs: --lag-soft-ms delays writes, --lag-hard-ms sheds them.
    // The hard cap is the advertised worst-case failover RPO (per-tenant
    // tiers arrive with the control plane).
    let lag_soft = arg("--lag-soft-ms")
        .and_then(|v| v.parse().ok())
        .unwrap_or(repl_hub::DEFAULT_LAG_SOFT_MS);
    let lag_hard = arg("--lag-hard-ms")
        .and_then(|v| v.parse().ok())
        .unwrap_or(repl_hub::DEFAULT_LAG_HARD_MS);
    if lag_soft != repl_hub::DEFAULT_LAG_SOFT_MS || lag_hard != repl_hub::DEFAULT_LAG_HARD_MS {
        eprintln!("lag caps: soft={lag_soft}ms hard={lag_hard}ms");
    }
    // Safety gate: shed writes while live replicas < this (Redis
    // min-replicas-to-write). Set to 1 on replicated pairs so a widowed or
    // isolated master cannot accept unbounded at-risk writes; leave 0 for
    // standalone nodes.
    let min_replicas: u32 = arg("--min-replicas-to-write")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let hub = Arc::new(ReplHub::new(lag_soft, lag_hard, min_replicas));

    // Size policies. --max-value-bytes (Valkey's proto-max-bulk-len
    // analog, extended to collections): writes that would grow any single
    // value past it are rejected; 0 disables. --max-key-bytes: key-length
    // cap, clamped to the envelope's structural 64KB ceiling (the subkey
    // length frame is 2 bytes); 0 means ceiling only.
    let limits = commands::Limits {
        max_value_bytes: arg("--max-value-bytes")
            .and_then(|v| v.parse().ok())
            .unwrap_or(flint_storage::DEFAULT_MAX_VALUE_BYTES),
        max_key_bytes: arg("--max-key-bytes")
            .and_then(|v| v.parse().ok())
            .unwrap_or(flint_storage::MAX_KEY_BYTES),
    };
    if limits.max_value_bytes != flint_storage::DEFAULT_MAX_VALUE_BYTES {
        eprintln!(
            "max-value-bytes: {}",
            if limits.max_value_bytes == 0 {
                "unlimited".into()
            } else {
                limits.max_value_bytes.to_string()
            }
        );
    }
    if limits.max_key_bytes != flint_storage::MAX_KEY_BYTES {
        eprintln!(
            "max-key-bytes: {} (structural ceiling {})",
            limits.max_key_bytes,
            flint_storage::MAX_KEY_BYTES
        );
    }

    // Fast-path guard for per-slot ownership: only consult ownership (an
    // extra manifest read) when at least one migration override exists.
    // Set at boot from durable records and by FLINTSLOTMOVED; false is the
    // common case, so normal traffic pays nothing.
    let migration_active = Arc::new(AtomicBool::new(false));
    #[cfg(feature = "rocks")]
    if let Some(kv) = &rocks
        && !flint_storage::manifest::scan_all_migrations(kv.as_ref()).is_empty()
    {
        migration_active.store(true, Ordering::Relaxed);
    }

    // Lease watchdog: self-fence on expiry. Runtime-only (read-only flip);
    // durable fencing is FLINTDEMOTE. Never un-fences — a master that
    // self-fenced during a partition must not resume writing (a successor
    // may have been promoted); recovery is FLINTDEMOTE + resync.
    {
        let read_only = Arc::clone(&read_only);
        let lease_deadline = Arc::clone(&lease_deadline);
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_millis(100));
                let d = lease_deadline.load(Ordering::Relaxed);
                if d != 0
                    && flint_storage::strings::system_clock() > d
                    && !read_only.load(Ordering::Relaxed)
                {
                    read_only.store(true, Ordering::Relaxed);
                    eprintln!(
                        "lease expired: self-fenced to read-only (partitioned from controllers)"
                    );
                    journal_event(
                        flint_journal::EventKind::SelfFenced,
                        None,
                        "lease expired: no controller renewal",
                    );
                }
            }
        });
    }
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    eprintln!(
        "flint-server listening on 127.0.0.1:{port} ({})",
        if internal_tls.is_some() {
            "internal mTLS"
        } else {
            "plaintext"
        }
    );
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let store: Arc<dyn Kv> = Arc::clone(&store);
        // Option<Arc<RocksKv>> with the feature; Option<()> (Copy) without.
        #[allow(clippy::clone_on_copy)]
        let rocks = rocks.clone();
        let hub = Arc::clone(&hub);
        let read_only = Arc::clone(&read_only);
        let tailer_stop = Arc::clone(&tailer_stop);
        let lease_deadline = Arc::clone(&lease_deadline);
        let migration_active = Arc::clone(&migration_active);
        let internal_tls = internal_tls.clone();
        std::thread::spawn(move || {
            // TLS handshake (incl. mutual client-cert verification) runs lazily
            // on serve's first read; a peer that fails it errors out there.
            let conn = match flint_tls::accept(stream, &internal_tls) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("internal tls accept: {e}");
                    return;
                }
            };
            let _ = serve(
                conn,
                store.as_ref(),
                &read_only,
                &tailer_stop,
                &lease_deadline,
                rocks,
                &hub,
                &migration_active,
                limits,
            );
        });
    }
    Ok(())
}

/// Per-connection input ceiling (Redis `client-query-buffer-limit`). With
/// bulk strings capped at `flint_resp::MAX_BULK_LEN`, a legitimate
/// connection can never accumulate this much undecoded input; hitting it
/// means a hostile or broken client, and the connection is closed.
const MAX_QUERY_BUF: usize = 1024 * 1024 * 1024;
/// Longest accepted inline (non-RESP) command line without a newline.
const MAX_INLINE_LEN: usize = 64 * 1024;
/// Flush accumulated pipeline replies past this size. Without it, replies
/// buffer until the whole pipeline drains — and a few MB of pipelined GETs
/// against large values can demand an arbitrarily large reply buffer.
const OUT_FLUSH_THRESHOLD: usize = 1024 * 1024;

#[allow(clippy::too_many_arguments)]
fn serve(
    mut stream: flint_tls::Stream,
    store: &dyn Kv,
    read_only: &Arc<AtomicBool>,
    tailer_stop: &Arc<AtomicBool>,
    lease_deadline: &Arc<std::sync::atomic::AtomicU64>,
    rocks: Option<RocksHandle>,
    hub: &Arc<ReplHub>,
    migration_active: &Arc<AtomicBool>,
    limits: commands::Limits,
) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut chunk = [0u8; 16 * 1024];
    let mut out: Vec<u8> = Vec::with_capacity(4 * 1024);
    // Connection-scoped namespace (FLINTNS): the tenant boundary.
    let mut conn_ns: Vec<u8> = commands::DEFAULT_NS.to_vec();
    loop {
        let mut consumed = 0;
        out.clear();
        loop {
            let pending = &buf[consumed..];
            let Some(&first) = pending.first() else { break };
            // Inline commands (redis-cli --pipe handshakes, telnet): any
            // line not starting with a RESP array marker, split on spaces.
            if first != b'*' {
                let Some(nl) = pending.iter().position(|&b| b == b'\n') else {
                    if pending.len() > MAX_INLINE_LEN {
                        encode(
                            &Value::Error("ERR Protocol error: too big inline request".into()),
                            &mut out,
                        );
                        stream.write_all(&out)?;
                        return Ok(());
                    }
                    break;
                };
                let line = &pending[..nl];
                let line = line.strip_suffix(b"\r").unwrap_or(line);
                consumed += nl + 1;
                let args: Vec<Vec<u8>> = line
                    .split(|&b| b == b' ')
                    .filter(|part| !part.is_empty())
                    .map(|part| part.to_vec())
                    .collect();
                if args.is_empty() {
                    continue;
                }
                let reply = execute(
                    store,
                    read_only,
                    tailer_stop,
                    lease_deadline,
                    &rocks,
                    hub,
                    migration_active,
                    limits,
                    &mut conn_ns,
                    &args,
                );
                encode(&reply, &mut out);
                if out.len() >= OUT_FLUSH_THRESHOLD {
                    stream.write_all(&out)?;
                    out.clear();
                }
                continue;
            }
            match decode(pending) {
                Ok(Decoded::Complete(frame, used)) => {
                    consumed += used;
                    let Some(args) = frame_to_args(frame) else {
                        encode(
                            &Value::Error(
                                "ERR Protocol error: expected array of bulk strings".into(),
                            ),
                            &mut out,
                        );
                        stream.write_all(&out)?;
                        return Ok(());
                    };
                    // FLINTSYNC/FLINTFULLSYNC/FLINTMIGRATEOUT hijack the
                    // connection into a long-lived stream. All three are
                    // single-threaded over the generic stream (flintsync
                    // drains ACKs inline), so they run over plaintext and
                    // internal TLS alike.
                    if args
                        .first()
                        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTSYNC"))
                    {
                        buf.drain(..consumed);
                        stream.write_all(&out)?;
                        return flintsync(stream, rocks, hub, &args);
                    }
                    if args
                        .first()
                        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTFULLSYNC"))
                    {
                        buf.drain(..consumed);
                        stream.write_all(&out)?;
                        return flintfullsync(stream, rocks);
                    }
                    if args
                        .first()
                        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTMIGRATEOUT"))
                    {
                        buf.drain(..consumed);
                        stream.write_all(&out)?;
                        return flintmigrateout(stream, rocks, &args);
                    }
                    let reply = execute(
                        store,
                        read_only,
                        tailer_stop,
                        lease_deadline,
                        &rocks,
                        hub,
                        migration_active,
                        limits,
                        &mut conn_ns,
                        &args,
                    );
                    encode(&reply, &mut out);
                    if out.len() >= OUT_FLUSH_THRESHOLD {
                        stream.write_all(&out)?;
                        out.clear();
                    }
                }
                Ok(Decoded::NeedMore) => break,
                Err(_) => {
                    encode(&Value::Error("ERR Protocol error".into()), &mut out);
                    stream.write_all(&out)?;
                    return Ok(());
                }
            }
        }
        if consumed > 0 {
            buf.drain(..consumed);
            if !out.is_empty() {
                stream.write_all(&out)?;
            }
        }
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > MAX_QUERY_BUF {
            out.clear();
            encode(
                &Value::Error("ERR Protocol error: query buffer limit exceeded".into()),
                &mut out,
            );
            stream.write_all(&out)?;
            return Ok(());
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn execute(
    store: &dyn Kv,
    read_only: &Arc<AtomicBool>,
    tailer_stop: &Arc<AtomicBool>,
    lease_deadline: &Arc<std::sync::atomic::AtomicU64>,
    rocks: &Option<RocksHandle>,
    hub: &Arc<ReplHub>,
    migration_active: &Arc<AtomicBool>,
    limits: commands::Limits,
    conn_ns: &mut Vec<u8>,
    args: &[Vec<u8>],
) -> Value {
    // FLINTNS <ns>: select this connection's namespace — the tenant
    // boundary. Sent by the proxy right after token auth; every subsequent
    // data command, DBSIZE, FLUSHALL, and the slot gate are scoped to it.
    // The server trusts its callers here (the data port is the internal
    // surface; the proxy is what faces tenants — mTLS hardens this at M3).
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTNS"))
    {
        let Some(ns) = args.get(1) else {
            return Value::Error("ERR FLINTNS <namespace>".into());
        };
        let ok_byte = |b: &u8| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.');
        if ns.is_empty() || ns.len() > 64 || !ns.iter().all(ok_byte) {
            return Value::Error("ERR invalid namespace (1..=64 chars of [A-Za-z0-9._-])".into());
        }
        *conn_ns = ns.clone();
        return Value::Simple("OK".into());
    }
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTLEASE"))
    {
        // FLINTLEASE <ttl_ms>: renew the lease. Idempotent — any controller
        // may renew; renewal only extends life, it never un-fences a node
        // that already self-fenced or was demoted.
        let ttl: u64 = args
            .get(1)
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if ttl == 0 {
            return Value::Error("ERR usage: FLINTLEASE <ttl_ms>".into());
        }
        lease_deadline.store(
            flint_storage::strings::system_clock() + ttl,
            Ordering::Relaxed,
        );
        return Value::Simple(format!(
            "OK lease {}",
            if read_only.load(Ordering::Relaxed) {
                "renewed (node is read-only)"
            } else {
                "renewed"
            }
        ));
    }
    let ro = read_only.load(Ordering::Relaxed);
    let is_write = args
        .first()
        .is_some_and(|name| commands::is_write_command(name));
    if ro && is_write {
        return Value::Error("READONLY You can't write against a read only replica.".into());
    }
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTINFO"))
    {
        return flintinfo(ro, rocks, hub);
    }
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTPROMOTE"))
    {
        return flintpromote(read_only, tailer_stop, rocks, args);
    }
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTDEMOTE"))
    {
        return flintdemote(read_only, rocks, args);
    }
    // FLINTMIGRATEIN <src host:port> <slot>: pull a slot's data from a live
    // source into this master (bulk snapshot + slot-filtered tail). Rejected
    // on a replica — the imported slot must land on the destination's master.
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTMIGRATEIN"))
    {
        if ro {
            return Value::Error("READONLY cannot migrate into a replica".into());
        }
        return flintmigratein(rocks, migration_active, args);
    }
    // FLINTSLOTMOVED <slot> <host:port>: durably mark this slot handed off to
    // `peer`; subsequent commands for a key in it answer -MOVED. This is the
    // terminal cutover step on the source.
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTSLOTMOVED"))
    {
        if ro {
            return Value::Error("READONLY cannot change slot ownership on a replica".into());
        }
        return flintslotmoved(rocks, migration_active, args);
    }
    // FLINTSLOTFREEZE <slot> <dest>: source-side freeze — mark the slot
    // Migrating so writes to it are shed with -TRYAGAIN (reads still served)
    // while the destination drains the final tail before the flip.
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTSLOTFREEZE"))
    {
        if ro {
            return Value::Error("READONLY cannot freeze a slot on a replica".into());
        }
        return flintslotfreeze(rocks, migration_active, args);
    }
    // FLINTMIGRATIONS: list this node's IN-FLIGHT migration records (one
    // "slot phase peer" bulk per line), so a recovering controller can
    // observe and reconcile moves interrupted by a restart. Terminal Moved
    // overrides are ownership state, not in-flight, and are excluded.
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTMIGRATIONS"))
    {
        return flintmigrations(rocks);
    }
    // FLINTSNAPSHOT <root>: durable off-node snapshot — a consistent RocksDB
    // checkpoint written under <root>/<id>/ with <root>/LATEST atomically
    // repointed. The manifest CF travels inside the checkpoint, so a restore
    // knows the lineage (role epoch) it came from. <root> is any mounted
    // path; S3 is a mount/sync integration on top of the same layout.
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTSNAPSHOT"))
    {
        return flintsnapshot(rocks, args);
    }
    // FLINTSLOTSTATS: per-slot live-key counts ("slot count" bulk per
    // non-empty slot) — the input the rebalance executor's slot selection
    // needs. One streaming pass over the metadata CF, bounded memory.
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTSLOTSTATS"))
    {
        return flintslotstats(store);
    }
    // FLINTSLOTABORT <slot>: clear an IN-FLIGHT record (rollback an Importing
    // or unfreeze a Migrating). Refuses to touch a terminal Moved override —
    // that is settled ownership, undone only by moving the slot back.
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTSLOTABORT"))
    {
        if ro {
            return Value::Error("READONLY cannot abort a migration on a replica".into());
        }
        return flintslotabort(rocks, args);
    }
    // Per-slot gate: after a migration, a command for a key in a slot this
    // node no longer owns is redirected with -MOVED; a write to a slot frozen
    // mid-cutover is shed with -TRYAGAIN. Guarded by `migration_active` so
    // ordinary traffic (no overrides) never pays the extra manifest read.
    if migration_active.load(Ordering::Relaxed)
        && let Some(reply) = check_slot_gate(rocks, conn_ns, args, is_write)
    {
        return reply;
    }
    // Lag-cap backpressure: the write path enforces the RPO bound. The
    // min-replicas gate comes first — with no live replica there is no lag
    // to measure, and that widowed state is exactly where accepted writes
    // are most at risk (isolated master, dead pair peer).
    if is_write && !ro {
        let now = flint_storage::strings::system_clock();
        if hub.below_write_quorum(now) {
            return Value::Error(
                "THROTTLED live replicas below min-replicas-to-write, retry with backoff".into(),
            );
        }
        match hub.lag_ms(now) {
            Some(lag) if lag >= hub.lag_hard_ms => {
                return Value::Error(
                    "THROTTLED replication lag exceeds limit, retry with backoff".into(),
                );
            }
            Some(lag) if lag >= hub.lag_soft_ms => {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            _ => {}
        }
    }
    // On a replica, wrap the store so lazy-expiry deletes buried in read
    // paths become no-ops: a replica must not write to its own store (that
    // would diverge it from the master); the master's replicated DELETE and
    // the compaction filter reclaim expired rows. Reads still return None
    // for expired keys — correctness is unchanged, only the local write is
    // suppressed.
    if ro {
        let ro_store = flint_storage::ReadOnlyKv(store);
        Dispatcher::with_limits(
            &ro_store,
            flint_storage::strings::system_clock,
            limits,
            conn_ns,
        )
        .dispatch(args)
    } else {
        Dispatcher::with_limits(store, flint_storage::strings::system_clock, limits, conn_ns)
            .dispatch(args)
    }
}

/// FLINTPROMOTE <generation> <counter>: epoch-fenced promotion of a
/// replica to master. The epoch must strictly exceed the stored role
/// epoch (manifest fencing) — a stale promoter gets -FENCED with the
/// current epoch. On success: role persisted first, then the tailer is
/// stopped and writes open. Until the trio exists this is invoked by an
/// operator (or a drill); the trio will call the same path.
#[cfg(feature = "rocks")]
fn flintpromote(
    read_only: &Arc<AtomicBool>,
    tailer_stop: &Arc<AtomicBool>,
    rocks: &Option<RocksHandle>,
    args: &[Vec<u8>],
) -> Value {
    use flint_storage::manifest::{self, Epoch, ManifestError, Role, RoleClaim};
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTPROMOTE requires the rocks engine".into());
    };
    let (Some(generation), Some(counter)) = (
        args.get(1)
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|s| s.parse().ok()),
        args.get(2)
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|s| s.parse().ok()),
    ) else {
        return Value::Error("ERR usage: FLINTPROMOTE <generation> <counter>".into());
    };
    let epoch = Epoch {
        generation,
        counter,
    };
    match manifest::set_role(
        kv.as_ref(),
        RoleClaim {
            role: Role::Master,
            epoch,
        },
    ) {
        Ok(()) => {
            // Durable role first; only then flip runtime state.
            tailer_stop.store(true, Ordering::Relaxed);
            read_only.store(false, Ordering::Relaxed);
            eprintln!("promoted to master at role epoch {epoch}");
            journal_event(
                flint_journal::EventKind::Promoted,
                Some(epoch.to_string()),
                "promotion command applied (epoch-fenced)",
            );
            Value::Simple(format!("OK promoted at {epoch}"))
        }
        Err(ManifestError::Fenced { current }) => Value::Error(format!(
            "FENCED current role epoch is {current}, promotion epoch must exceed it"
        )),
        Err(e) => Value::Error(format!("ERR manifest: {e:?}")),
    }
}

#[cfg(not(feature = "rocks"))]
fn flintpromote(
    _read_only: &Arc<AtomicBool>,
    _tailer_stop: &Arc<AtomicBool>,
    _rocks: &Option<RocksHandle>,
    _args: &[Vec<u8>],
) -> Value {
    Value::Error("ERR FLINTPROMOTE requires a build with --features rocks".into())
}

/// FLINTDEMOTE <generation> <counter>: epoch-fenced fencing of a (possibly
/// stale, possibly returning) master. The counterpart of FLINTPROMOTE and
/// the tool the trio (or an operator) uses to silence a zombie: a killed
/// master restarted on its old data dir still holds role:Master durably and
/// would accept writes alongside the promoted successor. Demotion persists
/// role:Replica at a strictly higher epoch — surviving restarts — and flips
/// the node read-only immediately.
///
/// Demotion does NOT start tailing: the node's data may hold a divergent
/// suffix (writes the successor never saw). v0 resync is the spare pattern
/// — wipe the data dir and restart with --replica-of for a fresh full sync.
/// The delta-rejoin with divergence quarantine is trio-era work.
#[cfg(feature = "rocks")]
fn flintdemote(
    read_only: &Arc<AtomicBool>,
    rocks: &Option<RocksHandle>,
    args: &[Vec<u8>],
) -> Value {
    use flint_storage::manifest::{self, Epoch, ManifestError, Role, RoleClaim};
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTDEMOTE requires the rocks engine".into());
    };
    let (Some(generation), Some(counter)) = (
        args.get(1)
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|s| s.parse().ok()),
        args.get(2)
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|s| s.parse().ok()),
    ) else {
        return Value::Error("ERR usage: FLINTDEMOTE <generation> <counter>".into());
    };
    let epoch = Epoch {
        generation,
        counter,
    };
    match manifest::set_role(
        kv.as_ref(),
        RoleClaim {
            role: Role::Replica,
            epoch,
        },
    ) {
        Ok(()) => {
            // Durable role first, then flip runtime state: no window where a
            // crash resurrects a writable master.
            read_only.store(true, Ordering::Relaxed);
            eprintln!(
                "demoted to replica at role epoch {epoch} (fenced; wipe + --replica-of to resync)"
            );
            journal_event(
                flint_journal::EventKind::Demoted,
                Some(epoch.to_string()),
                "demotion command applied (epoch-fenced)",
            );
            Value::Simple(format!("OK demoted at {epoch}"))
        }
        Err(ManifestError::Fenced { current }) => Value::Error(format!(
            "FENCED current role epoch is {current}, demotion epoch must exceed it"
        )),
        Err(e) => Value::Error(format!("ERR manifest: {e:?}")),
    }
}

#[cfg(not(feature = "rocks"))]
fn flintdemote(
    _read_only: &Arc<AtomicBool>,
    _rocks: &Option<RocksHandle>,
    _args: &[Vec<u8>],
) -> Value {
    Value::Error("ERR FLINTDEMOTE requires a build with --features rocks".into())
}

#[cfg(feature = "rocks")]
fn flintinfo(read_only: bool, rocks: &Option<RocksHandle>, hub: &Arc<ReplHub>) -> Value {
    let now = flint_storage::strings::system_clock();
    let latest = rocks.as_ref().map(|kv| kv.latest_seq()).unwrap_or(0);
    let last_applied = rocks.as_ref().map(|kv| kv.last_applied()).unwrap_or(0);
    let role_epoch = rocks
        .as_ref()
        .and_then(|kv| flint_storage::manifest::read_role(kv.as_ref()))
        .map(|c| c.epoch.to_string())
        .unwrap_or_else(|| "none".into());
    // Sequence lag: how many master sequence numbers the freshest live
    // replica still trails. Unlike time-lag (age of the oldest un-acked
    // write, which the RPO cap uses), this stays large while a replica
    // drains a backlog even after writes stop — so it is the correct
    // promotion-READINESS signal. "none" when no live replica.
    let seq_lag = match hub.effective_acked(now) {
        Some(acked) => latest.saturating_sub(acked).to_string(),
        None => "none".into(),
    };
    let info = format!(
        "role:{}\r\nrole_epoch:{role_epoch}\r\nbuild:{build}\r\nsst_bytes:{sst}\r\nlatest_seq:{latest}\r\nlast_applied:{last_applied}\r\nacked_seq:{}\r\nseq_lag:{seq_lag}\r\nlive_replicas:{}\r\nlag_ms:{}\r\nlag_soft_ms:{soft}\r\nlag_hard_ms:{hard}\r\nmin_replicas_to_write:{minr}\r\n",
        if read_only { "replica" } else { "master" },
        hub.effective_acked(now)
            .map_or_else(|| "none".into(), |a| a.to_string()),
        hub.live_replica_count(now),
        hub.lag_ms(now)
            .map_or_else(|| "none".into(), |l| l.to_string()),
        soft = hub.lag_soft_ms,
        hard = hub.lag_hard_ms,
        minr = hub.min_replicas_to_write,
        build = build_version(),
        sst = rocks.as_ref().map(|kv| kv.sst_bytes()).unwrap_or(0),
    );
    Value::Bulk(Some(info.into_bytes()))
}

#[cfg(not(feature = "rocks"))]
fn flintinfo(read_only: bool, _rocks: &Option<RocksHandle>, hub: &Arc<ReplHub>) -> Value {
    let now = flint_storage::strings::system_clock();
    let info = format!(
        "role:{}\r\nlive_replica:{}\r\n",
        if read_only { "replica" } else { "master" },
        hub.has_live_replica(now) as u8,
    );
    Value::Bulk(Some(info.into_bytes()))
}

fn frame_to_args(frame: Value) -> Option<Vec<Vec<u8>>> {
    let Value::Array(Some(items)) = frame else {
        return None;
    };
    let mut args = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Value::Bulk(Some(bytes)) => args.push(bytes),
            _ => return None,
        }
    }
    Some(args)
}

/// Master side of replication: stream WAL batches from the requested
/// sequence until the replica disconnects.
///
/// One loop owns the whole connection: each cycle drains the replica's ACKs
/// (bounded by a 20ms read timeout, which is also the loop's idle pacing —
/// it replaces the old sleep), then pushes any new WAL batches. Single-
/// threaded duplex means no socket cloning, so the stream can be TLS — a
/// rustls session is one stateful object and cannot be `try_clone`d the way
/// the old dedicated ACK-reader thread required. Throughput ceiling is
/// REPL_TAIL_BUDGET_BYTES per ~20ms cycle (~200MB/s), far above a link.
/// Drain whatever ACK frames are already available on the replication
/// stream without blocking beyond the socket's read timeout. Returns false
/// when the replica is gone (clean close or protocol garbage). Called from
/// the push loop between cycles AND whenever a chunked batch write would
/// block — the master must keep consuming ACKs while it writes, or the two
/// sides write-write deadlock once both socket buffers fill (found by the
/// chain drill after the single-threaded duplex landed).
#[cfg(feature = "rocks")]
fn drain_acks(
    stream: &mut flint_tls::Stream,
    ack_buf: &mut Vec<u8>,
    hub: &Arc<ReplHub>,
    replica_id: u64,
) -> std::io::Result<bool> {
    let mut chunk = [0u8; 4096];
    loop {
        match decode(ack_buf) {
            Ok(Decoded::Complete(frame, used)) => {
                ack_buf.drain(..used);
                if let Value::Array(Some(items)) = frame
                    && let [Value::Bulk(Some(tag)), Value::Bulk(Some(raw))] = items.as_slice()
                    && tag.eq_ignore_ascii_case(b"ACK")
                    && let Some(seq) = std::str::from_utf8(raw).ok().and_then(|s| s.parse().ok())
                {
                    hub.record_ack(replica_id, seq, flint_storage::strings::system_clock());
                }
            }
            Ok(Decoded::NeedMore) => match stream.read(&mut chunk) {
                Ok(0) => {
                    eprintln!("replication stream ended: replica closed");
                    return Ok(false);
                }
                Ok(n) => ack_buf.extend_from_slice(&chunk[..n]),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    return Ok(true); // no more ACK data this tick
                }
                Err(e) => {
                    eprintln!("replication stream ended: ack read error {e}");
                    return Err(e);
                }
            },
            Err(e) => {
                eprintln!("replication stream ended: ack protocol garbage {e:?}");
                return Ok(false);
            }
        }
    }
}

#[cfg(feature = "rocks")]
fn flintsync(
    mut stream: flint_tls::Stream,
    rocks: Option<RocksHandle>,
    hub: &Arc<ReplHub>,
    args: &[Vec<u8>],
) -> std::io::Result<()> {
    use flint_storage::repl::{ReplError, ReplOp};

    let Some(kv) = rocks else {
        let mut out = Vec::new();
        encode(
            &Value::Error("ERR FLINTSYNC requires the rocks engine".into()),
            &mut out,
        );
        return stream.write_all(&out);
    };
    let mut cursor: u64 = args
        .get(1)
        .and_then(|raw| std::str::from_utf8(raw).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut out = Vec::new();
    encode(&Value::Simple(format!("FLINTSYNC-OK {cursor}")), &mut out);
    stream.write_all(&out)?;
    // ACK-drain read timeout doubles as loop pacing, ADAPTIVELY: while
    // batches are flowing the drain must be near-nonblocking (1ms) or the
    // pacing caps replication throughput at one WAL batch per tick (found
    // by the chain drill: a 200k-write burst converged at ~3k ops/s); only
    // an IDLE stream uses the 20ms tick, replacing the old idle sleep.
    stream.set_read_timeout(Some(std::time::Duration::from_millis(1)))?;
    let mut idle = false;
    eprintln!("replica connected, streaming from seq {cursor}");
    let replica_id = hub.register_replica();
    let mut ack_buf: Vec<u8> = Vec::new();
    let result: std::io::Result<()> = (|| {
        loop {
            stream.set_read_timeout(Some(std::time::Duration::from_millis(if idle {
                20
            } else {
                1
            })))?;
            if !drain_acks(&mut stream, &mut ack_buf, hub, replica_id)? {
                return Ok(());
            }
            hub.record_sample(kv.latest_seq(), flint_storage::strings::system_clock());
            // Budgeted: a laggard reconnecting near the WAL retention limit
            // must not make this loop materialize (and clone twice more into
            // frames) a multi-GB tail in one poll.
            match kv.updates_since_budgeted(cursor, flint_storage::repl::REPL_TAIL_BUDGET_BYTES) {
                Ok(batches) if !batches.is_empty() => {
                    out.clear();
                    for batch in &batches {
                        let ops: Vec<Value> = batch
                            .ops
                            .iter()
                            .map(|op| match op {
                                ReplOp::Put { key, value } => Value::Array(Some(vec![
                                    Value::Bulk(Some(b"P".to_vec())),
                                    Value::Bulk(Some(key.clone())),
                                    Value::Bulk(Some(value.clone())),
                                ])),
                                ReplOp::Delete { key } => Value::Array(Some(vec![
                                    Value::Bulk(Some(b"D".to_vec())),
                                    Value::Bulk(Some(key.clone())),
                                ])),
                            })
                            .collect();
                        let frame = Value::Array(Some(vec![
                            Value::Integer(batch.first_seq as i64),
                            Value::Integer(batch.last_seq as i64),
                            Value::Array(Some(ops)),
                        ]));
                        encode(&frame, &mut out);
                        cursor = batch.last_seq;
                    }
                    // Chunked send that KEEPS DRAINING ACKS: a blocking
                    // write_all here deadlocks under burst load — the
                    // replica ACKs every applied batch, those ACKs fill our
                    // recv queue while we are stuck in write_all, the
                    // replica's ACK write then blocks and it stops reading.
                    // Bounded write timeout + drain-on-block breaks the
                    // cycle; the drain is what un-sticks the replica.
                    stream.set_write_timeout(Some(std::time::Duration::from_millis(50)))?;
                    let mut off = 0;
                    while off < out.len() {
                        let end = (off + 64 * 1024).min(out.len());
                        match stream.write(&out[off..end]) {
                            Ok(0) => {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::WriteZero,
                                    "replica socket closed mid-batch",
                                ));
                            }
                            Ok(n) => off += n,
                            Err(e)
                                if e.kind() == std::io::ErrorKind::WouldBlock
                                    || e.kind() == std::io::ErrorKind::TimedOut =>
                            {
                                if !drain_acks(&mut stream, &mut ack_buf, hub, replica_id)? {
                                    return Ok(());
                                }
                            }
                            Err(e) => {
                                eprintln!("replication stream ended: batch write error {e}");
                                return Err(e);
                            }
                        }
                    }
                    idle = false;
                }
                // Idle: the ACK drain's timeout already paced this cycle.
                Ok(_) => idle = true,
                Err(ReplError::WalGap(e)) => {
                    out.clear();
                    encode(
                        &Value::Error(format!("WALGAP full sync required: {e}")),
                        &mut out,
                    );
                    stream.write_all(&out)?;
                    return Ok(());
                }
                // SequenceGap is apply-side; a master seeing it is a bug —
                // fail the stream loudly rather than continue.
                Err(e @ (ReplError::Storage(_) | ReplError::SequenceGap { .. })) => {
                    eprintln!("replication stream error: {e:?}");
                    return Ok(());
                }
            }
        }
    })();
    hub.unregister_replica(replica_id);
    result
}

/// Source side of a slot move: ship every row of `slot` (all CFs) as a bulk
/// snapshot, then stream the slot-filtered live tail so writes that landed
/// during the copy also reach the destination. Ownership does NOT change here
/// — this is the data-shipping half; the atomic cutover (IMPORTING/MIGRATING
/// plus -MOVED routing) is a separate step (ADR-0004). The destination
/// decides when it is drained and closes the connection.
#[cfg(feature = "rocks")]
fn flintmigrateout(
    mut stream: flint_tls::Stream,
    rocks: Option<RocksHandle>,
    args: &[Vec<u8>],
) -> std::io::Result<()> {
    use flint_storage::encoding::{Cf, slot_prefix};
    use flint_storage::repl::{ReplError, ReplOp};

    let mut out = Vec::new();
    let Some(kv) = rocks else {
        encode(
            &Value::Error("ERR FLINTMIGRATEOUT requires the rocks engine".into()),
            &mut out,
        );
        return stream.write_all(&out);
    };
    let Some(slot) = args
        .get(1)
        .and_then(|r| std::str::from_utf8(r).ok())
        .and_then(|s| s.parse::<u16>().ok())
    else {
        encode(
            &Value::Error("ERR FLINTMIGRATEOUT <slot> [ns]".into()),
            &mut out,
        );
        return stream.write_all(&out);
    };
    let ns: &[u8] = args.get(2).map(|v| v.as_slice()).unwrap_or(b"0");
    // Refuse to ship a slot this node does not own: after a cutover the
    // rows here (if any) are purge-pending ghosts, and re-exporting them
    // would hand out data whose ownership already changed hands.
    {
        use flint_storage::manifest::{self, MigrationPhase};
        if let Some(rec) = manifest::read_migration(kv.as_ref(), ns, slot)
            && matches!(rec.phase, MigrationPhase::Moved | MigrationPhase::Importing)
        {
            encode(
                &Value::Error(format!("ERR slot {slot} not owned (phase {:?})", rec.phase)),
                &mut out,
            );
            return stream.write_all(&out);
        }
    }
    let prefixes: Vec<Vec<u8>> = [Cf::Metadata, Cf::Subkey, Cf::ZScore]
        .iter()
        .map(|&cf| slot_prefix(cf, ns, slot))
        .collect();
    let matches = |key: &[u8]| prefixes.iter().any(|p| key.starts_with(p));

    // Snapshot BEFORE scanning: any write after this point reappears in the
    // tail (Put is idempotent, so double-shipping a row is harmless).
    let snapshot = kv.latest_seq();
    encode(&Value::Simple("MIGRATEOUT-OK".into()), &mut out);
    stream.write_all(&out)?;

    // Bulk phase: every row of the slot, across all CFs — streamed with
    // periodic flushes, so neither the row list nor the encode buffer ever
    // holds a whole slot in memory.
    let mut bulk_rows: u64 = 0;
    for prefix in &prefixes {
        out.clear();
        let mut io_err: Option<std::io::Error> = None;
        kv.for_each_prefix(prefix, &mut |k, v| {
            encode(
                &Value::Array(Some(vec![
                    Value::Bulk(Some(b"P".to_vec())),
                    Value::Bulk(Some(k.to_vec())),
                    Value::Bulk(Some(v.to_vec())),
                ])),
                &mut out,
            );
            bulk_rows += 1;
            if out.len() >= 1 << 20 {
                if let Err(e) = stream.write_all(&out) {
                    io_err = Some(e);
                    return false;
                }
                out.clear();
            }
            true
        });
        if let Some(e) = io_err {
            return Err(e);
        }
        if !out.is_empty() {
            stream.write_all(&out)?;
        }
    }
    out.clear();
    encode(
        &Value::Array(Some(vec![
            Value::Bulk(Some(b"BULK-END".to_vec())),
            Value::Integer(snapshot as i64),
            Value::Integer(bulk_rows as i64),
        ])),
        &mut out,
    );
    stream.write_all(&out)?;

    // Tail phase: slot-filtered live ops from the snapshot, plus a CAUGHTUP
    // heartbeat carrying (cursor, head) so the destination knows when it has
    // drained. Loops until the destination closes (write error) — same
    // lifetime model as flintsync.
    let mut cursor = snapshot;
    loop {
        match kv.updates_since_budgeted(cursor, flint_storage::repl::REPL_TAIL_BUDGET_BYTES) {
            Ok(batches) => {
                out.clear();
                for batch in &batches {
                    for op in &batch.ops {
                        match op {
                            ReplOp::Put { key, value } if matches(key) => encode(
                                &Value::Array(Some(vec![
                                    Value::Bulk(Some(b"P".to_vec())),
                                    Value::Bulk(Some(key.clone())),
                                    Value::Bulk(Some(value.clone())),
                                ])),
                                &mut out,
                            ),
                            ReplOp::Delete { key } if matches(key) => encode(
                                &Value::Array(Some(vec![
                                    Value::Bulk(Some(b"D".to_vec())),
                                    Value::Bulk(Some(key.clone())),
                                ])),
                                &mut out,
                            ),
                            _ => {}
                        }
                    }
                    cursor = batch.last_seq;
                }
                encode(
                    &Value::Array(Some(vec![
                        Value::Bulk(Some(b"CAUGHTUP".to_vec())),
                        Value::Integer(cursor as i64),
                        Value::Integer(kv.latest_seq() as i64),
                    ])),
                    &mut out,
                );
                if stream.write_all(&out).is_err() {
                    return Ok(());
                }
                if batches.is_empty() {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            }
            Err(ReplError::WalGap(_)) => {
                out.clear();
                encode(
                    &Value::Error("WALGAP migration restart required".into()),
                    &mut out,
                );
                let _ = stream.write_all(&out);
                return Ok(());
            }
            Err(e) => {
                eprintln!("migrateout stream error: {e:?}");
                return Ok(());
            }
        }
    }
}

/// Destination side of a slot move (drives FLINTMIGRATEOUT on the source):
/// apply the bulk snapshot then the slot-filtered tail as local writes, so
/// the imported slot joins THIS node's replicated data.
///
/// With `self_addr = Some(me)` it also performs the CUTOVER, in the order
/// that is safe under interruption (see docs/design.md §2.3, ADR-0004):
///   1. mark this slot Importing here (we redirect it to the source until we
///      own it);
///   2. pull bulk + tail to a first caught-up point;
///   3. FREEZE the slot on the source (Migrating: writes shed -TRYAGAIN);
///   4. drain the final tail to a second caught-up point;
///   5. flip DEST-FIRST: clear our Importing (we now own via the base claim),
///      then tell the source FLINTSLOTMOVED (it now answers -MOVED to us).
///
/// Dest-owns-before-source-disowns avoids a redirect loop, and because writes
/// are frozen from step 3 the brief dual-read window is over identical data.
/// Any pre-flip failure rolls back our Importing so we don't strand the slot.
#[cfg(feature = "rocks")]
fn migrate_in(
    kv: &Arc<RocksKv>,
    src: &str,
    slot: u16,
    self_addr: Option<&str>,
    migration_active: &Arc<AtomicBool>,
    ns: &[u8],
) -> Value {
    use flint_storage::manifest::{self, MigrationPhase};

    // Step 1 (cutover): mark Importing before pulling.
    if let Some(_me) = self_addr {
        if let Err(e) = set_slot_phase(kv.as_ref(), ns, slot, MigrationPhase::Importing, src) {
            return Value::Error(format!("ERR set importing fenced: {e:?}"));
        }
        migration_active.store(true, Ordering::Relaxed);
    }
    let rollback = || {
        if self_addr.is_some() {
            manifest::clear_migration(kv.as_ref(), ns, slot);
        }
    };

    let mut stream = match internal_connect(src) {
        Ok(s) => s,
        Err(e) => {
            rollback();
            return Value::Error(format!("ERR migrate connect {src}: {e}"));
        }
    };
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(500)));
    let mut out = Vec::new();
    encode(
        &Value::Array(Some(vec![
            Value::Bulk(Some(b"FLINTMIGRATEOUT".to_vec())),
            Value::Bulk(Some(slot.to_string().into_bytes())),
            Value::Bulk(Some(ns.to_vec())),
        ])),
        &mut out,
    );
    if stream.write_all(&out).is_err() {
        rollback();
        return Value::Error("ERR migrate send failed".into());
    }

    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 64 * 1024];
    let mut applied: u64 = 0;
    let mut past_bulk = false;
    let mut frozen = false; // cutover: has the source been frozen yet?
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        match decode(&buf) {
            Ok(Decoded::Complete(frame, used)) => {
                buf.drain(..used);
                match frame {
                    Value::Simple(s) if s == "MIGRATEOUT-OK" => {}
                    Value::Error(e) => {
                        rollback();
                        return Value::Error(format!("ERR migrate source: {e}"));
                    }
                    Value::Array(Some(items)) => match items.as_slice() {
                        [
                            Value::Bulk(Some(t)),
                            Value::Bulk(Some(k)),
                            Value::Bulk(Some(v)),
                        ] if t == b"P" => {
                            kv.put(k, v);
                            applied += 1;
                        }
                        [Value::Bulk(Some(t)), Value::Bulk(Some(k))] if t == b"D" => {
                            kv.delete(k);
                        }
                        [
                            Value::Bulk(Some(t)),
                            Value::Integer(_snap),
                            Value::Integer(_rows),
                        ] if t == b"BULK-END" => {
                            past_bulk = true;
                        }
                        [
                            Value::Bulk(Some(t)),
                            Value::Integer(cursor),
                            Value::Integer(head),
                        ] if t == b"CAUGHTUP" && past_bulk && cursor >= head => {
                            match self_addr {
                                // Data-ship only: caught up, done.
                                None => return Value::Simple(format!("MIGRATEIN-OK {applied}")),
                                Some(me) => {
                                    if !frozen {
                                        // Step 3: freeze the slot on the source.
                                        match call_once(
                                            src,
                                            &[
                                                b"FLINTSLOTFREEZE",
                                                slot.to_string().as_bytes(),
                                                me.as_bytes(),
                                                ns,
                                            ],
                                        ) {
                                            Ok(Value::Simple(_)) => frozen = true,
                                            other => {
                                                rollback();
                                                return Value::Error(format!(
                                                    "ERR freeze failed: {other:?}"
                                                ));
                                            }
                                        }
                                        // Loop again to drain the frozen tail.
                                    } else {
                                        // Step 5: flip dest-first, then source.
                                        manifest::clear_migration(kv.as_ref(), ns, slot);
                                        match call_once(
                                            src,
                                            &[
                                                b"FLINTSLOTMOVED",
                                                slot.to_string().as_bytes(),
                                                me.as_bytes(),
                                                ns,
                                            ],
                                        ) {
                                            Ok(Value::Simple(_)) => {
                                                return Value::Simple(format!(
                                                    "MIGRATEIN-OK {applied} cutover"
                                                ));
                                            }
                                            other => {
                                                // We already own (Importing
                                                // cleared); the source failing
                                                // to disown is a controller
                                                // reconcile, not data loss.
                                                return Value::Error(format!(
                                                    "ERR cutover handoff incomplete (we own; source not disowned): {other:?}"
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
            Ok(Decoded::NeedMore) => {
                if std::time::Instant::now() > deadline {
                    rollback();
                    return Value::Error("ERR migrate timed out before drain".into());
                }
                match stream.read(&mut chunk) {
                    Ok(0) => {
                        rollback();
                        return Value::Error("ERR migrate source closed".into());
                    }
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(ref e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(e) => {
                        rollback();
                        return Value::Error(format!("ERR migrate read: {e}"));
                    }
                }
            }
            Err(_) => {
                rollback();
                return Value::Error("ERR migrate decode".into());
            }
        }
    }
}

/// FLINTMIGRATEIN <src host:port> <slot> [<self advertise addr>]. With the
/// self address it performs the full cutover (freeze/drain/flip); without it,
/// only ships the data (no ownership change).
#[cfg(feature = "rocks")]
fn flintmigratein(
    rocks: &Option<RocksHandle>,
    migration_active: &Arc<AtomicBool>,
    args: &[Vec<u8>],
) -> Value {
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTMIGRATEIN requires the rocks engine".into());
    };
    let (Some(src), Some(slot)) = (
        args.get(1).and_then(|r| std::str::from_utf8(r).ok()),
        args.get(2)
            .and_then(|r| std::str::from_utf8(r).ok())
            .and_then(|s| s.parse::<u16>().ok()),
    ) else {
        return Value::Error("ERR FLINTMIGRATEIN <host:port> <slot> [self-addr] [ns]".into());
    };
    let self_addr = args.get(3).and_then(|r| std::str::from_utf8(r).ok());
    let ns: &[u8] = args.get(4).map(|v| v.as_slice()).unwrap_or(b"0");
    migrate_in(kv, src, slot, self_addr, migration_active, ns)
}

#[cfg(not(feature = "rocks"))]
fn flintmigratein(
    _rocks: &Option<RocksHandle>,
    _migration_active: &Arc<AtomicBool>,
    _args: &[Vec<u8>],
) -> Value {
    Value::Error("ERR FLINTMIGRATEIN requires a build with --features rocks".into())
}

/// The next monotonic, fence-safe epoch for a slot's migration record: above
/// both any existing record and the base claim.
#[cfg(feature = "rocks")]
fn next_migration_epoch(kv: &RocksKv, ns: &[u8], slot: u16) -> flint_storage::manifest::Epoch {
    use flint_storage::manifest::{self, Epoch};
    // The base claim is node-wide (ns "0" carries it in v0); the record
    // fence is per (ns, slot).
    let base = manifest::read_claim(kv, b"0")
        .map(|c| c.epoch)
        .unwrap_or(Epoch::ZERO);
    let cur = manifest::read_migration(kv, ns, slot)
        .map(|r| r.epoch)
        .unwrap_or(Epoch::ZERO);
    let hi = base.max(cur);
    Epoch {
        generation: hi.generation,
        counter: hi.counter + 1,
    }
}

/// Write a phase override for one slot at a fenced next epoch.
#[cfg(feature = "rocks")]
fn set_slot_phase(
    kv: &RocksKv,
    ns: &[u8],
    slot: u16,
    phase: flint_storage::manifest::MigrationPhase,
    peer: &str,
) -> Result<(), flint_storage::manifest::ManifestError> {
    use flint_storage::manifest::{self, MigrationRecord};
    manifest::set_migration(
        kv,
        &MigrationRecord {
            ns: ns.to_vec(),
            slot,
            phase,
            peer: peer.to_string(),
            epoch: next_migration_epoch(kv, ns, slot),
        },
    )
}

/// FLINTSLOTMOVED <slot> <host:port>: terminal cutover on the source — durable
/// Moved override so this node answers -MOVED for the slot.
#[cfg(feature = "rocks")]
fn flintslotmoved(
    rocks: &Option<RocksHandle>,
    migration_active: &Arc<AtomicBool>,
    args: &[Vec<u8>],
) -> Value {
    use flint_storage::manifest::MigrationPhase;
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTSLOTMOVED requires the rocks engine".into());
    };
    let (Some(slot), Some(peer)) = (
        args.get(1)
            .and_then(|r| std::str::from_utf8(r).ok())
            .and_then(|s| s.parse::<u16>().ok()),
        args.get(2).and_then(|r| std::str::from_utf8(r).ok()),
    ) else {
        return Value::Error("ERR FLINTSLOTMOVED <slot> <host:port> [ns]".into());
    };
    let ns: &[u8] = args.get(3).map(|v| v.as_slice()).unwrap_or(b"0");
    match set_slot_phase(kv.as_ref(), ns, slot, MigrationPhase::Moved, peer) {
        Ok(()) => {
            migration_active.store(true, Ordering::Relaxed);
            // Purge the disowned slot's rows (all CFs): the destination owns
            // the data now, this copy is dead weight that would keep skewing
            // DBSIZE/fill metrics — the rebalance planner would chase it
            // forever (found by the execute drill). Ordering is safe: the
            // durable Moved record lands FIRST, so clients already get
            // -MOVED; a crash mid-purge leaves invisible orphans that a
            // retried FLINTSLOTMOVED (idempotent at a bumped epoch) or the
            // GC can finish clearing. The deletes ride the WAL, so replicas
            // drop the slot too.
            let purged = purge_slot_rows(kv.as_ref(), ns, slot);
            Value::Simple(format!(
                "OK slot {slot} moved to {peer} ({purged} rows purged)"
            ))
        }
        Err(e) => Value::Error(format!("ERR slot move fenced: {e:?}")),
    }
}

/// Delete every row of `slot` across all CFs. Streaming collect-then-delete
/// (v0; a real range-delete is an engine refinement). Returns rows removed.
#[cfg(feature = "rocks")]
fn purge_slot_rows(kv: &RocksKv, ns: &[u8], slot: u16) -> usize {
    use flint_storage::encoding::{Cf, slot_prefix};
    let mut purged = 0;
    for cf in [Cf::Metadata, Cf::Subkey, Cf::ZScore] {
        let prefix = slot_prefix(cf, ns, slot);
        let mut keys: Vec<Vec<u8>> = Vec::new();
        kv.for_each_prefix(&prefix, &mut |k, _| {
            keys.push(k.to_vec());
            true
        });
        for k in &keys {
            kv.delete(k);
        }
        purged += keys.len();
    }
    purged
}

#[cfg(not(feature = "rocks"))]
fn flintslotmoved(
    _rocks: &Option<RocksHandle>,
    _migration_active: &Arc<AtomicBool>,
    _args: &[Vec<u8>],
) -> Value {
    Value::Error("ERR FLINTSLOTMOVED requires a build with --features rocks".into())
}

/// FLINTSLOTFREEZE <slot> <dest>: mark the slot Migrating so the source sheds
/// writes to it (-TRYAGAIN) while the destination drains the final tail.
#[cfg(feature = "rocks")]
fn flintslotfreeze(
    rocks: &Option<RocksHandle>,
    migration_active: &Arc<AtomicBool>,
    args: &[Vec<u8>],
) -> Value {
    use flint_storage::manifest::MigrationPhase;
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTSLOTFREEZE requires the rocks engine".into());
    };
    let (Some(slot), Some(peer)) = (
        args.get(1)
            .and_then(|r| std::str::from_utf8(r).ok())
            .and_then(|s| s.parse::<u16>().ok()),
        args.get(2).and_then(|r| std::str::from_utf8(r).ok()),
    ) else {
        return Value::Error("ERR FLINTSLOTFREEZE <slot> <dest> [ns]".into());
    };
    let ns: &[u8] = args.get(3).map(|v| v.as_slice()).unwrap_or(b"0");
    match set_slot_phase(kv.as_ref(), ns, slot, MigrationPhase::Migrating, peer) {
        Ok(()) => {
            migration_active.store(true, Ordering::Relaxed);
            Value::Simple(format!("OK slot {slot} frozen"))
        }
        Err(e) => Value::Error(format!("ERR slot freeze fenced: {e:?}")),
    }
}

#[cfg(not(feature = "rocks"))]
fn flintslotfreeze(
    _rocks: &Option<RocksHandle>,
    _migration_active: &Arc<AtomicBool>,
    _args: &[Vec<u8>],
) -> Value {
    Value::Error("ERR FLINTSLOTFREEZE requires a build with --features rocks".into())
}

/// FLINTSLOTSTATS: live metadata-row counts per (namespace, slot) — one
/// "slot count ns" bulk per non-empty unit — in one streaming pass over the
/// 'M' CF across ALL namespaces (subkey/zscore rows belong to a key counted
/// via its meta row). Envelope: `M | ns_len | ns | slot(2 BE) | user_key`.
/// The (ns, slot) unit is the migration unit, so this is exactly the
/// executor's selection input. Memory is bounded by the number of distinct
/// non-empty (ns, slot) units. v0 counts expired-but-unswept rows too — a
/// fill approximation, not billing.
fn flintslotstats(store: &dyn Kv) -> Value {
    use std::collections::HashMap;
    let mut counts: HashMap<(Vec<u8>, u16), u64> = HashMap::new();
    store.for_each_prefix(b"M", &mut |k, _| {
        // k = M | ns_len | ns | slot(2) | key
        if let Some(&ns_len) = k.get(1) {
            let ns_end = 2 + ns_len as usize;
            if let Some(ns) = k.get(2..ns_end)
                && let Some(raw) = k.get(ns_end..ns_end + 2)
            {
                let slot = u16::from_be_bytes([raw[0], raw[1]]);
                *counts.entry((ns.to_vec(), slot)).or_insert(0) += 1;
            }
        }
        true
    });
    // A (ns, slot) this node has disowned (Moved) or is still importing must
    // not be offered to the rebalance planner: its rows are purge-pending
    // ghosts or an incomplete copy (found by the execute drill: ghost rows
    // kept the planner chasing the same slots forever).
    use flint_storage::manifest::{MigrationPhase, scan_all_migrations};
    for rec in scan_all_migrations(store) {
        if matches!(rec.phase, MigrationPhase::Moved | MigrationPhase::Importing) {
            counts.remove(&(rec.ns.clone(), rec.slot));
        }
    }
    let mut rows: Vec<((Vec<u8>, u16), u64)> = counts.into_iter().collect();
    rows.sort();
    Value::Array(Some(
        rows.into_iter()
            .map(|((ns, slot), n)| {
                Value::Bulk(Some(
                    format!("{slot} {n} {}", String::from_utf8_lossy(&ns)).into_bytes(),
                ))
            })
            .collect(),
    ))
}

/// FLINTMIGRATIONS: the in-flight (Importing/Migrating) records on this node
/// across ALL namespaces, one "slot phase peer ns" bulk each — the recovery
/// input for the controller.
/// FLINTSNAPSHOT <root>: checkpoint into <root>/<id>, repoint <root>/LATEST.
/// The id embeds time + latest sequence, so ordering and staleness are
/// readable from the name alone.
#[cfg(feature = "rocks")]
fn flintsnapshot(rocks: &Option<RocksHandle>, args: &[Vec<u8>]) -> Value {
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTSNAPSHOT requires the rocks engine".into());
    };
    let Some(root) = args.get(1).and_then(|r| std::str::from_utf8(r).ok()) else {
        return Value::Error("ERR FLINTSNAPSHOT <root-dir>".into());
    };
    let root = std::path::Path::new(root);
    if let Err(e) = std::fs::create_dir_all(root) {
        return Value::Error(format!("ERR snapshot root: {e}"));
    }
    let id = format!(
        "snap-{}-seq{}",
        flint_storage::strings::system_clock(),
        kv.latest_seq()
    );
    let dest = root.join(&id);
    if let Err(e) = kv.checkpoint_to(&dest) {
        return Value::Error(format!("ERR checkpoint: {e}"));
    }
    // Atomic LATEST repoint: write-then-rename, same as every manifest here.
    let tmp = root.join("LATEST.tmp");
    if let Err(e) =
        std::fs::write(&tmp, &id).and_then(|_| std::fs::rename(&tmp, root.join("LATEST")))
    {
        return Value::Error(format!("ERR LATEST repoint: {e}"));
    }
    eprintln!("snapshot {id} written to {}", root.display());
    Value::Simple(format!("OK {id}"))
}

#[cfg(not(feature = "rocks"))]
fn flintsnapshot(_rocks: &Option<RocksHandle>, _args: &[Vec<u8>]) -> Value {
    Value::Error("ERR FLINTSNAPSHOT requires a build with --features rocks".into())
}

#[cfg(feature = "rocks")]
fn flintmigrations(rocks: &Option<RocksHandle>) -> Value {
    use flint_storage::manifest::{self, MigrationPhase};
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTMIGRATIONS requires the rocks engine".into());
    };
    let rows: Vec<Value> = manifest::scan_all_migrations(kv.as_ref())
        .into_iter()
        .filter(|r| r.phase.is_inflight())
        .map(|r| {
            let phase = match r.phase {
                MigrationPhase::Importing => "importing",
                MigrationPhase::Migrating => "migrating",
                MigrationPhase::Moved => "moved",
            };
            Value::Bulk(Some(
                format!(
                    "{} {phase} {} {}",
                    r.slot,
                    r.peer,
                    String::from_utf8_lossy(&r.ns)
                )
                .into_bytes(),
            ))
        })
        .collect();
    Value::Array(Some(rows))
}

#[cfg(not(feature = "rocks"))]
fn flintmigrations(_rocks: &Option<RocksHandle>) -> Value {
    Value::Error("ERR FLINTMIGRATIONS requires a build with --features rocks".into())
}

/// FLINTSLOTABORT <slot>: clear an in-flight record (rollback/unfreeze).
/// Refuses a terminal Moved override — settled ownership is not aborted.
#[cfg(feature = "rocks")]
fn flintslotabort(rocks: &Option<RocksHandle>, args: &[Vec<u8>]) -> Value {
    use flint_storage::manifest::{self, MigrationPhase};
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTSLOTABORT requires the rocks engine".into());
    };
    let Some(slot) = args
        .get(1)
        .and_then(|r| std::str::from_utf8(r).ok())
        .and_then(|s| s.parse::<u16>().ok())
    else {
        return Value::Error("ERR FLINTSLOTABORT <slot> [ns]".into());
    };
    let ns: &[u8] = args.get(2).map(|v| v.as_slice()).unwrap_or(b"0");
    match manifest::read_migration(kv.as_ref(), ns, slot) {
        Some(r) if r.phase == MigrationPhase::Moved => {
            Value::Error("ERR slot is Moved (settled ownership), not in-flight".into())
        }
        Some(_) => {
            manifest::clear_migration(kv.as_ref(), ns, slot);
            Value::Simple(format!("OK slot {slot} migration aborted"))
        }
        None => Value::Simple(format!("OK slot {slot} had no in-flight migration")),
    }
}

#[cfg(not(feature = "rocks"))]
fn flintslotabort(_rocks: &Option<RocksHandle>, _args: &[Vec<u8>]) -> Value {
    Value::Error("ERR FLINTSLOTABORT requires a build with --features rocks".into())
}

/// Per-slot gate for a command's key: -MOVED if the slot was handed off
/// (Moved) or is being imported here (Importing); -TRYAGAIN if a WRITE hits a
/// slot frozen mid-cutover (Migrating). None means serve normally.
#[cfg(feature = "rocks")]
fn check_slot_gate(
    rocks: &Option<RocksHandle>,
    ns: &[u8],
    args: &[Vec<u8>],
    is_write: bool,
) -> Option<Value> {
    use flint_storage::manifest::{self, MigrationPhase};
    let kv = rocks.as_ref()?;
    let key = commands::command_key(args)?;
    let slot = flint_slot::slot_for_key(key);
    // Migration records are per (ns, slot): tenant A's moved slot must not
    // redirect tenant B, whose rows did not move.
    match manifest::read_migration(kv.as_ref(), ns, slot)?.phase {
        MigrationPhase::Moved | MigrationPhase::Importing => {
            let peer = manifest::read_migration(kv.as_ref(), ns, slot)?.peer;
            Some(Value::Error(format!("MOVED {slot} {peer}")))
        }
        MigrationPhase::Migrating if is_write => {
            Some(Value::Error("TRYAGAIN slot migrating, retry".into()))
        }
        MigrationPhase::Migrating => None, // reads still served by the source
    }
}

#[cfg(not(feature = "rocks"))]
fn check_slot_gate(
    _rocks: &Option<RocksHandle>,
    _ns: &[u8],
    _args: &[Vec<u8>],
    _is_write: bool,
) -> Option<Value> {
    None
}

/// Send one command to `addr` and read one reply (used by the cutover
/// orchestration to freeze and hand off the slot on the source).
#[cfg(feature = "rocks")]
fn call_once(addr: &str, args: &[&[u8]]) -> std::io::Result<Value> {
    let mut s = internal_connect(addr)?;
    s.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    let mut out = Vec::new();
    encode(
        &Value::Array(Some(
            args.iter().map(|a| Value::Bulk(Some(a.to_vec()))).collect(),
        )),
        &mut out,
    );
    s.write_all(&out)?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match decode(&buf) {
            Ok(Decoded::Complete(v, _)) => return Ok(v),
            Ok(Decoded::NeedMore) => {
                let n = s.read(&mut chunk)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "closed",
                    ));
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{e:?}"),
                ));
            }
        }
    }
}

/// Max payload per full-sync `F` frame. Bounds BOTH sides: this send
/// buffer, and the replica's decode buffer (it must hold a whole frame).
/// Files larger than this ship as multiple frames the replica appends in
/// order.
#[cfg(feature = "rocks")]
const FULLSYNC_CHUNK: usize = 4 * 1024 * 1024;

/// Master side of a checkpoint full sync: stream every file of a fresh
/// checkpoint in `FULLSYNC_CHUNK`-sized frames, then FULLSYNC-END. No
/// whole file is ever held in memory — SSTs are usually ~64MB, but
/// compaction settings can make them arbitrarily large.
#[cfg(feature = "rocks")]
fn flintfullsync(mut stream: flint_tls::Stream, rocks: Option<RocksHandle>) -> std::io::Result<()> {
    let mut out = Vec::new();
    let Some(kv) = rocks else {
        encode(
            &Value::Error("ERR FLINTFULLSYNC requires the rocks engine".into()),
            &mut out,
        );
        return stream.write_all(&out);
    };
    // Unique per checkpoint: a millisecond clock collides when two replicas
    // full-sync at once, and RocksDB's create_checkpoint fails if the dest
    // exists — which left both replicas unable to seed (found wiring D7's
    // 3-member pair). A process-global counter guarantees uniqueness.
    static FULLSYNC_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let ckpt = std::env::temp_dir().join(format!(
        "flint-fullsync-{}-{}-{}",
        std::process::id(),
        flint_storage::strings::system_clock(),
        FULLSYNC_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    kv.checkpoint_to(&ckpt)
        .map_err(|e| std::io::Error::other(format!("checkpoint: {e}")))?;
    let result = (|| -> std::io::Result<()> {
        for entry in std::fs::read_dir(&ckpt)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            let mut file = std::fs::File::open(entry.path())?;
            let mut chunk = vec![0u8; FULLSYNC_CHUNK];
            let mut sent_any = false;
            loop {
                let n = file.read(&mut chunk)?;
                if n == 0 && sent_any {
                    break;
                }
                // An empty file still ships one (empty) frame so the
                // replica creates it.
                out.clear();
                encode(
                    &Value::Array(Some(vec![
                        Value::Bulk(Some(b"F".to_vec())),
                        Value::Bulk(Some(name.clone().into_bytes())),
                        Value::Bulk(Some(chunk[..n].to_vec())),
                    ])),
                    &mut out,
                );
                stream.write_all(&out)?;
                sent_any = true;
                if n == 0 {
                    break;
                }
            }
        }
        out.clear();
        encode(&Value::Simple("FULLSYNC-END".into()), &mut out);
        stream.write_all(&out)
    })();
    let _ = std::fs::remove_dir_all(&ckpt);
    eprintln!("full sync served");
    result
}

#[cfg(not(feature = "rocks"))]
fn flintfullsync(
    mut stream: flint_tls::Stream,
    _rocks: Option<RocksHandle>,
) -> std::io::Result<()> {
    let mut out = Vec::new();
    encode(
        &Value::Error("ERR FLINTFULLSYNC requires a build with --features rocks".into()),
        &mut out,
    );
    stream.write_all(&out)
}

#[cfg(not(feature = "rocks"))]
fn flintsync(
    mut stream: flint_tls::Stream,
    _rocks: Option<RocksHandle>,
    _hub: &Arc<ReplHub>,
    _args: &[Vec<u8>],
) -> std::io::Result<()> {
    let mut out = Vec::new();
    encode(
        &Value::Error("ERR FLINTSYNC requires a build with --features rocks".into()),
        &mut out,
    );
    stream.write_all(&out)
}

#[cfg(not(feature = "rocks"))]
fn flintmigrateout(
    mut stream: flint_tls::Stream,
    _rocks: Option<RocksHandle>,
    _args: &[Vec<u8>],
) -> std::io::Result<()> {
    let mut out = Vec::new();
    encode(
        &Value::Error("ERR FLINTMIGRATEOUT requires a build with --features rocks".into()),
        &mut out,
    );
    stream.write_all(&out)
}

/// Replica side: connect, request the tail from our durable cursor, apply
/// batches atomically; reconnect with backoff on any error.
#[cfg(feature = "rocks")]
mod replica {
    use super::*;
    use flint_storage::repl::{ReplBatch, ReplOp};

    pub fn run(target: &str, kv: &Arc<RocksKv>, stop: &Arc<AtomicBool>) {
        loop {
            if stop.load(Ordering::Relaxed) {
                eprintln!("tailer stopped (promoted)");
                return;
            }
            if let Err(e) = tail_once(target, kv, stop)
                && !stop.load(Ordering::Relaxed)
            {
                eprintln!("replication link lost ({e}); reconnecting in 1s");
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }

    /// Download a checkpoint into `dir` before the DB is first opened.
    pub fn full_sync_download(target: &str, dir: &std::path::Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        let mut stream = internal_connect(target)?;
        let mut out = Vec::new();
        encode(
            &Value::Array(Some(vec![Value::Bulk(Some(b"FLINTFULLSYNC".to_vec()))])),
            &mut out,
        );
        stream.write_all(&out)?;
        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 256 * 1024];
        // Large files arrive as multiple F frames, appended in order. The
        // first frame for a name truncates: leftovers from an interrupted
        // earlier attempt must not be appended onto.
        let mut seen = std::collections::HashSet::<String>::new();
        loop {
            match decode(&buf) {
                Ok(Decoded::Complete(frame, used)) => {
                    buf.drain(..used);
                    match frame {
                        Value::Simple(s) if s == "FULLSYNC-END" => {
                            eprintln!("full sync: received {} files", seen.len());
                            return Ok(());
                        }
                        Value::Error(e) => {
                            return Err(std::io::Error::other(format!("master error: {e}")));
                        }
                        Value::Array(Some(items)) => {
                            let [
                                Value::Bulk(Some(tag)),
                                Value::Bulk(Some(name)),
                                Value::Bulk(Some(bytes)),
                            ] = items.as_slice()
                            else {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "malformed full-sync frame",
                                ));
                            };
                            if tag != b"F" {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "unexpected full-sync tag",
                                ));
                            }
                            let fname = String::from_utf8_lossy(name);
                            // Checkpoint dirs are flat; reject path tricks.
                            if fname.contains('/') || fname.contains("..") {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "unsafe file name in full sync",
                                ));
                            }
                            let mut opts = std::fs::OpenOptions::new();
                            if seen.insert(fname.clone().into_owned()) {
                                opts.write(true).create(true).truncate(true);
                            } else {
                                opts.append(true);
                            }
                            opts.open(dir.join(fname.as_ref()))?.write_all(bytes)?;
                        }
                        _ => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "malformed full-sync frame",
                            ));
                        }
                    }
                }
                Ok(Decoded::NeedMore) => {
                    let n = stream.read(&mut chunk)?;
                    if n == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "master closed during full sync",
                        ));
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("full-sync protocol error: {e:?}"),
                    ));
                }
            }
        }
    }

    fn tail_once(target: &str, kv: &Arc<RocksKv>, stop: &Arc<AtomicBool>) -> std::io::Result<()> {
        let mut stream = internal_connect(target)?;
        // Short read timeout so the stop flag is honored promptly.
        stream.set_read_timeout(Some(std::time::Duration::from_millis(300)))?;
        let cursor = kv.last_applied();
        let mut out = Vec::new();
        encode(
            &Value::Array(Some(vec![
                Value::Bulk(Some(b"FLINTSYNC".to_vec())),
                Value::Bulk(Some(cursor.to_string().into_bytes())),
            ])),
            &mut out,
        );
        stream.write_all(&out)?;
        eprintln!("replicating from {target} starting at seq {cursor}");

        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 64 * 1024];
        // Heartbeat: an idle replica must still prove liveness — ACKs only
        // after applied batches would make a healthy idle pair look dead
        // after the liveness window.
        let mut last_ack_sent = std::time::Instant::now();
        loop {
            if last_ack_sent.elapsed() >= std::time::Duration::from_millis(500) {
                out.clear();
                encode(
                    &Value::Array(Some(vec![
                        Value::Bulk(Some(b"ACK".to_vec())),
                        Value::Bulk(Some(kv.last_applied().to_string().into_bytes())),
                    ])),
                    &mut out,
                );
                stream.write_all(&out)?;
                last_ack_sent = std::time::Instant::now();
            }
            match decode(&buf) {
                Ok(Decoded::Complete(frame, used)) => {
                    buf.drain(..used);
                    match frame {
                        Value::Simple(s) if s.starts_with("FLINTSYNC-OK") => {
                            out.clear();
                            encode(
                                &Value::Array(Some(vec![
                                    Value::Bulk(Some(b"ACK".to_vec())),
                                    Value::Bulk(Some(cursor.to_string().into_bytes())),
                                ])),
                                &mut out,
                            );
                            stream.write_all(&out)?;
                        }
                        Value::Error(e) => {
                            return Err(std::io::Error::other(format!("master error: {e}")));
                        }
                        other => {
                            let batch = parse_batch(other).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "malformed replication frame",
                                )
                            })?;
                            kv.apply_batch(&batch)
                                .map_err(|e| std::io::Error::other(format!("apply: {e:?}")))?;
                            out.clear();
                            encode(
                                &Value::Array(Some(vec![
                                    Value::Bulk(Some(b"ACK".to_vec())),
                                    Value::Bulk(Some(batch.last_seq.to_string().into_bytes())),
                                ])),
                                &mut out,
                            );
                            stream.write_all(&out)?;
                        }
                    }
                }
                Ok(Decoded::NeedMore) => {
                    if stop.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    match stream.read(&mut chunk) {
                        Ok(0) => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "master closed",
                            ));
                        }
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                || e.kind() == std::io::ErrorKind::TimedOut =>
                        {
                            continue; // timeout tick: loop re-checks stop
                        }
                        Err(e) => return Err(e),
                    }
                }
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("replication protocol error: {e:?}"),
                    ));
                }
            }
        }
    }

    fn parse_batch(frame: Value) -> Option<ReplBatch> {
        let Value::Array(Some(items)) = frame else {
            return None;
        };
        let [
            Value::Integer(first_seq),
            Value::Integer(last_seq),
            Value::Array(Some(raw_ops)),
        ] = items.as_slice()
        else {
            return None;
        };
        let mut ops = Vec::with_capacity(raw_ops.len());
        for raw in raw_ops {
            let Value::Array(Some(parts)) = raw else {
                return None;
            };
            match parts.as_slice() {
                [
                    Value::Bulk(Some(tag)),
                    Value::Bulk(Some(key)),
                    Value::Bulk(Some(value)),
                ] if tag == b"P" => {
                    ops.push(ReplOp::Put {
                        key: key.clone(),
                        value: value.clone(),
                    });
                }
                [Value::Bulk(Some(tag)), Value::Bulk(Some(key))] if tag == b"D" => {
                    ops.push(ReplOp::Delete { key: key.clone() });
                }
                _ => return None,
            }
        }
        Some(ReplBatch {
            first_seq: *first_seq as u64,
            last_seq: *last_seq as u64,
            ops,
        })
    }
}

#[cfg(test)]
mod serve_tests {
    use super::*;
    use std::net::TcpStream;

    /// Ephemeral server running `serve` with a per-connection MemKv.
    fn spawn_server() -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                std::thread::spawn(move || {
                    let store = MemKv::new();
                    let _ = serve(
                        flint_tls::Stream::Plain(stream),
                        &store,
                        &Arc::new(AtomicBool::new(false)),
                        &Arc::new(AtomicBool::new(false)),
                        &Arc::new(std::sync::atomic::AtomicU64::new(0)),
                        None,
                        &Arc::new(ReplHub::default()),
                        &Arc::new(AtomicBool::new(false)),
                        commands::Limits::default(),
                    );
                });
            }
        });
        addr
    }

    fn connect(addr: std::net::SocketAddr) -> TcpStream {
        let s = TcpStream::connect(addr).expect("connect");
        s.set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .expect("timeout");
        s
    }

    /// Read frames until `n` complete values arrived (or the reader times out).
    fn read_frames(stream: &mut TcpStream, n: usize) -> Vec<Value> {
        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 64 * 1024];
        let mut frames = Vec::new();
        while frames.len() < n {
            match decode(&buf) {
                Ok(Decoded::Complete(v, used)) => {
                    buf.drain(..used);
                    frames.push(v);
                }
                Ok(Decoded::NeedMore) => {
                    let got = stream.read(&mut chunk).expect("read");
                    assert!(got > 0, "server closed after {} frames", frames.len());
                    buf.extend_from_slice(&chunk[..got]);
                }
                Err(e) => panic!("protocol error from server: {e:?}"),
            }
        }
        frames
    }

    /// The DBSIZE-class fix for the wire: a 5-byte header declaring a 4GB
    /// bulk must be refused at parse time, not buffered until OOM.
    #[test]
    fn oversized_bulk_declaration_is_refused_not_buffered() {
        let mut s = connect(spawn_server());
        s.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4294967296\r\n")
            .expect("send");
        let mut reply = Vec::new();
        // Error reply, then the server closes the connection.
        s.read_to_end(&mut reply).expect("read");
        let text = String::from_utf8_lossy(&reply);
        assert!(
            text.starts_with("-ERR Protocol error"),
            "expected protocol error, got: {text:?}"
        );
    }

    #[test]
    fn runaway_inline_line_is_refused() {
        let mut s = connect(spawn_server());
        // An inline command that never terminates must not accumulate
        // forever; past MAX_INLINE_LEN the server errors and closes.
        s.write_all(&vec![b'a'; MAX_INLINE_LEN + 1024])
            .expect("send");
        let mut reply = Vec::new();
        s.read_to_end(&mut reply).expect("read");
        let text = String::from_utf8_lossy(&reply);
        assert!(
            text.starts_with("-ERR Protocol error"),
            "expected protocol error, got: {text:?}"
        );
    }

    /// A pipeline whose replies overflow OUT_FLUSH_THRESHOLD must flush
    /// incrementally and stay correct — every reply arrives, in order, and
    /// the connection remains usable.
    #[test]
    fn pipelined_replies_flush_incrementally_and_stay_correct() {
        let mut s = connect(spawn_server());
        let value = vec![b'v'; 64 * 1024];
        let mut pipeline = Vec::new();
        encode(
            &Value::Array(Some(vec![
                Value::Bulk(Some(b"SET".to_vec())),
                Value::Bulk(Some(b"k".to_vec())),
                Value::Bulk(Some(value.clone())),
            ])),
            &mut pipeline,
        );
        let gets = 40; // 40 * 64KB of replies ≈ 2.5 * OUT_FLUSH_THRESHOLD
        for _ in 0..gets {
            encode(
                &Value::Array(Some(vec![
                    Value::Bulk(Some(b"GET".to_vec())),
                    Value::Bulk(Some(b"k".to_vec())),
                ])),
                &mut pipeline,
            );
        }
        s.write_all(&pipeline).expect("send pipeline");
        let frames = read_frames(&mut s, gets + 1);
        assert_eq!(frames[0], Value::Simple("OK".into()));
        for f in &frames[1..] {
            assert_eq!(f, &Value::Bulk(Some(value.clone())), "GET reply corrupted");
        }
        // Connection still healthy after the flushed pipeline.
        let mut ping = Vec::new();
        encode(
            &Value::Array(Some(vec![Value::Bulk(Some(b"PING".to_vec()))])),
            &mut ping,
        );
        s.write_all(&ping).expect("send ping");
        assert_eq!(read_frames(&mut s, 1)[0], Value::Simple("PONG".into()));
    }
}
