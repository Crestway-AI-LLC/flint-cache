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
use std::net::{TcpListener, TcpStream};
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

fn main() -> std::io::Result<()> {
    let port = arg("--port")
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(6380);
    let engine = arg("--engine").unwrap_or_else(|| "mem".into());
    let replica_of = arg("--replica-of");
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
    let hub = Arc::new(ReplHub::new(lag_soft, lag_hard));

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
                }
            }
        });
    }
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    eprintln!("flint-server listening on 127.0.0.1:{port}");
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
        std::thread::spawn(move || {
            let _ = serve(
                stream,
                store.as_ref(),
                &read_only,
                &tailer_stop,
                &lease_deadline,
                rocks,
                &hub,
            );
        });
    }
    Ok(())
}

fn serve(
    mut stream: TcpStream,
    store: &dyn Kv,
    read_only: &Arc<AtomicBool>,
    tailer_stop: &Arc<AtomicBool>,
    lease_deadline: &Arc<std::sync::atomic::AtomicU64>,
    rocks: Option<RocksHandle>,
    hub: &Arc<ReplHub>,
) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut chunk = [0u8; 16 * 1024];
    let mut out: Vec<u8> = Vec::with_capacity(4 * 1024);
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
                    &args,
                );
                encode(&reply, &mut out);
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
                    // FLINTSYNC/FLINTFULLSYNC hijack the connection.
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
                    let reply = execute(
                        store,
                        read_only,
                        tailer_stop,
                        lease_deadline,
                        &rocks,
                        hub,
                        &args,
                    );
                    encode(&reply, &mut out);
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
    }
}

fn execute(
    store: &dyn Kv,
    read_only: &Arc<AtomicBool>,
    tailer_stop: &Arc<AtomicBool>,
    lease_deadline: &Arc<std::sync::atomic::AtomicU64>,
    rocks: &Option<RocksHandle>,
    hub: &Arc<ReplHub>,
    args: &[Vec<u8>],
) -> Value {
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
    // Lag-cap backpressure: the write path enforces the RPO bound.
    if is_write && !ro {
        let now = flint_storage::strings::system_clock();
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
        Dispatcher::new(&ro_store, flint_storage::strings::system_clock).dispatch(args)
    } else {
        Dispatcher::new(store, flint_storage::strings::system_clock).dispatch(args)
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
        "role:{}\r\nrole_epoch:{role_epoch}\r\nlatest_seq:{latest}\r\nlast_applied:{last_applied}\r\nacked_seq:{}\r\nseq_lag:{seq_lag}\r\nlive_replicas:{}\r\nlag_ms:{}\r\nlag_soft_ms:{soft}\r\nlag_hard_ms:{hard}\r\n",
        if read_only { "replica" } else { "master" },
        hub.effective_acked(now)
            .map_or_else(|| "none".into(), |a| a.to_string()),
        hub.live_replica_count(now),
        hub.lag_ms(now)
            .map_or_else(|| "none".into(), |l| l.to_string()),
        soft = hub.lag_soft_ms,
        hard = hub.lag_hard_ms,
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
#[cfg(feature = "rocks")]
fn flintsync(
    mut stream: TcpStream,
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
    eprintln!("replica connected, streaming from seq {cursor}");
    // ACK reader: the replica confirms applied sequences on the same socket.
    let replica_id = hub.register_replica();
    {
        let reader = stream.try_clone()?;
        let hub = Arc::clone(hub);
        std::thread::spawn(move || {
            ack_reader(reader, &hub, replica_id);
            hub.unregister_replica(replica_id);
        });
    }
    loop {
        hub.record_sample(kv.latest_seq(), flint_storage::strings::system_clock());
        match kv.updates_since(cursor) {
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
                stream.write_all(&out)?;
            }
            Ok(_) => std::thread::sleep(std::time::Duration::from_millis(20)),
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
}

#[cfg(feature = "rocks")]
fn ack_reader(mut stream: TcpStream, hub: &Arc<ReplHub>, replica_id: u64) {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match decode(&buf) {
            Ok(Decoded::Complete(frame, used)) => {
                buf.drain(..used);
                if let Value::Array(Some(items)) = frame
                    && let [Value::Bulk(Some(tag)), Value::Bulk(Some(raw))] = items.as_slice()
                    && tag.eq_ignore_ascii_case(b"ACK")
                    && let Some(seq) = std::str::from_utf8(raw).ok().and_then(|s| s.parse().ok())
                {
                    hub.record_ack(replica_id, seq, flint_storage::strings::system_clock());
                }
            }
            Ok(Decoded::NeedMore) => {
                let Ok(n) = stream.read(&mut chunk) else {
                    return;
                };
                if n == 0 {
                    return;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(_) => return,
        }
    }
}

/// Master side of a checkpoint full sync: stream every file of a fresh
/// checkpoint, then FULLSYNC-END. v0 buffers each file in memory (fine at
/// drill scale; chunked streaming is a later refinement).
#[cfg(feature = "rocks")]
fn flintfullsync(mut stream: TcpStream, rocks: Option<RocksHandle>) -> std::io::Result<()> {
    let mut out = Vec::new();
    let Some(kv) = rocks else {
        encode(
            &Value::Error("ERR FLINTFULLSYNC requires the rocks engine".into()),
            &mut out,
        );
        return stream.write_all(&out);
    };
    let ckpt = std::env::temp_dir().join(format!(
        "flint-fullsync-{}-{}",
        std::process::id(),
        flint_storage::strings::system_clock()
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
            let bytes = std::fs::read(entry.path())?;
            out.clear();
            encode(
                &Value::Array(Some(vec![
                    Value::Bulk(Some(b"F".to_vec())),
                    Value::Bulk(Some(name.into_bytes())),
                    Value::Bulk(Some(bytes)),
                ])),
                &mut out,
            );
            stream.write_all(&out)?;
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
fn flintfullsync(mut stream: TcpStream, _rocks: Option<RocksHandle>) -> std::io::Result<()> {
    let mut out = Vec::new();
    encode(
        &Value::Error("ERR FLINTFULLSYNC requires a build with --features rocks".into()),
        &mut out,
    );
    stream.write_all(&out)
}

#[cfg(not(feature = "rocks"))]
fn flintsync(
    mut stream: TcpStream,
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
        let mut stream = TcpStream::connect(target)?;
        let mut out = Vec::new();
        encode(
            &Value::Array(Some(vec![Value::Bulk(Some(b"FLINTFULLSYNC".to_vec()))])),
            &mut out,
        );
        stream.write_all(&out)?;
        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 256 * 1024];
        let mut files = 0u32;
        loop {
            match decode(&buf) {
                Ok(Decoded::Complete(frame, used)) => {
                    buf.drain(..used);
                    match frame {
                        Value::Simple(s) if s == "FULLSYNC-END" => {
                            eprintln!("full sync: received {files} files");
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
                            std::fs::write(dir.join(fname.as_ref()), bytes)?;
                            files += 1;
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
        let mut stream = TcpStream::connect(target)?;
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
