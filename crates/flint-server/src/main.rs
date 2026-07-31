// SPDX-License-Identifier: Elastic-2.0
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
mod diskguard;
mod heat;
mod json_path;
mod migrate;
mod repl_hub;
mod write_lock;
mod write_queue;

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use flint_resp::{Decoded, Value, decode, encode, encode_proto};
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

/// Marker file dropped beside a data dir whose contents can no longer be
/// continued from a master: the next start with `--replica-of` discards the
/// directory and full-syncs instead of resuming.
///
/// A file rather than a system row because the decision is made BEFORE the
/// DB is opened — and the directory it describes is about to be deleted
/// anyway. Two situations write it, and they are the same situation seen
/// from two sides:
///
/// * demotion — an ex-master's unreplicated suffix may have diverged from
///   the successor's lineage, so FLINTDEMOTE's contract has always been
///   "wipe and resync"; it now records that itself instead of relying on
///   whichever tool happens to restart the seat;
/// * a replication cursor that has fallen outside the master's retained WAL,
///   which no amount of reconnecting can fix.
///
/// Promotion clears it: a promoted node IS the lineage, and a stale marker
/// would make its next start-as-replica destroy good data.
#[cfg(feature = "rocks")]
const NEEDS_RESEED: &str = "NEEDS_RESEED";

/// Record that `dir`'s contents must be thrown away before this node can
/// tail a master again. Best-effort and loud on failure: losing the marker
/// costs a manual wipe later, which is exactly where we were before.
#[cfg(feature = "rocks")]
fn mark_needs_reseed(dir: &std::path::Path, why: &str) {
    let marker = dir.join(NEEDS_RESEED);
    if let Err(e) = std::fs::write(&marker, format!("{why}\n")) {
        eprintln!("could not write {}: {e}", marker.display());
    }
}

/// Drop the marker — this copy is authoritative again.
#[cfg(feature = "rocks")]
fn clear_needs_reseed(dir: &std::path::Path) {
    let marker = dir.join(NEEDS_RESEED);
    if marker.exists() {
        if let Err(e) = std::fs::remove_file(&marker) {
            eprintln!("could not remove {}: {e}", marker.display());
        } else {
            eprintln!("cleared {NEEDS_RESEED}: this node is the lineage now");
        }
    }
}

/// Internal-mesh mutual-TLS client config (the --internal-* triple in the
/// client role), set once at startup as a HOT-RELOADING handle (ADR-0006
/// D4 follow-on): every node→node dial — replication tail, full-sync
/// download, migrate-in, cutover orchestration — goes through
/// [`internal_connect`], which snapshots the current leaf per dial, so the
/// whole data plane picks up a rotated cert with no restart.
static INTERNAL_CLIENT: std::sync::OnceLock<Option<Arc<flint_tls::ReloadableClientConfig>>> =
    std::sync::OnceLock::new();

/// Full-sync admission control: a checkpoint full sync creates a consistent
/// checkpoint and streams every SST — GBs of disk I/O and network, and it
/// pins those SSTs against compaction reclamation for its duration. Many at
/// once (a herd after a master restart, or adding several replicas) would
/// saturate the master and starve the live workload and healthy replicas'
/// WAL tails. Cap the number in flight; over the cap the master replies
/// `-THROTTLED` and the requesting replica retries with backoff (the WAL
/// tail is unaffected — this bounds only bulk SEEDING). Default 2; tune with
/// `--max-fullsync`.
static MAX_FULLSYNC: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(2);
/// Wall-clock ms of this replica's last contact FROM the master over the
/// FLINTSYNC stream (batch or idle keepalive). A replica whose contact is
/// older than REPLICA_STALE_MS is not in live sync and self-fences READS
/// (R1): it returns -TRYAGAIN, and the proxy's existing dead-replica
/// fallback retries the master — so a wedged/partitioned replica can never
/// serve arbitrarily stale data past the bound the tenant was promised.
/// 0 = never contacted (a fresh, still-catching-up replica fences until its
/// first keepalive). A MASTER never consults this.
static REPLICA_CONTACT_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Read-staleness bound (ms). Well above the 500ms keepalive so a healthy
/// replica never false-fences; tunable with --replica-read-stale-ms.
static REPLICA_STALE_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(3000);
/// Data-port connection cap (B1 back-pressure symmetry with the proxy's
/// admission control). Thread-per-connection: without a bound, a connection
/// storm exhausts the node's threads. This is the INTERNAL mesh surface, so
/// only a buggy proxy fleet or runaway internal tooling can approach it — a
/// generous safety valve, not a tenant-facing limit. Over the cap a new
/// connection is DROPPED (a reset the peer backs off on; writing a
/// pre-handshake shed frame over mutual TLS is not possible). Tunable with
/// --max-conns.
static MAX_CONNS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(2048);
static ACTIVE_CONNS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static CONNS_SHED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Decrements the live-connection counter on any exit (incl. panic).
struct ConnGuard;
impl Drop for ConnGuard {
    fn drop(&mut self) {
        ACTIVE_CONNS.fetch_sub(1, Ordering::Relaxed);
    }
}
// Only the rocks paths (full-sync serving + its FLINTINFO fields) read this.
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
static FULLSYNC_ACTIVE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

// Disk headroom for the data directory. A node-wide condition (it is the
// host's filesystem, not any tenant's), so it lives beside the other
// process statics rather than threading through every call.
static DISK: std::sync::LazyLock<diskguard::DiskGuard> =
    std::sync::LazyLock::new(diskguard::DiskGuard::default);

// WAL fsync cadence in ms (0 = disabled), for FLINTINFO. Rocks-only.
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
static WAL_FSYNC_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

// Slot-migration copy rate cap in bytes/sec (0 = unlimited). The source of a
// FLINTMIGRATEIN paces its bulk+tail stream to this, so rebalancing — above
// all a traffic-policy move, which by definition ships the HOTTEST slots —
// doesn't saturate disk/net and inflate live p99. Read live by the stream and
// hot-reloadable via FLINTCONFIG. Rocks-only.
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
static MIGRATE_RATE_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Releases a full-sync slot on any exit (including a mid-stream error), so a
/// dropped connection can never leak a slot and wedge the cap.
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
struct FullSyncGuard;
impl Drop for FullSyncGuard {
    fn drop(&mut self) {
        FULLSYNC_ACTIVE.fetch_sub(1, Ordering::Relaxed);
    }
}

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
    flint_tls::connect_reloadable(addr, INTERNAL_CLIENT.get().unwrap_or(&None))
}

/// Fleet-journal target (--journal <cp-addr>) and this node's own address,
/// for role-transition events. Reporting is best-effort and detached — a
/// transition never waits on (or fails because of) the journal.
static JOURNAL_TARGET: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
static SELF_ADDR: std::sync::OnceLock<String> = std::sync::OnceLock::new();
/// This node's own mesh leaf cert path (--internal-cert), for the
/// cert-expiry gauge in FLINTINFO (ADR-0006 cert hygiene). Read live so a
/// hot-reload's new leaf is reflected.
static CERT_PATH: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();

fn journal_event(kind: flint_journal::EventKind, epoch: Option<String>, cause: &str) {
    let Some(Some(target)) = JOURNAL_TARGET.get().cloned() else {
        return;
    };
    let me = SELF_ADDR.get().cloned().unwrap_or_default();
    flint_journal::emit_detached(
        target,
        INTERNAL_CLIENT
            .get()
            .cloned()
            .unwrap_or(None)
            .map(|r| r.current()),
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
    if let Some(n) = arg("--max-fullsync").and_then(|v| v.parse::<usize>().ok()) {
        MAX_FULLSYNC.store(n.max(1), Ordering::Relaxed);
    }
    if let Some(n) = arg("--replica-read-stale-ms").and_then(|v| v.parse::<u64>().ok()) {
        REPLICA_STALE_MS.store(n, Ordering::Relaxed);
    }
    if let Some(n) = arg("--max-conns").and_then(|v| v.parse::<usize>().ok()) {
        MAX_CONNS.store(n.max(1), Ordering::Relaxed);
    }
    let _ = JOURNAL_TARGET.set(arg("--journal"));
    let engine = arg("--engine").unwrap_or_else(|| "mem".into());
    let replica_of = arg("--replica-of");

    // Internal-mesh mutual TLS: one --internal-* triple, used in BOTH roles —
    // server config on the data-port listener (peers must present a cert
    // chaining to the CA), client config on every node→node dial (replication
    // tail, full sync, migrate, cutover). Parsed before anything dials out:
    // a fresh replica's checkpoint download happens during startup, below.
    // BOTH roles hot-reload their leaf on disk change (ADR-0006 D4 + its
    // follow-on): `flintctl rotate-certs` re-signs the leaf; the listener
    // picks it up within a poll and every node->node dial snapshots the
    // current leaf at dial time — no restarts anywhere.
    let internal_reload: Option<Arc<flint_tls::ReloadableServerConfig>> = match (
        arg("--internal-ca"),
        arg("--internal-cert"),
        arg("--internal-key"),
    ) {
        (Some(ca), Some(cert), Some(key)) => {
            let _ = CERT_PATH.set(Some(cert.clone()));
            let _ = INTERNAL_CLIENT.set(Some(
                flint_tls::ReloadableClientConfig::watch(&ca, &cert, &key)
                    .expect("build internal TLS client config"),
            ));
            Some(
                flint_tls::ReloadableServerConfig::watch(&ca, &cert, &key)
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
    // The filesystem the disk guard watches. Only set for an engine that
    // actually writes to one — the mem engine has no disk to run out of.
    #[allow(unused_mut)]
    let mut data_dir_for_guard: Option<String> = None;
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
            data_dir_for_guard = Some(dir.clone());
            // Honour a re-seed marker BEFORE anything opens the directory.
            // It is only ever acted on when someone asked us to tail a
            // master: with no --replica-of we are being started AS the
            // lineage, and the marker describes a replication position we no
            // longer follow, so it is cleared and the data kept. Deliberately
            // narrow — silently turning ordinary restarts into full syncs
            // would be a far worse bug than the one this fixes.
            let dir_path = std::path::Path::new(&dir).to_path_buf();
            let reseed = dir_path.join(NEEDS_RESEED).exists();
            if reseed {
                if replica_of.is_some() {
                    eprintln!(
                        "{NEEDS_RESEED} present: this copy cannot be continued ({}) — discarding \
                         it and re-seeding from a checkpoint",
                        std::fs::read_to_string(dir_path.join(NEEDS_RESEED))
                            .unwrap_or_default()
                            .trim()
                    );
                    std::fs::remove_dir_all(&dir_path)?;
                } else {
                    clear_needs_reseed(&dir_path);
                }
            }
            let fresh = !dir_path.join("CURRENT").exists();
            // A fresh replica seeds from a checkpoint (the spare-seeding
            // path), then tails the WAL from the copied DB's own sequence.
            // Retry on the master's full-sync admission -THROTTLED (a herd
            // draining) with backoff — a throttled seed must not crash the
            // replica. The master rejects BEFORE any file ships, so each
            // retry re-downloads cleanly.
            if fresh && let Some(target) = &replica_of {
                let mut attempt = 0;
                loop {
                    match replica::full_sync_download(target, std::path::Path::new(&dir)) {
                        Ok(()) => break,
                        // Retryable: the master admitting a herd (-THROTTLED)
                        // or simply NOT LISTENING YET — a fleet boot starts
                        // master and replica within the same second, and the
                        // replica losing that race must wait, not die (the
                        // CFN stack lost it; the smoke instance won it).
                        // Genuinely malformed streams stay fatal.
                        Err(e)
                            if attempt < 120
                                && (e.to_string().contains("THROTTLED")
                                    || matches!(
                                        e.kind(),
                                        std::io::ErrorKind::ConnectionRefused
                                            | std::io::ErrorKind::ConnectionReset
                                            | std::io::ErrorKind::ConnectionAborted
                                            | std::io::ErrorKind::TimedOut
                                            | std::io::ErrorKind::UnexpectedEof
                                    )) =>
                        {
                            attempt += 1;
                            eprintln!("full sync not ready ({e}); retry {attempt} in 1s");
                            std::thread::sleep(std::time::Duration::from_secs(1));
                        }
                        Err(e) => return Err(e),
                    }
                }
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

    // Bounded-cadence WAL fsync (--wal-fsync-ms, default 500; 0 disables).
    // Ordinary writes append to the WAL unsynced — zero acked loss across a
    // process crash (the OS holds the pages) — and this tick group-commits
    // an fsync every cadence, bounding a HOST failure's loss window (power,
    // kernel, instance loss) to the cadence. Together with the lag-hard
    // replication cap this is the structural basis of the published RPO.
    // The cadence lives in WAL_FSYNC_MS (atomic) and the tick reads it EVERY
    // iteration, so FLINTCONFIG retunes — or toggles (0 = disabled) — it
    // live, no restart. The thread always runs (on rocks); at 0 it idles a
    // fixed poll and skips the fsync.
    #[cfg(feature = "rocks")]
    {
        let wal_fsync_ms: u64 = arg("--wal-fsync-ms")
            .and_then(|v| v.parse().ok())
            .unwrap_or(500);
        WAL_FSYNC_MS.store(wal_fsync_ms, Ordering::Relaxed);
        // Slot-migration copy-rate cap (bytes/sec; 0 = unlimited, the default).
        let migrate_rate_bytes: u64 = arg("--migrate-rate-bytes")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        MIGRATE_RATE_BYTES.store(migrate_rate_bytes, Ordering::Relaxed);
        if wal_fsync_ms == 0 {
            eprintln!("wal fsync cadence DISABLED (--wal-fsync-ms 0): host-loss window unbounded");
        }
        if let Some(kv) = rocks.clone() {
            std::thread::spawn(move || {
                loop {
                    let cadence = WAL_FSYNC_MS.load(Ordering::Relaxed);
                    if cadence == 0 {
                        std::thread::sleep(std::time::Duration::from_millis(500));
                        continue;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(cadence));
                    // Re-check: a retune to 0 during the sleep means skip.
                    if WAL_FSYNC_MS.load(Ordering::Relaxed) == 0 {
                        continue;
                    }
                    if let Err(e) = kv.flush_wal_sync() {
                        eprintln!("wal fsync tick failed: {e}");
                    }
                }
            });
        }
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
    // cap, 4 KiB by default (ElastiCache Serverless's ceiling), clamped to
    // the envelope's structural 64KB one (the subkey length frame is 2
    // bytes); 0 means ceiling only.
    let limits = commands::Limits {
        max_value_bytes: arg("--max-value-bytes")
            .and_then(|v| v.parse().ok())
            .unwrap_or(flint_storage::DEFAULT_MAX_VALUE_BYTES),
        max_key_bytes: arg("--max-key-bytes")
            .and_then(|v| v.parse().ok())
            .unwrap_or(flint_storage::DEFAULT_MAX_KEY_BYTES),
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
    // Disk headroom sampler. Only meaningful when data actually lands on a
    // filesystem, so the mem engine has nothing to guard and starts no
    // thread. Thresholds: --disk-min-free-pct (default 10) and
    // --disk-min-free-bytes (default 2 GiB); either can be set to 0 to
    // disable that half, and both to 0 to turn the gate off entirely.
    if let Some(dir) = data_dir_for_guard.clone() {
        let thresholds = diskguard::Thresholds {
            min_free_pct: arg("--disk-min-free-pct")
                .and_then(|v| v.parse().ok())
                .unwrap_or(diskguard::Thresholds::default().min_free_pct),
            min_free_bytes: arg("--disk-min-free-bytes")
                .and_then(|v| v.parse().ok())
                .unwrap_or(diskguard::Thresholds::default().min_free_bytes),
        };
        let every = std::time::Duration::from_millis(
            arg("--disk-sample-ms")
                .and_then(|v| v.parse().ok())
                .unwrap_or(2_000),
        );
        eprintln!(
            "disk guard: min-free {}% or {} bytes, sampling {} every {:?}",
            thresholds.min_free_pct, thresholds.min_free_bytes, dir, every
        );
        std::thread::spawn(move || {
            let path = std::path::PathBuf::from(&dir);
            let mut last = diskguard::Verdict::Ok;
            loop {
                let usage = flint_storage::disk::sample(&path);
                let v = diskguard::verdict(usage, thresholds, last);
                if v != last {
                    // Transitions are the thing an operator needs in the
                    // log; steady state is what the metrics are for.
                    eprintln!(
                        "disk guard: {last:?} -> {v:?} (free {} of {} bytes)",
                        usage.map(|u| u.free_bytes).unwrap_or(0),
                        usage.map(|u| u.total_bytes).unwrap_or(0)
                    );
                }
                DISK.apply(usage, v);
                last = v;
                std::thread::sleep(every);
            }
        });
    }

    if limits.max_key_bytes != flint_storage::DEFAULT_MAX_KEY_BYTES {
        eprintln!(
            "max-key-bytes: {} (structural ceiling {})",
            limits.max_key_bytes,
            flint_storage::MAX_KEY_BYTES
        );
    }

    // ADR-0005 D4: opt-in async write queue. For an opted-in namespace, a
    // batchable string/counter write enqueues and the connection blocks on
    // its ack (ack-after-apply); a single consumer commits each batch as one
    // engine WriteBatch (group commit) — trading ~2-3x write latency for far
    // fewer engine writes under a write-hot workload. rocks-only: the batch
    // commit needs RocksKv::apply_writes.
    //
    // The queue is ALWAYS constructed on a rocks node (an idle consumer
    // costs one parked thread) so the opt-in can arrive two ways:
    //   - `--async-writes ns1,ns2|all` — static node-level scope
    //     (self-hosters, drills);
    //   - the FLINTNS 'a' handshake flag — per-connection, sent by the
    //     proxy for tenants whose CP 'a' flag is set (CPTENANTASYNC).
    #[allow(unused_variables)]
    let write_queue: Option<Arc<write_queue::WriteQueue>> = {
        #[cfg(feature = "rocks")]
        {
            match (arg("--async-writes"), rocks.clone()) {
                (spec, Some(rk)) => {
                    let cap = arg("--async-queue-cap")
                        .and_then(|v| v.parse::<usize>().ok())
                        .unwrap_or(write_queue::DEFAULT_QUEUE_CAP);
                    let scope = match &spec {
                        Some(s) => {
                            eprintln!("async-writes ENABLED (opt-in write queue): {s} (cap {cap})");
                            write_queue::AsyncScope::parse(s)
                        }
                        // No static scope: the queue serves only
                        // handshake-flagged connections.
                        None => write_queue::AsyncScope::Only(Default::default()),
                    };
                    Some(write_queue::WriteQueue::start(
                        scope,
                        cap,
                        Arc::clone(&store),
                        rk,
                        flint_storage::strings::system_clock,
                        limits,
                    ))
                }
                (Some(_), None) => {
                    eprintln!("--async-writes requires --engine rocks");
                    std::process::exit(2);
                }
                (None, None) => None,
            }
        }
        #[cfg(not(feature = "rocks"))]
        {
            if arg("--async-writes").is_some() {
                eprintln!("--async-writes requires a rocks build");
                std::process::exit(2);
            }
            None
        }
    };

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
    // Loopback by default — a node serves the internal mesh, and on the
    // single-host fleets that have existed until now that is both correct and
    // the safer default. `--bind` is what makes a node reachable from ANOTHER
    // machine: without it a pair split across two hosts is unreachable in
    // both directions, however correctly it was placed there. The proxy has
    // carried the same flag since the marketplace needed external clients.
    let bind = arg("--bind").unwrap_or_else(|| "127.0.0.1".into());
    let listener = TcpListener::bind((bind.as_str(), port))?;
    eprintln!(
        "flint-server listening on {bind}:{port} ({})",
        if internal_reload.is_some() {
            "internal mTLS"
        } else {
            "plaintext"
        }
    );
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        // B1: shed over the connection cap (drop = reset; the peer backs
        // off). Reserve the slot before spawning so the count is accurate.
        if ACTIVE_CONNS.fetch_add(1, Ordering::Relaxed) >= MAX_CONNS.load(Ordering::Relaxed) {
            ACTIVE_CONNS.fetch_sub(1, Ordering::Relaxed);
            CONNS_SHED.fetch_add(1, Ordering::Relaxed);
            drop(stream);
            continue;
        }
        let store: Arc<dyn Kv> = Arc::clone(&store);
        // Option<Arc<RocksKv>> with the feature; Option<()> (Copy) without.
        #[allow(clippy::clone_on_copy)]
        let rocks = rocks.clone();
        let hub = Arc::clone(&hub);
        let read_only = Arc::clone(&read_only);
        let tailer_stop = Arc::clone(&tailer_stop);
        let lease_deadline = Arc::clone(&lease_deadline);
        let migration_active = Arc::clone(&migration_active);
        let write_queue = write_queue.clone();
        // Snapshot the CURRENT leaf per connection — a hot-reload between
        // connections is picked up here (ADR-0006 D4).
        let internal_tls = internal_reload.as_ref().and_then(|r| r.current());
        std::thread::spawn(move || {
            let _conn_guard = ConnGuard;
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
                write_queue.as_ref(),
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
    write_queue: Option<&Arc<write_queue::WriteQueue>>,
) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut chunk = [0u8; 16 * 1024];
    let mut out: Vec<u8> = Vec::with_capacity(4 * 1024);
    // Connection-scoped namespace (FLINTNS): the tenant boundary.
    let mut conn_ns: Vec<u8> = commands::DEFAULT_NS.to_vec();
    // Connection-scoped async-write opt-in (FLINTNS 'a' flag, D4).
    let mut conn_async = false;
    // Connection-scoped RESP dialect. Starts at RESP2, so every existing
    // caller — flintctl, the controller, the agent, a bare redis-cli — is
    // untouched until something sends HELLO 3.
    let mut conn_proto = flint_resp::Proto::default();
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
                    write_queue,
                    &mut conn_ns,
                    &mut conn_async,
                    &mut conn_proto,
                    &args,
                );
                encode_proto(&reply, conn_proto, &mut out);
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
                        return migrate::flintmigrateout(stream, rocks, &args);
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
                        write_queue,
                        &mut conn_ns,
                        &mut conn_async,
                        &mut conn_proto,
                        &args,
                    );
                    encode_proto(&reply, conn_proto, &mut out);
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
    write_queue: Option<&Arc<write_queue::WriteQueue>>,
    conn_ns: &mut Vec<u8>,
    conn_async: &mut bool,
    conn_proto: &mut flint_resp::Proto,
    args: &[Vec<u8>],
) -> Value {
    // HELLO: protocol negotiation, answered here rather than in the
    // command table because it MUTATES this connection's dialect.
    //
    // The node's data port is internal (the proxy is what faces tenants),
    // so the AUTH clause carries no credentials worth checking here — but
    // the proxy speaks HELLO 3 to us to get typed replies it can downgrade
    // for whichever dialect its own client negotiated.
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"HELLO"))
    {
        let req = match flint_resp::parse_hello(args) {
            Ok(r) => r,
            Err(e) => return e.reply(),
        };
        if let Some(p) = req.proto {
            *conn_proto = p;
        }
        let role = if read_only.load(Ordering::Relaxed) {
            "replica"
        } else {
            "master"
        };
        return flint_resp::hello_reply(*conn_proto, env!("CARGO_PKG_VERSION"), role);
    }
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
            return Value::Error("ERR FLINTNS <namespace> [a]".into());
        };
        let ok_byte = |b: &u8| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.');
        if ns.is_empty() || ns.len() > 64 || !ns.iter().all(ok_byte) {
            return Value::Error("ERR invalid namespace (1..=64 chars of [A-Za-z0-9._-])".into());
        }
        // Optional 'a' (D4): the proxy pins this connection for a tenant
        // whose CP flag opts batchable writes into the async write queue.
        match args.get(2) {
            None => *conn_async = false,
            Some(f) if f.eq_ignore_ascii_case(b"a") => *conn_async = true,
            Some(_) => return Value::Error("ERR FLINTNS <namespace> [a]".into()),
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
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTCONFIG"))
    {
        // FLINTCONFIG [<key> <value>]: hot-reload a runtime tunable with no
        // restart. No args -> dump current values. Valid on any node (admin
        // op, not a tenant write); the values are read live on the hot
        // paths, so a change takes effect on the next write/tick/accept.
        let field = |k: &str, v: String| format!("{k}:{v}");
        if args.len() == 1 {
            let dump = [
                field(
                    "wal-fsync-ms",
                    WAL_FSYNC_MS.load(Ordering::Relaxed).to_string(),
                ),
                field("lag-soft-ms", hub.lag_soft_ms().to_string()),
                field("lag-hard-ms", hub.lag_hard_ms().to_string()),
                field(
                    "min-replicas-to-write",
                    hub.min_replicas_to_write().to_string(),
                ),
                field("max-conns", MAX_CONNS.load(Ordering::Relaxed).to_string()),
                field(
                    "migrate-rate-bytes",
                    MIGRATE_RATE_BYTES.load(Ordering::Relaxed).to_string(),
                ),
            ]
            .join("\r\n");
            return Value::Bulk(Some(dump.into_bytes()));
        }
        let (Some(key), Some(val)) = (
            args.get(1).map(|k| k.to_ascii_lowercase()),
            args.get(2)
                .and_then(|v| std::str::from_utf8(v).ok())
                .map(str::to_string),
        ) else {
            return Value::Error("ERR usage: FLINTCONFIG [<key> <value>]".into());
        };
        macro_rules! parse {
            () => {
                match val.parse() {
                    Ok(v) => v,
                    Err(_) => return Value::Error(format!("ERR bad value {val:?}")),
                }
            };
        }
        match key.as_slice() {
            b"wal-fsync-ms" => WAL_FSYNC_MS.store(parse!(), Ordering::Relaxed),
            b"lag-soft-ms" => hub.set_lag_soft_ms(parse!()),
            b"lag-hard-ms" => hub.set_lag_hard_ms(parse!()),
            b"min-replicas-to-write" => hub.set_min_replicas_to_write(parse!()),
            b"max-conns" => MAX_CONNS.store(std::cmp::max(1, parse!()), Ordering::Relaxed),
            b"migrate-rate-bytes" => MIGRATE_RATE_BYTES.store(parse!(), Ordering::Relaxed),
            other => {
                return Value::Error(format!(
                    "ERR unknown or restart-only config key {:?} (hot: wal-fsync-ms, \
                     lag-soft-ms, lag-hard-ms, min-replicas-to-write, max-conns, \
                     migrate-rate-bytes)",
                    String::from_utf8_lossy(other)
                ));
            }
        }
        return Value::Simple("OK".into());
    }
    let ro = read_only.load(Ordering::Relaxed);
    let is_write = args
        .first()
        .is_some_and(|name| commands::is_write_command(name));
    if ro && is_write {
        return Value::Error("READONLY You can't write against a read only replica.".into());
    }
    // Disk headroom. Space-REDUCING writes stay allowed, because deleting is
    // the only way out and blocking it makes the condition self-sustaining;
    // reads are untouched. Same classifier the proxy uses for the per-tenant
    // quota verdict, so the two planes cannot disagree about what frees
    // space.
    if is_write
        && DISK.shedding()
        && !args
            .first()
            .is_some_and(|n| flint_commands::reduces_space(n))
    {
        return Value::Error(diskguard::DISK_FULL_ERROR.into());
    }
    // R1: a replica self-fences READS once it has lost live contact with the
    // master for longer than the staleness bound. Admin/FLINT* commands are
    // exempt (they are diagnostics, not tenant reads); a fresh replica that
    // has never heard from the master (contact 0) also fences.
    if ro
        && args
            .first()
            .is_some_and(|name| flint_commands::is_read_command(name))
    {
        let contact = REPLICA_CONTACT_MS.load(Ordering::Relaxed);
        let now = flint_storage::strings::system_clock();
        let stale = REPLICA_STALE_MS.load(Ordering::Relaxed);
        if contact == 0 || now.saturating_sub(contact) > stale {
            return Value::Error(
                "TRYAGAIN replica out of sync (stale reads fenced); retry — the proxy will route to the master".into(),
            );
        }
    }
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTINFO"))
    {
        return flintinfo(ro, rocks, hub, write_queue.map(|q| q.depth()));
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
        return migrate::flintmigratein(rocks, migration_active, args);
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
        return migrate::flintslotmoved(rocks, migration_active, args);
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
        return migrate::flintslotfreeze(rocks, migration_active, args);
    }
    // FLINTMIGRATIONS: list this node's IN-FLIGHT migration records (one
    // "slot phase peer" bulk per line), so a recovering controller can
    // observe and reconcile moves interrupted by a restart. Terminal Moved
    // overrides are ownership state, not in-flight, and are excluded.
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTMIGRATIONS"))
    {
        return migrate::flintmigrations(rocks);
    }
    // FLINTSNAPSHOT <root>: durable off-node snapshot — a consistent RocksDB
    // checkpoint written under <root>/<id>/ with <root>/LATEST atomically
    // repointed. The manifest CF travels inside the checkpoint, so a restore
    // knows the lineage (role epoch) it came from. <root> is any mounted
    // path; S3 is a mount/sync integration on top of the same layout.
    // FLINTNSBYTES <ns>: approximate resident bytes for one namespace (the
    // engine's SST range estimator — O(file metadata), no keyspace walk).
    // The storage-metering input (M5): the agent sweeps this per tenant.
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTNSBYTES"))
    {
        let Some(ns) = args.get(1) else {
            return Value::Error("ERR FLINTNSBYTES <namespace>".into());
        };
        #[cfg(feature = "rocks")]
        if let Some(kv) = rocks {
            return Value::Integer(kv.ns_bytes(ns) as i64);
        }
        let _ = ns;
        return Value::Error("ERR FLINTNSBYTES requires the rocks engine".into());
    }
    // FLINTCOMPACT <ns>: compact the namespace's ranges so deletions become
    // visible to FLINTNSBYTES now instead of at the next background
    // compaction — the metering loop's self-clear path in compressed time.
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTCOMPACT"))
    {
        let Some(ns) = args.get(1) else {
            return Value::Error("ERR FLINTCOMPACT <namespace>".into());
        };
        #[cfg(feature = "rocks")]
        if let Some(kv) = rocks {
            kv.compact_ns(ns);
            return Value::Simple("OK".into());
        }
        let _ = ns;
        return Value::Error("ERR FLINTCOMPACT requires the rocks engine".into());
    }
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
        return migrate::flintslotstats(store);
    }
    // FLINTSLOTHEAT: per-slot cumulative op counts — the traffic-balance
    // signal (ops/sec = delta between two scrapes / elapsed). First bulk is a
    // header "uptime_ms <ms>"; the rest are "slot ops" for non-empty slots.
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTSLOTHEAT"))
    {
        let mut rows = vec![Value::Bulk(Some(
            format!("uptime_ms {}", heat::uptime_ms()).into_bytes(),
        ))];
        rows.extend(
            heat::snapshot()
                .into_iter()
                .map(|(slot, ops)| Value::Bulk(Some(format!("{slot} {ops}").into_bytes()))),
        );
        return Value::Array(Some(rows));
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
        return migrate::flintslotabort(rocks, args);
    }
    // Per-slot gate: after a migration, a command for a key in a slot this
    // node no longer owns is redirected with -MOVED; a write to a slot frozen
    // mid-cutover is shed with -TRYAGAIN. Guarded by `migration_active` so
    // ordinary traffic (no overrides) never pays the extra manifest read.
    if migration_active.load(Ordering::Relaxed)
        && let Some(reply) = migrate::check_slot_gate(rocks, conn_ns, args, is_write)
    {
        return reply;
    }
    // Per-slot heat: count every keyed op destined for a slot this node owns
    // (the slot gate above already redirected/shed the ones it doesn't). Done
    // before the throttle/queue branches diverge so async-queued writes are
    // counted too. Cheap (a CRC16 + relaxed add); FLINTSLOTHEAT exposes it for
    // the traffic-balance policy.
    if let Some(k) = commands::command_key(args) {
        heat::record_key(k);
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
            Some(lag) if lag >= hub.lag_hard_ms() => {
                return Value::Error(
                    "THROTTLED replication lag exceeds limit, retry with backoff".into(),
                );
            }
            Some(lag) if lag >= hub.lag_soft_ms() => {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            _ => {}
        }
    }
    // ADR-0005 D4: for an opted-in namespace, route a batchable string/counter
    // write through the async queue — the connection blocks on the consumer's
    // ack-after-apply (one group-committed engine WriteBatch per drained
    // batch). Placed AFTER the quorum/lag gates (a queued write is still shed
    // by them) and the slot gate; reads, non-batchable writes, and
    // non-opted-in namespaces fall through to inline dispatch below. Reached
    // only on the master (is_write && !ro).
    if is_write
        && let Some(q) = write_queue
        && (*conn_async || q.wants(conn_ns))
        && args
            .first()
            .is_some_and(|name| write_queue::is_batchable(name))
    {
        return q.submit(conn_ns.clone(), args.to_vec());
    }
    // Writer-writer exclusion (lost-update fix — see write_lock.rs): a
    // single-key inline write serializes with other writers of that key;
    // multi-key or keyless writes exclude every writer. Readers take no
    // lock. Acquired AFTER the queue branch (a queued submit blocks on the
    // consumer, which itself takes the global lock — holding a stripe
    // across that wait would deadlock).
    let _write_guard = if is_write && !ro {
        let name = args
            .first()
            .map(|n| n.to_ascii_uppercase())
            .unwrap_or_default();
        let multi = (name == b"MSET" && args.len() > 3)
            || ((name == b"DEL" || name == b"UNLINK") && args.len() > 2);
        match (multi, commands::command_key(args)) {
            (false, Some(k)) => Some(write_lock::lock_key(conn_ns, k)),
            _ => Some(write_lock::lock_all()),
        }
    } else {
        None
    };
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
            // This node IS the lineage now. Any re-seed marker left by an
            // earlier demotion describes a position nobody follows any more,
            // and leaving it would make a later start-as-replica throw away
            // the very history everyone else is now descended from.
            clear_needs_reseed(kv.path());
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
            // Record the resync contract this command's own docstring states,
            // rather than trusting whichever tool restarts the seat to know
            // it. `flintctl roll-node` wipes; `flintctl start` did not, and a
            // demoted ex-master would then tail a new lineage carrying a
            // suffix the successor never saw.
            mark_needs_reseed(
                kv.path(),
                &format!(
                    "demoted to replica at role epoch {epoch}; the unreplicated suffix may have diverged"
                ),
            );
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
fn flintinfo(
    read_only: bool,
    rocks: &Option<RocksHandle>,
    hub: &Arc<ReplHub>,
    async_queue_depth: Option<usize>,
) -> Value {
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
    let (disk_free, disk_total, disk_unknown) = DISK.snapshot();
    let info = format!(
        "role:{}\r\nrole_epoch:{role_epoch}\r\nbuild:{build}\r\nsst_bytes:{sst}\r\nlatest_seq:{latest}\r\nlast_applied:{last_applied}\r\nacked_seq:{}\r\nseq_lag:{seq_lag}\r\nlive_replicas:{}\r\nlag_ms:{}\r\nlag_soft_ms:{soft}\r\nlag_hard_ms:{hard}\r\nmin_replicas_to_write:{minr}\r\nfullsync_active:{fsa}\r\nfullsync_max:{fsm}\r\nasync_write_queue:{aqd}\r\nwal_fsync_ms:{wfm}\r\nwal_fsync_total:{wft}\r\ncert_days_remaining:{cdr}\r\nactive_conns:{ac}\r\nmax_conns:{mc}\r\nconns_shed_total:{cs}\r\nwrite_stopped:{wst}\r\ndelayed_write_rate:{dwr}\r\ndisk_free_bytes:{dfb}\r\ndisk_total_bytes:{dtb}\r\ndisk_free_pct:{dfp}\r\ndisk_verdict:{dv}\r\ndisk_unknown_samples:{dus}\r\n",
        if read_only { "replica" } else { "master" },
        hub.effective_acked(now)
            .map_or_else(|| "none".into(), |a| a.to_string()),
        hub.live_replica_count(now),
        hub.lag_ms(now)
            .map_or_else(|| "none".into(), |l| l.to_string()),
        soft = hub.lag_soft_ms(),
        hard = hub.lag_hard_ms(),
        minr = hub.min_replicas_to_write(),
        build = build_version(),
        sst = rocks.as_ref().map(|kv| kv.sst_bytes()).unwrap_or(0),
        fsa = FULLSYNC_ACTIVE.load(Ordering::Relaxed),
        fsm = MAX_FULLSYNC.load(Ordering::Relaxed),
        aqd = async_queue_depth.map_or_else(|| "off".into(), |d| d.to_string()),
        wfm = WAL_FSYNC_MS.load(Ordering::Relaxed),
        wft = rocks.as_ref().map(|kv| kv.wal_fsync_total()).unwrap_or(0),
        cdr = CERT_PATH
            .get()
            .and_then(|p| p.as_deref())
            .and_then(flint_tls::cert_days_remaining)
            .map_or_else(|| "none".into(), |d| d.to_string()),
        ac = ACTIVE_CONNS.load(Ordering::Relaxed),
        mc = MAX_CONNS.load(Ordering::Relaxed),
        cs = CONNS_SHED.load(Ordering::Relaxed),
        wst = rocks.as_ref().map(|kv| kv.write_stall().0).unwrap_or(0),
        dwr = rocks.as_ref().map(|kv| kv.write_stall().1).unwrap_or(0),
        dfb = disk_free,
        dtb = disk_total,
        dfp = disk_free
            .saturating_mul(100)
            .checked_div(disk_total)
            .map_or_else(|| "none".into(), |p| p.to_string()),
        dv = match DISK.current() {
            diskguard::Verdict::Ok => "ok",
            diskguard::Verdict::Shed => "shed",
        },
        dus = disk_unknown,
    );
    Value::Bulk(Some(info.into_bytes()))
}

#[cfg(not(feature = "rocks"))]
fn flintinfo(
    read_only: bool,
    _rocks: &Option<RocksHandle>,
    hub: &Arc<ReplHub>,
    _async_queue_depth: Option<usize>,
) -> Value {
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
    let mut last_keepalive = std::time::Instant::now();
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
                // Send a liveness keepalive (re-FLINTSYNC-OK) every ~500ms so
                // an idle replica can tell "caught up" from "cut off" (R1).
                Ok(_) => {
                    idle = true;
                    if last_keepalive.elapsed() >= std::time::Duration::from_millis(500) {
                        out.clear();
                        encode(&Value::Simple(format!("FLINTSYNC-OK {cursor}")), &mut out);
                        stream.write_all(&out)?;
                        last_keepalive = std::time::Instant::now();
                    }
                }
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
    // Admission control: bound concurrent full-syncs. Reserve a slot with a
    // single fetch_add and roll back if it put us over the cap; the guard
    // releases on any exit. Over the cap -> -THROTTLED (the replica retries
    // with backoff); its WAL tail, if any, is untouched.
    if FULLSYNC_ACTIVE.fetch_add(1, Ordering::Relaxed) >= MAX_FULLSYNC.load(Ordering::Relaxed) {
        FULLSYNC_ACTIVE.fetch_sub(1, Ordering::Relaxed);
        encode(
            &Value::Error("THROTTLED full-sync slots busy, retry with backoff".into()),
            &mut out,
        );
        return stream.write_all(&out);
    }
    let _slot = FullSyncGuard;
    eprintln!(
        "full sync starting ({}/{} slots in use)",
        FULLSYNC_ACTIVE.load(Ordering::Relaxed),
        MAX_FULLSYNC.load(Ordering::Relaxed)
    );
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

/// Replica side: connect, request the tail from our durable cursor, apply
/// batches atomically; reconnect with backoff on any error.
#[cfg(feature = "rocks")]
mod replica {
    use super::*;
    use flint_storage::repl::{ReplBatch, ReplError, ReplOp};

    /// How a tail attempt ended.
    ///
    /// The distinction is the whole point: everything a replica normally
    /// meets — the master restarting, a dropped link, a short read — is
    /// fixed by reconnecting from the durable cursor, and the loop must not
    /// give up on it. Exactly one condition is not, and treating it like the
    /// others is what left a node reconnecting once a second all night.
    enum TailError {
        Transient(std::io::Error),
        /// The bytes we are waiting for no longer exist on the master.
        ///
        /// It reaches us two ways, and both were being retried forever: the
        /// master can say so outright (`-WALGAP`, when RocksDB reports the
        /// requested sequence as unavailable), or it can ship the oldest
        /// batch it still has, which then starts past our cursor and fails
        /// apply-side. Same condition, one hop apart.
        WalPurged(String),
    }

    /// Lets `tail_once` keep using `?` on ordinary I/O.
    impl From<std::io::Error> for TailError {
        fn from(e: std::io::Error) -> Self {
            Self::Transient(e)
        }
    }

    impl std::fmt::Display for TailError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Transient(e) => write!(f, "{e}"),
                Self::WalPurged(why) => write!(f, "{why}"),
            }
        }
    }

    pub fn run(target: &str, kv: &Arc<RocksKv>, stop: &Arc<AtomicBool>) {
        loop {
            if stop.load(Ordering::Relaxed) {
                eprintln!("tailer stopped (promoted)");
                return;
            }
            if let Err(e) = tail_once(target, kv, stop)
                && !stop.load(Ordering::Relaxed)
            {
                // Retrying a purged span cannot work: the next request asks
                // for the same missing sequence and fails identically,
                // forever. Observed on the playground — a replica looped
                // ~1/second all night at live_replicas 0 while the pair ran
                // unprotected, and only a manual wipe recovered it. So take
                // the remedy WalGap already prescribes: re-seed.
                if let TailError::WalPurged(why) = &e {
                    eprintln!(
                        "FATAL: {why} — this link can never resume. Marking for re-seed and \
                         exiting; the next start will full-sync from a checkpoint."
                    );
                    // A marker + exit rather than an in-process re-seed: the
                    // DB handle is shared with the serving path, and tearing
                    // it down underneath live readers is a much larger change
                    // than this failure warrants. Exiting is also the honest
                    // signal — under systemd (`Restart=on-failure`) the next
                    // start re-seeds unattended.
                    super::mark_needs_reseed(
                        kv.path(),
                        "replication cursor fell outside the master's retained WAL",
                    );
                    std::process::exit(3);
                }
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

    fn tail_once(target: &str, kv: &Arc<RocksKv>, stop: &Arc<AtomicBool>) -> Result<(), TailError> {
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
                    // Any frame from the master = live contact (R1).
                    super::REPLICA_CONTACT_MS
                        .store(flint_storage::strings::system_clock(), Ordering::Relaxed);
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
                        // The master diagnosing the purge itself. Retrying
                        // asks the same question and gets the same answer.
                        Value::Error(e) if e.starts_with("WALGAP") => {
                            return Err(TailError::WalPurged(e));
                        }
                        Value::Error(e) => {
                            return Err(std::io::Error::other(format!("master error: {e}")).into());
                        }
                        other => {
                            let batch = parse_batch(other).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "malformed replication frame",
                                )
                            })?;
                            kv.apply_batch(&batch).map_err(|e| match e {
                                // A batch starting AFTER the sequence we need
                                // means the span between was purged from the
                                // master's WAL: this is WalGap arriving one
                                // hop late, because the master shipped what it
                                // still had instead of refusing outright.
                                ReplError::SequenceGap { expected, got } if got > expected => {
                                    TailError::WalPurged(format!(
                                        "master's oldest batch starts at {got}, past the {expected} \
                                         we still need"
                                    ))
                                }
                                // A batch starting BEFORE it is reordering or
                                // a lost frame — re-requesting fixes that.
                                other => TailError::Transient(std::io::Error::other(format!(
                                    "apply: {other:?}"
                                ))),
                            })?;
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
                            )
                            .into());
                        }
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                || e.kind() == std::io::ErrorKind::TimedOut =>
                        {
                            continue; // timeout tick: loop re-checks stop
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("replication protocol error: {e:?}"),
                    )
                    .into());
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
                        None,
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
