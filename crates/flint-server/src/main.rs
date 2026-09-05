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

use flint_resp::{Decoded, Value, decode, encode, encode_proto, encode_proto_flushing};
use flint_storage::{Kv, MemKv};

use crate::commands::Dispatcher;
use crate::repl_hub::ReplHub;

#[cfg(feature = "rocks")]
use flint_storage::rocks::RocksKv;

#[cfg(not(feature = "rocks"))]
type RocksHandle = ();
#[cfg(feature = "rocks")]
type RocksHandle = Arc<RocksKv>;

/// EVERY flag this binary accepts. An unrecognised argument is REFUSED
/// (BUG-0034) rather than silently ignored, because ignoring it means a
/// typo'd `--prot 7001` starts a node on the DEFAULT port 6380 and looks like
/// it worked.
///
/// This list is checked against the `arg()` call sites by
/// `accepted_flags::every_arg_call_site_is_listed` — a list maintained by
/// hand beside call sites added by hand drifts, and the drift is invisible
/// until a caller is refused a flag the binary really does read.
///
/// THE PREVIOUS ATTEMPT AT THIS BROKE THE SUITE AND WAS REVERTED THE SAME
/// HOUR. It enumerated the CALLEE correctly and never checked the CALLER:
/// `slot_map_drill.sh` passed `--advertise`, a PROXY flag flint-server has
/// never read, so rejection turned a silent no-op into `exit 2` and the drill
/// waited forever for a seat that would never bind. Before re-landing this,
/// all 142 flint-server invocations in `tools/` were enumerated and every
/// flag they pass is one this binary reads; `--advertise` now goes to the
/// proxy, which is the binary that reads it. An argument list has two ends
/// and enumerating one of them proves nothing about the other.
const ACCEPTED_FLAGS: &[&str] = &[
    "--async-queue-cap",
    "--async-writes",
    "--bind",
    "--data-dir",
    "--disk-min-free-bytes",
    "--disk-min-free-pct",
    "--disk-sample-ms",
    "--engine",
    "--evictable-ns",
    "--collection-read-budget-pct",
    "--collection-read-mode",
    "--fullsync-rate-bytes",
    "--internal-ca",
    "--internal-cert",
    "--internal-key",
    "--journal",
    "--lag-hard-ms",
    "--lag-soft-ms",
    "--lease-cp",
    "--lease-ttl-ms",
    "--max-conns",
    "--max-fullsync",
    "--max-key-bytes",
    "--max-value-bytes",
    "--migrate-rate-bytes",
    "--min-replicas-to-write",
    "--port",
    "--replica-of",
    "--replica-read-stale-ms",
    "--restore-from",
    "--rewind-snaps",
    "--wal-fsync-ms",
    "--wal-headroom-seq",
    "--wal-size-limit-mb",
    "--wal-ttl-seconds",
    "--widowed-grace-ms",
    "--write-deadline-ms",
    // Handled before this check, listed so the drift test sees a complete set.
    "--build-version",
    "--version",
    "--help",
];

/// Refuse an argument this binary does not read. Called AFTER the --help and
/// --version early exits so those keep working, and before anything binds so
/// a refusal costs nothing.
///
/// Only `--`-prefixed tokens are inspected. Every accepted flag takes a value
/// (they are all read through `arg()`, which returns the token after the
/// name), and no value this binary takes begins with `--`: they are ports,
/// paths, addresses, byte counts and engine names.
/// Leave NOW, without running anyone's static destructors.
///
/// `std::process::exit` skips Rust destructors but still runs libc `atexit` /
/// `__cxa_atexit` handlers, and RocksDB is C++ with statics registered there:
/// caches, env singletons, its background thread pool. Called from a thread
/// that is not the only one running, that teardown races every other live
/// thread — the serving path, the disk sampler, RocksDB's own compaction
/// threads — and one of them touching a freed static is a SIGSEGV.
///
/// That is BUG-0048: the replication tailer diagnosed an unclosable WAL gap,
/// wrote the re-seed marker, logged FATAL, and then segfaulted on the way out.
/// Correct in every respect except the exit code, which is the one thing a
/// supervisor reads. `expected exit 3, got 139`.
///
/// `_exit` bypasses the handlers entirely. Nothing here needs tidying: the
/// marker is already on disk and the DB is about to be resynced from a
/// checkpoint. Flush by hand first, because `_exit` will not — stderr is
/// unbuffered in Rust but stdout is block-buffered when it is a pipe, which
/// is exactly how it is captured in a drill and in systemd.
///
/// USE THIS ONLY FROM A PATH THAT MAY RUN WHILE THE DB IS OPEN. The five
/// startup exits (argument and engine validation) run on the main thread
/// before any store exists, and should keep `std::process::exit` — it flushes
/// properly and there is no teardown to race.
/// Gated with `rocks` because that is where the hazard lives, not to silence a
/// lint: the teardown being raced is RocksDB's C++ statics, and a `mem` build
/// has none, no `mod replica`, and no background compaction threads. If a
/// non-rocks path ever needs to leave from a live thread, widen this — and the
/// test below will already be failing, because the count will have moved.
#[cfg(feature = "rocks")]
fn hard_exit(code: i32) -> ! {
    use std::io::Write;
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
    #[cfg(unix)]
    unsafe {
        libc::_exit(code)
    }
    #[cfg(not(unix))]
    std::process::exit(code)
}

fn reject_unknown_flags() {
    for a in std::env::args().skip(1) {
        if a.starts_with("--") && !ACCEPTED_FLAGS.contains(&a.as_str()) {
            eprintln!("flint-server: unrecognised argument `{a}`");
            eprintln!("             run `flint-server --help` for the accepted set");
            std::process::exit(2);
        }
    }
}

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

/// One RESP request/response over the internal mesh — enough protocol for
/// the boot-time FLINTFENCE query below, nothing more.
#[cfg(feature = "rocks")]
fn internal_call_once(target: &str, args: &[&[u8]]) -> std::io::Result<Value> {
    use std::io::Read;
    let mut stream = internal_connect(target)?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    let mut out = Vec::new();
    encode(
        &Value::Array(Some(
            args.iter().map(|a| Value::Bulk(Some(a.to_vec()))).collect(),
        )),
        &mut out,
    );
    stream.write_all(&out)?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        if let Ok(Decoded::Complete(v, _)) = decode(&buf) {
            return Ok(v);
        }
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// A quarantined snapshot's coordinates plus the cursor it was disqualified
/// against: `unresumable-c<cursor>-snap-<ms>-seq<n>-e<g>.<c>`.
///
/// Names written before BUG-0071 carry no cursor and return `None` here, so
/// they stay quarantined exactly as they are — an upgrade re-admits nothing
/// retroactively.
#[cfg(feature = "rocks")]
fn parse_quarantined_candidate(name: &str) -> Option<(u64, u64, flint_storage::manifest::Epoch)> {
    let rest = name.strip_prefix(UNRESUMABLE_PREFIX)?;
    let (cur, snap) = rest.strip_prefix('c')?.split_once('-')?;
    let cursor: u64 = cur.parse().ok()?;
    let (seq, epoch) = parse_rewind_candidate(snap)?;
    Some((cursor, seq, epoch))
}

/// A rewind candidate parsed from a snapshot id:
/// `snap-<ms>-seq<N>-e<gen>.<counter>` (the epoch label FLINTSNAPSHOT adds
/// on masters). Unlabeled ids are not candidates — see the label's comment.
#[cfg(feature = "rocks")]
fn parse_rewind_candidate(name: &str) -> Option<(u64, flint_storage::manifest::Epoch)> {
    let mut parts = name.strip_prefix("snap-")?.splitn(3, '-');
    let _ms = parts.next()?;
    let seq: u64 = parts.next()?.strip_prefix("seq")?.parse().ok()?;
    let (g, c) = parts.next()?.strip_prefix('e')?.split_once('.')?;
    Some((
        seq,
        flint_storage::manifest::Epoch {
            generation: g.parse().ok()?,
            counter: c.parse().ok()?,
        },
    ))
}

/// Renamed onto a snapshot that can never again be resumed from. It survives
/// on disk for forensics but stops being a rewind candidate, because
/// `parse_rewind_candidate` only accepts names starting `snap-`.
///
/// Gated with `rocks` because `parse_rewind_candidate` is: the whole rewind
/// path only exists there, and an ungated helper that calls a gated one fails
/// the no-rocks build.
#[cfg(feature = "rocks")]
const UNRESUMABLE_PREFIX: &str = "unresumable-";

/// Disqualify every local snapshot that a purged WAL has put permanently out
/// of reach (BUG-0062).
///
/// WHY THIS EXISTS AT ALL. On `WalPurged` the tailer marks the copy for
/// re-seed and exits, and the next boot is supposed to full-sync. It did not:
/// the marker's text is what the boot keys off (`why.contains("promotion
/// fence")`), the purged-WAL marker does not say that, so the boot took the
/// rewind path again, re-picked the SAME snapshot, re-ran the same probe and
/// lost the same race. Soak 2026-08-27 cycle 2 shows
/// `snap-…-seq207196-e0.7` rewound to twice, fatal both times. A retry that
/// re-derives its inputs is not a retry — so the fix removes the candidate
/// rather than adding a second string to match on, which is the check that
/// let this through in the first place.
///
/// WHY `seq <= cursor` AND NOT JUST THE ONE THAT FAILED. A master's WAL only
/// advances, so a span it has already recycled never comes back: a snapshot
/// at `cursor` is unresumable *permanently*, not just now. And an OLDER
/// snapshot needs an even earlier batch, so it is unresumable too, by the
/// same retention bound. Quarantining only the snapshot that just failed
/// would leave the next boot to pick the next-older one, fail identically,
/// and repeat — converging only after one boot per snapshot, each burning a
/// reconvergence deadline. This run held three (seq 130042, 196517, 207196),
/// which is three deadlines to reach a re-seed it could have taken at once.
///
/// Renaming, not deleting: the bytes are usually still a perfectly good
/// snapshot of a past state, and they are evidence when someone asks why a
/// node re-seeded. What they can no longer be is a candidate.
///
/// Returns how many were quarantined.
#[cfg(feature = "rocks")]
fn quarantine_unresumable(snaps_dir: &str, cursor: u64) -> usize {
    let Ok(entries) = std::fs::read_dir(std::path::Path::new(snaps_dir)) else {
        eprintln!("quarantine: no snapshot dir at {snaps_dir}; nothing to disqualify");
        return 0;
    };
    let mut n = 0;
    for e in entries.flatten() {
        let name = e.file_name();
        // Already-quarantined names fail this parse, so a repeat run is a
        // no-op rather than `unresumable-unresumable-…`.
        let Some(name) = name.to_str() else { continue };
        let Some((seq, _epoch)) = parse_rewind_candidate(name) else {
            continue;
        };
        if seq > cursor {
            continue;
        }
        // BUG-0071: record WHAT this was disqualified against. The premise --
        // "this master's WAL can no longer reach these sequences" -- is a fact
        // about one master at one moment, and a promotion invalidates it.
        // Without the cursor in the name there is nothing a later decision can
        // reconsider, so a snapshot removed for a purge stays removed for every
        // future fence, including the one that needed it: a 94.2 s full re-seed
        // at min-replicas-to-write=1.
        let to = e
            .path()
            .with_file_name(format!("{UNRESUMABLE_PREFIX}c{cursor}-{name}"));
        match std::fs::rename(e.path(), &to) {
            Ok(()) => {
                n += 1;
                eprintln!(
                    "quarantine: {name} (seq {seq} <= unresumable cursor {cursor}) is no \
                     longer a rewind candidate; kept as \
                     {UNRESUMABLE_PREFIX}c{cursor}-{name}"
                );
            }
            // Best effort by design: failing to rename must not stop the
            // re-seed, which is the actual recovery. It costs another boot.
            Err(err) => eprintln!("quarantine: renaming {name}: {err}"),
        }
    }
    n
}

/// Ask `target` whether a copy at (`cursor`, `epoch`) can resume tailing its
/// lineage — the exact FLINTSYNC handshake the tailer would send, dropped
/// after the first reply. The master runs its full admission logic (fence
/// check, cursor translation, WAL-span retention) and answers before any
/// batch ships, so a refusal here is cheap where the same refusal at the
/// tailer's first attach is not: by then the boot has committed to the copy,
/// consumed the marker, and the tailer's WALGAP escalation EXITS a process
/// that, in a fleet, nothing restarts (soak run 34 cycle 6).
#[cfg(feature = "rocks")]
/// What a probe reply MEANS, which is not the same as whether it succeeded.
///
/// A master answering `-WALGAP` or a fence refusal has made a DECISION about
/// this copy, and repeating the question gets the same answer. A socket that
/// returned EAGAIN has decided nothing; the master never spoke. Collapsing
/// both into one `Err` is what cost soak 2026-08-27 cycle 3 ninety-five
/// seconds: the rewind had already selected a valid snapshot (seq 177753
/// under a fence of 203474), the probe hit `Resource temporarily unavailable
/// (os error 11)`, the caller read that as a refusal, deleted a good copy and
/// full re-seeded 93 files with the new master's write gates shut.
#[derive(Debug, PartialEq)]
enum ProbeVerdict {
    /// The master accepted the cursor.
    Resumable,
    /// The master answered, and said no. A verdict; do not retry it.
    Refused(String),
    /// The master did not answer. Says nothing about the copy.
    Transport(String),
}

/// Classify a probe reply. Split out from the I/O so the distinction that
/// matters can be asserted without a socket.
#[cfg(feature = "rocks")]
fn classify_probe(reply: std::io::Result<Value>) -> ProbeVerdict {
    match reply {
        Ok(Value::Simple(s)) if s.starts_with("FLINTSYNC-OK") => ProbeVerdict::Resumable,
        // The master spoke. Whatever it said, asking again will not change it.
        Ok(Value::Error(e)) => ProbeVerdict::Refused(e),
        // A reply we cannot parse is a protocol problem, not a hiccup. Treated
        // as a refusal deliberately: retrying a malformed exchange is how a
        // boot spins instead of falling back to the path that works.
        Ok(other) => ProbeVerdict::Refused(format!("unexpected reply {other:?}")),
        Err(e) => ProbeVerdict::Transport(e.to_string()),
    }
}

/// Ask `target` whether a copy at (`cursor`, `epoch`) can resume tailing its
/// lineage, RETRYING when the transport fails.
///
/// The retry is not politeness. Discarding a fence-vouched snapshot because a
/// socket blocked converts a bounded tail into a full re-seed, and on a
/// freshly promoted master with `--min-replicas-to-write 1` that re-seed is a
/// write outage for its whole duration. A refusal still returns immediately:
/// the master has decided, and the fallback is correct.
#[cfg(feature = "rocks")]
fn probe_resume(
    target: &str,
    cursor: u64,
    epoch: flint_storage::manifest::Epoch,
) -> Result<(), String> {
    const ATTEMPTS: u32 = 5;
    let mut last = String::new();
    for attempt in 1..=ATTEMPTS {
        let verdict = classify_probe(internal_call_once(
            target,
            &[
                b"FLINTSYNC",
                cursor.to_string().as_bytes(),
                epoch.generation.to_string().as_bytes(),
                epoch.counter.to_string().as_bytes(),
            ],
        ));
        match verdict {
            ProbeVerdict::Resumable => return Ok(()),
            ProbeVerdict::Refused(e) => return Err(format!("refused: {e}")),
            ProbeVerdict::Transport(e) => {
                last = e;
                if attempt < ATTEMPTS {
                    eprintln!(
                        "rewind: probe attempt {attempt}/{ATTEMPTS} to {target} did not \
                         complete ({last}) — the master has not answered, so this says \
                         nothing about the copy; retrying"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(200 * attempt as u64));
                }
            }
        }
    }
    Err(format!(
        "unreachable: {ATTEMPTS} probe attempts did not complete ({last})"
    ))
}

/// The rewind rejoin (#187): instead of discarding a superseded copy and
/// full re-seeding — a transfer that grows with the dataset while the new
/// master's write gates hold every write — replace the data dir with this
/// node's own newest LOCAL snapshot whose seq the master vouches for
/// (FLINTFENCE: at or before the first branch point after the snapshot's
/// epoch), and tail the difference. Catch-up is then bounded by the
/// snapshot cadence, not the dataset.
///
/// Returns true when the data dir now holds the restored snapshot. Any
/// failure returns false and the caller takes today's path: discard and
/// full-sync. The master re-checks the same fence at FLINTSYNC, so a stale
/// answer here (a promotion racing this boot) downgrades to a re-seed
/// rather than a divergent copy.
#[cfg(feature = "rocks")]
fn try_rewind(data_dir: &std::path::Path, snaps_dir: &str, target: &str) -> bool {
    use flint_storage::manifest::Epoch;
    // SUB-PHASE TIMING. The decision window -- process up until a catch-up path
    // is chosen -- measured at 7.6 s of a 10.9 s budget-breaching outage, 70%
    // of it, on a fence gap of only 8,988 sequences. So the cost is not
    // replaying data, and the window contains four candidates: enumerating
    // snapshots, asking the master to vouch for each distinct epoch
    // (FLINTFENCE, one NETWORK round trip per epoch, to a master that is
    // simultaneously refusing writes), restoring the chosen copy, and probing
    // that the restore can actually resume. Carried in the RejoinDecided row's
    // cause rather than as new event kinds -- four more kinds is what just
    // broke fleet_journal's "a healthy pair's journal is quiet" invariant.
    let t_start = std::time::Instant::now();
    let mut t_fence_done: Option<std::time::Instant> = None;
    let mut t_restore_done: Option<std::time::Instant> = None;
    let snaps = std::path::Path::new(snaps_dir);
    let Ok(entries) = std::fs::read_dir(snaps) else {
        eprintln!("rewind: no snapshot dir at {snaps_dir}; full re-seed");
        return false;
    };
    // The third element is the cursor a quarantined snapshot was disqualified
    // against, or None for an ordinary candidate. Quarantined ones are carried
    // here rather than filtered out so the fence can RECONSIDER them; the
    // condition that permits it is applied below, not here.
    let mut candidates: Vec<(u64, Epoch, Option<u64>, std::path::PathBuf)> = entries
        .flatten()
        .filter_map(|e| {
            let name = e.file_name();
            let name = name.to_str()?;
            if let Some((cursor, seq, epoch)) = parse_quarantined_candidate(name) {
                return Some((seq, epoch, Some(cursor), e.path()));
            }
            let (seq, epoch) = parse_rewind_candidate(name)?;
            Some((seq, epoch, None, e.path()))
        })
        .collect();
    candidates.sort_by_key(|c| std::cmp::Reverse(c.0));
    // Declared here, not above: the two early returns between the entry and
    // this point (no snapshot dir, no epoch-labeled snapshots) leave without
    // reading it, and a pre-initialised value would be dead.
    let t_enum = std::time::Instant::now();
    if candidates.is_empty() {
        eprintln!("rewind: no epoch-labeled snapshots in {snaps_dir}; full re-seed");
        return false;
    }
    // One fence query per distinct epoch; the master's answer is the highest
    // seq it vouches for from that epoch's timeline (nil = it cannot).
    let mut bounds: std::collections::BTreeMap<Epoch, Option<u64>> = Default::default();
    for (seq, epoch, quarantined_at, path) in candidates {
        let bound = *bounds.entry(epoch).or_insert_with(|| {
            match internal_call_once(
                target,
                &[
                    b"FLINTFENCE",
                    epoch.generation.to_string().as_bytes(),
                    epoch.counter.to_string().as_bytes(),
                ],
            ) {
                Ok(Value::Integer(b)) if b >= 0 => Some(b as u64),
                Ok(_) => {
                    eprintln!("rewind: {target} cannot vouch for epoch {epoch}");
                    None
                }
                Err(e) => {
                    eprintln!("rewind: FLINTFENCE {epoch} against {target} failed: {e}");
                    None
                }
            }
        });
        let Some(bound) = bound else { continue };
        // BUG-0071. Reconsider a quarantined snapshot only when the fence now
        // sits BELOW the cursor it was disqualified against: we are asking to
        // resume at a LOWER position than the one that failed, and against a
        // different lineage, since a fence row exists only because a promotion
        // recorded one.
        //
        // This terminates. If a re-admitted candidate is refused, the
        // re-quarantine runs at the restored copy's own cursor -- at or below
        // this snapshot's seq -- so trying it again would need `bound < seq`
        // while USING it needs `seq <= bound`. It cannot be tried twice against
        // one fence.
        //
        // The pure purge case is untouched: with no promotion there is no fence
        // row, promo_fence_bound answers Unfenced, and the candidate is skipped
        // above. The one-boot path to a re-seed still holds.
        if let Some(at) = quarantined_at
            && bound >= at
        {
            continue;
        }
        if seq > bound {
            eprintln!(
                "rewind: snapshot {} is past the fence ({seq} > {bound}); trying older",
                path.display()
            );
            continue;
        }
        // BUG-0071. The REJECTED path has always named its bound; the accepted
        // one named nothing, so a rejoin that cleared the fence and was then
        // refused downstream left no record of what it cleared. That is the
        // evidence the 2026-08-28 94.2 s re-seed turned out to need and not
        // have — the absence of a "past the fence" line was all that
        // distinguished "accepted" from "never asked".
        eprintln!(
            "rewind: candidate {} clears the fence for epoch {epoch} ({seq} <= {bound})",
            path.display()
        );
        // First candidate to clear the fence: every FLINTFENCE round trip for
        // this rejoin has now happened, including those for candidates that
        // were rejected as past the fence.
        t_fence_done.get_or_insert_with(std::time::Instant::now);
        // Build beside, then swap: a crash mid-restore must leave either the
        // old dir (marker intact -> retried next boot) or the finished copy,
        // never a half-restored dir that opens.
        let tmp = data_dir.with_extension("rewind-tmp");
        let _ = std::fs::remove_dir_all(&tmp);
        if let Err(e) = std::fs::create_dir_all(&tmp) {
            eprintln!("rewind: create {}: {e}; full re-seed", tmp.display());
            return false;
        }
        let restored = std::fs::read_dir(&path).map(|dir| {
            for entry in dir.flatten() {
                if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                    continue;
                }
                let dst = tmp.join(entry.file_name());
                // Hard links make the restore O(files), not O(bytes) — the
                // snapshot and the data dir share the statedir filesystem.
                // SSTs are immutable so sharing is safe; rocks rewrites the
                // mutable members (MANIFEST/CURRENT/OPTIONS/log) by replace,
                // and the copy fallback covers a cross-device snaps dir.
                if std::fs::hard_link(entry.path(), &dst).is_err()
                    && let Err(e) = std::fs::copy(entry.path(), &dst)
                {
                    return Err(e);
                }
            }
            Ok(())
        });
        match restored {
            Ok(Ok(())) => {}
            Ok(Err(e)) | Err(e) => {
                eprintln!("rewind: restoring {}: {e}; full re-seed", path.display());
                let _ = std::fs::remove_dir_all(&tmp);
                return false;
            }
        }
        if let Err(e) =
            std::fs::remove_dir_all(data_dir).and_then(|_| std::fs::rename(&tmp, data_dir))
        {
            eprintln!("rewind: swapping in the restored copy: {e}; full re-seed");
            let _ = std::fs::remove_dir_all(&tmp);
            return false;
        }
        // PROVE the restored copy opens before reporting success. Every
        // failure past this point in the boot is a `?` that EXITS — and in a
        // fleet nothing restarts a start-spawned seat, so a restore that
        // does not open is not a degraded rejoin, it is a seat that stays
        // DOWN until an operator notices (how soak run 28's bring-up died:
        // one unopenable state and the recovery check timed out at 180s).
        // Open-drop-open is safe — the lock releases on drop.
        //
        // And make the copy SAFE BY CONSTRUCTION while it is open: the
        // snapshot carries its taker's MASTER role row, and the swap above
        // consumed the marker. A kill landing between here and the boot's
        // own reassert would otherwise leave a master-role dir with no
        // marker — whose next start says "durable role is master; ignoring
        // --replica-of", gets fenced by the controller as a zombie, and
        // then sits as a demoted, never-tailing replica: a pair that is
        // "fully staffed" and SINGLE-COPY at once. Reasserting replica
        // identity (and the cursor) here closes the window: any boot of
        // this dir, marker or not, is a replica that tails.
        let restored_cursor = match flint_storage::rocks::RocksKv::open(data_dir) {
            Ok(kv) => {
                use flint_storage::manifest::{self, Role, RoleClaim};
                let cursor = kv.latest_seq();
                if let Err(e) = kv.set_last_applied(cursor) {
                    eprintln!("rewind: cursor init on restored copy ({e:?}); full re-seed");
                    drop(kv);
                    let _ = std::fs::remove_dir_all(data_dir);
                    return false;
                }
                manifest::force_role(
                    &kv,
                    RoleClaim {
                        role: Role::Replica,
                        epoch,
                    },
                );
                drop(kv);
                cursor
            }
            Err(e) => {
                eprintln!("rewind: restored copy does not open ({e}); full re-seed");
                let _ = std::fs::remove_dir_all(data_dir);
                return false;
            }
        };
        // The fence vouched for the snapshot's PAST; nothing above vouched
        // for its FUTURE — the catch-up span from the snapshot to the
        // master's tip must still be in the master's WAL. A replica takes
        // no snapshots, so its newest labeled snapshot dates from its LAST
        // MASTERSHIP and ages without bound; soak run 34 cycle 6 rewound a
        // caught-up replica to a 15-minute-old snapshot whose catch-up span
        // the master had long recycled, and the seat died on the attach's
        // WALGAP where a probe here would have chosen the re-seed while it
        // was still cheap to choose.
        t_restore_done.get_or_insert_with(std::time::Instant::now);
        match probe_resume(target, restored_cursor, epoch) {
            Ok(()) => {}
            Err(e) => {
                // Says WHICH it was. This line used to read "refused" for
                // every failure, including a socket that never reached the
                // master — and that wording sent the 95s investigation at the
                // fence logic rather than at the transport.
                eprintln!(
                    "rewind: cannot resume from the restored copy against {target} ({e}); \
                     full re-seed"
                );
                let _ = std::fs::remove_dir_all(data_dir);
                return false;
            }
        }
        eprintln!(
            "rewound to {} (seq {seq} <= fence {bound}, epoch {epoch}): tailing incrementally \
             instead of a full re-seed",
            path.display()
        );
        // The journal, not only stderr. Seat logs are per-host, untimestamped
        // and discarded on a passing run; this row is what lets the window
        // between promotion and writes-resuming be split into boot, decision
        // and catch-up instead of being one silent 9.9 s.
        // The breakdown, in the row that already exists. A phase that did not
        // run reports 0 rather than being omitted, so a reader never has to
        // guess whether a missing term was fast or skipped.
        let ms = |a: std::time::Instant, b: std::time::Instant| b.duration_since(a).as_millis();
        let now = std::time::Instant::now();
        let fd = t_fence_done.unwrap_or(now);
        let rd = t_restore_done.unwrap_or(fd);
        journal_event(
            flint_journal::EventKind::RejoinDecided,
            Some(epoch.to_string()),
            &format!(
                "rewound to seq {seq} (fence {bound}): tailing incrementally \
                 [enumerate={}ms fence={}ms restore={}ms probe={}ms]",
                ms(t_start, t_enum),
                ms(t_enum, fd),
                ms(fd, rd),
                ms(rd, now),
            ),
        );
        return true;
    }
    eprintln!("rewind: no snapshot at or before the fence; full re-seed");
    journal_event(
        flint_journal::EventKind::RejoinDecided,
        None,
        "no snapshot at or before the fence: full re-seed",
    );
    false
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

/// Startup state (#176). Set from process start until the real accept loop
/// takes the listener over; while it holds, the port is OPEN and answers, but
/// there is no store behind it yet.
///
/// This is Redis's `LOADING`, and it is deliberately the same word on the
/// wire: `-LOADING` is a documented reply every mainstream client library
/// already retries rather than treating as a hard failure.
static LOADING: AtomicBool = AtomicBool::new(true);
/// The error data commands get while [`LOADING`] holds. The prefix is the
/// contract — Redis's own text after it, so a client matching on the message
/// as well as the code still recognises it.
const LOADING_ERR: &str = "LOADING Flint is loading the dataset in memory";

/// Where this replica tails from, and a pending request to change it.
///
/// BUG-0076: a replica learned its master exactly ONCE, from `--replica-of`
/// at startup, so when a promotion moved the role the survivor that was
/// neither killed nor promoted went on dialling a dead address forever. On a
/// two-member pair that never showed, because the only other member is the
/// one `restart-node` brings back with a freshly computed master.
///
/// A process static for the same reason `INTERNAL_CLIENT` is one: the command
/// handler and the tailer thread are six call frames apart, and the value is
/// per-process by nature. Widening every signature between them would say
/// less about the design than this comment does.
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
struct ReplicaLink {
    /// The address the tailer dials. Read at the top of every reconnect.
    target: std::sync::Mutex<String>,
    /// Set by FLINTFOLLOW so an in-flight tail drops promptly, exactly the way
    /// the stop flag does; cleared by the tailer when it re-reads the target.
    repoint: AtomicBool,
}
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
static REPLICA_LINK: std::sync::OnceLock<Arc<ReplicaLink>> = std::sync::OnceLock::new();

/// The WAL archive budget this node opened with, in MB.
///
/// Written where the engine is opened and read where the shed gate is
/// configured, because the two are hundreds of lines apart and MUST agree:
/// the gate exists to stop a replica reaching a segment this budget has
/// already deleted, and a gate calibrated against a different number than
/// the one RocksDB prunes on is the whole of BUG-0079.
/// The literal mirrors `flint_storage::rocks::DEFAULT_WAL_SIZE_LIMIT_MB`
/// rather than naming it: that module only exists under the `rocks` feature,
/// and a static initialiser cannot be feature-gated where this one is read.
/// It is only ever a placeholder — the value is overwritten where the engine
/// opens, which is the only build that has an archive at all.
/// True when the headroom threshold was DERIVED rather than pinned with
/// --wal-headroom-seq. Only then may the sampler re-derive it: an operator who
/// names a number has overridden the policy, not asked for a starting point.
static HEADROOM_AUTO: AtomicBool = AtomicBool::new(false);
static WAL_BUDGET_MB: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(8_192);
/// HOW the archive budget above was chosen — BUG-0079's remaining half.
///
/// The budget was only ever announced in one boot log line that nothing reads.
/// That is how a 3.5 TB seat came up clamped to the 1 GiB FLOOR, on a healthy
/// disk, differently on each boot, and passed a green gate: the number was
/// correct-looking in isolation and wrong only against the volume it was
/// supposed to describe.
///
/// The value alone cannot say that. 1024 MB is the right answer on a 4 GB dev
/// box and a 256x under-provisioning on an NVMe, and an operator who pinned it
/// is not a defect at all. So the PROVENANCE ships beside the number, and the
/// two together state a checkable invariant: a seat that says `measured` must
/// have a budget consistent with the volume it is currently sampling.
/// ROCKS-ONLY, because its only reader is: `flintinfo` is itself
/// `#[cfg(feature = "rocks")]`, and the mem engine has no WAL archive to
/// describe. Gating matches the reader rather than silencing the lint,
/// which would leave a mem build carrying a budget nothing can report.
#[cfg(feature = "rocks")]
static WAL_BUDGET_SRC: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
/// No archive budget was derived at all — the mem engine, which has no WAL
/// archive. Distinct from every other value here: reporting the 8192 default
/// as though it had been chosen would be a fabricated measurement.
#[cfg(feature = "rocks")]
const WAL_SRC_NONE: u8 = 0;
/// Derived from a volume this node successfully measured.
#[cfg(feature = "rocks")]
const WAL_SRC_MEASURED: u8 = 1;
/// Pinned by `--wal-size-limit-mb`. Never a defect, and the reason the
/// invariant is conditional rather than absolute.
#[cfg(feature = "rocks")]
const WAL_SRC_OVERRIDE: u8 = 2;
/// No filesystem could be measured, so the compiled default was used. Says "I
/// could not look" rather than letting a failed syscall pass for a small disk.
#[cfg(feature = "rocks")]
const WAL_SRC_UNMEASURED: u8 = 3;

#[cfg(feature = "rocks")]
fn wal_budget_src_str() -> &'static str {
    match WAL_BUDGET_SRC.load(Ordering::Relaxed) {
        WAL_SRC_MEASURED => "measured",
        WAL_SRC_OVERRIDE => "override",
        WAL_SRC_UNMEASURED => "unmeasured",
        WAL_SRC_NONE => "none",
        // A u8 holds values this set does not, and nothing stores them. If one
        // ever appears, say so rather than folding it into `none` -- an
        // unrecognised state reported as "no archive" is the collapse of an
        // unknown into a fact, which is the defect this whole field exists to
        // undo.
        _ => "unknown",
    }
}

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
/// Whether capacity reclaim is currently engaged, and the free-bytes target it
/// is working toward (0 when idle).
///
/// REPORTED BEFORE IT IS ACTED ON, deliberately. This is a feature whose whole
/// job is to delete data, so the shipping order is: decide, publish the
/// decision, let an operator watch it decide on real traffic, and only then let
/// it act. An eviction trigger that starts life invisible is one whose first
/// observable behaviour is missing data.
static RECLAIM_ACTIVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static RECLAIM_TARGET_FREE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

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

/// Cap on how fast this node SERVES a full sync (bytes/sec; 0 = unlimited),
/// hot-reloadable via FLINTCONFIG. Rocks-only.
///
/// Unlike the migration cap this defaults to a REAL number, because the
/// default is the case that hurts. A re-seeding replica pulls its checkpoint
/// from the pair's master, and after a failover that master was promoted
/// seconds ago and is carrying the pair's whole write load by itself. Soak
/// run 23 measured the consequence with no cap: promotion finished 700 ms
/// after the kill, then the write path stalled 11.9 s while a 104-file
/// checkpoint streamed out — a client waiting 11.9 s for a write, against a
/// published 10 s budget, for a failover that was over in under a second
/// (#184; #177's 43.6 s and #181's 11.5 s are the same mechanism at other
/// dataset sizes).
///
/// 64 MiB/s is a FIRST CALIBRATION, not a measured optimum: it is an order of
/// magnitude above the foreground demand those runs put on a pair (~5 MB/s)
/// while still re-seeding a 5 GB checkpoint in ~80 s. The trade it encodes is
/// deliberate — a slower re-seed means longer at RF=1, so this buys write
/// latency with redundancy-restore time, and both are bounded and observable.
/// Operators who would rather have redundancy back sooner set it higher or to
/// 0; the number wants revisiting against a measured p99 curve.
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
static FULLSYNC_RATE_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(DEFAULT_FULLSYNC_RATE_BYTES);

/// See `FULLSYNC_RATE_BYTES`. 64 MiB/s.
const DEFAULT_FULLSYNC_RATE_BYTES: u64 = 64 * 1024 * 1024;

/// Writes executing on this node right now, and an EWMA of how long recent
/// ones took (microseconds). Together they ESTIMATE the wait a write arriving
/// now would face — Little's law, `wait ~= inflight x service` — which is the
/// only thing that can decide at ADMISSION whether the deadline is reachable
/// (#186).
///
/// Deciding at admission is the point. Queue-then-timeout spends the whole
/// deadline of capacity on work it then discards, which is the worst of both:
/// the client waits AND the node is busy not serving anyone else.
static WRITE_INFLIGHT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static WRITE_SERVICE_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Per-write MARGINAL cost, which is what a projection may multiply by.
///
/// `WRITE_SERVICE_US` above is RESIDENCY: `WriteInFlight` is entered when a
/// write is staged into a batch and dropped when that batch COMMITS, so every
/// write in a batch records the whole commit. They commit together, so
/// multiplying residency by the number in flight charges the same wall clock
/// once per write -- for a full 256-write batch, 256 times over.
///
/// That is BUG-0088. A batch of 65 committing in 31ms reported
/// `65 x 30942us = 2011ms` against a 2000ms deadline, for work that took 31ms,
/// and shed one write in 101000. The deadline was never the problem: raising
/// it would have moved a wrong number past a threshold instead of correcting
/// it.
///
/// Little's Law is the correction. With L in flight and residency W, the
/// throughput is L/W, so the wait a new write can expect is `L / (L/W)` = W,
/// not `L x W`. Dividing each residency sample by the number that shared it
/// gives the marginal cost, and `inflight x cost` then lands back on W when
/// the queue is stable and grows only when inflight outruns throughput --
/// which is the thing a deadline should shed on.
static WRITE_COST_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Writes refused because their estimated wait exceeded the deadline. Exposed
/// by FLINTINFO so the shed is observable rather than inferred — the lag cap
/// shipped unexercised for months precisely because nothing counted it (#121).
static WRITES_SHED_DEADLINE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// HIGH-WATER MARK of the projected write wait, in ms (BUG-0088).
///
/// `write_wait_est_ms` is the estimate RIGHT NOW, which once a load stops is
/// ~0 — so a run that came within one millisecond of refusing a write and a
/// run that never passed 10 ms report an identical INFO afterwards. The shed
/// COUNTER has the mirror-image problem: it moves only once the deadline has
/// already been crossed, so every sample it gives is above the line by
/// construction and the distribution below it is invisible. Two gate failures
/// at 2017 ms and 2033 ms against a 2000 ms deadline could therefore say
/// nothing about whether passing runs sit at 200 ms or at 1999 ms, which are
/// very different bugs.
///
/// Recorded whenever the estimate is non-zero. `estimated_write_wait_ms` is
/// `inflight x service_us / 1000`, so an idle or lightly loaded path yields 0
/// by integer division and takes no atomic write at all — the common case
/// costs one compare. Everything with anything to see is kept at full
/// resolution, which matters because the first draft floored this at HALF the
/// deadline and a healthy run then reported a flat 0: no gradient, so no trend,
/// so no distribution to compare a future outlier against. That defeats the
/// purpose. A zero here means "no write ever projected a measurable wait" — a
/// measurement, not an absence.
static WRITE_WAIT_PEAK_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The peak, PACKED WITH THE TWO TERMS THAT PRODUCED IT.
///
/// `est_ms` is `inflight x cost_us / 1000`, and the factors fail for opposite
/// reasons: a high inflight is more offered load than the seat can take, a
/// high cost is the seat itself having got slow.
///
/// TWO CORRECTIONS FROM BUG-0088, both of which this comment previously got
/// wrong. The third term was RESIDENCY, and multiplying it by inflight charged
/// one batch commit once per member -- 256 times over at a full batch. It is
/// now the marginal cost (see `WRITE_COST_US`). And `inflight` was read here as
/// offered load, which is why "four-way parallelism raises this peak 8.6x"
/// looked like a contention finding: inflight is batch OCCUPANCY, capped at
/// `MAX_PURE_BATCH` = 256, so the 256 observed at P=4 was a full batch rather
/// than a measure of load. The contention was real -- it is in the cost term --
/// but inflight was never the evidence for it.
///
/// ONE atomic, not three, and the layout is the reason: `fetch_max` on a u64
/// orders by the high bits first, so putting `est_ms` there makes the winner
/// carry its OWN terms. Three separate atomics would let a later write's
/// inflight land beside an earlier write's peak -- a torn reading that looks
/// like data.
///
/// Layout: `est_ms:16 | inflight:16 | service_us:32`, each saturating. est_ms
/// is bounded by a deadline in the low thousands and inflight by max-conns, so
/// saturation is unreachable in practice and is there so it cannot corrupt the
/// ordering if it ever is.
static WRITE_WAIT_PEAK_PACKED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn pack_write_wait_peak(est_ms: u64, inflight: u64, cost_us: u64) -> u64 {
    (est_ms.min(0xFFFF) << 48) | (inflight.min(0xFFFF) << 32) | cost_us.min(0xFFFF_FFFF)
}
/// -> (est_ms, inflight, service_us)
///
/// Gated on its only reader, the FLINTINFO block, rather than silenced with an
/// allow: a mem build has no such block, so an ungated version is dead code
/// there and `-D warnings` reds the gate. Same shape as BUG-0079.
#[cfg(feature = "rocks")]
fn unpack_write_wait_peak(v: u64) -> (u64, u64, u64) {
    ((v >> 48) & 0xFFFF, (v >> 32) & 0xFFFF, v & 0xFFFF_FFFF)
}

#[cfg(all(test, feature = "rocks"))]
mod write_wait_peak_packing_tests {
    use super::{pack_write_wait_peak, unpack_write_wait_peak};
    #[test]
    fn the_terms_survive_a_round_trip() {
        let v = pack_write_wait_peak(1322, 37, 35_729);
        assert_eq!(unpack_write_wait_peak(v), (1322, 37, 35_729));
    }

    /// THE PROPERTY THE LAYOUT EXISTS FOR. `fetch_max` on the packed u64 must
    /// order by `est_ms` alone, so the surviving value carries the terms of the
    /// peak that won rather than a mixture of two writes. Three separate
    /// atomics would have no such guarantee, and the torn reading would look
    /// exactly like data.
    #[test]
    fn a_larger_peak_wins_regardless_of_its_terms() {
        // Smaller peak, but maximal terms -- it must still lose.
        let small_big_terms = pack_write_wait_peak(100, 0xFFFF, 0xFFFF_FFFF);
        let large_tiny_terms = pack_write_wait_peak(101, 0, 0);
        assert!(
            large_tiny_terms > small_big_terms,
            "est_ms must dominate the ordering, else fetch_max keeps the wrong terms"
        );
        assert_eq!(
            unpack_write_wait_peak(large_tiny_terms.max(small_big_terms)).0,
            101
        );
    }

    #[test]
    fn saturation_cannot_corrupt_the_ordering() {
        // An est_ms past the field width saturates rather than wrapping into
        // the inflight bits, which would make a huge peak compare as a tiny one.
        let huge = pack_write_wait_peak(1_000_000, 5, 5);
        let ordinary = pack_write_wait_peak(2_000, 5, 5);
        assert!(
            huge > ordinary,
            "a saturating peak must still compare as large"
        );
        assert_eq!(unpack_write_wait_peak(huge).0, 0xFFFF);
    }
}
/// Writes refused by each REPLICATION gate. The note above applies with more
/// force here: these gates have counted nothing since they shipped, so
/// BUG-0035 had to reconstruct "20328 of 50500" from a drill's client-side
/// error log. The master refused 40% of a run and kept no record of it.
static WRITES_SHED_LAG: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static WRITES_SHED_QUORUM: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static WRITES_SHED_WIDOWED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static WRITES_SHED_HEADROOM: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// DELAYED, not refused — the soft band's sleep. Counted apart from the shed
/// because the two say opposite things about the same node: a run that is all
/// delay is backpressure doing its job, while one that jumps straight to shed
/// with this near zero never spent time in the band that exists to absorb it.
static WRITES_DELAYED_SOFT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Peak lag seen by the write path, and the master-to-replica sequence gap at
/// that peak. FLINTINFO's `lag_ms` is instantaneous, so a spike short enough
/// to shed thousands of writes is invisible to every poll that straddles it —
/// which is how four reproduction attempts of BUG-0035 each found a healthy
/// node. The peak alone still cannot say WHY; the gap beside it can, and the
/// three signatures were measured rather than reasoned about:
///
///   gap large and GROWING with the lag  -> replica slower than the master
///   gap large and FROZEN while lag climbs -> replica not running at all
///                                            (SIGSTOP: 78157, unmoving)
///   gap small while lag climbs          -> the ack path, not the data path
static LAG_MS_MAX: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static LAG_MAX_GAP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How long a write may be EXPECTED to wait before we refuse it outright
/// (milliseconds; 0 = no deadline, unbounded queueing).
///
/// A write that completes after its caller gave up is worse than one refused
/// up front. It spends capacity live traffic needed, and it is ambiguous: the
/// caller timed out and retried, so the mutation may apply twice — harmless
/// for SET, a wrong answer for INCR. `-THROTTLED` already means
/// retry-with-backoff and the chaos ledger already treats it as never-acked,
/// so refusing is the one failure this system is built to absorb.
///
/// 2000 ms was a first calibration, justified as "orders of magnitude above
/// normal service (sub-millisecond at any sane concurrency)".
///
/// **A margin table used to stand here, and it has been WITHDRAWN.** It read
/// 90-1574 ms under CI contention against 20-72 ms on an idle laptop, and
/// concluded the headroom was a factor of about 1.5 rather than orders of
/// magnitude. Every figure in it was `inflight x residency`, which charges one
/// batch commit once per member of the batch -- up to 256 times over. The
/// numbers were the model's, not the system's, and they are not evidence about
/// this deadline. See BUG-0088 and `WRITE_COST_US`.
///
/// The table's own strongest clue was there and misread: "`inflight` is 256 in
/// both arms, identically". 256 is `MAX_PURE_BATCH`. A quantity that is pinned
/// to a constant in both arms of an A/B is not measuring the thing being
/// varied, and that should have been read as a full batch rather than as
/// steady queue depth.
///
/// What survives is the other half, and it is still worth knowing: per-write
/// service under four-way CI contention ran about 11x an idle laptop's while
/// every engine backpressure signal stayed flat. That contention is real. What
/// does not follow is that it brought writes near this deadline -- under the
/// corrected projection the same runs sit far below it, which is why the four
/// historical refusals were a modelling artifact and not a capacity signal.
///
/// The VALUE is deliberately unchanged. Raising it to stop a drill going red
/// would convert a measurement into a silence, and lowering it would refuse
/// writes a client can still use. What the number should be is a policy
/// question -- it is the promise made to a client -- and it now has data
/// underneath it instead of an estimate.
///
/// The 8931 ms a client waited on soak run 24, while a re-seed held the write
/// path, remains the other end of the range and is unaffected by any of this.
static WRITE_DEADLINE_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(DEFAULT_WRITE_DEADLINE_MS);
const DEFAULT_WRITE_DEADLINE_MS: u64 = 2_000;

/// Counts a write for as long as it runs, and folds its duration into the
/// service-time EWMA on the way out. Drop-based so every return path,
/// including an error or a panic, is accounted.
struct WriteInFlight(std::time::Instant);

impl WriteInFlight {
    fn enter() -> Self {
        WRITE_INFLIGHT.fetch_add(1, Ordering::Relaxed);
        Self(std::time::Instant::now())
    }
}

impl Drop for WriteInFlight {
    fn drop(&mut self) {
        // fetch_sub returns the count INCLUDING this write, which is exactly
        // the number that shared the residency about to be measured: a batch
        // enters together and commits together.
        let shared = WRITE_INFLIGHT.fetch_sub(1, Ordering::Relaxed).max(1);
        let us = self.0.elapsed().as_micros() as u64;
        // 1/8-weight EWMA: recent enough to notice a stall within a handful of
        // writes, damped enough that one slow write does not shed the next.
        let ewma = |prev: u64, sample: u64| {
            if prev == 0 {
                sample
            } else {
                prev - prev / 8 + sample / 8
            }
        };
        // Residency, kept for diagnosis: it is what an operator sees a write
        // take, and it is the honest input to "is this seat slow".
        WRITE_SERVICE_US.store(
            ewma(WRITE_SERVICE_US.load(Ordering::Relaxed), us),
            Ordering::Relaxed,
        );
        // Marginal cost, which is what the projection multiplies. See
        // WRITE_COST_US: charging a batch's commit once per member is BUG-0088.
        WRITE_COST_US.store(
            ewma(
                WRITE_COST_US.load(Ordering::Relaxed),
                marginal_cost_us(us, shared),
            ),
            Ordering::Relaxed,
        );
    }
}

/// The estimated wait, in ms, for a write arriving now. Public to the crate so
/// FLINTINFO can report the same number the gate decides on — a gauge that
/// disagrees with the gate is worse than no gauge.
fn estimated_write_wait_ms() -> u64 {
    projected_wait_ms(
        WRITE_INFLIGHT.load(Ordering::Relaxed),
        WRITE_COST_US.load(Ordering::Relaxed),
    )
}

/// What one write cost, given a residency that `shared` writes paid together.
/// A batch enters and commits as a unit, so its commit time is shared by every
/// member; charging each member the whole commit is BUG-0088.
fn marginal_cost_us(residency_us: u64, shared: u64) -> u64 {
    residency_us / shared.max(1)
}

/// The wait a write arriving now can expect: inflight x MARGINAL cost, never
/// x residency. Residency already contains the wait behind the rest of the
/// batch, so multiplying it by the batch size counts that wait once per member.
fn projected_wait_ms(inflight: u64, cost_us: u64) -> u64 {
    inflight.saturating_mul(cost_us) / 1_000
}

/// Sweep cadence for the GC pass that reclaims expired metadata and
/// orphaned collection bodies (#133). 0 disables. Hot via FLINTCONFIG.
static GC_SWEEP_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(10 * 60 * 1000);
/// Lifetime counts, for FLINTINFO and the drill's positive control: a
/// sweeper whose counters never leave zero is not known to sweep.
static GC_EXPIRED_TOTAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static GC_ORPHANS_TOTAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

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
/// One definition for every Flint binary; see the flint-build crate for why
/// it is not written out here.
/// The flags an operator can reach for, printed by --help.
///
/// Deliberately NOT generated from a parser: this binary has no parser, it
/// scans `env::args()` per flag. So this is a hand-kept list and it WILL drift
/// — which is still strictly better than the previous behaviour of starting a
/// node when asked for help.
fn usage() -> String {
    concat!(
        "flint-server — a Flint data-plane node\n",
        "\n",
        "Usage: flint-server [--port N] [--bind ADDR] [--engine mem|rocks]\n",
        "                    [--data-dir DIR] [--replica-of HOST:PORT]\n",
        "                    [--journal HOST:PORT] [--rewind-snaps DIR]\n",
        "                    [--wal-fsync-ms N] [--max-fullsync N]\n",
        "                    [--wal-ttl-seconds N] [--wal-size-limit-mb N]\n",
        "\n",
        "  --build-version, --version, -V   print the build stamp and exit\n",
        "  --help, -h        print this and exit\n",
        "\n",
        "Defaults: --port 6380, --bind 127.0.0.1, --engine mem.\n",
        "Unrecognised arguments are IGNORED, not rejected (bugs/0034).\n",
    )
    .to_string()
}

fn build_version() -> String {
    flint_build::version(env!("CARGO_PKG_VERSION"))
}

// Callers are all rocks-gated dial sites (replication/migration/cutover);
// the mem-only build still parses --internal-* for its listener.
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
/// Namespaces whose contents this seat is permitted to RECLAIM (ADR-0023 D7.1).
///
/// Empty by default and inert today: nothing evicts yet. It exists so the
/// decision has a home before the policy that reads it, because the shape of
/// the CHANNEL was the open question, not the policy.
///
/// SEAT-SIDE CONFIG, NOT A TENANT FLAG. The obvious model was the ADR-0005
/// D6/D7 opt-ins, which pack per-tenant flags into a snapshot suffix — but
/// that mechanism terminates at the PROXY. `flint-server` has no tenant
/// awareness at all: no CPWATCH, no registry, no tenant record, and `ns` is
/// an opaque key prefix used for isolation. Eviction has to run where the
/// data is, so the flag has to reach the seat, and there was no channel to
/// reach it by. This is the smallest one that fits what the seat already does.
static EVICTABLE_NS: std::sync::RwLock<Vec<String>> = std::sync::RwLock::new(Vec::new());

/// Does this seat agree with its master about which namespaces are evictable?
/// 1 = agree, 0 = MISMATCH, -1 = not known yet (or not a replica).
///
/// Per-seat config lets the two members of a pair silently disagree, and a
/// pair where one side reclaims while the other fills to `-QUOTA` is divergent
/// POLICY rather than divergent decisions — strictly worse, and nothing else
/// would surface it.
static EVICTABLE_AGREE: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(-1);

/// Canonical form: trimmed, empties dropped, sorted, deduped. Two seats given
/// the same namespaces in a different ORDER must compare equal, or the
/// agreement check reports a mismatch that is not one.
fn parse_evictable_ns(raw: &str) -> Vec<String> {
    let mut v: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    v.sort();
    v.dedup();
    v
}

/// Read one FLINTINFO field from a peer seat over the internal mesh.
///
/// Returns None when the field is ABSENT — an older peer that predates this
/// field, or an unreachable one. That is deliberately distinct from
/// `Some("")`, which is a peer that answered and has no evictable namespaces:
/// the first is "cannot compare", the second is a comparable value, and
/// collapsing them would report agreement with a seat that never answered.
#[cfg(feature = "rocks")]
fn peer_info_field(addr: &str, field: &str) -> Option<String> {
    use std::io::{Read, Write};
    let mut st = internal_connect(addr).ok()?;
    st.write_all(b"*1\r\n$9\r\nFLINTINFO\r\n").ok()?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match st.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                // Bounded: a peer that streams forever must not wedge this.
                if buf.len() > 128 * 1024 {
                    break;
                }
                if find_info_field(&buf, field).is_some() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    find_info_field(&buf, field)
}

/// `<field>:<value>\r\n` inside a FLINTINFO payload. Anchored on a preceding
/// newline (or the payload start) so `writes_shed_lag` cannot be matched by a
/// search for `lag` — the substring trap that has cost this repository four
/// separate defects.
#[cfg(feature = "rocks")]
fn find_info_field(buf: &[u8], field: &str) -> Option<String> {
    let needle = format!("{field}:");
    let hay = String::from_utf8_lossy(buf);
    for line in hay.split("\r\n") {
        if let Some(rest) = line.strip_prefix(needle.as_str()) {
            return Some(rest.to_string());
        }
    }
    None
}

#[cfg(feature = "rocks")]
fn evictable_ns_joined() -> String {
    EVICTABLE_NS.read().map(|g| g.join(",")).unwrap_or_default()
}

/// Capacity for each declared-evictable namespace, as `ns=bytes` pairs.
///
/// Empty when nothing is evictable — the default, and the overwhelmingly
/// common case — so a durable deployment sees an empty field rather than a
/// number implying a policy it has not opted into.
///
/// Uses `ns_capacity_bytes`, not `ns_bytes`, because this is the figure an
/// operator reads while deciding whether a namespace is about to fill, and
/// `ns_bytes` omits exactly the writes that are filling it (ADR-0023 D7.2).
///
/// A namespace whose figure could not be taken renders `ns=?`, never `ns=0`.
/// Zero is a legitimate reading — an empty namespace — so printing it for "the
/// engine did not answer" would make this field report maximum headroom at the
/// one moment it knows nothing, during the incident it exists to explain. Same
/// contract, and the same reason, as BUG-0022.
/// The eviction counters, as one FLINTINFO line.
///
/// Exported for TUNING THE BATCHING FLOORS, which is the whole reason they
/// exist. The number to read first is `marks_at_last_pass` against
/// `forced_passes`: a forced `compact_ns` rewrites the surviving rows, so it
/// costs the same whether it reclaims a thousand keys or a million, and a low
/// marks-per-pass means the node is paying a full-namespace rewrite for a
/// fraction of the benefit.
///
/// The two skip counters say which floor is binding, and they want opposite
/// responses: cooldown-dominated means pressure outruns the interval,
/// small-dominated means marks arrive too slowly to be worth forcing and
/// ordinary compaction is probably already draining them.
///
/// Empty when nothing is declared evictable, so a durable deployment reads no
/// counters for a feature it has not turned on.
#[cfg(feature = "rocks")]
fn eviction_metrics_line(rocks: &Option<RocksHandle>) -> String {
    let Some(kv) = rocks.as_ref() else {
        return String::new();
    };
    let ev = kv.eviction();
    if ev.evictable_count() == 0 {
        return String::new();
    }
    let m = ev.metrics();
    format!(
        "marked_total={} marked_now={} dropped={} refused={} overflow={} \
forced_passes={} skipped_cooldown={} skipped_small={} marks_at_last_pass={} \
reclaim_cycles={} bytes_requested={} policy_keys={} policy_bytes={} \
accesses={} accesses_dropped={}",
        m.marked_total,
        m.marked_now,
        m.dropped,
        m.refused,
        m.mark_overflow,
        m.forced_passes,
        m.forced_skipped_cooldown,
        m.forced_skipped_small,
        m.marks_at_last_pass,
        m.reclaim_cycles,
        m.bytes_requested,
        m.policy_keys,
        m.policy_bytes,
        m.accesses,
        m.accesses_dropped,
    )
}

/// Push the declared-evictable set into the ENGINE, where the compaction
/// filter's guard reads it.
///
/// There are deliberately two copies. `EVICTABLE_NS` is the seat's record of
/// what the operator declared — it answers FLINTINFO and the pair-agreement
/// check. The engine's copy is what actually authorises a delete. Two copies
/// can disagree, and only one direction is dangerous: the engine lagging a
/// REVOCATION would leave a namespace evictable after an operator turned it
/// off. So every site that writes `EVICTABLE_NS` calls this, and so does open.
///
/// Pushed rather than read lazily from the filter, because the filter runs per
/// key on a compaction thread and must not reach for this lock on a tier whose
/// compaction headroom is BUG-0013's subject.
#[cfg(feature = "rocks")]
fn sync_evictable_to_engine(ev: &flint_storage::eviction::EvictionState) {
    if let Ok(g) = EVICTABLE_NS.read() {
        ev.set_evictable(g.iter());
    }
}

/// Rocks-only: without an engine there is no capacity to report, and the
/// non-rocks `flintinfo` emits no evictable fields at all.
#[cfg(feature = "rocks")]
fn evictable_ns_bytes_joined(rocks: &Option<RocksHandle>) -> String {
    let Ok(names) = EVICTABLE_NS.read() else {
        return String::new();
    };
    names
        .iter()
        .map(|ns| {
            match rocks
                .as_ref()
                .and_then(|kv| kv.ns_capacity_bytes(ns.as_bytes()))
            {
                Some(b) => format!("{ns}={b}"),
                None => format!("{ns}=?"),
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn internal_connect(addr: &str) -> std::io::Result<flint_tls::Stream> {
    flint_tls::connect_reloadable(addr, INTERNAL_CLIENT.get().unwrap_or(&None))
}

enum LeaseRenewal {
    Ok,
    Superseded(String),
    Unreachable,
}

/// The journal cause for a lease-superseded self-fence. NAMES THE SUCCESSOR
/// (BUG-0065): the 2026-08-27 canary abort was undiagnosable because this row
/// carried a fixed string while stderr — per-seat, dead with the box — had the
/// one fact that mattered. Whether the CP named this pair's own stale record
/// or the other pair's fresh promotion is answerable from the successor and
/// from nothing else the gate prints.
fn superseded_cause(successor: &str) -> String {
    format!("lease superseded by {successor}: promotion on record at the CP")
}

/// One CPLEASE round trip (ADR-0018). A fresh dial per renewal, at ttl/3
/// cadence — the reconnect cost is noise, and a persistent connection would
/// need its own liveness machinery to avoid renewing into a dead socket.
fn cp_lease_renew(target: &str, me: &str) -> LeaseRenewal {
    use std::io::{Read, Write};
    let attempt = |target: &str| -> std::io::Result<flint_resp::Value> {
        let mut stream = internal_connect(target)?;
        let mut out = Vec::new();
        flint_resp::encode(
            &flint_resp::Value::Array(Some(vec![
                flint_resp::Value::Bulk(Some(b"CPLEASE".to_vec())),
                flint_resp::Value::Bulk(Some(me.as_bytes().to_vec())),
            ])),
            &mut out,
        );
        stream.write_all(&out)?;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = stream.read(&mut chunk)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "CP closed mid-reply",
                ));
            }
            buf.extend_from_slice(&chunk[..n]);
            match flint_resp::decode(&buf) {
                Ok(flint_resp::Decoded::Complete(v, _)) => return Ok(v),
                Ok(flint_resp::Decoded::NeedMore) => continue,
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("{e:?}"),
                    ));
                }
            }
        }
    };
    // On an HA control plane CPLEASE is served only by the LEADER — a
    // follower's applied state can be stale, and a stale +OK would keep a
    // superseded master alive. Followers answer `-LEADER <addr>`; follow
    // exactly one hop per renewal, fresh each time (leadership moves).
    let mut reply = attempt(target);
    if let Ok(flint_resp::Value::Error(e)) = &reply
        && let Some(leader) = e.strip_prefix("LEADER ")
    {
        reply = attempt(leader.trim());
    }
    match reply {
        Ok(flint_resp::Value::Simple(s)) if s.starts_with("OK") => LeaseRenewal::Ok,
        Ok(flint_resp::Value::Error(e)) if e.starts_with("SUPERSEDED") => {
            LeaseRenewal::Superseded(e.split_whitespace().nth(1).unwrap_or("?").to_string())
        }
        // Any other reply — an old CP without CPLEASE ("ERR unknown"), a
        // NOPAIR refusal, an election in progress, a protocol surprise — is
        // treated as unreachable: the deadline simply is not extended, and
        // the watchdog decides. Failing OPEN here (treating unknown as OK)
        // would disable the fence against exactly the component drift it
        // exists to survive.
        _ => LeaseRenewal::Unreachable,
    }
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

/// Set once, the first time an event is dropped for want of a journal target.
/// A no-op instrument is the failure mode every diagnostic in this tree has
/// been bitten by: it reports nothing and reads as nothing happening. Emitting
/// to stderr once (not per event) makes the drop visible without turning a
/// misconfigured node into a log flood.
static JOURNAL_DROP_WARNED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

fn journal_event(kind: flint_journal::EventKind, epoch: Option<String>, cause: &str) {
    let Some(Some(target)) = JOURNAL_TARGET.get().cloned() else {
        if !JOURNAL_DROP_WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
            eprintln!(
                "journal: no --journal target yet; dropping {kind:?} and any \
                 further events until one is set. Timeline gaps downstream are \
                 THIS, not an absence of the events."
            );
        }
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
    // Ask the binary what it is, without starting it. Until now the only way
    // to tell which release a file was came from `ls -la` and process start
    // times — inference, not an answer — and it is what the release build
    // asserts against to prove the stamp actually landed.
    // --version and -V answer the same question as --build-version. They are
    // here because a peer's release-acceptance check reached for `--version`,
    // got a RUNNING NODE, and hung a box with a 30-minute TTL (docs/bugs/0034).
    // Adding the aliases was the narrow fix; refusing every unrecognised
    // argument is the general one and now lands too — see ACCEPTED_FLAGS.
    if std::env::args().any(|a| a == "--build-version" || a == "--version" || a == "-V") {
        println!("{}", build_version());
        return Ok(());
    }
    // --help must not START A NODE. Until now `--build-version` was the only
    // flag handled before the listener, and every other argument was simply
    // ignored — so `flint-server --help` fell through, bound the DEFAULT port
    // 6380, printed "listening", and ran until killed (docs/bugs/0034).
    //
    // That is bad for an operator reaching for usage and worse for a gate: the
    // resulting process sits on a port no drill declares, so it is outside
    // every drill's scope, and fleet_guard correctly refuses. One stray --help
    // refused 64 drills in the run of 2026-08-19T22:27Z.
    //
    // The narrower defect — an UNRECOGNISED flag silently ignored, so a
    // typo'd `--prot 7001` starts a node on 6380 rather than failing — is
    // fixed by reject_unknown_flags() just below. It needed the accepted set
    // enumerated at BOTH ends first; doing it blind is what broke the suite
    // the first time.
    if std::env::args().any(|a| a == "--help" || a == "-h") {
        println!("{}", usage());
        return Ok(());
    }
    // The general fix, now that the caller side is enumerated (BUG-0034).
    reject_unknown_flags();
    // ADR-0014 D2: the drift check needs to know how long this seat has
    // been up, to tell real divergence from a node mid-roll.
    heat::mark_process_start();
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
    if let Some(raw) = arg("--evictable-ns") {
        let parsed = parse_evictable_ns(&raw);
        eprintln!("evictable namespaces: {}", parsed.join(","));
        if let Ok(mut g) = EVICTABLE_NS.write() {
            *g = parsed;
        }
    }
    // BUG-0060: an opt-in bound on the SUM of concurrent collection reads.
    // Each unit is already capped by --max-value-bytes; nothing caps their
    // total, and five concurrent max-size reads is ~8.1 GB. 0 disables, and
    // is the default: a new refusal path is not switched on for existing
    // workloads by inference, the way eviction shipped opt-in.
    let read_budget_pct: u8 = arg("--collection-read-budget-pct")
        .and_then(|v| v.parse().ok())
        .unwrap_or(flint_storage::admission::DEFAULT_COLLECTION_READ_BUDGET_PCT);
    // `observe` runs the same arithmetic and admits anyway, counting what it
    // would have refused. It exists because the bound ships OFF and the only
    // other way to learn what a budget costs a real workload is to enforce it
    // on that workload and watch what breaks.
    let read_mode = match arg("--collection-read-mode").as_deref() {
        None => flint_storage::admission::Mode::Enforce,
        Some(raw) => match flint_storage::admission::Mode::parse(raw) {
            Some(m) => m,
            None => {
                eprintln!("--collection-read-mode: expected `enforce` or `observe`, got `{raw}`");
                std::process::exit(2);
            }
        },
    };
    // Observing with no budget counts nothing and reports 0 -- which reads
    // exactly like "your workload never crossed the budget". That is the
    // reassuring answer and the wrong one, so the pairing is refused rather
    // than served. Same rule as `collection_read_unmeasured`: never let "I
    // did not look" render as "nothing was wrong".
    if read_mode == flint_storage::admission::Mode::Observe && read_budget_pct == 0 {
        eprintln!(
            "--collection-read-mode observe needs --collection-read-budget-pct > 0: there is \
             no budget to observe against, and collection_read_would_refuse would sit at 0 \
             whatever the workload did"
        );
        std::process::exit(2);
    }
    // Set BEFORE any connection is served, so no read can race the default in.
    // The whole block sits AHEAD of the bind and the store open, so a refusal
    // above costs no port and no DB: `std::process::exit` past an open
    // RocksDB races its C++ static teardown, which is BUG-0048.
    let _ = READ_ADMISSION.set(flint_storage::admission::ReadAdmission::new(
        read_budget_pct,
        read_mode,
    ));
    if read_budget_pct > 0 {
        let tenths = flint_storage::admission::READ_PEAK_MULTIPLIER_TENTHS;
        eprintln!(
            "collection-read-budget-pct: {read_budget_pct} ({}, a read is estimated at {}.{}x its ComplexMeta.bytes)",
            read_mode.as_str(),
            tenths / 10,
            tenths % 10,
        );
        if read_mode == flint_storage::admission::Mode::Observe {
            eprintln!(
                "collection-read-mode: OBSERVE -- nothing will be refused. \
                 collection_read_would_refuse counts what enforcement would have refused, \
                 and OVER-counts it: a refusal sheds the read, so enforcing leaves less \
                 in flight for the next one than observing does"
            );
        }
        if flint_storage::mem::sample().is_none() {
            eprintln!(
                "collection-read-budget-pct: node memory is UNREADABLE here, so reads will be \
                 admitted without a budget and counted in collection_read_unmeasured"
            );
        }
    }

    let read_only = Arc::new(AtomicBool::new(replica_of.is_some()));
    let tailer_stop = Arc::new(AtomicBool::new(false));
    // Lease deadline (unix-ms). 0 = unmanaged (standalone: never self-fences).
    // Once a controller sends FLINTLEASE, the node is lease-managed and
    // self-fences to read-only if the deadline passes without renewal — so a
    // master partitioned from ALL controllers stops accepting writes on its
    // own, closing the split-brain window without anyone reaching it.
    let lease_deadline = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // BIND BEFORE THE INITIAL FULL SYNC, not after it (#176).
    //
    // A fresh replica downloads its whole dataset before this function ever
    // reached the old bind site — minutes on a fleet, tens of minutes at
    // scale — and for that entire window the port was CLOSED. The node did
    // not merely look unhealthy: at the TCP layer it was indistinguishable
    // from a dead host, which is the strongest wrong signal available.
    // `flintctl start` reads it as absent and replaces it (#139's class);
    // the controller cannot tell it from a corpse (#189); `verify` calls the
    // pair single-copy.
    //
    // So the listener opens here and answers immediately, in the state Redis
    // already defined for exactly this: LOADING. Data commands get
    // `-LOADING`, which every mainstream client library already knows to
    // retry rather than treat as a hard failure; PING and FLINTINFO answer,
    // so liveness and progress are observable throughout.
    //
    // The listener is shared, not handed over: the loading acceptor and the
    // real accept loop both take connections from this same Arc, and the
    // loading one exits once LOADING clears (see `accept_while_loading`).
    let bind = arg("--bind").unwrap_or_else(|| "127.0.0.1".into());
    let listener = Arc::new(TcpListener::bind((bind.as_str(), port))?);
    eprintln!(
        "flint-server listening on {bind}:{port} ({}) — LOADING",
        if internal_reload.is_some() {
            "internal mTLS"
        } else {
            "plaintext"
        }
    );
    let loading_acceptor = {
        // Non-blocking for the loading acceptor ONLY, so it can notice
        // LOADING clearing without a connection arriving to wake it. The
        // main loop sets it back to blocking before it takes over, and the
        // handover is a join, not a sleep: exactly one loop ever owns the
        // listener. (Waking a blocked accept() with a throwaway self-
        // connection was the alternative and it is worse — a connect that
        // fails leaves the acceptor parked forever, holding a real client's
        // connection hostage.)
        listener.set_nonblocking(true)?;
        let listener = Arc::clone(&listener);
        let tls = internal_reload.clone();
        std::thread::spawn(move || accept_while_loading(&listener, tls.as_ref()))
    };

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
            // The start of the rejoin, and the only marker that separates BOOT
            // from the catch-up that follows it. Without it the window between
            // a promotion and writes resuming is one number; with it, restart
            // latency, the rewind decision and the tail are three.
            //
            // ONLY when there is prior state to reconcile. A replica booting
            // with no data directory has never joined, so it cannot REjoin --
            // the event was a misnomer there, and emitting it broke the
            // fleet_journal drill's invariant that a HEALTHY pair's journal
            // holds only the supervision arming and no transitions. That
            // invariant is worth more than the event: a journal that speaks
            // during ordinary startup teaches operators to ignore it.
            if replica_of.is_some() && dir_path.exists() {
                journal_event(
                    flint_journal::EventKind::RejoinStarted,
                    None,
                    "replica process up with existing state; choosing a catch-up path",
                );
            }
            let reseed = dir_path.join(NEEDS_RESEED).exists();
            let mut rewound = false;
            if reseed {
                if let Some(target) = &replica_of {
                    let why = std::fs::read_to_string(dir_path.join(NEEDS_RESEED))
                        .unwrap_or_default()
                        .trim()
                        .to_string();
                    // VERIFY the copy as-is before touching it. The marker
                    // means "this copy cannot be trusted BLINDLY", not "this
                    // copy is trash": flintctl marks every dead seat because
                    // a corpse's role cannot be observed, so the common
                    // marked boot is a killed REPLICA whose dir is a valid
                    // near-tip copy of the lineage it is rejoining. The
                    // master's admission logic can vet that copy exactly the
                    // way it vets a rewound snapshot — one handshake, no
                    // bytes moved. Skipping this and rewinding instead moves
                    // a caught-up replica BACKWARD to its last-mastership
                    // snapshot, which ages without bound (replicas take no
                    // snapshots): soak run 34 cycle 6 rewound one 15 minutes
                    // into the past and the catch-up span was already gone
                    // from the master's WAL. Ex-masters never pass this
                    // probe by construction — their durable role is Master,
                    // and a master-role dir must go through the fence-
                    // checked rewind below (#107's contract, unchanged).
                    let mut warm = false;
                    if let Ok(kv) = flint_storage::rocks::RocksKv::open(&dir_path) {
                        use flint_storage::manifest::{self, Role};
                        if let Some(claim) = manifest::read_role(&kv)
                            && claim.role == Role::Replica
                        {
                            let cursor = kv.last_applied();
                            drop(kv);
                            match probe_resume(target, cursor, claim.epoch) {
                                Ok(()) => {
                                    clear_needs_reseed(&dir_path);
                                    eprintln!(
                                        "marked copy verified against the lineage held by \
                                         {target}: warm rejoin at seq {cursor} (epoch {})",
                                        claim.epoch
                                    );
                                    journal_event(
                                        flint_journal::EventKind::RejoinDecided,
                                        Some(format!("{:?}", claim.epoch)),
                                        &format!("warm rejoin at seq {cursor}"),
                                    );
                                    warm = true;
                                }
                                Err(e) => eprintln!(
                                    "marked copy refused by {target} ({e}); trying a rewind"
                                ),
                            }
                        }
                    }
                    rewound = warm;
                    // Rewind next (#187): the marker says this copy's TAIL
                    // cannot be continued, but a local snapshot from before
                    // the branch point still can — and restoring one costs
                    // hard links, where the full re-seed costs the whole
                    // dataset over the wire with the new master's write
                    // gates held shut. A marker written because a master
                    // already REFUSED a rewound cursor ("promotion fence")
                    // must not retry the same snapshot forever, so it goes
                    // straight to the re-seed.
                    if !warm
                        && !why.contains("promotion fence")
                        && let Some(snaps) = arg("--rewind-snaps")
                    {
                        rewound = try_rewind(&dir_path, &snaps, target);
                    }
                    if !rewound {
                        eprintln!(
                            "{NEEDS_RESEED} present: this copy cannot be continued ({why}) — \
                             discarding it and re-seeding from a checkpoint"
                        );
                        // A failed rewind may already have removed the dir
                        // (its swap is remove-then-rename); NotFound here is
                        // the desired end state, not an error. Exiting on it
                        // would strand the seat DOWN with the marker gone.
                        match std::fs::remove_dir_all(&dir_path) {
                            Ok(()) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(e) => return Err(e),
                        }
                    }
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
                        //
                        // -LOADING IS THE SAME RACE WEARING A NEW SHAPE. Until
                        // #176 a master that had not finished starting was not
                        // listening, so losing this race arrived as
                        // ConnectionRefused and the arm below already covered
                        // it. #176 makes that master bind and answer -LOADING
                        // instead — deliberately, so it is not mistaken for
                        // dead — which turned a condition this loop was written
                        // to survive into a fatal one. promote_notice starts
                        // both seats in the same millisecond and died on it
                        // roughly half the time: "master error: LOADING Flint
                        // is loading the dataset in memory", replica exits,
                        // "nothing listening on 127.0.0.1:6911 after 30s".
                        Err(e)
                            if attempt < 120
                                && (e.to_string().contains("THROTTLED")
                                    || e.to_string().contains("LOADING")
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
            // BUG-0033: the retention seam existed with no caller, so nothing
            // could put a replica outside the WAL window without writing 8 GiB
            // or waiting six hours — which is why BUG-0031's class had no drill
            // and was found by a production incident instead.
            //
            // DEFAULTS ARE UNCHANGED on purpose. These are for tests and for
            // operators who have measured their own fleet, not a new
            // recommended value: a short window is the livelock in BUG-0012.
            let wal_ttl = arg("--wal-ttl-seconds")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(flint_storage::rocks::DEFAULT_WAL_TTL_SECONDS);
            // DERIVED FROM THE VOLUME, not a constant (BUG-0079). 8 GiB is
            // forty-one seconds of WAL at 200 MB/s, so the byte term pruned
            // the archive out from under a live replica long before the TTL
            // was relevant, and the shed gate — calibrated in sequences —
            // never fired. min(256 GiB, a quarter of the volume).
            // Create the data directory BEFORE measuring it. The engine makes
            // it anyway a few lines down; doing it here is what makes the
            // measurement answer for the volume the data will actually live
            // on. Skipping this was a boot-time RACE, not a constant error:
            // on a 5-host fleet the master sampled `dir` in the same second
            // the state directory was being created on the freshly formatted
            // NVMe, read None, mapped it onto 0 bytes and clamped a 3.5 TB
            // seat to the 1 GiB FLOOR — while its replica, starting 1.1 s
            // later, measured the same disk correctly and took 256 GiB. One
            // silent log line was the only difference (BUG-0079).
            //
            // Walking up to an ancestor is NOT sufficient on its own: with
            // the instance store not yet mounted, the nearest ancestor is the
            // small root volume, which lands on the same floor just as
            // quietly. `sample_nearest` stays below as the fallback for when
            // the create itself fails.
            if let Err(e) = std::fs::create_dir_all(&dir) {
                eprintln!("WAL budget: could not create {dir} before sizing the archive: {e}");
            }
            let mut measured = true;
            let derived_mb = match flint_storage::disk::sample_nearest(std::path::Path::new(&dir)) {
                Some(u) => flint_storage::rocks::wal_size_limit_mb_for_volume(u.total_bytes),
                None => {
                    measured = false;
                    // "Unknown" is not "small". disk.rs is explicit that a
                    // failed syscall is not evidence about disk space, and a
                    // budget is the one place where believing it costs a
                    // replica its archive.
                    eprintln!(
                        "WAL budget: no filesystem under {dir} could be measured; \
                         using {} MB rather than assuming a small volume",
                        flint_storage::rocks::DEFAULT_WAL_SIZE_LIMIT_MB
                    );
                    flint_storage::rocks::DEFAULT_WAL_SIZE_LIMIT_MB
                }
            };
            let pinned = arg("--wal-size-limit-mb").and_then(|v| v.parse::<u64>().ok());
            let wal_mb = pinned.unwrap_or(derived_mb);
            WAL_BUDGET_MB.store(wal_mb, Ordering::Relaxed);
            // Record WHY, not just what. Order matters: an operator pin wins
            // over how the derivation went, because a pinned budget is not
            // answerable to the volume and the invariant must not be asserted
            // against it.
            WAL_BUDGET_SRC.store(
                if pinned.is_some() {
                    WAL_SRC_OVERRIDE
                } else if measured {
                    WAL_SRC_MEASURED
                } else {
                    WAL_SRC_UNMEASURED
                },
                Ordering::Relaxed,
            );
            // Compare against what this node WOULD have chosen, not against
            // the bare constant: the size budget is derived from the volume
            // now, so a stock node differs from DEFAULT_WAL_SIZE_LIMIT_MB and
            // would report itself overridden on every boot. A warning that
            // fires when nothing is wrong is a warning nobody reads.
            if wal_ttl != flint_storage::rocks::DEFAULT_WAL_TTL_SECONDS || wal_mb != derived_mb {
                eprintln!(
                    "WAL retention OVERRIDDEN: ttl={wal_ttl}s size={wal_mb}MB \
                     (this node would have chosen {}s / {derived_mb}MB) — a \
                     replica that falls outside this window needs a full re-seed",
                    flint_storage::rocks::DEFAULT_WAL_TTL_SECONDS
                );
            }
            let kv = RocksKv::open_with_retention(std::path::Path::new(&dir), wal_ttl, wal_mb)
                .map_err(|e| std::io::Error::other(format!("rocksdb open: {e}")))?;
            // The flag is parsed before the engine exists, so the engine
            // starts with an empty set and would honour no declaration at all
            // until the first FLINTCONFIG. Seed it here.
            sync_evictable_to_engine(kv.eviction());
            if fresh && replica_of.is_some() {
                // The checkpoint copies the SOURCE's system rows — including
                // its replication cursor, which is the source's OLD upstream
                // position (frozen at its promotion), not ours. Like the
                // role row, the cursor is node-local identity and must be
                // reasserted at seed time: our true cursor is the copied
                // DB's own latest sequence. (Found by chaos: the inherited
                // marker caused a permanent SequenceGap loop against
                // promoted masters, leaving pairs unreplicated.)
                //
                // FRESH ONLY, never for a rewound copy: try_rewind already
                // asserted its cursor and role, and those asserts WROTE two
                // system rows — so latest_seq here is the snapshot seq plus
                // two, and re-asserting from it hands the master a cursor
                // claiming entries this copy never applied. The master then
                // dutifully skips them: two keys short, found by the loaded
                // drill's keyspace-parity check on this exact line.
                let cursor = kv.latest_seq();
                kv.set_last_applied(cursor)
                    .map_err(|e| std::io::Error::other(format!("cursor init: {e:?}")))?;
                eprintln!("full sync complete; tailing from seq {cursor}");
            } else if rewound {
                // Covers both marked-rejoin continuations: a verified warm
                // copy and a fence-checked rewind ("rewound to" names the
                // second in try_rewind's own log line).
                eprintln!(
                    "marked rejoin continues; tailing from seq {}",
                    kv.last_applied()
                );
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
                // The restore is a branch point like any promotion: nodes of
                // the dead generation must not resume past the snapshot.
                // The seq is this node's latest, which came from the
                // restored snapshot -- so it is a position in the OLD
                // lineage's space, and `old` is the epoch that names it.
                manifest::record_promo_fence(&kv, bumped, old, kv.latest_seq());
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
            // Fresh only — a rewound copy's identity was asserted inside
            // try_rewind, before any other write could move its sequence.
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
        // Cloned BEFORE the move: `target` goes into the replication closure.
        let agree_target = target.clone();
        let link = Arc::new(ReplicaLink {
            target: std::sync::Mutex::new(target.clone()),
            repoint: AtomicBool::new(false),
        });
        let _ = REPLICA_LINK.set(Arc::clone(&link));
        std::thread::spawn(move || replica::run(&link, &kv, &stop));

        // ADR-0023 D7.1 pair-agreement. Per-seat config lets the two members
        // of a pair disagree silently, and a pair where one side reclaims
        // while the other fills to -QUOTA is divergent POLICY — worse than
        // divergent decisions, and invisible without something that compares.
        //
        // REPLICA-INITIATED because only the replica knows its counterpart:
        // it has --replica-of, while the hub tracks replicas by id and never
        // records their addresses. A master therefore cannot run this check,
        // which is why it lives here rather than in some symmetric place.
        {
            let master = agree_target;
            std::thread::spawn(move || {
                loop {
                    let verdict = match peer_info_field(&master, "evictable_ns") {
                        Some(theirs) if theirs == evictable_ns_joined() => 1,
                        Some(theirs) => {
                            eprintln!(
                                "EVICTABLE-NS MISMATCH with master {master}: this seat [{}], master [{theirs}] \
                                 — one side may reclaim while the other fills to -QUOTA",
                                evictable_ns_joined()
                            );
                            0
                        }
                        None => -1,
                    };
                    EVICTABLE_AGREE.store(verdict, Ordering::Relaxed);
                    std::thread::sleep(std::time::Duration::from_secs(10));
                }
            });
        }
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
        // Full-sync serve cap (bytes/sec; 0 = unlimited). Defaults non-zero —
        // see FULLSYNC_RATE_BYTES for why the default is the case that hurts.
        let fullsync_rate_bytes: u64 = arg("--fullsync-rate-bytes")
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_FULLSYNC_RATE_BYTES);
        FULLSYNC_RATE_BYTES.store(fullsync_rate_bytes, Ordering::Relaxed);
        if fullsync_rate_bytes == 0 {
            eprintln!(
                "full-sync serve rate UNCAPPED (--fullsync-rate-bytes 0): a re-seed can \
                 starve this node's write path (#184)"
            );
        }
        // End-to-end write deadline (ms; 0 = no deadline, unbounded queueing).
        let write_deadline_ms: u64 = arg("--write-deadline-ms")
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_WRITE_DEADLINE_MS);
        WRITE_DEADLINE_MS.store(write_deadline_ms, Ordering::Relaxed);
        if write_deadline_ms == 0 {
            eprintln!(
                "write deadline DISABLED (--write-deadline-ms 0): writes queue without bound \
                 and may complete long after the client gave up (#186)"
            );
        }
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

    // WATCH's modification tracking (ADR-0012 D5). Wrapped around the store
    // HERE, once, before anything clones it — so the GC sweeper, the
    // read-path lazy expiry, a transaction's commit and the async queue's
    // commit are all covered by construction. Wrapping at the command layer
    // instead would miss every one of those, and a missed modification is a
    // lost update.
    let watch = Arc::new(flint_storage::watch::WatchTable::new());
    let store: Arc<dyn Kv> = Arc::new(flint_storage::watch::WatchedKv::new(
        store,
        Arc::clone(&watch),
    ));

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
    // min-replicas-to-write). Operator-settable, honoured at whatever value is
    // given, and hot-reloadable through FLINTCONFIG.
    //
    // DEFAULT 0, and it stays there. This is a cache with async replication,
    // and refusing a write is worse than risking one — the same reason Redis
    // ships min-replicas-to-write 0. flintctl passes the flag only when an
    // operator sets `min-replicas` in the inventory.
    //
    // 1 IS A REAL TRADE, NOT A STRICTLY SAFER SETTING, and this comment used
    // to recommend it. A freshly promoted master has no replica either, and
    // the gate cannot tell "my peer died" from "I was just promoted": it sheds
    // every write until a replacement attaches AND acks, which on a large
    // dataset is a whole full sync. So it trades failover RTO for durability
    // on every single failover. docs/failover.md states that trade for
    // operators; the guard that IS on by default for pair members is the
    // widowed grace, which buys the same bound without it.
    let min_replicas: u32 = arg("--min-replicas-to-write")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let hub = Arc::new(ReplHub::new(lag_soft, lag_hard, min_replicas));
    // ADR-0022: shed writes once the master outruns its SLOWEST live replica
    // by this many sequences, so a lagging replica meets backpressure before
    // it meets a WAL segment that has already been recycled. 0 disables.
    // Sequences are a proxy for retained bytes; retune from `wal_headroom_seq`
    // in INFO if values are much bigger or smaller than ~1 KB.
    // Default DERIVED from the archive budget rather than fixed, so the gate
    // and the thing it guards cannot drift apart again (BUG-0079).
    match arg("--wal-headroom-seq").and_then(|v| v.parse::<u64>().ok()) {
        Some(v) => {
            hub.set_wal_headroom_shed_seq(v);
            eprintln!("wal headroom shed threshold: {v} sequences (operator override)");
        }
        None => {
            let budget_mb = WAL_BUDGET_MB.load(Ordering::Relaxed);
            let v = repl_hub::headroom_shed_seq_for_budget(budget_mb);
            hub.set_wal_headroom_shed_seq(v);
            HEADROOM_AUTO.store(true, Ordering::Relaxed);
            eprintln!(
                "wal headroom shed threshold: {v} sequences (half of a {budget_mb} MB \
                 archive; starts from an assumed 16 KiB/sequence and re-derives \
                 from OBSERVED bytes/sequence once writes arrive — override \
                 with --wal-headroom-seq to pin it)"
            );
        }
    }
    // --lease-ttl-ms (ADR-0018): how long this node may serve as master
    // without a successful CPLEASE renewal before self-fencing. 0 = lease
    // management off (standalone / no CP), matching the old "no FLINTLEASE
    // ever arrived" behaviour. The TTL bounds the post-promotion split-brain
    // window, so it must stay SHORT — the availability win of ADR-0018 comes
    // from who holds the fence, not from loosening it.
    let lease_ttl_ms: u64 = arg("--lease-ttl-ms")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    // --widowed-grace-ms: how long this node may keep accepting writes with
    // NO live replica before it sheds. The lag cap cannot bound that window
    // (no replica, nothing to measure), and min-replicas-to-write bounds it
    // at zero, which also freezes every freshly promoted master until its
    // replacement syncs. 0 = off, which is right for a standalone node;
    // flintctl turns it on for pair members. See ReplHub::widowed_beyond_grace.
    if let Some(v) = arg("--widowed-grace-ms").and_then(|v| v.parse().ok()) {
        hub.set_widowed_grace_ms(v);
    }

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
            "disk guard: min-free {}% or {} bytes, sampling {} every {:?} OR SOONER",
            thresholds.min_free_pct, thresholds.min_free_bytes, dir, every
        );
        // The guard thread reclaims as well as sheds, so it needs the engine:
        // reclaim marks keys and forces the compaction that drops them. Cloned
        // in rather than reached for, because this thread outlives the setup
        // scope it is spawned from.
        #[cfg(feature = "rocks")]
        let ev_rocks = rocks.clone();
        std::thread::spawn(move || {
            let path = std::path::PathBuf::from(&dir);
            let mut last = diskguard::Verdict::Ok;
            // `every` is the CEILING, not the cadence. The interval shortens
            // as free space closes on the threshold, so a fast filler cannot
            // cross the whole headroom between two looks — which is what a
            // fixed cadence allowed, measured at 10-13 points of overshoot on
            // the self-fill drill. See `diskguard::pace`.
            let mut prev_free: Option<u64> = None;
            let mut slept = every;
            loop {
                let usage = flint_storage::disk::sample(&path);
                let v = diskguard::verdict(usage, thresholds, last);
                if v != last {
                    // Transitions are the thing an operator needs in the
                    // log; steady state is what the metrics are for. The
                    // journal event is the subscribable half (ADR-0013 D3):
                    // an external GC policy daemon triggers on this edge
                    // instead of tight-polling FLINTINFO.
                    let detail = format!(
                        "free {} of {} bytes",
                        usage.map(|u| u.free_bytes).unwrap_or(0),
                        usage.map(|u| u.total_bytes).unwrap_or(0)
                    );
                    eprintln!("disk guard: {last:?} -> {v:?} ({detail})");
                    journal_event(
                        if v == diskguard::Verdict::Shed {
                            flint_journal::EventKind::DiskShed
                        } else {
                            flint_journal::EventKind::DiskResumed
                        },
                        None,
                        &detail,
                    );
                }
                DISK.apply(usage, v);
                // Capacity reclaim (ADR-0023 D7 req 2/4). Decided on the same
                // sample as the shed verdict, and by construction engages
                // above it, so an evictable namespace evicts rather than ever
                // reaching -QUOTA. It does not act yet: nothing marks keys
                // until the policy is fed from the request path.
                let was_reclaiming = RECLAIM_ACTIVE.load(Ordering::Relaxed);
                let action = diskguard::reclaim_action(usage, thresholds, was_reclaiming);
                let now_reclaiming = action != diskguard::ReclaimAction::Idle;
                if now_reclaiming != was_reclaiming {
                    let target = match action {
                        diskguard::ReclaimAction::ReclaimToFreeBytes(b) => b,
                        diskguard::ReclaimAction::Idle => 0,
                    };
                    eprintln!(
                        "capacity reclaim: {} (free {} of {} bytes, target {target})",
                        if now_reclaiming {
                            "ENGAGED"
                        } else {
                            "released"
                        },
                        usage.map(|u| u.free_bytes).unwrap_or(0),
                        usage.map(|u| u.total_bytes).unwrap_or(0),
                    );
                    RECLAIM_TARGET_FREE.store(target, Ordering::Relaxed);
                    RECLAIM_ACTIVE.store(now_reclaiming, Ordering::Relaxed);
                }
                // ACT. Every sample while engaged, not only on the transition:
                // one pass rarely clears the deficit, and waiting for the next
                // edge would mean waiting for a state change that reclaiming is
                // supposed to cause.
                // THE CAPACITY HINT IS SET ON EVERY SAMPLE, not only while
                // reclaiming, and the difference is the whole feature.
                //
                // `note_write` skips admission while the hint is zero, so
                // setting it inside the reclaim branch meant the policy learned
                // nothing until pressure arrived — and a policy with no history
                // at the moment it is first asked for cold keys has none to
                // give. It would evict blind, or more precisely not at all.
                // The drill caught this at policy_keys=0; every unit test in
                // the crate passed, because each one sets the hint itself.
                #[cfg(feature = "rocks")]
                if let (Some(kv), Some(u)) = (ev_rocks.as_ref(), usage) {
                    kv.eviction().set_capacity_hint(u.total_bytes);
                }
                #[cfg(feature = "rocks")]
                if let diskguard::ReclaimAction::ReclaimToFreeBytes(target) = action
                    && let Some(kv) = ev_rocks.as_ref()
                {
                    let ev = kv.eviction();
                    if let Some(u) = usage {
                        let deficit = target.saturating_sub(u.free_bytes);
                        if deficit > 0 {
                            ev.reclaim(deficit);
                        }
                    }
                    // Force the pass only where the floors allow it. They are
                    // what stops this loop -- which runs every couple of
                    // seconds -- from turning into a compaction per sample,
                    // and a compact_ns costs the same whatever it reclaims.
                    let now_ms = flint_storage::strings::system_clock();
                    let namespaces: Vec<String> =
                        EVICTABLE_NS.read().map(|g| g.clone()).unwrap_or_default();
                    for ns in namespaces {
                        if ev.should_force_pass(ns.as_bytes(), now_ms) {
                            kv.compact_ns(ns.as_bytes());
                        }
                    }
                }
                last = v;
                slept = match usage {
                    Some(u) => diskguard::pace(prev_free, u, thresholds, slept, every),
                    // An unreadable filesystem is never treated as fullness
                    // (see `verdict`), and pacing off a reading we do not
                    // have would be inventing one. Fall back to the ceiling.
                    None => every,
                };
                prev_free = usage.map(|u| u.free_bytes);
                std::thread::sleep(slept);
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
                        Arc::clone(&watch),
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

    // RE-DERIVE THE HEADROOM THRESHOLD FROM WHAT IS ACTUALLY BEING WRITTEN.
    //
    // The threshold is a byte budget expressed in SEQUENCES, and one sequence
    // is one write, so the conversion depends on record size -- which is a
    // property of the workload and cannot be known at startup. Started from
    // the 16 KiB assumption, it is wrong by the ratio at every other size:
    // ~16x tight at the 1 KiB the call site tells operators to expect.
    //
    // Only when the threshold was DERIVED. `--wal-headroom-seq` and
    // `FLINTCONFIG wal-headroom-seq` both clear HEADROOM_AUTO, because an
    // operator who names a number has overridden the policy.
    #[cfg(feature = "rocks")]
    if let Some(rk) = rocks.clone() {
        let hub_re = Arc::clone(&hub);
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(30));
                if !HEADROOM_AUTO.load(Ordering::Relaxed) {
                    continue;
                }
                let observed = rk.bytes_per_seq();
                if observed == 0 {
                    continue; // nothing written yet: keep the assumption
                }
                let budget_mb = WAL_BUDGET_MB.load(Ordering::Relaxed);
                hub_re.set_wal_headroom_shed_seq(repl_hub::headroom_shed_seq_for(
                    budget_mb, observed,
                ));
            }
        });
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

    // LEASE SELF-RENEWAL (ADR-0018). The write lease used to be PUSHED by the
    // controller (FLINTLEASE per tick), which anchored the fence to the least
    // available component in the system: five scale runs in a row turned some
    // flavour of controller silence into a fleet-wide self-fence of healthy
    // masters (#168, #171, #172). The master now renews its OWN lease against
    // the control plane — 3-seat Raft, already dialled for the journal —
    // asking the only question that matters: "has a promotion been recorded
    // over me?" (CPFENCE writes that record before any FLINTPROMOTE).
    //
    //   OK          -> extend the deadline by the TTL.
    //   SUPERSEDED  -> a successor is on record: fence NOW, not at expiry.
    //   unreachable -> deadline unchanged; the watchdog above fences at TTL,
    //                  because while the CP is unreachable we cannot rule out
    //                  a promotion committed on the other side of the split.
    //
    // Arming matches the old semantics exactly: the deadline stays 0 (never
    // fences) until the FIRST successful renewal, so standalone and
    // CP-less fleets behave as before. Renewal stops while read-only — a
    // replica never held the lease, and a fenced ex-master must stay fenced
    // (its way back is FLINTPROMOTE, which resets the deadline).
    // --lease-cp: the CP seats renewals may dial, comma-separated. The
    // journal target is ONE seat, and a renewer pinned to one seat dies
    // with it: kill exactly that seat (it is usually cp[0], which is also
    // usually the first elected leader) and every renewal is a connection
    // refused — no -LEADER reply to follow — until the fleet fences at
    // TTL. Found by ctl_cpha the moment nodes carried leases. flintctl
    // passes the full seat list; absent, the journal target is the list.
    let lease_cps: Vec<String> = arg("--lease-cp")
        .map(|v| v.split(',').map(str::to_string).collect())
        .or_else(|| JOURNAL_TARGET.get().cloned().flatten().map(|t| vec![t]))
        .unwrap_or_default();
    if lease_ttl_ms > 0 && !lease_cps.is_empty() {
        let read_only = Arc::clone(&read_only);
        let lease_deadline = Arc::clone(&lease_deadline);
        let me = SELF_ADDR.get().cloned().unwrap_or_default();
        std::thread::spawn(move || {
            let every = std::time::Duration::from_millis((lease_ttl_ms / 3).max(200));
            // After a FAILED attempt, retry fast instead of burning a whole
            // ttl/3 slot. The failure that matters here is a routine CP
            // leader election (a seat roll, a leader crash): renewals bounce
            // for the ~1-2s the election takes, and at ttl/3 cadence a
            // 3000ms TTL tolerates only TWO consecutive misses before a
            // HEALTHY master fences — on a controller-less fleet,
            // permanently (found by ctl_cpha once nodes carried leases).
            // Fast retry turns an election into a blip well inside the TTL.
            // The TTL itself is untouched: this narrows the miss window,
            // it does not loosen the fence.
            let retry = std::time::Duration::from_millis(250).min(every);
            let mut last_failed = false;
            loop {
                std::thread::sleep(if last_failed { retry } else { every });
                if read_only.load(Ordering::Relaxed) {
                    last_failed = false;
                    continue;
                }
                // Rotate across the seats until one ANSWERS (OK or
                // SUPERSEDED both count — a definitive reply from the
                // leader, reached directly or via one -LEADER hop). Only
                // "every seat unreachable" is Unreachable.
                let mut verdict = LeaseRenewal::Unreachable;
                for cp in &lease_cps {
                    match cp_lease_renew(cp, &me) {
                        LeaseRenewal::Unreachable => continue,
                        v => {
                            verdict = v;
                            break;
                        }
                    }
                }
                match verdict {
                    LeaseRenewal::Ok => {
                        last_failed = false;
                        lease_deadline.store(
                            flint_storage::strings::system_clock() + lease_ttl_ms,
                            Ordering::Relaxed,
                        );
                    }
                    LeaseRenewal::Superseded(successor) => {
                        read_only.store(true, Ordering::Relaxed);
                        eprintln!(
                            "lease superseded by {successor}: a promotion is on record — self-fenced to read-only"
                        );
                        // NAME THE SUCCESSOR IN THE JOURNAL, not only on stderr
                        // (BUG-0065). The 2026-08-27 canary abort printed this
                        // row with the fixed string below and nothing else, and
                        // the whole question — did the CP name this pair's own
                        // stale record, or the OTHER pair's fresh promotion? —
                        // was answerable from the successor and from nothing
                        // else in the artifact. stderr is per-seat and dies
                        // with the box; the journal is what the gate prints.
                        journal_event(
                            flint_journal::EventKind::SelfFenced,
                            None,
                            &superseded_cause(&successor),
                        );
                    }
                    LeaseRenewal::Unreachable => {
                        last_failed = true;
                    }
                }
            }
        });
    }

    // The GC sweeper (#133): the compaction filter reclaims expired
    // METADATA rows, but a filter sees one row at a time and cannot look up
    // a subkey row's metadata — so the bodies of expired collections are
    // reclaimable only by this pass. Unwired, they leak forever: a slow,
    // unbounded loss of the very disk the guard is trying to protect.
    //
    // Master-only per ITERATION, not at spawn: a replica must never write
    // to its own store (the master's swept deletes replicate through the
    // WAL), and roles change at runtime — a node promoted at 3am starts
    // sweeping on its next tick without a restart. Every delete runs under
    // the same per-key write lock every writer takes, with the judgment
    // re-run inside it, so a key revived mid-sweep is spared (the race is
    // pinned by a test in gc.rs).
    {
        let store = Arc::clone(&store);
        let read_only = Arc::clone(&read_only);
        std::thread::spawn(move || {
            let mut since_ms: u64 = 0;
            loop {
                std::thread::sleep(std::time::Duration::from_secs(1));
                let cadence = GC_SWEEP_MS.load(Ordering::Relaxed);
                since_ms += 1000;
                if cadence == 0 || since_ms < cadence || read_only.load(Ordering::Relaxed) {
                    continue;
                }
                since_ms = 0;
                let report = flint_storage::gc::sweep(
                    store.as_ref(),
                    flint_storage::strings::system_clock(),
                    &|ns, key| Box::new(write_lock::lock_key(ns, key)),
                );
                GC_EXPIRED_TOTAL.fetch_add(report.expired_meta, Ordering::Relaxed);
                GC_ORPHANS_TOTAL.fetch_add(report.orphan_rows, Ordering::Relaxed);
                if report.expired_meta + report.orphan_rows > 0 {
                    eprintln!(
                        "gc sweep: reclaimed {} expired meta row(s), {} orphaned subkey row(s)",
                        report.expired_meta, report.orphan_rows
                    );
                }
            }
        });
    }
    // Everything the store needs is up: take the listener back (#176). The
    // order is the whole point — clear the flag, then wait for the loading
    // acceptor to actually be gone, THEN go blocking and serve. A node that
    // announced itself ready while a second loop was still answering
    // `-LOADING` from the same socket would be a worse signal than the closed
    // port this replaced.
    LOADING.store(false, Ordering::SeqCst);
    let _ = loading_acceptor.join();
    listener.set_nonblocking(false)?;
    eprintln!("flint-server serving on {bind}:{port}");
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        // Nagle OFF, or a pipelining client pays a delayed ACK per round trip.
        //
        // The node reads 16 KiB at a time and answers each read, so a client
        // whose pipeline is larger gets a small reply written while its own
        // send is still in flight -- precisely the case Nagle holds, and the
        // peer then waits out its delayed-ACK timer. Measured on loopback,
        // direct to the node, waiting for replies, 1 KiB values:
        //
        //   depth 12 -> 137,597 writes/s        depth 16 ->    323 writes/s
        //
        // and past the cliff the rate is exactly LINEAR in depth -- 20 round
        // trips a second at every depth from 16 to 512, a flat ~50ms each.
        // The cliff tracks BYTES, not commands: at 32 B values it moves to
        // depth 256, which is the same ~16 KiB. With this line, depth 256
        // goes 5,106 -> 317,940 writes/s on one connection.
        //
        // This is what BUG-0078 was. The proxy looked like the culprit only
        // because it issues ~16 KiB per backend hop, which put it exactly in
        // the collapsed regime; the "direct" baseline it was measured against
        // was paying the same stall, amortised over a larger batch.
        //
        // FLINT_NAGLE_TEST exists so the drill can ARM its positive control.
        // Nothing else may set it: it re-creates a 60x throughput defect.
        if std::env::var_os("FLINT_NAGLE_TEST").is_none() {
            let _ = stream.set_nodelay(true);
        }
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
        let watch = Arc::clone(&watch);
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
                &watch,
            );
        });
    }
    Ok(())
}

/// Accept and answer connections while the node is still coming up (#176).
///
/// Runs on the SAME listener the real loop will take over, from just after
/// the bind until [`LOADING`] clears. It exists because the alternative — the
/// port staying shut through a fresh replica's initial full sync — is not
/// "looks unhealthy": at the TCP layer a syncing node is indistinguishable
/// from a dead one, which is the strongest wrong signal available. `flintctl
/// start` read it as absent and replaced a seat that was busy syncing
/// (#139's class); the controller could not tell it from a corpse (#189);
/// `verify` called the pair single-copy.
///
/// Polls rather than blocks so it can exit on the flag alone — see the
/// handover comment at the call site for why a wake-up connection is not
/// used. The 20ms cadence bounds the handover, and nothing latency-sensitive
/// runs here: a client that reaches this loop is by definition about to be
/// told to come back.
fn accept_while_loading(
    listener: &TcpListener,
    tls: Option<&Arc<flint_tls::ReloadableServerConfig>>,
) {
    while LOADING.load(Ordering::SeqCst) {
        let (stream, _) = match listener.accept() {
            Ok(pair) => pair,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(20));
                continue;
            }
            Err(_) => continue,
        };
        // A socket accepted from a non-blocking listener inherits O_NONBLOCK
        // on the BSDs (macOS included) and does not on Linux. Say which one
        // we want rather than inherit a platform's answer — a non-blocking
        // stream would turn every read on this connection into a spin.
        if stream.set_nonblocking(false).is_err() {
            continue;
        }
        // The cap applies here too: a connection storm against a node that
        // is merely syncing must not exhaust its threads either.
        if ACTIVE_CONNS.fetch_add(1, Ordering::Relaxed) >= MAX_CONNS.load(Ordering::Relaxed) {
            ACTIVE_CONNS.fetch_sub(1, Ordering::Relaxed);
            CONNS_SHED.fetch_add(1, Ordering::Relaxed);
            drop(stream);
            continue;
        }
        let tls = tls.and_then(|r| r.current());
        std::thread::spawn(move || {
            let _conn_guard = ConnGuard;
            let Ok(conn) = flint_tls::accept(stream, &tls) else {
                return;
            };
            let _ = serve_loading(conn);
        });
    }
}

/// One connection's worth of the `-LOADING` state (#176).
///
/// Deliberately not `serve` with a flag: there is no store, no replication
/// hub and no manifest to consult yet, so this answers only what is TRUE
/// without them — the connection is up, the build is this, and the node is
/// not ready — and refuses everything else with `-LOADING`.
///
/// FLINTNS is refused with the rest, and that is what makes the proxy safe
/// with no change of its own: the proxy pins every backend connection to a
/// namespace before any data command can travel on it, so a node in this
/// state fails the handshake and lands in the existing dead-backend path
/// (replica reads fall back to the master; a keyed call rediscovers).
fn serve_loading(mut stream: flint_tls::Stream) -> std::io::Result<()> {
    let started = std::time::Instant::now();
    let mut buf: Vec<u8> = Vec::with_capacity(4 * 1024);
    let mut chunk = [0u8; 4 * 1024];
    let mut out: Vec<u8> = Vec::with_capacity(1024);
    let mut proto = flint_resp::Proto::default();
    loop {
        let mut consumed = 0;
        out.clear();
        while consumed < buf.len() {
            let pending = &buf[consumed..];
            // Inline commands as well as RESP arrays: `redis-cli`, a bare
            // telnet and flintctl's edge probe all speak the inline form,
            // and being unreachable to them is the bug being fixed.
            let args = if pending[0] == b'*' {
                match decode(pending) {
                    Ok(Decoded::Complete(frame, used)) => {
                        consumed += used;
                        match frame_to_args(frame) {
                            Some(a) => a,
                            None => return Ok(()),
                        }
                    }
                    Ok(Decoded::NeedMore) => break,
                    Err(_) => return Ok(()),
                }
            } else {
                let Some(nl) = pending.iter().position(|&b| b == b'\n') else {
                    if pending.len() > MAX_INLINE_LEN {
                        return Ok(());
                    }
                    break;
                };
                let line = pending[..nl].strip_suffix(b"\r").unwrap_or(&pending[..nl]);
                consumed += nl + 1;
                line.split(|&b| b == b' ')
                    .filter(|p| !p.is_empty())
                    .map(<[u8]>::to_vec)
                    .collect()
            };
            if args.is_empty() {
                continue;
            }
            // The node became ready under this connection. It must not keep
            // answering -LOADING — that would be a lie told forever, and the
            // client would retry against a node that is serving everyone
            // else. There is no store reachable from here to switch to, so
            // close: RESP has no "reconnect" signal, and a closed connection
            // is one every client already handles. Only connections opened
            // DURING the sync can be here, and they are already retrying.
            if !LOADING.load(Ordering::SeqCst) {
                if !out.is_empty() {
                    let _ = stream.write_all(&out);
                }
                return Ok(());
            }
            let name = args[0].to_ascii_uppercase();
            let reply = match name.as_slice() {
                // Liveness. Answering it is the whole point: PING is what
                // every supervisor, controller and readiness wait asks.
                b"PING" => match args.len() {
                    1 => Value::Simple("PONG".into()),
                    2 => Value::Bulk(Some(args[1].clone())),
                    _ => Value::Error("ERR wrong number of arguments for 'ping' command".into()),
                },
                // Protocol negotiation mutates nothing but this connection,
                // so it is answerable without a store — and refusing it
                // would only hide the -LOADING behind a handshake failure.
                b"HELLO" => match flint_resp::parse_hello(&args) {
                    Ok(req) => {
                        if let Some(p) = req.proto {
                            proto = p;
                        }
                        flint_resp::hello_reply(
                            proto,
                            flint_build::wire(&build_version()),
                            "loading",
                        )
                    }
                    Err(e) => e.reply(),
                },
                // Progress, so "not ready" is observable and bounded rather
                // than merely asserted. `role:loading` is a value no
                // consumer mistakes for a master or a healthy replica.
                b"FLINTINFO" => Value::Bulk(Some(
                    format!(
                        "role:loading\r\nloading:1\r\nloading_ms:{}\r\nbuild:{}\r\n",
                        started.elapsed().as_millis(),
                        build_version(),
                    )
                    .into_bytes(),
                )),
                b"QUIT" => {
                    encode_proto(&Value::Simple("OK".into()), proto, &mut out);
                    let _ = stream.write_all(&out);
                    return Ok(());
                }
                _ => Value::Error(LOADING_ERR.into()),
            };
            encode_proto(&reply, proto, &mut out);
            // Flush a long pipeline incrementally, exactly as `serve` does.
            // Refusals are small, but a client may pipeline up to the query
            // buffer's whole gigabyte and each refused command still owes a
            // reply — buffering all of them is a multiple of the input, on a
            // node that is already busy pulling a checkpoint.
            if out.len() >= OUT_FLUSH_THRESHOLD {
                stream.write_all(&out)?;
                out.clear();
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
            return Ok(());
        }
    }
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

/// A run of consecutive PURE writes from one connection's pipeline, held
/// until something forces it out (ADR-0027 part 2).
///
/// Each command still dispatches individually against `kv`, so it computes
/// its own exact reply -- `BatchingKv` overlays reads and scans, so nothing
/// observes a half-applied run. What is deferred is only the ENGINE write:
/// the whole run commits as one `WriteBatch`, which is one grouped WAL append
/// and one write-group join instead of N of each. N of each is the measured
/// ceiling (~80k seq/s, `WriteThread::AwaitState` the top profile symbol).
///
/// The guards are held for the batch's whole life, not per command. They are
/// SHARED stripe guards, so two connections batching at once do not exclude
/// each other -- that is the entire point of part 1, and the reason this can
/// exist at all where the async queue's `lock_all()` per batch could not.
///
/// Replies are held rather than written. Nothing may reach the client before
/// the commit that makes it true, so a failed commit rewrites them as errors
/// instead of having to un-send a `+OK`.
struct PendingBatch<'a> {
    kv: flint_storage::batch::BatchingKv<'a>,
    /// ONE `GLOBAL.read()` for the whole run. Never per key: `RwLock` is
    /// writer-preferring, so a batch that re-acquired it would block against
    /// a `lock_all()` that is itself waiting for the batch to finish. See
    /// `write_lock::lock_global_shared`.
    _global: std::sync::RwLockReadGuard<'static, ()>,
    /// Stripe guards, each taken AT MOST ONCE -- `held` is what makes that
    /// true, and re-entering one stripe would reproduce the same deadlock a
    /// level down.
    _stripes: Vec<std::sync::RwLockReadGuard<'static, ()>>,
    held: std::collections::HashSet<usize>,
    /// One `WriteInFlight` per staged write, held until the batch COMMITS.
    ///
    /// `execute` only STAGES a batched write, so a guard scoped to that call
    /// stops the clock before the commit it exists to measure -- and the
    /// deadline gate is exactly `inflight x service`. Measured on a
    /// c7i.4xlarge, same binary and same load: a plain (batched) SET reported
    /// `write_service_us:7` against 11-15 for `SET .. EX`, which is not
    /// batchable and so keeps its commit inside the window -- while wall
    /// clock for both was 0.32s. Identical real cost, half the measured cost.
    /// The missing half was the commit, and it made the gate under-read: on
    /// this box the write_deadline drill could no longer arm its positive
    /// control at any rung, having armed at 128 threads before ADR-0027.
    _inflight: Vec<WriteInFlight>,
    replies: Vec<Value>,
}

/// How many pure writes may accumulate before the batch is forced out.
///
/// A bound on the stripe guards held at once: an RMW writer of any of those
/// keys waits behind the whole batch.
///
/// It is NOT a bound on the size of a WAL sequence, though this comment used
/// to say so. RocksDB numbers sequences PER KEY, so a 256-key WriteBatch
/// advances `latest_seq` by 256, not by one, and bytes-per-sequence is the
/// size of a single write whether or not it was batched. Measured directly,
/// batchable `SET` against non-batchable `SET .. EX` at depths 1 to 256:
/// 1024 bytes per sequence in every arm at 1 KiB values, and across value
/// sizes it is exactly the value size (32 -> 32, 1024 -> 1024, 16384 ->
/// 16384). Batching does not move the WAL headroom guard's denominator, and
/// the hazard this comment claimed does not exist.
const MAX_PURE_BATCH: usize = 256;

impl<'a> PendingBatch<'a> {
    fn new(store: &'a dyn Kv) -> Self {
        Self {
            kv: flint_storage::batch::BatchingKv::new(store),
            _global: write_lock::lock_global_shared(),
            _stripes: Vec::new(),
            held: std::collections::HashSet::new(),
            _inflight: Vec::new(),
            replies: Vec::new(),
        }
    }

    /// Take this key's stripe if the batch does not already hold it.
    fn hold_stripe(&mut self, ns: &[u8], key: &[u8]) {
        let idx = write_lock::stripe_for(ns, key);
        if self.held.insert(idx) {
            self._stripes.push(write_lock::lock_stripe_pure(ns, key));
        }
    }
}

/// May this command join a deferred pure-write batch?
///
/// Everything here is a REFUSAL to batch, and each one falls through to the
/// unchanged single-command path. Deferring a write is only safe when nothing
/// else can observe the gap between its reply and its commit:
///
/// - a PURE write, by `is_pure_write` -- anything that reads its own key must
///   hold the stripe exclusively and cannot share a batch;
/// - a master. On a replica the write path is suppressed entirely;
/// - no MULTI open, and no WATCH armed. A transaction is already a batch with
///   its own atomicity, and a watch is a promise about when a change becomes
///   visible -- neither should have a second deferral layered under it;
/// - not routed to the async write queue. That is a different batching path
///   with a different lock discipline (`lock_all()` per batch); the two must
///   never interleave;
/// - a rocks engine, because the whole benefit is `apply_writes` collapsing
///   the run into one WAL append. The mem engine would be correct and
///   pointless.
#[allow(clippy::too_many_arguments)]
fn batch_eligible(
    upper_name: &[u8],
    args: &[Vec<u8>],
    ro: bool,
    txn: &TxnState,
    rocks: &Option<RocksHandle>,
    write_queue: Option<&Arc<write_queue::WriteQueue>>,
    conn_async: bool,
    conn_ns: &[u8],
) -> bool {
    if ro || rocks.is_none() || !is_pure_write(upper_name, args) {
        return false;
    }
    if txn.open.is_some() || !txn.watches.is_empty() {
        return false;
    }
    if let Some(q) = write_queue
        && (conn_async || q.wants(conn_ns))
    {
        return false;
    }
    true
}

/// Commit a pending batch and return its replies, in order.
///
/// On failure every reply becomes an error: the run committed as one
/// `WriteBatch`, so it landed entirely or not at all, and saying `+OK` to any
/// of it would be a lie. This is only expressible because the replies were
/// never written -- see `PendingBatch`.
fn commit_pending(
    store: &dyn Kv,
    rocks: &Option<RocksHandle>,
    watch: &Arc<flint_storage::watch::WatchTable>,
    batch: Option<PendingBatch<'_>>,
) -> Vec<Value> {
    let Some(b) = batch else { return Vec::new() };
    let PendingBatch {
        kv,
        _global,
        _stripes,
        held: _,
        _inflight,
        replies,
    } = b;
    let ops = kv.into_ops();
    if ops.is_empty() {
        return replies;
    }
    // BUMP THE WATCH TABLE FOR EVERY KEY IN THE RUN.
    //
    // `WatchedKv` bumps on put/delete, which is how a write records itself
    // against a WATCH -- but a batched write never reaches it: it buffers in
    // BatchingKv, and `commit_ops` then calls `RocksKv::apply_writes`
    // DIRECTLY, under the wrapper. Without this the change is invisible to a
    // watcher and EXEC commits when it must abort.
    //
    // Found by proxy_conformance, and only through the proxy: on one
    // connection WATCH leaves `txn.watches` non-empty so `batch_eligible`
    // refuses, while the proxy puts the write on a different backend
    // connection whose watches are empty. The corpus case is explicit that
    // "WATCH tracks the key, not the author".
    //
    // Bumped BEFORE the commit deliberately. A spurious bump can only abort a
    // transaction that might have committed, which WATCH permits -- it is
    // optimistic. A MISSED bump commits one that must abort, which it does
    // not.
    for (k, _) in &ops {
        watch.bump(k);
    }
    let n = replies.len();
    let out = match commit_ops(store, rocks, &ops) {
        Ok(()) => replies,
        Err(e) => (0..n)
            .map(|_| Value::Error(format!("ERR batch commit failed: {e}")))
            .collect(),
    };
    // The guards drop HERE, after the commit -- they are what kept an RMW
    // writer of these keys out while the batch was computing against values it
    // had not yet written.
    drop(_stripes);
    drop(_global);
    // AFTER the commit, for the same reason: each staged write was in flight
    // from its arrival to here, and its service time is what the next
    // arrival is judged against.
    drop(_inflight);
    out
}

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
    watch: &Arc<flint_storage::watch::WatchTable>,
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
    // Connection-scoped transaction (ADR-0012). None = no MULTI open. It
    // dies with the connection, which is what makes a transaction unable to
    // survive a failover half-applied.
    let mut conn_txn = TxnState::default();
    loop {
        let mut consumed = 0;
        out.clear();
        // ADR-0027 part 2: a run of consecutive pure writes accumulates here
        // and commits as ONE WriteBatch. Scoped to a single read's worth of
        // pipeline, which is the unit a client actually sends.
        let mut pending_batch: Option<PendingBatch> = None;
        // Commit whatever has accumulated and write its replies. Nothing may
        // reach the client before the commit that makes it true, so this runs
        // BEFORE any other reply is encoded and before the connection is
        // handed off or closed.
        macro_rules! flush_pending {
            () => {
                for r in commit_pending(store, &rocks, watch, pending_batch.take()) {
                    encode_proto_flushing(
                        &r,
                        conn_proto,
                        &mut out,
                        OUT_FLUSH_THRESHOLD,
                        &mut |o| {
                            stream.write_all(o)?;
                            o.clear();
                            Ok(())
                        },
                    )?;
                }
            };
        }
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
                // BUG-0060: bound the SUM of concurrent collection reads.
                // `_adm` is dropped at the END of this block -- after the reply
                // has been encoded -- because the materialised reply owns the
                // collection until then, and releasing at `execute`'s return
                // would admit a second read against memory not yet given back.
                let (_adm, refused) = match admit_collection_read(store, limits, &conn_ns, &args) {
                    Ok(guard) => (guard, None),
                    Err(refusal) => (None, Some(refusal)),
                };
                let reply = match refused {
                    Some(r) => r,
                    None => execute(
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
                        &mut conn_txn,
                        watch,
                        &args,
                        None,
                    ),
                };
                // Drain BETWEEN elements instead of only after the whole reply.
                // Serializing a collection into this buffer whole is the second of
                // the two dataset-sized copies behind the +557 MB HGETALL
                // (ADR-0025) -- the threshold already existed here, and nothing
                // gave a reply any way to reach it mid-encode. Small replies are
                // untouched: below the threshold the sink is never called.
                encode_proto_flushing(
                    &reply,
                    conn_proto,
                    &mut out,
                    OUT_FLUSH_THRESHOLD,
                    &mut |o| {
                        stream.write_all(o)?;
                        o.clear();
                        Ok(())
                    },
                )?;
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
                        // A hijack never returns to this loop, so a buffered
                        // batch would be lost with it.
                        flush_pending!();
                        buf.drain(..consumed);
                        stream.write_all(&out)?;
                        return flintsync(stream, rocks, hub, &args);
                    }
                    if args
                        .first()
                        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTFULLSYNC"))
                    {
                        // A hijack never returns to this loop, so a buffered
                        // batch would be lost with it.
                        flush_pending!();
                        buf.drain(..consumed);
                        stream.write_all(&out)?;
                        return flintfullsync(stream, rocks);
                    }
                    if args
                        .first()
                        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTMIGRATEOUT"))
                    {
                        // A hijack never returns to this loop, so a buffered
                        // batch would be lost with it.
                        flush_pending!();
                        buf.drain(..consumed);
                        stream.write_all(&out)?;
                        return migrate::flintmigrateout(stream, rocks, &args);
                    }
                    let upper = args
                        .first()
                        .map(|n| n.to_ascii_uppercase())
                        .unwrap_or_default();
                    if batch_eligible(
                        &upper,
                        &args,
                        read_only.load(Ordering::Relaxed),
                        &conn_txn,
                        &rocks,
                        write_queue,
                        conn_async,
                        &conn_ns,
                    ) {
                        let b = pending_batch.get_or_insert_with(|| PendingBatch::new(store));
                        // The batch owns its locking: GLOBAL once at
                        // construction, this key's stripe now if it is not
                        // already held. `execute` therefore takes no guard of
                        // its own for a batched write.
                        if let Some(k) = commands::command_key(&args) {
                            b.hold_stripe(&conn_ns, k);
                        }
                        // Enter here, not in `execute`: the write is in
                        // flight until the batch commits, not until it is
                        // staged.
                        b._inflight.push(WriteInFlight::enter());
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
                            &mut conn_txn,
                            watch,
                            &args,
                            Some(&b.kv),
                        );
                        // A refusal -- shed by the lag cap, the deadline, a
                        // lost lease -- is NOT a batched write. It wrote
                        // nothing, so it is answered now rather than held.
                        if matches!(reply, Value::Error(_)) {
                            flush_pending!();
                            encode_proto_flushing(
                                &reply,
                                conn_proto,
                                &mut out,
                                OUT_FLUSH_THRESHOLD,
                                &mut |o| {
                                    stream.write_all(o)?;
                                    o.clear();
                                    Ok(())
                                },
                            )?;
                        } else {
                            b.replies.push(reply);
                            if b.replies.len() >= MAX_PURE_BATCH {
                                flush_pending!();
                            }
                        }
                        continue;
                    }
                    // Anything else observes the store, so the batch lands
                    // first: a GET later in the same pipeline must read what
                    // the SETs before it wrote.
                    flush_pending!();
                    // BUG-0060: bound the SUM of concurrent collection reads.
                    // `_adm` is dropped at the END of this block -- after the reply
                    // has been encoded -- because the materialised reply owns the
                    // collection until then, and releasing at `execute`'s return
                    // would admit a second read against memory not yet given back.
                    let (_adm, refused) =
                        match admit_collection_read(store, limits, &conn_ns, &args) {
                            Ok(guard) => (guard, None),
                            Err(refusal) => (None, Some(refusal)),
                        };
                    let reply = match refused {
                        Some(r) => r,
                        None => execute(
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
                            &mut conn_txn,
                            watch,
                            &args,
                            None,
                        ),
                    };
                    // Drain BETWEEN elements instead of only after the whole reply.
                    // Serializing a collection into this buffer whole is the second of
                    // the two dataset-sized copies behind the +557 MB HGETALL
                    // (ADR-0025) -- the threshold already existed here, and nothing
                    // gave a reply any way to reach it mid-encode. Small replies are
                    // untouched: below the threshold the sink is never called.
                    encode_proto_flushing(
                        &reply,
                        conn_proto,
                        &mut out,
                        OUT_FLUSH_THRESHOLD,
                        &mut |o| {
                            stream.write_all(o)?;
                            o.clear();
                            Ok(())
                        },
                    )?;
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
        // The pipeline is drained. THIS is the commit that matters: a run of
        // pure writes ending the pipeline has no following command to force
        // it out, and its replies have not been written. Both happen here,
        // before anything reaches the client.
        flush_pending!();
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

/// A transaction being assembled on ONE connection (ADR-0012).
///
/// Lives on the connection, dies with it. That is not incidental: a
/// promoted master has no queue and no watch state, so a transaction can
/// never survive a failover into a half-applied form — the connection to
/// the dead master goes with it (D8).
/// Everything transactional about ONE connection.
///
/// Watches and the command queue are separate because their lifetimes are:
/// WATCH is issued BEFORE MULTI and must survive until EXEC or DISCARD,
/// while the queue exists only between MULTI and EXEC.
#[derive(Default)]
pub(crate) struct TxnState {
    open: Option<Txn>,
    /// (metadata envelope of a watched key, its stripe value when watched).
    /// Empty is the overwhelmingly common case, so this allocates nothing
    /// for connections that never WATCH.
    watches: Vec<(Vec<u8>, u64)>,
}

impl TxnState {
    /// Every path out of a transaction clears the watches — EXEC whether it
    /// committed or aborted, and DISCARD. Upstream does this and it matters:
    /// a watch left armed would abort the NEXT transaction for a change the
    /// client has already accounted for.
    fn end(&mut self) {
        self.open = None;
        self.watches.clear();
    }

    /// Has anything we watched moved since we watched it?
    fn watches_broken(&self, table: &flint_storage::watch::WatchTable) -> bool {
        self.watches
            .iter()
            .any(|(envelope, seen)| table.version(envelope) != *seen)
    }
}

#[derive(Default)]
pub(crate) struct Txn {
    /// Commands accepted so far, in order.
    queued: Vec<Vec<Vec<u8>>>,
    /// The one slot every key must hash to. None until a key is seen — a
    /// transaction of keyless commands has no slot to violate.
    slot: Option<u16>,
    /// A queue-time error was reported. EXEC must refuse the whole
    /// transaction rather than apply the commands that did parse.
    poisoned: bool,
}

/// MULTI / EXEC / DISCARD, or the queueing of a command inside an open
/// transaction. `None` means this is not transaction business and the
/// caller should carry on.
#[allow(clippy::too_many_arguments)]
fn transaction_control(
    store: &dyn Kv,
    read_only: &Arc<AtomicBool>,
    rocks: &Option<RocksHandle>,
    migration_active: &AtomicBool,
    hub: &ReplHub,
    limits: commands::Limits,
    conn_ns: &[u8],
    conn_txn: &mut TxnState,
    watch: &Arc<flint_storage::watch::WatchTable>,
    args: &[Vec<u8>],
) -> Option<Value> {
    let name = args
        .first()
        .map(|n| n.to_ascii_uppercase())
        .unwrap_or_default();
    match name.as_slice() {
        b"MULTI" => {
            if conn_txn.open.is_some() {
                // Upstream's wording, checked against a live server rather
                // than recalled — it is the generic "not allowed inside a
                // transaction" form, not the older "cannot be nested".
                return Some(Value::Error(
                    "ERR Command 'multi' not allowed inside a transaction".into(),
                ));
            }
            conn_txn.open = Some(Txn::default());
            return Some(Value::Simple("OK".into()));
        }
        // WATCH / UNWATCH. Both are connection state, and WATCH is refused
        // inside a transaction: the point of a watch is to be armed BEFORE
        // the commands that depend on it are queued, so one added midway
        // could only ever describe a window that has already closed.
        b"WATCH" => {
            if conn_txn.open.is_some() {
                return Some(Value::Error(
                    "ERR Command 'watch' not allowed inside a transaction".into(),
                ));
            }
            if args.len() < 2 {
                return Some(Value::Error(
                    "ERR wrong number of arguments for 'watch' command".into(),
                ));
            }
            for key in &args[1..] {
                // A watch records the key's METADATA row, which every
                // mutation of every type writes — including the lazy-expiry
                // and GC deletes that never pass through a command.
                let envelope = flint_storage::encoding::envelope(
                    flint_storage::encoding::Cf::Metadata,
                    conn_ns,
                    flint_slot::slot_for_key(key),
                    key,
                );
                let seen = watch.version(&envelope);
                conn_txn.watches.push((envelope, seen));
            }
            return Some(Value::Simple("OK".into()));
        }
        b"UNWATCH" => {
            conn_txn.watches.clear();
            return Some(Value::Simple("OK".into()));
        }
        b"DISCARD" => {
            let had = conn_txn.open.is_some();
            conn_txn.end();
            return Some(if had {
                Value::Simple("OK".into())
            } else {
                Value::Error("ERR DISCARD without MULTI".into())
            });
        }
        b"EXEC" => {
            let Some(txn) = conn_txn.open.take() else {
                return Some(Value::Error("ERR EXEC without MULTI".into()));
            };
            if txn.poisoned {
                conn_txn.end();
                return Some(Value::Error(
                    "EXECABORT Transaction discarded because of previous errors.".into(),
                ));
            }
            // The optimistic check. A watched key that moved means the
            // world changed under the client's assumptions, so the whole
            // transaction is abandoned and the client retries — reported as
            // a null reply, which is how a Redis client recognises it.
            let broken = conn_txn.watches_broken(watch);
            conn_txn.end();
            if broken {
                return Some(Value::Array(None));
            }
            return Some(exec_transaction(
                store,
                read_only,
                rocks,
                migration_active,
                hub,
                limits,
                conn_ns,
                txn,
            ));
        }
        _ => {}
    }
    // Not a transaction verb. If one is open, this command is queued
    // instead of run.
    let txn = conn_txn.open.as_mut()?;
    if let Some(e) = commands::queue_time_error(args) {
        // Poison and report. The client sees the error now, while it still
        // knows which command it sent; EXEC will refuse the whole batch.
        txn.poisoned = true;
        return Some(e);
    }
    if let Some(key) = commands::command_key(args) {
        let slot = flint_slot::slot_for_key(key);
        match txn.slot {
            None => txn.slot = Some(slot),
            Some(open) if open != slot => {
                txn.poisoned = true;
                return Some(Value::Error(format!(
                    "CROSSSLOT Keys in request don't hash to the same slot ({} is slot {}, \
                     the transaction is on slot {}) — use a hash tag such as {{tag}}key \
                     to colocate them",
                    String::from_utf8_lossy(key),
                    slot,
                    open
                )));
            }
            Some(_) => {}
        }
    }
    txn.queued.push(args.to_vec());
    Some(Value::Simple("QUEUED".into()))
}

/// How the admission gates see a unit of work.
///
/// A single command classifies itself. A transaction classifies its WHOLE
/// queue, because a transaction is admitted or refused as one unit — half a
/// transaction is precisely the outcome ADR-0012 exists to prevent. So one
/// write among ten reads makes the unit a write, and it clears the write
/// gates or none of it runs.
#[derive(Clone, Copy)]
pub(crate) struct Work<'a> {
    /// Does anything in the unit mutate? Eager, because every command asks.
    write: bool,
    /// The unit's commands — one for a single command, the whole queue for
    /// a transaction. The other classifications are computed on demand:
    /// each one costs an uppercase copy of the command name, and a healthy
    /// master never asks whether a command frees space (only a shedding
    /// disk does) or whether it is a read (only a replica does). Eager
    /// fields here would put two extra allocations on every command.
    cmds: &'a [&'a [Vec<u8>]],
}

impl<'a> Work<'a> {
    fn new(cmds: &'a [&'a [Vec<u8>]]) -> Self {
        Self {
            write: cmds.iter().any(|c| Self::is_write(c)),
            cmds,
        }
    }

    fn is_write(cmd: &[Vec<u8>]) -> bool {
        cmd.first().is_some_and(|n| commands::is_write_command(n))
    }

    /// True only when EVERY write in the unit frees space. One growing
    /// write is enough to put the whole unit behind the disk guard, because
    /// the unit lands whole or not at all.
    fn frees_space(&self) -> bool {
        self.cmds
            .iter()
            .filter(|c| Self::is_write(c))
            .all(|c| c.first().is_some_and(|n| flint_commands::reduces_space(n)))
    }

    fn reads(&self) -> bool {
        self.cmds.iter().any(|c| {
            c.first()
                .is_some_and(|n| flint_commands::is_read_command(n))
        })
    }
}

/// Gate 1 of 2: may this node do this KIND of work at all right now?
///
/// Role, disk headroom, and a replica's staleness fence — the conditions
/// that depend on nothing but the node's own state. Two callers, ONE
/// implementation: a gate that refuses `SET` but waves through
/// `MULTI; SET; EXEC` is a hole exactly the size of the gate, and before
/// ADR-0012 Phase E every gate here was one (a write on a read-only replica
/// answered `+OK` inside a transaction and wrote nothing).
fn check_node_health(work: Work<'_>, ro: bool) -> Option<Value> {
    if ro && work.write {
        return Some(Value::Error(
            "READONLY You can't write against a read only replica.".into(),
        ));
    }
    // Disk headroom. Space-REDUCING writes stay allowed, because deleting is
    // the only way out and blocking it makes the condition self-sustaining;
    // reads are untouched. Same classifier the proxy uses for the per-tenant
    // quota verdict, so the two planes cannot disagree about what frees
    // space.
    if work.write && DISK.shedding() && !work.frees_space() {
        return Some(Value::Error(diskguard::DISK_FULL_ERROR.into()));
    }
    // R1: a replica self-fences READS once it has lost live contact with the
    // master for longer than the staleness bound. Admin/FLINT* commands are
    // exempt (they are diagnostics, not tenant reads); a fresh replica that
    // has never heard from the master (contact 0) also fences.
    if ro && work.reads() {
        let contact = REPLICA_CONTACT_MS.load(Ordering::Relaxed);
        let now = flint_storage::strings::system_clock();
        let stale = REPLICA_STALE_MS.load(Ordering::Relaxed);
        if contact == 0 || now.saturating_sub(contact) > stale {
            return Some(Value::Error(
                "TRYAGAIN replica out of sync (stale reads fenced); retry — the proxy will route to the master".into(),
            ));
        }
    }
    None
}

/// Gate 2 of 2: may this land HERE, NOW? Slot ownership and the
/// replication backpressure that bounds the RPO — the conditions that
/// depend on the fleet around the node.
///
/// The slot gate reads only the unit's first keyed command, which is sound
/// because D1 already forced every key in a transaction into one slot.
///
/// Also records slot heat, positioned exactly where the single-command path
/// has always recorded it — between the slot gate and the throttles, so a
/// shed write still counts as demand on its slot. Recorded per command, so
/// a transaction weighs what it actually is.
/// The engine's newest sequence, or 0 when there is no engine to ask.
///
/// Feature-split because `admit_write_path` is compiled for both engines
/// while only rocks has a WAL. Returning 0 under `mem` makes the ADR-0022
/// headroom gate inert there by construction — there is no WAL to recycle,
/// so there is nothing for a replica to fall off the back of — rather than
/// by a separate `cfg` around the gate itself, which would drift.
#[cfg(feature = "rocks")]
fn latest_seq_of(rocks: &Option<RocksHandle>) -> u64 {
    rocks.as_ref().map(|kv| kv.latest_seq()).unwrap_or(0)
}
#[cfg(not(feature = "rocks"))]
fn latest_seq_of(_rocks: &Option<RocksHandle>) -> u64 {
    0
}

fn admit_write_path(
    work: Work<'_>,
    ro: bool,
    conn_ns: &[u8],
    rocks: &Option<RocksHandle>,
    migration_active: &AtomicBool,
    hub: &ReplHub,
) -> Option<Value> {
    // Per-slot gate: after a migration, a command for a key in a slot this
    // node no longer owns is redirected with -MOVED; a write to a slot frozen
    // mid-cutover is shed with -TRYAGAIN. Guarded by `migration_active` so
    // ordinary traffic (no overrides) never pays the extra manifest read.
    if migration_active.load(Ordering::Relaxed)
        && let Some(keyed) = work
            .cmds
            .iter()
            .find(|c| commands::command_key(c).is_some())
        && let Some(reply) = migrate::check_slot_gate(rocks, conn_ns, keyed, work.write)
    {
        return Some(reply);
    }
    // Per-slot heat: count every keyed op destined for a slot this node owns
    // (the slot gate above already redirected/shed the ones it doesn't). Done
    // before the throttle/queue branches diverge so async-queued writes are
    // counted too. Cheap (a CRC16 + relaxed add); FLINTSLOTHEAT exposes it for
    // the traffic-balance policy.
    for cmd in work.cmds {
        if let Some(k) = commands::command_key(cmd) {
            heat::record_key(k);
        }
    }
    // Feed the eviction policy from the SAME point, and for the same reason
    // heat is counted here: this is user traffic that reached a slot this node
    // owns. Doing it in the storage layer instead would also catch replication
    // apply and GC scans, and a GC scan registering as access is precisely the
    // pollution S3-FIFO was chosen to avoid — a policy that treats a sweep as
    // interest is the LRU failure wearing a different name.
    //
    // The evictable check comes FIRST and is one relaxed load. Everything
    // costly — building the envelope key, which allocates — happens only for a
    // namespace someone declared. A durable deployment pays the load and
    // nothing else.
    #[cfg(feature = "rocks")]
    if let Some(kv) = rocks.as_ref() {
        let ev = kv.eviction();
        if ev.evictable_count() > 0 {
            for cmd in work.cmds {
                if let Some(k) = commands::command_key(cmd) {
                    let ekey = flint_storage::encoding::envelope(
                        flint_storage::encoding::Cf::Metadata,
                        conn_ns,
                        flint_slot::slot_for_key(k),
                        k,
                    );
                    if work.write {
                        // Approximate: the argument bytes past the key. The
                        // policy's accounting only orders candidates, while the
                        // decision to reclaim comes from the disk.
                        let approx: u64 = cmd.iter().skip(2).map(|a| a.len() as u64).sum();
                        ev.note_write(&ekey, approx);
                    } else {
                        ev.note_read(&ekey);
                    }
                }
            }
        }
    }
    // Lag-cap backpressure: the write path enforces the RPO bound. The
    // min-replicas gate comes first — with no live replica there is no lag
    // to measure, and that widowed state is exactly where accepted writes
    // are most at risk (isolated master, dead pair peer).
    if work.write && !ro {
        let now = flint_storage::strings::system_clock();
        let below = hub.below_write_quorum(now);
        // EDGES ONLY. A refused write happens thousands of times inside one
        // outage; the two instants that BOUND it happen once each, and they are
        // what an RTO number decomposes into. Emitted here rather than at
        // `Promoted` or `Supervised` because neither of those is the outage: the
        // gate shuts as a CONSEQUENCE of a promotion, moments after it, and
        // `Supervised` is a controller OBSERVING convergence on its own poll
        // cadence, well after writes resumed. Between those two the journal was
        // silent, which is why a 9.9 s recovery could not be attributed.
        if let Some(now_below) = hub.note_quorum_transition(below) {
            let live = hub.live_replica_count(now);
            let min = hub.min_replicas_to_write();
            journal_event(
                if now_below {
                    flint_journal::EventKind::WriteQuorumLost
                } else {
                    flint_journal::EventKind::WriteQuorumRestored
                },
                None,
                &format!("live replicas {live}, min-replicas-to-write {min}"),
            );
        }
        if below {
            WRITES_SHED_QUORUM.fetch_add(1, Ordering::Relaxed);
            return Some(Value::Error(
                "THROTTLED live replicas below min-replicas-to-write, retry with backoff".into(),
            ));
        }
        // The widowed grace: the only gate that bounds how OLD the at-risk
        // tail may get. Checked after the quorum gate (which is stricter and
        // shares the cause) and before the lag cap, which cannot fire at all
        // in this state because there is no replica to measure lag against.
        if hub.widowed_beyond_grace_arming(now) {
            WRITES_SHED_WIDOWED.fetch_add(1, Ordering::Relaxed);
            return Some(Value::Error(
                "THROTTLED no live replica for longer than --widowed-grace-ms, retry with backoff"
                    .into(),
            ));
        }
        if let Some(lag) = hub.lag_ms(now) {
            // The peak is recorded BEFORE the gates read it, so a sample that
            // sheds is also a sample that is counted. The gap is read only
            // when this sample RAISES the peak, so the extra `replicas` lock
            // is paid a handful of times per process life instead of on every
            // write — a monotone maximum stops updating almost at once. Two
            // threads raising it together can leave the gap belonging to the
            // lower of the two peaks; for a diagnostic that is the right trade
            // against another lock on the write path.
            if LAG_MS_MAX.fetch_max(lag, Ordering::Relaxed) < lag {
                let acked = hub.effective_acked(now).unwrap_or(0);
                LAG_MAX_GAP.store(
                    latest_seq_of(rocks).saturating_sub(acked),
                    Ordering::Relaxed,
                );
            }
            if lag >= hub.lag_hard_ms() {
                WRITES_SHED_LAG.fetch_add(1, Ordering::Relaxed);
                return Some(Value::Error(
                    "THROTTLED replication lag exceeds limit, retry with backoff".into(),
                ));
            }
            if lag >= hub.lag_soft_ms() {
                WRITES_DELAYED_SOFT.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        }
        // ADR-0022: WAL headroom. Distinct from the lag gates above, which
        // bound the RPO in TIME; this one bounds how far the master may
        // outrun the WAL its slowest live replica still has to read.
        //
        // A replica that loses this race does not degrade, it DIES: the
        // segment holding its cursor is recycled, `updates_since` yields
        // nothing, and the seat exits for a re-seed that re-runs the same
        // race (docs/bugs/0012). Shedding converts an unbounded outage into
        // a bounded, retryable, nameable one — the trade the lag caps
        // already make for the same underlying problem.
        //
        // After the lag gates on purpose: when both would fire, lag is the
        // more familiar cause and the more actionable message.
        if hub.wal_headroom_exhausted(latest_seq_of(rocks), now) {
            WRITES_SHED_HEADROOM.fetch_add(1, Ordering::Relaxed);
            return Some(Value::Error(
                "THROTTLED replica too far behind the retained WAL, retry with backoff".into(),
            ));
        }
        // The deadline gate (#186), last because the gates above can NAME
        // their cause and this one cannot: it is the catch-all for "this node
        // cannot serve the write in time", whatever is making it slow —
        // a re-seed on the disk, a retry burst after a promotion, or simply
        // more offered load than the node has.
        //
        // Decided on ARRIVAL, not on expiry. Little's law says a write
        // arriving now waits about `inflight x service`; if that is already
        // past the deadline, queueing it only adds a deadline's worth of work
        // that will be thrown away, on a node that by construction has none
        // to spare. Refusing keeps the accepted set inside what we can serve.
        let deadline_ms = WRITE_DEADLINE_MS.load(Ordering::Relaxed);
        if deadline_ms > 0 {
            let est_ms = estimated_write_wait_ms();
            // The high-water mark, kept before the refusal test so that a run
            // which never refuses still reports how close it came.
            if est_ms > 0 {
                WRITE_WAIT_PEAK_MS.fetch_max(est_ms, Ordering::Relaxed);
                WRITE_WAIT_PEAK_PACKED.fetch_max(
                    pack_write_wait_peak(
                        est_ms,
                        WRITE_INFLIGHT.load(Ordering::Relaxed),
                        WRITE_COST_US.load(Ordering::Relaxed),
                    ),
                    Ordering::Relaxed,
                );
            }
            if est_ms > deadline_ms {
                WRITES_SHED_DEADLINE.fetch_add(1, Ordering::Relaxed);
                // NAME THE TERMS, not just their product (BUG-0088).
                //
                // `est_ms` is `inflight x service_us`, and the two factors fail
                // for opposite reasons: a high inflight is more offered load
                // than the node can take, a high service time is the node
                // itself having got slow (compaction, a stalled disk, a
                // descheduled runner). The product cannot tell them apart, and
                // they need different responses. Measured CI peaks sit at
                // 124-557 ms while the refusals landed at 2017-2033 ms -- a
                // 4x jump past the observed maximum, which is a spike in ONE of
                // these and the message did not say which.
                let inflight = WRITE_INFLIGHT.load(Ordering::Relaxed);
                let cost_us = WRITE_COST_US.load(Ordering::Relaxed);
                let service_us = WRITE_SERVICE_US.load(Ordering::Relaxed);
                return Some(Value::Error(format!(
                    "THROTTLED write would wait ~{est_ms}ms (inflight {inflight} x cost {cost_us}us; residency {service_us}us), past --write-deadline-ms {deadline_ms}, retry with backoff"
                )));
            }
        }
    }
    None
}

/// Run a transaction's queued commands and commit them as one write.
///
/// The shape is the async write queue's, deliberately (ADR-0012 D3): hold
/// `lock_all()` so no other writer interleaves, dispatch every command
/// against ONE `BatchingKv` so each sees its predecessors' effects, then
/// commit the accumulated buffer in a single engine write. On rocks that
/// is one `WriteBatch`, hence one WAL group, so a replica applies the
/// whole transaction or none of it.
#[allow(clippy::too_many_arguments)]
fn exec_transaction(
    store: &dyn Kv,
    read_only: &Arc<AtomicBool>,
    rocks: &Option<RocksHandle>,
    migration_active: &AtomicBool,
    hub: &ReplHub,
    limits: commands::Limits,
    conn_ns: &[u8],
    txn: Txn,
) -> Value {
    if txn.queued.is_empty() {
        return Value::Array(Some(Vec::new()));
    }
    // D8, and the whole of it: a transaction clears the SAME admission gates
    // a single command clears, evaluated over the queue as one unit and in
    // the same order — so a demoted master, a shedding disk, a slot
    // mid-handoff or an unmet write quorum aborts the transaction instead of
    // half-applying it or, worse, acking a write that went nowhere.
    //
    // Gates run BEFORE the lock, exactly as the single-command path runs
    // them before its write guard. That leaves the same narrow window (a
    // demotion landing between the check and the commit) that a single write
    // has always had, rather than a new one: durable role fencing is what
    // closes it, at the manifest.
    let ro = read_only.load(Ordering::Relaxed);
    let cmds: Vec<&[Vec<u8>]> = txn.queued.iter().map(Vec::as_slice).collect();
    let work = Work::new(&cmds);
    if let Some(refusal) = check_node_health(work, ro) {
        return refusal;
    }
    if let Some(refusal) = admit_write_path(work, ro, conn_ns, rocks, migration_active, hub) {
        return refusal;
    }
    // Held for the whole transaction, so the lock wait and the commit both
    // land in the service-time estimate the next arrival is judged against.
    let _inflight = (work.write && !ro).then(WriteInFlight::enter);

    let _all = write_lock::lock_all();
    let batching = flint_storage::batch::BatchingKv::new(store);
    let mut replies = Vec::with_capacity(txn.queued.len());
    {
        // A replica must not write to its own store; the read-only wrapper
        // turns lazy-expiry deletes buried in read paths into no-ops, the
        // same as the single-command path does.
        let ro_store = flint_storage::ReadOnlyKv(&batching);
        let engine: &dyn Kv = if ro { &ro_store } else { &batching };
        for cmd in &txn.queued {
            replies.push(
                Dispatcher::with_limits(
                    engine,
                    flint_storage::strings::system_clock,
                    limits,
                    conn_ns,
                )
                .dispatch(cmd),
            );
        }
    }
    let ops = batching.into_ops();
    if !ops.is_empty()
        && let Err(e) = commit_ops(store, rocks, &ops)
    {
        return Value::Error(format!("ERR transaction commit failed: {e}"));
    }
    Value::Array(Some(replies))
}

/// Commit a transaction's buffered mutations.
///
/// On rocks this is one `WriteBatch` — atomic at the engine and a single
/// WAL group, which is what gives replicas all-or-nothing. The mem engine
/// has no batch primitive, so there the transaction's atomicity rests on
/// `lock_all()` alone; mem is the dev/test engine and rocks is what ships,
/// and D6 already declines to promise readers a serial history either way.
/// Observed bytes per WAL sequence; 0 when nothing has been written yet.
///
/// Rocks-only, like its single caller: the mem build's FLINTINFO emits a
/// deliberately minimal surface and has never carried the replication fields.
#[cfg(feature = "rocks")]
fn wal_bytes_per_seq(rocks: &Option<RocksHandle>) -> u64 {
    rocks.as_ref().map_or(0, |r| r.bytes_per_seq())
}

#[cfg(feature = "rocks")]
fn commit_ops(
    store: &dyn Kv,
    rocks: &Option<RocksHandle>,
    ops: &[(Vec<u8>, Option<Vec<u8>>)],
) -> Result<(), String> {
    // FLINT_BATCH_COMMIT_FAIL exists so the failure path can be ARMED.
    //
    // Only `apply_writes` can fail here, and forcing a real RocksDB write
    // error means a full disk or a closed handle -- neither of which a drill
    // can create without becoming a test of something else. So the path that
    // rewrites a whole batch's replies as errors shipped reasoned rather than
    // verified, which is the state every defect found this week was in.
    //
    // Nothing outside the drill may set it: it discards writes the client was
    // about to be told about.
    if std::env::var_os("FLINT_BATCH_COMMIT_FAIL").is_some() {
        return Err("injected commit failure (FLINT_BATCH_COMMIT_FAIL)".into());
    }
    match rocks {
        Some(r) => r.apply_writes(ops).map_err(|e| e.to_string()),
        None => {
            apply_inline(store, ops);
            Ok(())
        }
    }
}

#[cfg(not(feature = "rocks"))]
fn commit_ops(
    store: &dyn Kv,
    _rocks: &Option<RocksHandle>,
    ops: &[(Vec<u8>, Option<Vec<u8>>)],
) -> Result<(), String> {
    apply_inline(store, ops);
    Ok(())
}

/// Replay the buffer onto the store one op at a time — the fallback when
/// there is no engine batch to commit into.
fn apply_inline(store: &dyn Kv, ops: &[(Vec<u8>, Option<Vec<u8>>)]) {
    for (k, v) in ops {
        match v {
            Some(val) => store.put(k, val),
            None => {
                store.delete(k);
            }
        }
    }
}

/// A write that reads NOTHING, and may therefore take its stripe in shared
/// mode (ADR-0027).
///
/// Deliberately the narrowest possible test: `SET key value`, exactly three
/// tokens. Every option that would make SET read its old header -- NX, XX,
/// KEEPTTL, GET, and the EX/PX/EXAT/PXAT family, which `SetExpiry::Keep`
/// reaches -- adds at least one token, so a three-token SET cannot carry any
/// of them. Widening this by NAME rather than by arity is how a lost update
/// gets in: `write_queue::is_batchable` looks like the right predicate and is
/// not, because it admits INCR, DECR, APPEND and SETNX.
///
/// SET's own read became conditional in d3ef6d7; this predicate must stay in
/// step with the condition there (`opts.nx || opts.xx || expiry == Keep`).
fn is_pure_write(upper_name: &[u8], args: &[Vec<u8>]) -> bool {
    upper_name == b"SET" && args.len() == 3
}

/// BUG-0060's node-level bound on the SUM of concurrent collection reads.
/// Built once from `--collection-read-budget-pct`; 0 (the default) disables it
/// and every call below becomes a branch that does nothing.
static READ_ADMISSION: std::sync::OnceLock<flint_storage::admission::ReadAdmission> =
    std::sync::OnceLock::new();

fn read_admission() -> &'static flint_storage::admission::ReadAdmission {
    READ_ADMISSION.get_or_init(|| {
        flint_storage::admission::ReadAdmission::new(
            flint_storage::admission::DEFAULT_COLLECTION_READ_BUDGET_PCT,
            flint_storage::admission::Mode::Enforce,
        )
    })
}

/// Holds a reservation for as long as the reply owns the collection. Dropped
/// AFTER encoding, not when `execute` returns: `encode_proto_flushing` drains
/// the out-buffer incrementally, but the materialised reply still owns every
/// byte until it goes out of scope, and releasing early would let a second
/// read be admitted against memory the first has not given back.
struct AdmissionGuard(u64);

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        read_admission().release(self.0);
    }
}

/// Decide whether this command may run. `Ok(None)` is the ordinary answer:
/// admission is off, or this is not a whole-collection read.
fn admit_collection_read(
    store: &dyn Kv,
    limits: commands::Limits,
    conn_ns: &[u8],
    args: &[Vec<u8>],
) -> Result<Option<AdmissionGuard>, Value> {
    use flint_storage::admission::Admit;
    let adm = read_admission();
    if !adm.enabled() {
        return Ok(None);
    }
    let Some(name) = args.first() else {
        return Ok(None);
    };
    let name_upper = name.to_ascii_uppercase();
    let dispatcher = commands::Dispatcher::with_limits(
        store,
        flint_storage::strings::system_clock,
        limits,
        conn_ns,
    );
    let Some(bytes) = dispatcher.collection_read_bytes(&name_upper, args) else {
        return Ok(None);
    };
    let decision = adm.admit(bytes);
    match decision {
        Admit::Disabled => Ok(None),
        // `WouldRefuse` is an ADMIT: it crossed the budget and observe mode
        // let it through, so it holds a reservation exactly like the others.
        // Grouped here rather than beside `Refused` because what matters at
        // this call site is what happens to the read, not what was noticed
        // about it.
        Admit::Admitted { est }
        | Admit::AdmittedUnmeasured { est }
        | Admit::WouldRefuse { est, .. } => Ok(Some(AdmissionGuard(est))),
        // The message is built in `flint-storage` beside the arithmetic it
        // describes, so the terms it quotes cannot drift from the terms that
        // produced them, and so it can be asserted in a unit test.
        Admit::Refused { .. } => Err(Value::Error(
            decision
                .refusal(bytes, adm.pct())
                .unwrap_or_else(|| "THROTTLED collection read".into()),
        )),
    }
}

/// `batch_kv` / `guard_sink`: ADR-0027 part 2. When a caller is accumulating a
/// deferred pure-write batch, it passes the batch's `BatchingKv` as the engine
/// and a sink for the stripe guard. Everything else here -- health, the
/// admission gates, the in-flight accounting -- runs UNCHANGED, which is the
/// point of threading the batch through this function instead of around it: a
/// batched write must still be shed by the lag cap and the deadline, or the
/// batch would be a hole in the back-pressure this whole path exists for.
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
    conn_txn: &mut TxnState,
    watch: &Arc<flint_storage::watch::WatchTable>,
    args: &[Vec<u8>],
    batch_kv: Option<&dyn Kv>,
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
        // The build, not the crate version — see flint_build::wire.
        let build = build_version();
        return flint_resp::hello_reply(*conn_proto, flint_build::wire(&build), role);
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
    // MULTI / EXEC / DISCARD (ADR-0012). Answered here rather than in the
    // command table for the reason HELLO and FLINTNS are: they mutate THIS
    // connection's state, and the dispatcher is deliberately stateless.
    if let Some(reply) = transaction_control(
        store,
        read_only,
        rocks,
        migration_active,
        hub,
        limits,
        conn_ns,
        conn_txn,
        watch,
        args,
    ) {
        return reply;
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
                field("wal-headroom-seq", hub.wal_headroom_shed_seq().to_string()),
                field(
                    "min-replicas-to-write",
                    hub.min_replicas_to_write().to_string(),
                ),
                field("max-conns", MAX_CONNS.load(Ordering::Relaxed).to_string()),
                field(
                    "migrate-rate-bytes",
                    MIGRATE_RATE_BYTES.load(Ordering::Relaxed).to_string(),
                ),
                field(
                    "fullsync-rate-bytes",
                    FULLSYNC_RATE_BYTES.load(Ordering::Relaxed).to_string(),
                ),
                field(
                    "write-deadline-ms",
                    WRITE_DEADLINE_MS.load(Ordering::Relaxed).to_string(),
                ),
                field(
                    "gc-sweep-ms",
                    GC_SWEEP_MS.load(Ordering::Relaxed).to_string(),
                ),
                // Reported even on a node with no queue, so the absence is a
                // stated value rather than a missing line someone has to
                // interpret. `-` means this build/engine has no queue at all.
                field(
                    "async-writes",
                    write_queue
                        .map(|q| q.scope_desc())
                        .unwrap_or_else(|| "-".to_string()),
                ),
                // Both numbers, because only one of them is tunable and the
                // other is why: the cap cannot be raised past the channel
                // capacity fixed at startup.
                field(
                    "async-queue-cap",
                    write_queue
                        .map(|q| format!("{} (hard {})", q.soft_cap(), q.hard_cap()))
                        .unwrap_or_else(|| "-".to_string()),
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
            // BOTH REFUSE RATHER THAN CLAMP (BUG-0043). The setters keep
            // `soft <= hard` by silently moving the other end, which is right
            // for the config-file path — a fleet should boot on a coherent
            // pair rather than refuse to start. It is wrong for an
            // interactive knob: `FLINTCONFIG lag-hard-ms 200` against the
            // shipped 500ms soft cap returned success and applied 500, and
            // the caller had no way to learn that. A drill ramping a
            // threshold down then tests one value five times while reporting
            // five, which is how a positive control goes green having never
            // armed. Same choice WriteQueue::set_soft_cap already made.
            // ADR-0023 D7.1: hot-reloadable, because the whole point of a
            // seat-side channel is that the decision can be changed without a
            // restart. Set-and-read-back: the value stored is the CANONICAL
            // one (sorted, deduped), so an operator comparing two seats
            // compares the same string rather than two orderings of it.
            b"evictable-ns" => {
                let parsed = parse_evictable_ns(&val);
                match EVICTABLE_NS.write() {
                    Ok(mut g) => *g = parsed,
                    Err(_) => return Value::Error("ERR evictable-ns lock poisoned".into()),
                }
                // The engine authorises the deletes, so it has to hear about
                // this before the next compaction — above all when the change
                // is a revocation. Rocks-only: the mem engine has no
                // compaction filter, so there is nothing that could act on it.
                #[cfg(feature = "rocks")]
                if let Some(kv) = rocks.as_ref() {
                    sync_evictable_to_engine(kv.eviction());
                }
                // The pair may now disagree; say "unknown" until the next
                // check rather than leaving a stale verdict that reads as
                // agreement.
                EVICTABLE_AGREE.store(-1, Ordering::Relaxed);
            }
            b"lag-soft-ms" => {
                let v: u64 = parse!();
                let hard = hub.lag_hard_ms();
                if v > hard {
                    return Value::Error(format!(
                        "ERR lag-soft-ms {v} is above lag-hard-ms {hard}; raise lag-hard-ms first"
                    ));
                }
                hub.set_lag_soft_ms(v)
            }
            b"lag-hard-ms" => {
                let v: u64 = parse!();
                let soft = hub.lag_soft_ms();
                if v < soft {
                    return Value::Error(format!(
                        "ERR lag-hard-ms {v} is below lag-soft-ms {soft}; lower lag-soft-ms first"
                    ));
                }
                hub.set_lag_hard_ms(v)
            }
            // Live-tunable on purpose: the right value depends on value size
            // and on how far replicas actually fall behind under this
            // workload, and neither is knowable before the fleet runs.
            b"wal-headroom-seq" => {
                // An operator naming a number has overridden the policy, not
                // offered a starting point, so the re-derivation stands down
                // for the life of the process.
                HEADROOM_AUTO.store(false, Ordering::Relaxed);
                hub.set_wal_headroom_shed_seq(parse!())
            }
            b"min-replicas-to-write" => hub.set_min_replicas_to_write(parse!()),
            b"widowed-grace-ms" => hub.set_widowed_grace_ms(parse!()),
            b"max-conns" => MAX_CONNS.store(std::cmp::max(1, parse!()), Ordering::Relaxed),
            b"migrate-rate-bytes" => MIGRATE_RATE_BYTES.store(parse!(), Ordering::Relaxed),
            b"fullsync-rate-bytes" => FULLSYNC_RATE_BYTES.store(parse!(), Ordering::Relaxed),
            b"write-deadline-ms" => WRITE_DEADLINE_MS.store(parse!(), Ordering::Relaxed),
            b"gc-sweep-ms" => GC_SWEEP_MS.store(parse!(), Ordering::Relaxed),
            // The queue is constructed on every rocks node whether or not
            // --async-writes was passed, so turning it on is a scope swap and
            // never needs a restart. On a node without one, say so instead of
            // accepting a value that would do nothing.
            b"async-writes" => match write_queue {
                Some(q) => {
                    if let Err(e) = q.set_scope(write_queue::AsyncScope::parse(&val)) {
                        return Value::Error(e);
                    }
                }
                None => {
                    return Value::Error(
                        "ERR async-writes needs a rocks node with a write queue".into(),
                    );
                }
            },
            b"async-queue-cap" => match write_queue {
                Some(q) => {
                    let n: usize = match val.parse() {
                        Ok(v) => v,
                        Err(_) => return Value::Error(format!("ERR bad value {val:?}")),
                    };
                    if let Err(e) = q.set_soft_cap(n) {
                        return Value::Error(e);
                    }
                }
                None => {
                    return Value::Error(
                        "ERR async-queue-cap needs a rocks node with a write queue".into(),
                    );
                }
            },
            other => {
                return Value::Error(format!(
                    "ERR unknown or restart-only config key {:?} (hot: wal-fsync-ms, \
                     lag-soft-ms, lag-hard-ms, wal-headroom-seq, \
                     min-replicas-to-write, widowed-grace-ms, \
                     max-conns, migrate-rate-bytes, fullsync-rate-bytes, \
                     write-deadline-ms, gc-sweep-ms, async-writes, \
                     async-queue-cap)",
                    String::from_utf8_lossy(other)
                ));
            }
        }
        return Value::Simple("OK".into());
    }
    let ro = read_only.load(Ordering::Relaxed);
    let unit = std::slice::from_ref(&args);
    let work = Work::new(unit);
    let is_write = work.write;
    if let Some(refusal) = check_node_health(work, ro) {
        return refusal;
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
        return flintpromote(read_only, tailer_stop, lease_deadline, hub, rocks, args);
    }
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTDEMOTE"))
    {
        return flintdemote(read_only, rocks, args);
    }
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTFOLLOW"))
    {
        return flintfollow(read_only, rocks, args);
    }
    // FLINTFENCE <generation> <counter>: the highest sequence a copy from
    // that role epoch may resume from — the earliest recorded branch point
    // after it (#187). A rejoining ex-master asks this to pick which local
    // snapshot to rewind to. Nil = this node cannot vouch (no promotion
    // recorded after that epoch while its own claim is newer), and the only
    // safe rejoin is a full re-seed.
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTFENCE"))
    {
        return flintfence(rocks, args);
    }
    // FLINTNSRESTORE <ns> <k> <v> [<k> <v> ...]: apply pre-enveloped rows
    // for namespace <ns> as ONE engine batch — the write half of the
    // namespace-scoped restore (ADR-0011 D5). The CALLER routes: placement
    // by current ownership comes from the CP map, which nodes do not hold
    // for tenant namespaces, so this command applies what it is given the
    // way FLINTNS trusts its caller — the data port is the internal
    // surface. What it does NOT trust is the rows themselves: every key
    // must be a well-formed envelope whose embedded namespace is <ns>,
    // because the blast radius of a malformed batch is another tenant's
    // keyspace.
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTNSRESTORE"))
    {
        if ro {
            return Value::Error("READONLY cannot restore into a replica".into());
        }
        return flintnsrestore(rocks, args);
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
    //
    // FLINTMIGRATIONS ALL includes them. Read-only, and the one way to turn
    // "the source did not answer" into a fact about the source rather than an
    // inference from its silence (docs/bugs/0024, docs/bugs/0025).
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTMIGRATIONS"))
    {
        return migrate::flintmigrations(rocks, args);
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
    if let Some(refusal) = admit_write_path(work, ro, conn_ns, rocks, migration_active, hub) {
        return refusal;
    }
    // Counted from here to the end of the call, so a write that blocks on the
    // async queue's consumer or on a contended stripe is measured as the slow
    // write it is — those waits are exactly what the deadline is about.
    // `batch_kv.is_some()` means this call only stages; the batch holds the
    // guard across its commit instead, so entering here would double-count
    // the write and still stop the clock too early.
    let _inflight = (is_write && !ro && batch_kv.is_none()).then(WriteInFlight::enter);
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
    let write_guard = if is_write && !ro && batch_kv.is_none() {
        let name = args
            .first()
            .map(|n| n.to_ascii_uppercase())
            .unwrap_or_default();
        let multi = (name == b"MSET" && args.len() > 3)
            || ((name == b"DEL" || name == b"UNLINK") && args.len() > 2);
        match (multi, commands::command_key(args)) {
            // ADR-0027: a PURE write reads nothing, so it excludes the
            // read-modify-write writers of its key but not other pure writes.
            (false, Some(k)) if is_pure_write(&name, args) => {
                Some(write_lock::lock_key_pure(conn_ns, k))
            }
            (false, Some(k)) => Some(write_lock::lock_key(conn_ns, k)),
            _ => Some(write_lock::lock_all()),
        }
    } else {
        None
    };
    // A BATCHED write takes no guard here: the batch holds GLOBAL once for
    // its whole run and this key's stripe already (see `PendingBatch`).
    // Acquiring GLOBAL.read() again per key is the deadlock this design was
    // corrected for -- writer-preferring RwLock blocks the second acquisition
    // behind a lock_all() that is waiting on the batch itself.
    let _write_guard = write_guard;
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
        // The batch's BatchingKv when one is accumulating, the real store
        // otherwise. BatchingKv overlays reads and scans, so this command
        // still computes its exact reply against everything the run has
        // written so far.
        let engine: &dyn Kv = batch_kv.unwrap_or(store);
        Dispatcher::with_limits(
            engine,
            flint_storage::strings::system_clock,
            limits,
            conn_ns,
        )
        .dispatch(args)
    }
}

/// The apply half of FLINTNSRESTORE (see the dispatch comment). One
/// `apply_writes` per invocation: one WriteBatch, one WAL group, so a
/// replica tailing this master applies the batch whole.
#[cfg(feature = "rocks")]
fn flintnsrestore(rocks: &Option<RocksHandle>, args: &[Vec<u8>]) -> Value {
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTNSRESTORE requires the rocks engine".into());
    };
    let (Some(ns), rows) = (args.get(1), &args[2..]) else {
        return Value::Error("ERR FLINTNSRESTORE <ns> <key> <value> [...]".into());
    };
    if rows.is_empty() || !rows.len().is_multiple_of(2) {
        return Value::Error("ERR FLINTNSRESTORE takes key/value pairs".into());
    }
    let mut ops = Vec::with_capacity(rows.len() / 2);
    for pair in rows.chunks(2) {
        let key = &pair[0];
        // Envelope: cf(1) | ns_len(1) | ns | slot(2 BE) | user_key. The
        // namespace check is the tenant boundary; the shape checks keep a
        // truncated frame from becoming a row that no scan will ever find
        // (or worse, a row in whatever namespace the garbage bytes spell).
        let valid = key.len() >= 4
            && matches!(key[0], b'M' | b'S' | b'Z')
            && key.get(1).is_some_and(|&l| {
                key.len() >= 2 + l as usize + 2 && &key[2..2 + l as usize] == ns.as_slice()
            });
        if !valid {
            return Value::Error(format!(
                "ERR row is not a well-formed envelope for namespace {:?} — nothing applied",
                String::from_utf8_lossy(ns)
            ));
        }
        ops.push((key.clone(), Some(pair[1].clone())));
    }
    match kv.apply_writes(&ops) {
        Ok(()) => Value::Simple(format!("OK {} rows", ops.len())),
        Err(e) => Value::Error(format!("ERR restore batch: {e}")),
    }
}

#[cfg(not(feature = "rocks"))]
fn flintnsrestore(_rocks: &Option<RocksHandle>, _args: &[Vec<u8>]) -> Value {
    Value::Error("ERR FLINTNSRESTORE requires a build with --features rocks".into())
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
    lease_deadline: &Arc<std::sync::atomic::AtomicU64>,
    hub: &Arc<repl_hub::ReplHub>,
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
    // The branch point, recorded BEFORE the role flips: everything this node
    // has applied up to now is shared history; everything after is a new
    // timeline the superseded master never had. A rejoining ex-master may
    // rewind to a local snapshot at seq <= this fence and tail incrementally
    // instead of full re-seeding (#187). Recording first means a crash
    // between the two writes leaves a fence for an epoch never claimed —
    // which only ever makes a later bound MORE conservative.
    //
    // last_applied, NOT latest_seq: sequence numbers are node-local, and the
    // rejoiners this fence exists for present positions in the SUPERSEDED
    // stream's space — which is exactly what last_applied tracks on the
    // replica being promoted. latest_seq is this node's own space, drifted
    // ahead by one cursor row per applied batch; recording it made the fence
    // comparison cross-space and accidentally permissive (soak run 30). A
    // same-node demote/re-promote records a stale last_applied — harmlessly
    // conservative: rejoins it cannot vouch for fall back to a full sync.
    //
    // The lineage recorded alongside it is this node's role epoch as it
    // stands BEFORE the flip -- the stream last_applied counts in. Without
    // it a later promotion that never saw this one answers in the wrong
    // node's space (BUG-0075).
    let superseded = manifest::read_role(kv.as_ref())
        .map(|c| c.epoch)
        .unwrap_or(Epoch::ZERO);
    manifest::record_promo_fence(kv.as_ref(), epoch, superseded, kv.last_applied());
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
            // DROP THE OLD LEASE DEADLINE (#168). It was issued to the
            // PREVIOUS lineage and says nothing about this one — but the
            // watchdog only compares it to the clock, so a deadline already in
            // the past re-fences this node within 100ms, before the promoting
            // controller's next renewal (~poll interval) can land. That turned
            // recovery from a self-fence into a promote/re-fence flap. 0 =
            // unmanaged until the next FLINTLEASE, which the controller that
            // just promoted us sends on its next tick.
            lease_deadline.store(0, Ordering::Relaxed);
            // The widow clock likewise belonged to the previous life: this
            // node earns the whole --widowed-grace-ms to attach a replacement
            // before the age gate may shed a write.
            hub.rearm_widow_clock(flint_storage::strings::system_clock());
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
    _lease_deadline: &Arc<std::sync::atomic::AtomicU64>,
    _hub: &Arc<repl_hub::ReplHub>,
    _rocks: &Option<RocksHandle>,
    _args: &[Vec<u8>],
) -> Value {
    Value::Error("ERR FLINTPROMOTE requires a build with --features rocks".into())
}

/// FLINTFOLLOW <host:port>: re-point this replica's tail at a new master.
///
/// BUG-0076. Sent by the controller to every OTHER member of a pair right
/// after it promotes one, because nothing else ever tells them: a replica
/// reads `--replica-of` once at startup, and the survivor that was neither
/// killed nor promoted would otherwise dial the dead master forever.
///
/// This moves an ADDRESS, never a decision about lineage. Whether this copy
/// may resume from its cursor is still decided by the new master's FLINTSYNC
/// attach guard, which refuses a cursor past the promotion fence with -WALGAP
/// and sends this replica to a re-seed (BUG-0075). Re-pointing therefore
/// cannot widen what a replica is allowed to apply — it only lets it ask.
///
/// What IS checked here is the target: it must answer as a master at an epoch
/// at or above this node's own. A controller that lost a promotion race can
/// still send this, and following its fenced candidate would attach this
/// replica to a superseded branch.
#[cfg(feature = "rocks")]
fn flintfollow(
    read_only: &Arc<AtomicBool>,
    rocks: &Option<RocksHandle>,
    args: &[Vec<u8>],
) -> Value {
    use flint_storage::manifest::{self, Epoch};
    if !read_only.load(Ordering::Relaxed) {
        return Value::Error("ERR FLINTFOLLOW: this node is master, not a replica".into());
    }
    let Some(addr) = args.get(1).and_then(|raw| std::str::from_utf8(raw).ok()) else {
        return Value::Error("ERR usage: FLINTFOLLOW <host:port>".into());
    };
    let Some(link) = REPLICA_LINK.get() else {
        return Value::Error("ERR FLINTFOLLOW: this node has no replication link".into());
    };
    let mine = rocks
        .as_ref()
        .and_then(|kv| manifest::read_role(kv.as_ref()))
        .map(|c| c.epoch)
        .unwrap_or(Epoch::ZERO);
    // Ask the target what it is, rather than trusting the caller.
    let info = match internal_call_once(addr, &[b"FLINTINFO"]) {
        Ok(Value::Bulk(Some(raw))) => String::from_utf8_lossy(&raw).into_owned(),
        Ok(_) => return Value::Error(format!("ERR FLINTFOLLOW: {addr} gave no FLINTINFO")),
        Err(e) => return Value::Error(format!("ERR FLINTFOLLOW: {addr} unreachable: {e}")),
    };
    let field = |k: &str| {
        info.lines()
            .find_map(|l| l.strip_prefix(k).map(|v| v.trim().to_string()))
    };
    if field("role:").as_deref() != Some("master") {
        return Value::Error(format!("ERR FLINTFOLLOW: {addr} is not a master"));
    }
    // role_epoch, which is what FLINTINFO actually calls it. Reading `epoch:`
    // here silently parsed every target as (0,0) and refused every re-point as
    // "behind this node" — a check that cannot pass is the same shape as one
    // that always does, and only the drill told the difference.
    let theirs = field("role_epoch:")
        .and_then(|e| {
            let inner = e.trim_start_matches('(').trim_end_matches(')').to_string();
            let (g, c) = inner.split_once(',')?;
            Some(Epoch {
                generation: g.trim().parse().ok()?,
                counter: c.trim().parse().ok()?,
            })
        })
        .unwrap_or(Epoch::ZERO);
    if theirs < mine {
        return Value::Error(format!(
            "ERR FLINTFOLLOW: {addr} claims {theirs}, behind this node's {mine} — refusing to \
             follow a superseded lineage"
        ));
    }
    // ALREADY FOLLOWING IT: say so and touch nothing.
    //
    // Re-pointing is not free. It drops the stream and re-attaches, and an
    // attach re-runs the promotion-fence check -- which a healthy replica can
    // FAIL, because the fence is about where a lineage branched and not about
    // whether this link is working. A same-node re-promotion records the
    // promoting node's stale last_applied (often 0), so a replica quietly
    // streaming since before that promotion is past the fence for its epoch
    // and gets -WALGAP, marks itself for re-seed, and exits.
    //
    // That is exactly what this did to the two-member `failover_bystander`
    // pair: the controller re-pointed a replica at the master it was ALREADY
    // streaming from, and a working pair became single-copy. The bug being
    // fixed here is a replica pointed at the WRONG address; a replica pointed
    // at the right one needs nothing done to it.
    let unchanged = match link.target.lock() {
        Ok(mut t) => {
            let same = *t == addr;
            if !same {
                *t = addr.to_string();
            }
            same
        }
        Err(p) => {
            let mut t = p.into_inner();
            let same = *t == addr;
            if !same {
                *t = addr.to_string();
            }
            same
        }
    };
    if unchanged {
        return Value::Simple(format!("OK already following {addr}"));
    }
    link.repoint.store(true, Ordering::Relaxed);
    eprintln!("FLINTFOLLOW: re-pointing replication at {addr} (epoch {theirs})");
    Value::Simple(format!("OK following {addr}"))
}

#[cfg(not(feature = "rocks"))]
fn flintfollow(_ro: &Arc<AtomicBool>, _rocks: &Option<RocksHandle>, _args: &[Vec<u8>]) -> Value {
    Value::Error("ERR FLINTFOLLOW requires a build with --features rocks".into())
}

/// FLINTFENCE <generation> <counter>: answer with the branch-point bound for
/// a copy from that epoch (see manifest::promo_fence_bound for the safety
/// argument). Integer = resume is safe from any seq <= it; nil = cannot
/// vouch, re-seed. An epoch at or above this node's own claim is bounded by
/// the node's latest sequence — the asker's copy claims to already be on
/// this timeline.
#[cfg(feature = "rocks")]
fn flintfence(rocks: &Option<RocksHandle>, args: &[Vec<u8>]) -> Value {
    use flint_storage::manifest::{self, Epoch, FenceBound};
    let Some(kv) = rocks else {
        return Value::Error("ERR FLINTFENCE requires the rocks engine".into());
    };
    let (Some(generation), Some(counter)) = (
        args.get(1)
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|s| s.parse().ok()),
        args.get(2)
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|s| s.parse().ok()),
    ) else {
        return Value::Error("ERR usage: FLINTFENCE <generation> <counter>".into());
    };
    let since = Epoch {
        generation,
        counter,
    };
    let mine = manifest::read_role(kv.as_ref())
        .map(|c| c.epoch)
        .unwrap_or(Epoch::ZERO);
    if since >= mine {
        return Value::Integer(kv.latest_seq() as i64);
    }
    match manifest::promo_fence_bound(kv.as_ref(), since) {
        FenceBound::Bound(seq) => Value::Integer(seq as i64),
        // My claim is newer than the asker's epoch, yet I hold no fence row
        // above it: promotions happened that were never recorded (pre-fence
        // binaries). Vouching here would be guessing.
        FenceBound::Unfenced => Value::Bulk(None),
    }
}

#[cfg(not(feature = "rocks"))]
fn flintfence(_rocks: &Option<RocksHandle>, _args: &[Vec<u8>]) -> Value {
    Value::Error("ERR FLINTFENCE requires a build with --features rocks".into())
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
    // Read the stall pair ONCE. Two reads could straddle a change, and would
    // let write_stall_readable describe a different call than the values
    // beside it. None here means the engine could not answer, which is a
    // different fact from a healthy zero (docs/bugs/0022).
    let write_stall = rocks.as_ref().and_then(|kv| kv.write_stall());
    let compaction = rocks.as_ref().and_then(|kv| kv.compaction_pressure());
    let mem_sample = flint_storage::mem::sample();
    let info = format!(
        "role:{}\r\nloading:0\r\nrole_epoch:{role_epoch}\r\nbuild:{build}\r\nsst_bytes:{sst}\r\nlatest_seq:{latest}\r\nlast_applied:{last_applied}\r\nacked_seq:{}\r\nseq_lag:{seq_lag}\r\nwal_headroom_seq:{whs}\r\nwal_min_acked_seq:{wma}\r\nwal_headroom_shed_seq:{whl}\r\nwal_bytes_per_seq:{wbps}\r\nwal_archive_mb:{wamb}\r\nwal_archive_src:{wasrc}\r\nlive_replicas:{}\r\nlag_ms:{}\r\nlag_ms_max:{lmx}\r\nlag_max_gap:{lmg}\r\nlag_soft_ms:{soft}\r\nlag_hard_ms:{hard}\r\nmin_replicas_to_write:{minr}\r\nwidowed_grace_ms:{wgm}\r\nwidowed_shed:{wsh}\r\nfullsync_active:{fsa}\r\nfullsync_max:{fsm}\r\nasync_write_queue:{aqd}\r\nwrite_deadline_ms:{wdm}\r\nwrite_inflight:{wif}\r\nwrite_service_us:{wsu}\r\nwrite_cost_us:{wcu}\r\nwrite_wait_est_ms:{wwe}\r\nwrite_wait_peak_ms:{wwp}\r\nwrite_wait_peak_inflight:{wwpi}\r\nwrite_wait_peak_cost_us:{wwps}\r\nwrites_shed_deadline:{wsd}\r\nwrites_shed_lag:{wsl}\r\nwrites_shed_quorum:{wsq}\r\nwrites_shed_widowed:{wswd}\r\nwrites_shed_headroom:{wshr}\r\nwrites_delayed_soft:{wdsf}\r\nwal_fsync_ms:{wfm}\r\nwal_fsync_total:{wft}\r\ncert_days_remaining:{cdr}\r\nactive_conns:{ac}\r\nmax_conns:{mc}\r\nconns_shed_total:{cs}\r\nwrite_stopped:{wst}\r\ndelayed_write_rate:{dwr}\r\nwrite_stall_readable:{wsr}\r\nl0_files:{l0f}\r\npending_compaction_bytes:{pcb}\r\ncompaction_readable:{cr}\r\ndisk_free_bytes:{dfb}\r\ndisk_total_bytes:{dtb}\r\ndisk_free_pct:{dfp}\r\ndisk_verdict:{dv}\r\ndisk_unknown_samples:{dus}\r\nmem_avail_bytes:{mab}\r\nmem_total_bytes:{mtb}\r\nmem_avail_pct:{map}\r\nmem_src:{msrc}\r\nevictable_ns:{ens}\r\nevictable_ns_agree:{ensa}\r\nevictable_ns_bytes:{ensb}\r\nreclaim_active:{rca}\r\nreclaim_target_free_bytes:{rctf}\r\nevict:{evm}\r\ncollection_read_budget_pct:{crbp}\r\ncollection_read_in_flight_bytes:{crif}\r\ncollection_read_mode:{crm}\r\ncollection_read_refused:{crr}\r\ncollection_read_would_refuse:{crwr}\r\ncollection_read_unmeasured:{cru}\r\ngc_swept_expired:{gse}\r\ngc_swept_orphans:{gso}\r\nuptime_ms:{upms}\r\n",
        if read_only { "replica" } else { "master" },
        hub.effective_acked(now)
            .map_or_else(|| "none".into(), |a| a.to_string()),
        hub.live_replica_count(now),
        hub.lag_ms(now)
            .map_or_else(|| "none".into(), |l| l.to_string()),
        soft = hub.lag_soft_ms(),
        hard = hub.lag_hard_ms(),
        // BUG-0079. The archive budget and HOW it was chosen, so a
        // floor-clamped seat is visible to a check instead of to one boot log
        // line. Paired with disk_total_bytes below, these state an invariant a
        // drill can assert: when the source is `measured`, the budget must be
        // the one this volume implies. The boot race that produced a 1 GiB
        // archive on a 3.5 TB NVMe violates exactly that -- the failed sample
        // happened once, at boot, while the disk guard goes on sampling the
        // same directory successfully forever after, so the seat's own INFO
        // contradicts itself and anything reading it can say so.
        wamb = WAL_BUDGET_MB.load(Ordering::Relaxed),
        wasrc = wal_budget_src_str(),
        // ADR-0023 D7.1. Reported even while nothing evicts, because the
        // operator-visible question is "does this pair agree", and a value
        // nobody can read cannot be compared. -1 means not yet known, which
        // is deliberately distinct from 0 (a real mismatch).
        ens = evictable_ns_joined(),
        ensb = evictable_ns_bytes_joined(rocks),
        rca = RECLAIM_ACTIVE.load(Ordering::Relaxed) as u8,
        rctf = RECLAIM_TARGET_FREE.load(Ordering::Relaxed),
        evm = eviction_metrics_line(rocks),
        ensa = EVICTABLE_AGREE.load(Ordering::Relaxed),
        // ADR-0022. Exported because the shed threshold is expressed in
        // SEQUENCES while RocksDB budgets BYTES: an operator can only pick a
        // sane threshold by watching what their own workload actually does.
        // `-1` for "no live replica", which is a different state from zero
        // headroom and must not read as healthy.
        whs = hub
            .wal_headroom_seq(latest, now)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-1".into()),
        wma = hub
            .min_acked_live(now)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-1".into()),
        whl = hub.wal_headroom_shed_seq(),
        // OBSERVED bytes per WAL sequence. The headroom threshold is a byte
        // budget expressed in sequences, and one sequence is one write, so
        // this is the conversion factor -- published because the call site
        // tells operators to retune from it, and until now the number it
        // depended on was assumed rather than shown. 0 = nothing written yet.
        wbps = wal_bytes_per_seq(rocks),
        minr = hub.min_replicas_to_write(),
        wgm = hub.widowed_grace_ms(),
        // Whether the grace is CURRENTLY shedding, not just configured.
        // A knob that is set and a knob that is biting are different
        // facts, and only the second explains a -THROTTLED to an
        // operator reading this during an incident.
        wsh = hub.widowed_beyond_grace(now) as u8,
        build = build_version(),
        sst = rocks.as_ref().map(|kv| kv.sst_bytes()).unwrap_or(0),
        fsa = FULLSYNC_ACTIVE.load(Ordering::Relaxed),
        fsm = MAX_FULLSYNC.load(Ordering::Relaxed),
        aqd = async_queue_depth.map_or_else(|| "off".into(), |d| d.to_string()),
        // The deadline, the two terms it is compared against, the estimate
        // itself, and how often it has actually refused (#186). All five,
        // because a shed counter with no visible estimate is unexplainable
        // during an incident and an estimate with no counter is unfalsifiable.
        wdm = WRITE_DEADLINE_MS.load(Ordering::Relaxed),
        wif = WRITE_INFLIGHT.load(Ordering::Relaxed),
        wsu = WRITE_SERVICE_US.load(Ordering::Relaxed),
        // Residency above, marginal cost here. The projection multiplies the
        // SECOND one; reporting only the first is what let BUG-0088 read as a
        // slow seat rather than a wrong model.
        wcu = WRITE_COST_US.load(Ordering::Relaxed),
        wwe = estimated_write_wait_ms(),
        wwp = WRITE_WAIT_PEAK_MS.load(Ordering::Relaxed),
        wwpi = unpack_write_wait_peak(WRITE_WAIT_PEAK_PACKED.load(Ordering::Relaxed)).1,
        wwps = unpack_write_wait_peak(WRITE_WAIT_PEAK_PACKED.load(Ordering::Relaxed)).2,
        wsd = WRITES_SHED_DEADLINE.load(Ordering::Relaxed),
        wsl = WRITES_SHED_LAG.load(Ordering::Relaxed),
        wsq = WRITES_SHED_QUORUM.load(Ordering::Relaxed),
        wswd = WRITES_SHED_WIDOWED.load(Ordering::Relaxed),
        wshr = WRITES_SHED_HEADROOM.load(Ordering::Relaxed),
        wdsf = WRITES_DELAYED_SOFT.load(Ordering::Relaxed),
        lmx = LAG_MS_MAX.load(Ordering::Relaxed),
        lmg = LAG_MAX_GAP.load(Ordering::Relaxed),
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
        // ONE read, not two: the pair has to describe one moment, and
        // write_stall() is now fallible, so calling it twice could also
        // report readable=1 beside a value from a read that failed.
        wst = write_stall.map(|s| s.0).unwrap_or(0),
        dwr = write_stall.map(|s| s.1).unwrap_or(0),
        // Says whether the two fields above were MEASURED. Without it a
        // disabled, absent or unreadable counter and a genuinely idle engine
        // are the same bytes on the wire (docs/bugs/0022), and the reading
        // that means "never measured" is also the healthiest-looking one.
        // Same shape as disk_unknown_samples: keep the metric numeric and
        // publish its readability beside it rather than poisoning the value.
        // 0 on the mem engine, which honestly cannot answer at all.
        wsr = u8::from(write_stall.is_some()),
        // Same three-way contract as the stall pair: 0 from a live instrument
        // and 0 from an absent one must not read alike (BUG-0022, BUG-0013).
        l0f = compaction.map(|c| c.0).unwrap_or(0),
        pcb = compaction.map(|c| c.1).unwrap_or(0),
        cr = u8::from(compaction.is_some()),
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
        // NODE MEMORY, reported and not acted on (BUG-0060). Both candidate
        // fixes in that bug -- a node-level budget, or a per-unit limit derived
        // from the node's memory -- need a number the seat does not currently
        // have. `mem_src` distinguishes a real reading from an absent one the
        // way `wal_archive_src` does: a zero here would otherwise be
        // indistinguishable from a machine with no memory free, and those are
        // opposite facts.
        mab = mem_sample.map_or(0, |m| m.avail_bytes),
        mtb = mem_sample.map_or(0, |m| m.total_bytes),
        map = mem_sample.map_or(100, |m| m.avail_pct()),
        msrc = if mem_sample.is_some() {
            "proc"
        } else {
            "unreadable"
        },
        // BUG-0060 admission. `collection_read_unmeasured` is the one to
        // watch: it counts reads admitted while node memory could not be
        // read, which means the bound was NOT in force for them. A budget
        // that cannot be looked up must not be reported as a budget that
        // passed -- the same rule `mem_src` states one field up.
        //
        // `collection_read_mode` is published because a count is unreadable
        // without it: `refused` and `would_refuse` are the same event seen
        // under the two modes, and a dashboard that shows one without the
        // other cannot tell "the bound is holding" from "the bound is off and
        // taking notes".
        crbp = read_admission().pct(),
        crm = read_admission().mode().as_str(),
        crif = read_admission().in_flight_bytes(),
        crr = read_admission().refused_total(),
        crwr = read_admission().would_refuse_total(),
        cru = read_admission().unmeasured_total(),
        gse = GC_EXPIRED_TOTAL.load(Ordering::Relaxed),
        upms = heat::process_uptime_ms(),
        gso = GC_ORPHANS_TOTAL.load(Ordering::Relaxed),
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
        "role:{}\r\nloading:0\r\nlive_replica:{}\r\n",
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
/// the old dedicated ACK-reader thread required — so a fix for the rate
/// below may NOT simply put that reader back on its own thread.
///
/// MEASURED ceiling is REPL_TAIL_BUDGET_BYTES per ~80ms cycle, about
/// 50 MiB/s on loopback — not the ~20ms and ~200MB/s this comment claimed,
/// and on loopback there is no link for it to be "far above". Per 4.4 MiB
/// cycle: 42ms socket write, 19ms drain_acks, 13ms materialize, 5ms encode,
/// so ~77% is spent blocked or draining rather than producing, and the two
/// halves never overlap. That cycle time times the queue depth IS the
/// steady-state lag the caps are set against (BUG-0038; it is also the
/// whole of BUG-0035's thin margin).
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
            // drain_read, NOT read: the plain read on a TLS stream flushes
            // pending write data before reading, so with the batch send
            // jammed this drain consumed nothing — the replica sat blocked
            // writing the very ACK this loop exists to consume, and the two
            // sides deadlocked with both send buffers full (soak run 32).
            Ok(Decoded::NeedMore) => match stream.drain_read(&mut chunk) {
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
    let my_epoch = flint_storage::manifest::read_role(kv.as_ref())
        .map(|c| c.epoch)
        .unwrap_or(flint_storage::manifest::Epoch::ZERO);
    // Optional 3rd/4th args: the replica's role-claim epoch. A rewound
    // ex-master presents the epoch its restored snapshot was taken under;
    // if that predates this node's claim, the cursor must sit at or before
    // the first branch point after it, or the copy carries abandoned-branch
    // writes and only a re-seed can continue it (#187). The -WALGAP shape is
    // deliberate: the replica's tailer already escalates it to exactly that
    // re-seed. Absent args = a fresh full sync from THIS node (its cursor IS
    // this timeline) or a pre-fence binary — checked by neither, as before.
    if let (Some(g), Some(c)) = (
        args.get(2)
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|s| s.parse().ok()),
        args.get(3)
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|s| s.parse().ok()),
    ) {
        use flint_storage::manifest::{Epoch, FenceBound, promo_fence_bound};
        let theirs = Epoch {
            generation: g,
            counter: c,
        };
        if theirs < my_epoch {
            let refusal = match promo_fence_bound(kv.as_ref(), theirs) {
                FenceBound::Bound(fence) if cursor > fence => Some(format!(
                    "WALGAP cursor {cursor} is past the promotion fence {fence} for epoch \
                     {theirs}: that span was never on this timeline"
                )),
                FenceBound::Bound(_) => None,
                FenceBound::Unfenced => Some(format!(
                    "WALGAP promotion fence history for epoch {theirs} is incomplete on this \
                     node: cannot vouch for cursor {cursor}"
                )),
            };
            if let Some(msg) = refusal {
                let mut out = Vec::new();
                encode(&Value::Error(msg), &mut out);
                return stream.write_all(&out);
            }
            // The accepted cursor is in the OLD stream's space; this node's
            // WAL is indexed by its OWN. Translate before serving — the
            // mapping is durable in every apply batch's cursor row. Serving
            // the untranslated number was soak run 30's failure: the stream
            // came out off-position (a SequenceGap when the strict check
            // caught it, silent identical-value replays when it did not).
            // The client adopts the translated cursor from the OK line.
            match kv.own_seq_for_upstream(cursor) {
                Ok(own) => {
                    eprintln!(
                        "rewind attach: upstream cursor {cursor} (epoch {theirs}) maps to \
                         local seq {own}"
                    );
                    cursor = own;
                }
                Err(e) => {
                    let mut out = Vec::new();
                    encode(
                        &Value::Error(format!(
                            "WALGAP cannot map upstream cursor {cursor} into this WAL ({e:?}): \
                             the span needed for an incremental rejoin is gone"
                        )),
                        &mut out,
                    );
                    return stream.write_all(&out);
                }
            }
        }
    }
    // RETENTION, the last admission term (BUG-0015). The fence check above
    // says this cursor is on our timeline; it does not say the WAL can still
    // REACH it. Those are different questions and only the second one is
    // answered by the archive, which RocksDB prunes on a TTL/size budget that
    // consults no replica (ADR-0022 sheds writes to make that rare; it cannot
    // make it impossible).
    //
    // It matters here and not only in the stream because `probe_resume` drops
    // the connection after this reply. A marked boot asks "is my copy still
    // good?", and an OK makes it CLEAR its own NEEDS_RESEED marker and warm
    // rejoin — after which the tailer discovers the gap, re-marks, and exits.
    // The next start repeats it: a livelock that no supervisor can break,
    // where a plain refusal here costs one re-seed and recovers.
    //
    // Ask the STREAM's own question rather than a second implementation of
    // it: a budget of 1 byte materializes at most one batch (batches are
    // never split), and `updates_since_budgeted` returns an empty Ok for a
    // caught-up replica, so a healthy cursor never false-refuses.
    if let Err(ReplError::WalGap(why)) = kv.updates_since_budgeted(cursor, 1) {
        // BUG-0082, and this is the refusal a REJOIN meets first. `why` already
        // names the oldest retained batch -- as a SEQUENCE, which is precisely
        // the quantity that cannot discriminate: the same gap is unremarkable
        // at one write rate and impossible at another, and the rate is not in
        // the message. The archive's AGE can, and only this side can read it.
        let held = match kv.archive_span() {
            Some(s) => s.describe(),
            None => "archive state could not be read on this node".to_string(),
        };
        let mut out = Vec::new();
        encode(
            &Value::Error(format!(
                "WALGAP cursor {cursor} is no longer reachable from this WAL ({why}): \
                 full sync required ({held})"
            )),
            &mut out,
        );
        return stream.write_all(&out);
    }
    let mut out = Vec::new();
    // The OK carries this node's role epoch so an accepted lower-epoch
    // replica can ADOPT it durably: its next reconnect then presents the
    // adopted epoch, and a cursor that has legitimately grown past the old
    // fence is not refused as if it were still the abandoned branch.
    encode(
        &Value::Simple(format!(
            "FLINTSYNC-OK {cursor} e{}.{}",
            my_epoch.generation, my_epoch.counter
        )),
        &mut out,
    );
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
                    // BUG-0082. SAY WHAT THIS ARCHIVE ACTUALLY HOLDS.
                    //
                    // This refusal names the missing segment and nothing else,
                    // and the replica prints it verbatim as its FATAL. Two
                    // unrelated faults end there: retention too short for a
                    // correct cursor, or a correct archive asked for a cursor
                    // far older than the outage that produced it — which makes
                    // the fence rewind's translation the fault. The playground
                    // hit the second sixteen times in three weeks with a 278 MB
                    // archive holding a full twelve hours, i.e. nowhere near
                    // its budget.
                    //
                    // Distance in sequences cannot separate them: the same gap
                    // is plausible at one write rate and absurd at another, and
                    // the rate is not in the message. AGE can, and only this
                    // side knows it — the archive is the master's, so the
                    // replica cannot stat it however carefully it logs.
                    let held = match kv.archive_span() {
                        Some(s) => s.describe(),
                        // Not "empty": the refusal must not assert a retention
                        // fact it failed to read.
                        None => "archive state could not be read on the master".to_string(),
                    };
                    out.clear();
                    encode(
                        &Value::Error(format!("WALGAP full sync required: {e} ({held})")),
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
    // A MASTER's snapshot carries its role epoch in the id: that label is
    // what lets a rejoin later prove the snapshot predates the promotion
    // fence and rewind to it instead of full re-seeding (#187). A replica's
    // snapshot gets no label — the fence-bound argument (manifest.rs,
    // PROMO_FENCE_KEY_PREFIX) only covers epochs the labeling node itself
    // held as master, so an unlabeled snapshot is simply never
    // rewind-eligible.
    let epoch_label = flint_storage::manifest::read_role(kv.as_ref())
        .filter(|c| c.role == flint_storage::manifest::Role::Master)
        .map(|c| format!("-e{}.{}", c.epoch.generation, c.epoch.counter))
        .unwrap_or_default();
    let id = format!(
        "snap-{}-seq{}{}",
        flint_storage::strings::system_clock(),
        kv.latest_seq(),
        epoch_label
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
    // Paced: this node is serving a checkpoint while also taking live writes,
    // and after a failover it is the freshly promoted master carrying the
    // pair alone. Unthrottled, that starved the write path for 11.9s on soak
    // run 23 (#184). Same limiter as the migration copy (#83), same reason.
    let mut pacer = migrate::Pacer::new(&FULLSYNC_RATE_BYTES);
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
                // Paced on the PAYLOAD, after the write: an empty frame costs
                // nothing, and sleeping before the send would just move the
                // stall earlier.
                pacer.pace(n);
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

    pub fn run(link: &Arc<super::ReplicaLink>, kv: &Arc<RocksKv>, stop: &Arc<AtomicBool>) {
        loop {
            if stop.load(Ordering::Relaxed) {
                eprintln!("tailer stopped (promoted)");
                return;
            }
            // The address, every time round: FLINTFOLLOW may have moved it
            // while the last attempt was in flight. A stranded replica is
            // already in a tight retry loop against a refused address, so a
            // swap lands on the very next attempt.
            let target: &str = &link
                .target
                .lock()
                .map_or_else(|p| p.into_inner().clone(), |t| t.clone());
            // A re-point is not a failure: clear it and dial the new address
            // without the error backoff or the WalPurged remedy below, which
            // are about a link that broke, not one that moved.
            if link.repoint.swap(false, Ordering::Relaxed) {
                eprintln!("tailer now following {target}");
                if let Err(e) = tail_once(target, kv, stop, link) {
                    eprintln!("tail from {target} ended: {e}");
                }
                continue;
            }
            if let Err(e) = tail_once(target, kv, stop, link)
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
                    // The marker carries the master's own words: a refusal at
                    // the promotion fence must read differently from a purged
                    // WAL, because the next boot's rewind decision keys off it
                    // (retrying a fence-refused snapshot loops forever).
                    // BUG-0062. Disqualify the snapshots this cursor can
                    // never reach again, BEFORE the marker is written and this
                    // process leaves — the next boot reads that directory to
                    // decide, and if the losing snapshot is still a candidate
                    // it picks the same one and fails identically. The marker
                    // alone was not enough: the boot's guard is
                    // `why.contains("promotion fence")`, and a purged WAL does
                    // not say that, so the rewind path was taken again.
                    // Deliberately not a second string to match on — matching
                    // on the message is what let this recur.
                    if let Some(snaps) = arg("--rewind-snaps") {
                        let cursor = kv.last_applied();
                        let n = quarantine_unresumable(&snaps, cursor);
                        eprintln!(
                            "quarantine: {n} snapshot(s) at or below seq {cursor} disqualified; \
                             the next start has no rewind candidate to lose this race with"
                        );
                    }
                    super::mark_needs_reseed(kv.path(), &format!("cannot resume this tail: {why}"));
                    // hard_exit, not process::exit: this is the tailer THREAD,
                    // the DB is open, and the sampler and RocksDB's compaction
                    // threads are still running. See BUG-0048 — the old call
                    // ran C++ static teardown underneath them and turned a
                    // deliberate exit 3 into a segfault under load.
                    super::hard_exit(3);
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

    fn tail_once(
        target: &str,
        kv: &Arc<RocksKv>,
        stop: &Arc<AtomicBool>,
        link: &Arc<super::ReplicaLink>,
    ) -> Result<(), TailError> {
        let mut stream = internal_connect(target)?;
        // Short read timeout so the stop flag is honored promptly.
        stream.set_read_timeout(Some(std::time::Duration::from_millis(300)))?;
        // Bounded ACK writes. An unbounded write here parked this thread in
        // the kernel's send path for good when the master stopped draining
        // (run 32's deadlock): the master's drain now reads through a jammed
        // send (drain_read), so this should never fire — if it still does,
        // the error tears the connection down and the tailer reconnects from
        // its durable cursor, which is a 1s hiccup instead of a wedge.
        stream.set_write_timeout(Some(std::time::Duration::from_millis(5_000)))?;
        let cursor = kv.last_applied();
        // Present our role-claim epoch — our timeline identity. Equal to the
        // master's in steady state (fresh syncs copy it, and acceptance
        // below adopts it), lower exactly when this copy was rewound to a
        // pre-promotion snapshot — the case the master must fence-check
        // before serving (#187).
        let claim_epoch = flint_storage::manifest::read_role(kv.as_ref() as &dyn flint_storage::Kv)
            .map(|c| c.epoch)
            .unwrap_or(flint_storage::manifest::Epoch::ZERO);
        let mut out = Vec::new();
        encode(
            &Value::Array(Some(vec![
                Value::Bulk(Some(b"FLINTSYNC".to_vec())),
                Value::Bulk(Some(cursor.to_string().into_bytes())),
                Value::Bulk(Some(claim_epoch.generation.to_string().into_bytes())),
                Value::Bulk(Some(claim_epoch.counter.to_string().into_bytes())),
            ])),
            &mut out,
        );
        stream.write_all(&out)?;
        eprintln!("replicating from {target} starting at seq {cursor} (epoch {claim_epoch})");

        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 64 * 1024];
        // Heartbeat: an idle replica must still prove liveness — ACKs only
        // after applied batches would make a healthy idle pair look dead
        // after the liveness window.
        let mut last_ack_sent = std::time::Instant::now();
        // Applied but not yet told to the master. One ACK per WAL batch meant
        // a 30-byte write() per batch — 3626 of them per shipped cycle — and
        // the master spent 21ms of every 80ms cycle BLOCKED in read() draining
        // them, 179 reads to collect one cycle's worth (measured, BUG-0038).
        // Replication is ordered, so only the highest applied sequence carries
        // information; the ones before it are the same fact, retold.
        //
        // Flushed when this loop is about to block on read(), which is the
        // moment there is nothing better to do, so the master never learns
        // later than it could have. Measured interleaved on a quiet box, four
        // rounds each: steady-state lag_ms_max 615ms -> 115ms, and
        // writes_delayed_soft 250 -> ZERO, i.e. the soft cap stops being
        // entered by ordinary traffic at all. Throughput is unchanged; what
        // changes is that the replica spends its CPU applying instead of on
        // 3626 write() calls per cycle, and it is the replica that was the
        // bottleneck.
        //
        // THE HEARTBEAT IS NOW LOAD-BEARING FOR MORE THAN LIVENESS. It sends
        // kv.last_applied() every 500ms whatever else happens, which is what
        // bounds ack latency if the stream is ever saturated enough that this
        // loop stops reaching NeedMore. Do not weaken it to a liveness-only
        // check that skips when an ACK was recently sent.
        let mut pending_ack: Option<u64> = None;
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
                            // The master accepted us: "FLINTSYNC-OK <cursor>
                            // e<gen>.<counter>". On a rewind attach the
                            // cursor is TRANSLATED into the master's own
                            // sequence space — adopt it durably before any
                            // batch arrives, or every apply trips the
                            // contiguity check against the old-space number.
                            // Guarded to only move FORWARD: keepalive OKs
                            // repeat the serve-time cursor, which goes stale
                            // as applies progress.
                            if let Some(returned) = s
                                .split_whitespace()
                                .nth(1)
                                .and_then(|t| t.parse::<u64>().ok())
                                && returned > kv.last_applied()
                            {
                                let _ = kv.set_last_applied(returned);
                                eprintln!(
                                    "adopted the master's translated cursor {returned}: tailing \
                                     its sequence space now"
                                );
                            }
                            // And ADOPT its epoch if ours is older: from here
                            // on our cursor advances on the MASTER's
                            // timeline, and re-presenting the pre-rewind
                            // epoch would get a legitimately grown cursor
                            // refused at the old fence on the next reconnect
                            // (#187).
                            if let Some(adopted) = s
                                .split_whitespace()
                                .nth(2)
                                .and_then(|t| t.strip_prefix('e'))
                                .and_then(|t| t.split_once('.'))
                                .and_then(|(g, c)| {
                                    Some(flint_storage::manifest::Epoch {
                                        generation: g.parse().ok()?,
                                        counter: c.parse().ok()?,
                                    })
                                })
                            {
                                use flint_storage::manifest::{self, Role, RoleClaim};
                                let kv_dyn = kv.as_ref() as &dyn flint_storage::Kv;
                                let mine = manifest::read_role(kv_dyn).map(|c| c.epoch);
                                if mine.is_some_and(|m| m < adopted) {
                                    manifest::force_role(
                                        kv_dyn,
                                        RoleClaim {
                                            role: Role::Replica,
                                            epoch: adopted,
                                        },
                                    );
                                    eprintln!(
                                        "adopted the master's role epoch {adopted}: this copy is \
                                         on its timeline now"
                                    );
                                }
                            }
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
                            pending_ack = Some(batch.last_seq);
                        }
                    }
                }
                Ok(Decoded::NeedMore) => {
                    // About to block: tell the master everything applied since
                    // the last flush, as one frame.
                    if let Some(seq) = pending_ack.take() {
                        out.clear();
                        encode(
                            &Value::Array(Some(vec![
                                Value::Bulk(Some(b"ACK".to_vec())),
                                Value::Bulk(Some(seq.to_string().into_bytes())),
                            ])),
                            &mut out,
                        );
                        stream.write_all(&out)?;
                        last_ack_sent = std::time::Instant::now();
                    }
                    if stop.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    // Same 300ms granularity the stop flag gets: a promotion
                    // that re-points this replica should not wait out a quiet
                    // stream from a master that is no longer ours.
                    if link.repoint.load(Ordering::Relaxed) {
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

/// ADR-0012 D8: a transaction faces the same admission gates a single
/// command does, evaluated over the whole queue.
///
/// The end-to-end proof is `tools/txn_failure_drill.sh`, which needs a
/// replica, a frozen slot and a killed master to make each gate fire. These
/// cover the part that is pure and easy to get subtly wrong: how a queue of
/// mixed commands classifies. Every one of them fails if the aggregate is
/// taken from the FIRST command instead of all of them — which is the shape
/// the bug had.
/// The write-deadline projection (BUG-0088). Ungated, unlike the packing
/// tests, because this arithmetic runs in every build and a test that executes
/// in only one feature config is absent from the other.
///
/// IT LIVES DOWN HERE, away from the code it covers, for a reason worth
/// knowing before moving it back: `plain_process_exit_stays_out_of_the_running_paths`
/// reads this file and treats everything before the FIRST `#[cfg(test)]` as
/// production text. A bare `#[cfg(test)]` up beside `projected_wait_ms` cuts
/// that scan off at line ~1046 and the exit-site count silently collapses.
/// The packing module up there only escapes because it is written
/// `#[cfg(all(test, feature = "rocks"))]`, which does not match the needle --
/// an accident, not a design.
#[cfg(test)]
mod write_projection_tests {
    use super::{marginal_cost_us, projected_wait_ms};

    #[test]
    fn a_batch_commit_is_not_charged_once_per_member() {
        // The gate run that failed on 2026-09-04: 65 writes in flight, each
        // reporting 30942us of RESIDENCY, against a 2000ms deadline. They were
        // one batch and committed together, so the wall clock spent was 31ms
        // and not 2011ms.
        let residency_us = 30_942;
        let inflight = 65;
        assert_eq!(
            projected_wait_ms(inflight, residency_us),
            2011,
            "the OLD model, kept so the regression is legible"
        );
        let cost = marginal_cost_us(residency_us, inflight);
        assert_eq!(
            projected_wait_ms(inflight, cost),
            30,
            "the corrected model lands back on the commit that actually happened"
        );
    }

    #[test]
    fn a_genuinely_slow_seat_still_sheds() {
        // The positive control. If the correction only ever made the number
        // smaller it would have disarmed the deadline rather than fixed it.
        // One write, alone, taking 2.5s: nothing is shared, so cost IS
        // residency and the projection must still cross a 2000ms deadline.
        let cost = marginal_cost_us(2_500_000, 1);
        assert_eq!(cost, 2_500_000);
        assert!(
            projected_wait_ms(1, cost) > 2_000,
            "a real stall must still shed"
        );

        // And a genuine pile-up: 500 in flight against commits of 10 sharing
        // 100ms. Throughput is 100/s, so the queue really is 5s deep.
        let piled = marginal_cost_us(100_000, 10);
        assert!(
            projected_wait_ms(500, piled) > 2_000,
            "a queue that outruns throughput must still shed"
        );
    }

    #[test]
    fn sharing_cannot_divide_by_zero_or_wrap() {
        // `shared` comes from a fetch_sub return and is floored at 1, but the
        // arithmetic must not depend on the caller for that.
        assert_eq!(marginal_cost_us(1_000, 0), 1_000);
        assert_eq!(projected_wait_ms(u64::MAX, u64::MAX), u64::MAX / 1_000);
    }
}

#[cfg(test)]
mod superseded_cause_tests {
    use super::superseded_cause;

    /// BUG-0065's instrument: the journal row must NAME the successor. The
    /// 2026-08-27 canary abort carried a fixed string here, and the one fact
    /// that discriminates a stale own-pair record from a cross-pair promotion
    /// lived only on stderr, which dies with the box. A cause that mentions
    /// supersession without saying BY WHOM is the regression this pins.
    #[test]
    fn the_fence_row_names_who_superseded_it() {
        let c = superseded_cause("127.0.0.1:6923");
        assert!(
            c.contains("127.0.0.1:6923"),
            "the successor is missing from the journal cause: {c}"
        );
        // and it stays greppable by responders reading old incidents
        assert!(
            c.contains("lease superseded"),
            "lost the family prefix: {c}"
        );
    }
}

#[cfg(test)]
mod admission_tests {
    use super::*;

    fn cmd(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.as_bytes().to_vec()).collect()
    }

    /// `Work` borrows the unit's commands, so the queue has to outlive it;
    /// each test owns its queue and hands the borrowed view to `unit`.
    fn unit(queue: &[Vec<Vec<u8>>]) -> Vec<&[Vec<u8>]> {
        queue.iter().map(Vec::as_slice).collect()
    }

    #[test]
    fn a_queue_is_a_write_if_anything_in_it_writes() {
        let mixed = [cmd(&["GET", "k"]), cmd(&["SET", "k", "v"])];
        let m = unit(&mixed);
        let w = Work::new(&m);
        assert!(w.write, "a queue that ends in SET is a write");
        assert!(w.reads(), "and it is also a read");
        // The negative control — otherwise `write: true` could just be a
        // constant.
        let reads = [cmd(&["GET", "k"]), cmd(&["HGETALL", "h"])];
        let r = unit(&reads);
        assert!(!Work::new(&r).write);
    }

    #[test]
    fn one_growing_write_puts_the_whole_queue_behind_the_disk_guard() {
        // The disk guard lets space-FREEING writes through, because deleting
        // is the only way out of a full disk. A transaction qualifies only if
        // EVERY write in it frees space: the unit lands whole or not at all,
        // so one growing write commits the rest of them too.
        let freeing = [cmd(&["DEL", "a"]), cmd(&["UNLINK", "b"])];
        let f = unit(&freeing);
        assert!(Work::new(&f).frees_space());
        let mixed = [cmd(&["DEL", "a"]), cmd(&["SET", "b", "v"])];
        let m = unit(&mixed);
        assert!(!Work::new(&m).frees_space());
    }

    #[test]
    fn a_replica_refuses_a_queue_that_writes_even_if_it_reads_first() {
        // Before Phase E this exact queue answered `+OK` on a replica and
        // wrote nothing — the false ack the gate exists to prevent.
        let q = [cmd(&["GET", "k"]), cmd(&["SET", "k", "v"])];
        let u = unit(&q);
        match check_node_health(Work::new(&u), true) {
            Some(Value::Error(e)) => assert!(e.starts_with("READONLY"), "{e}"),
            other => panic!("a write on a replica must be refused, got {other:?}"),
        }
    }

    #[test]
    fn the_same_queue_is_admitted_on_a_master() {
        // Discrimination: the gate must be reading the ROLE, not refusing
        // every transaction it is shown.
        let q = [cmd(&["GET", "k"]), cmd(&["SET", "k", "v"])];
        let u = unit(&q);
        assert!(check_node_health(Work::new(&u), false).is_none());
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
                        &Arc::new(flint_storage::watch::WatchTable::new()),
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
    /// A rocks-backed server, because ADR-0027's batching is rocks-only:
    /// `spawn_server` uses MemKv with `rocks: None`, so every test through it
    /// takes the unbatched path and proves nothing about this.
    #[cfg(feature = "rocks")]
    fn spawn_rocks_server() -> (std::net::SocketAddr, std::path::PathBuf) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        // A monotonic counter, NOT the thread id: ids are reused once a
        // thread exits, so two servers could name the same directory and the
        // second's remove_dir_all would wipe the first's data out from under
        // it -- which is exactly how this test failed in the full run while
        // passing alone.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "flint-batch-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let kv = Arc::new(flint_storage::rocks::RocksKv::open(&dir).expect("open rocks"));
        let out = dir.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let kv = Arc::clone(&kv);
                std::thread::spawn(move || {
                    let store: &dyn Kv = &*kv;
                    let _ = serve(
                        flint_tls::Stream::Plain(stream),
                        store,
                        &Arc::new(AtomicBool::new(false)),
                        &Arc::new(AtomicBool::new(false)),
                        &Arc::new(std::sync::atomic::AtomicU64::new(0)),
                        Some(Arc::clone(&kv)),
                        &Arc::new(ReplHub::default()),
                        &Arc::new(AtomicBool::new(false)),
                        commands::Limits::default(),
                        None,
                        &Arc::new(flint_storage::watch::WatchTable::new()),
                    );
                });
            }
        });
        (addr, out)
    }

    #[cfg(feature = "rocks")]
    fn set_frame(k: &str, v: &str, into: &mut Vec<u8>) {
        encode(
            &Value::Array(Some(vec![
                Value::Bulk(Some(b"SET".to_vec())),
                Value::Bulk(Some(k.as_bytes().to_vec())),
                Value::Bulk(Some(v.as_bytes().to_vec())),
            ])),
            into,
        );
    }

    #[cfg(feature = "rocks")]
    fn get_frame(k: &str, into: &mut Vec<u8>) {
        encode(
            &Value::Array(Some(vec![
                Value::Bulk(Some(b"GET".to_vec())),
                Value::Bulk(Some(k.as_bytes().to_vec())),
            ])),
            into,
        );
    }

    /// A pipeline that is NOTHING BUT pure writes has no following command to
    /// force the batch out, so it commits only on the drain. If that commit
    /// were missing the replies would still say OK and the data would not be
    /// there -- silent loss, which is why this reads back on a SECOND
    /// connection rather than trusting the first.
    #[cfg(feature = "rocks")]
    #[test]
    fn a_pipeline_of_only_pure_sets_is_committed_on_drain() {
        // Serialised against write_lock's tests: these run real servers over
        // the same process-global locks (see write_lock::test_serial).
        let _serial = crate::write_lock::test_serial();
        let (addr, _dir) = spawn_rocks_server();
        let mut s = connect(addr);
        let n = 50;
        let mut pipeline = Vec::new();
        for i in 0..n {
            set_frame(&format!("k{i}"), &format!("v{i}"), &mut pipeline);
        }
        s.write_all(&pipeline).expect("send");
        let frames = read_frames(&mut s, n);
        for f in &frames {
            assert_eq!(f, &Value::Simple("OK".into()), "every batched SET acks OK");
        }
        drop(s);

        let mut s2 = connect(addr);
        let mut reads = Vec::new();
        for i in 0..n {
            get_frame(&format!("k{i}"), &mut reads);
        }
        s2.write_all(&reads).expect("send reads");
        let got = read_frames(&mut s2, n);
        for (i, f) in got.iter().enumerate() {
            assert_eq!(
                f,
                &Value::Bulk(Some(format!("v{i}").into_bytes())),
                "k{i} was acked but did not land"
            );
        }
    }

    /// Throughput of pipelined pure writes through a real connection.
    ///
    /// NOT a gate test -- `#[ignore]`d, and run by hand against two commits to
    /// A/B them. Loopback and plaintext on purpose: this measures the engine
    /// writes ADR-0027 removes, not the round trips it does not touch, so the
    /// absent network is a simplification rather than a distortion. It will
    /// UNDERSTATE the lock half, because a laptop cannot hold enough
    /// concurrent connections to contend 128 stripes the way a fleet does.
    #[cfg(feature = "rocks")]
    #[test]
    #[ignore]
    fn bench_pipelined_pure_writes() {
        // Serialised against write_lock's tests: these run real servers over
        // the same process-global locks (see write_lock::test_serial).
        let _serial = crate::write_lock::test_serial();
        let conns: usize = std::env::var("BENCH_CONNS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);
        let per: usize = std::env::var("BENCH_N")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20_000);
        let (addr, _dir) = spawn_rocks_server();
        let start = std::time::Instant::now();
        let workers: Vec<_> = (0..conns)
            .map(|c| {
                std::thread::spawn(move || {
                    let mut s = connect(addr);
                    let depth = 256;
                    let mut sent = 0;
                    while sent < per {
                        let batch = depth.min(per - sent);
                        let mut pipeline = Vec::new();
                        // Value size matters to the RATIO, not just the rate:
                        // the smaller the value, the larger a share per-write
                        // overhead is, and the more batching appears to win.
                        // 1 KB is the fleet workload; default to it.
                        let vs: usize = std::env::var("BENCH_VAL")
                            .ok()
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(1024);
                        let val = "x".repeat(vs);
                        for i in 0..batch {
                            set_frame(&format!("c{c}:{:012}", sent + i), &val, &mut pipeline);
                        }
                        s.write_all(&pipeline).expect("send");
                        let f = read_frames(&mut s, batch);
                        assert_eq!(f.len(), batch);
                        sent += batch;
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().expect("worker");
        }
        let el = start.elapsed().as_secs_f64();
        let total = conns * per;
        println!(
            "PIPELINED_SET conns={conns} total={total} in {el:.3}s = {:.0} sets/s",
            total as f64 / el
        );
    }

    /// The refusals in `batch_eligible` are correctness boundaries, so they
    /// are asserted rather than assumed. A transaction is already a batch with
    /// its own atomicity; layering a second deferral under it would let EXEC
    /// report success for writes still sitting in a buffer.
    #[cfg(feature = "rocks")]
    #[test]
    fn a_transaction_is_not_batched_and_still_reads_its_own_writes() {
        // Serialised against write_lock's tests: these run real servers over
        // the same process-global locks (see write_lock::test_serial).
        let _serial = crate::write_lock::test_serial();
        let (addr, _dir) = spawn_rocks_server();
        let mut s = connect(addr);
        let mut p = Vec::new();
        encode(
            &Value::Array(Some(vec![Value::Bulk(Some(b"MULTI".to_vec()))])),
            &mut p,
        );
        // Same-slot, per ADR-0012: a transaction spans one slot, so the
        // keys are colocated with a hash tag.
        set_frame("{tx}t1", "a", &mut p);
        set_frame("{tx}t2", "b", &mut p);
        encode(
            &Value::Array(Some(vec![Value::Bulk(Some(b"EXEC".to_vec()))])),
            &mut p,
        );
        s.write_all(&p).expect("send");
        let f = read_frames(&mut s, 4);
        assert_eq!(f[0], Value::Simple("OK".into()), "MULTI");
        assert_eq!(
            f[1],
            Value::Simple("QUEUED".into()),
            "SET queues, not batches"
        );
        assert_eq!(f[2], Value::Simple("QUEUED".into()));
        assert!(
            matches!(f[3], Value::Array(Some(_))),
            "EXEC returns the queued replies, got {:?}",
            f[3]
        );
        // And the transaction's writes are durable to a separate connection.
        let mut s2 = connect(addr);
        let mut r = Vec::new();
        get_frame("{tx}t1", &mut r);
        get_frame("{tx}t2", &mut r);
        s2.write_all(&r).expect("send");
        let g = read_frames(&mut s2, 2);
        assert_eq!(g[0], Value::Bulk(Some(b"a".to_vec())));
        assert_eq!(g[1], Value::Bulk(Some(b"b".to_vec())));
    }

    /// REGRESSION, and the sharpest test here. A batch used to take
    /// `GLOBAL.read()` once PER KEY. `RwLock` is writer-preferring, so with a
    /// `lock_all()` pending, the batch's second key blocked acquiring a lock
    /// the batch already held -- against a writer waiting for the batch to
    /// finish, which it could not, because finishing is what releases it. It
    /// wedged the suite for 25 minutes and would have reached a fleet as an
    /// unexplained stall: every MSET, FLUSHALL and multi-key DEL takes
    /// `lock_all()`, and the failure emits no error and no counter.
    ///
    /// Asserted on the INVARIANT, not on timing. The first version of this
    /// test raced a thread holding `lock_all()` and checked the batch finished
    /// inside a budget; it failed on a 16-vCPU gate box while passing on a
    /// laptop, because a writer-preferring lock starving readers looks exactly
    /// like a deadlock from the outside. Loosening the contention then made it
    /// pass WITH the bug reintroduced -- stable and useless. Counting the
    /// acquisitions cannot go either way: a batch takes GLOBAL once, so 120
    /// keys in one pipeline must not produce 120 acquisitions.
    #[cfg(feature = "rocks")]
    #[test]
    fn a_batch_takes_the_global_lock_once_not_once_per_key() {
        let _serial = crate::write_lock::test_serial();
        let (addr, _dir) = spawn_rocks_server();
        let mut s = connect(addr);
        let n = 120;
        let mut pipeline = Vec::new();
        for i in 0..n {
            set_frame(&format!("gk{i}"), &format!("v{i}"), &mut pipeline);
        }
        let before = crate::write_lock::global_acquires();
        s.write_all(&pipeline).expect("send");
        let frames = read_frames(&mut s, n);
        let taken = crate::write_lock::global_acquires() - before;
        assert_eq!(frames.len(), n);
        for f in &frames {
            assert_eq!(f, &Value::Simple("OK".into()));
        }
        // A pipeline may arrive in more than one read, so more than one batch
        // is legitimate -- but nowhere near one per key.
        assert!(
            taken < (n as u64) / 4,
            "{n} pure writes took GLOBAL {taken} times; a batch must take it \
             once, not once per key"
        );
    }

    /// A read later in the SAME pipeline must see the writes before it. The
    /// batch is forced out by any non-pure command for exactly this reason.
    #[cfg(feature = "rocks")]
    #[test]
    fn a_read_in_the_same_pipeline_sees_the_writes_before_it() {
        // Serialised against write_lock's tests: these run real servers over
        // the same process-global locks (see write_lock::test_serial).
        let _serial = crate::write_lock::test_serial();
        let (addr, _dir) = spawn_rocks_server();
        let mut s = connect(addr);
        let mut pipeline = Vec::new();
        set_frame("a", "1", &mut pipeline);
        set_frame("b", "2", &mut pipeline);
        get_frame("a", &mut pipeline);
        set_frame("c", "3", &mut pipeline);
        get_frame("c", &mut pipeline);
        s.write_all(&pipeline).expect("send");
        let f = read_frames(&mut s, 5);
        assert_eq!(f[0], Value::Simple("OK".into()));
        assert_eq!(f[1], Value::Simple("OK".into()));
        assert_eq!(
            f[2],
            Value::Bulk(Some(b"1".to_vec())),
            "GET must see the SET earlier in its own pipeline"
        );
        assert_eq!(f[3], Value::Simple("OK".into()));
        assert_eq!(
            f[4],
            Value::Bulk(Some(b"3".to_vec())),
            "and a SET immediately before it"
        );
    }

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

    /// Ephemeral server running `serve_loading` — a node that has bound its
    /// port but has no store behind it yet (#176).
    fn spawn_loading_server() -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                std::thread::spawn(move || {
                    let _ = serve_loading(flint_tls::Stream::Plain(stream));
                });
            }
        });
        addr
    }

    /// The whole point of #176: a node inside its initial full sync answers,
    /// so nothing mistakes it for a corpse — and says what it is.
    #[test]
    fn a_loading_node_answers_ping_and_reports_itself_loading() {
        let mut s = connect(spawn_loading_server());
        let mut out = Vec::new();
        encode(
            &Value::Array(Some(vec![Value::Bulk(Some(b"PING".to_vec()))])),
            &mut out,
        );
        encode(
            &Value::Array(Some(vec![Value::Bulk(Some(b"FLINTINFO".to_vec()))])),
            &mut out,
        );
        s.write_all(&out).expect("send");
        let frames = read_frames(&mut s, 2);
        assert_eq!(frames[0], Value::Simple("PONG".into()));
        let Value::Bulk(Some(info)) = &frames[1] else {
            panic!("FLINTINFO must answer during loading, got {:?}", frames[1]);
        };
        let info = String::from_utf8_lossy(info);
        let lines: Vec<&str> = info.split(['\r', '\n']).filter(|l| !l.is_empty()).collect();
        // `loading:1` is the field every consumer keys on, and `role:loading`
        // is what keeps the master/replica filters from matching this node.
        assert!(lines.contains(&"loading:1"), "no loading:1 in {info:?}");
        assert!(
            lines.contains(&"role:loading"),
            "no role:loading in {info:?}"
        );
        assert!(
            !lines
                .iter()
                .any(|l| *l == "role:master" || *l == "role:replica"),
            "a loading node must not claim a serving role: {info:?}"
        );
    }

    /// And refuses the work it cannot do, with the error Redis defined for
    /// exactly this and every mainstream client already retries on.
    #[test]
    fn a_loading_node_refuses_data_commands_with_loading() {
        let mut s = connect(spawn_loading_server());
        let mut out = Vec::new();
        for cmd in [
            vec!["GET", "k"],
            vec!["SET", "k", "v"],
            // FLINTNS is the one that matters for the fleet: the proxy pins
            // every backend connection with it before any data command can
            // travel, so refusing it is what keeps a syncing node out of the
            // routing path with no change to the proxy at all.
            vec!["FLINTNS", "tenant"],
            vec!["FLINTPROMOTE", "1"],
        ] {
            encode(
                &Value::Array(Some(
                    cmd.iter()
                        .map(|a| Value::Bulk(Some(a.as_bytes().to_vec())))
                        .collect(),
                )),
                &mut out,
            );
        }
        s.write_all(&out).expect("send");
        for (i, f) in read_frames(&mut s, 4).into_iter().enumerate() {
            match f {
                Value::Error(e) => assert!(
                    e.starts_with("LOADING "),
                    "reply {i} must carry the LOADING code, got {e:?}"
                ),
                other => panic!("reply {i} must be refused while loading, got {other:?}"),
            }
        }
    }

    /// Inline commands too — `redis-cli`, a bare telnet and flintctl's edge
    /// probe all speak the inline form, and being unreachable to them is the
    /// bug being fixed.
    #[test]
    fn a_loading_node_answers_an_inline_ping() {
        let mut s = connect(spawn_loading_server());
        s.write_all(b"PING\r\nGET k\r\n").expect("send");
        let frames = read_frames(&mut s, 2);
        assert_eq!(frames[0], Value::Simple("PONG".into()));
        match &frames[1] {
            Value::Error(e) => assert!(e.starts_with("LOADING "), "{e:?}"),
            other => panic!("inline GET must be refused while loading, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod accepted_flags {
    use super::ACCEPTED_FLAGS;

    /// BUG-0048. `std::process::exit` runs libc `atexit` handlers, and RocksDB
    /// is C++ with statics registered there. Called from a thread that is not
    /// the only one running, that teardown races the serving path, the disk
    /// sampler and RocksDB's own compaction threads, and the process
    /// segfaults on the way out — which is how a deliberate `exit 3` from the
    /// replication tailer was reported as 139 under CI load.
    ///
    /// Six call sites were audited when this was fixed. Five are argument and
    /// engine validation on the main thread, before any store exists: no
    /// teardown to race, and `process::exit` flushes properly, so they stay.
    /// The sixth is inside `hard_exit` itself, as the non-unix fallback.
    ///
    /// Now eight. The two added are `--collection-read-mode` validation: an
    /// unparseable mode, and `observe` without a budget to observe against.
    /// Both are in the same category as the original five, and deliberately
    /// so: the block that reads those flags was MOVED ahead of the listener
    /// bind and the store open to put them there. Written where it was first
    /// drafted, beside the rest of the admission setup, each would have been
    /// a plain exit past an open RocksDB -- this bug, freshly reintroduced by
    /// the test that exists to catch it.
    ///
    /// The count is asserted so a SEVENTH forces a decision instead of
    /// inheriting the bug: startup path, raise the number and say why in the
    /// commit; anything that can run while the DB is open, call `hard_exit`.
    #[test]
    fn plain_process_exit_stays_out_of_the_running_paths() {
        // PRODUCTION TEXT ONLY, and the needle is assembled rather than
        // written. The first draft of this counted 9 against an expected 6:
        // the scan was matching its own assertion messages, which quote the
        // very string they are about. A source-reading test is inside the
        // source it reads, and that is not a detail it can afford to forget.
        let src = include_str!("main.rs");
        let prod = src
            .split("\n#[cfg(test)]")
            .next()
            .expect("source is non-empty");
        let needle = concat!("std::process::", "exit(");
        let exits = prod.matches(needle).count();
        assert_eq!(
            exits, 8,
            "the number of `std::process::exit(` call sites changed. If the new \
             one runs on the main thread before the store is opened it is fine \
             — raise this count. If it can run while the DB is open, it must be \
             `hard_exit`, or it will race RocksDB's static teardown (BUG-0048)."
        );

        // STRUCTURAL, not just numeric: the tailer is the path that was
        // actually bitten, and it runs on its own thread for the whole life
        // of a replica. A plain exit anywhere inside it is the bug verbatim,
        // whatever the total count happens to be.
        let replica = prod
            .split_once("\nmod replica {")
            .expect("the replica module moved or was renamed; this check is now blind")
            .1;
        assert!(
            !replica.contains(needle),
            "a plain process::exit is back inside `mod replica` — that is the \
             tailer thread, the DB is open, and this is BUG-0048 exactly. Use \
             hard_exit."
        );

        // CONTROL. Both assertions above pass trivially against an empty or
        // unrecognised source, which is the same output as success. Anchor on
        // something that must be present.
        assert!(
            replica.contains("hard_exit("),
            "the replica module no longer calls hard_exit at all — either the \
             WALGAP exit path is gone, or this scan is reading the wrong text"
        );
    }

    /// ACCEPTED_FLAGS is written by hand; `arg()` call sites are added by
    /// hand; nothing connects them. A flag added to the code and forgotten
    /// here becomes an argument the binary READS and REFUSES — the worst of
    /// both behaviours, and invisible until a caller trips it.
    ///
    /// This reads the source and asserts the two agree.
    #[test]
    fn every_arg_call_site_is_listed() {
        let src = include_str!("main.rs");
        let needle = concat!("arg(", '"');
        let mut missing: Vec<String> = Vec::new();
        let mut seen = 0usize;
        for (i, _) in src.match_indices(needle) {
            let rest = &src[i + needle.len()..];
            let Some(end) = rest.find('"') else { continue };
            let flag = &rest[..end];
            if !flag.starts_with("--") {
                continue;
            }
            seen += 1;
            if !ACCEPTED_FLAGS.contains(&flag) {
                missing.push(flag.to_string());
            }
        }
        // CONTROL. If the scan stops matching — the call shape changes, the
        // file moves — it finds nothing and reports agreement, which is the
        // same output as success. A count floor makes that failure loud.
        assert!(
            seen > 25,
            "found only {seen} arg(\"--…\") call sites; this scan has stopped \
             matching, so its empty complaint list means nothing"
        );
        missing.sort();
        missing.dedup();
        assert!(
            missing.is_empty(),
            "arg() reads flags that ACCEPTED_FLAGS omits, so the binary would \
             refuse arguments it actually honours: {missing:?}"
        );
    }

    /// The half the reverted attempt got wrong. These are flags real callers
    /// pass — checked against `tools/` — and refusing any of them is how the
    /// first attempt hung two gates.
    #[test]
    fn flags_the_drills_actually_pass_are_accepted() {
        for f in [
            "--port",
            "--engine",
            "--data-dir",
            "--replica-of",
            "--bind",
            "--internal-ca",
            "--internal-cert",
            "--internal-key",
            "--lag-hard-ms",
            "--wal-fsync-ms",
        ] {
            assert!(
                ACCEPTED_FLAGS.contains(&f),
                "{f} is passed by drills in tools/ but would be REFUSED"
            );
        }
    }
}

#[cfg(test)]
mod evictable_ns_config {
    #[cfg(feature = "rocks")]
    use super::find_info_field;
    use super::parse_evictable_ns;

    /// An unanswerable capacity must render `?`, never `0`.
    ///
    /// Zero is a LEGITIMATE reading here — an empty evictable namespace — so
    /// the two cannot share a rendering. If they did, the field an operator
    /// consults while a cache fills would report maximum headroom in exactly
    /// the case where nothing is known, which is the reassuring direction and
    /// therefore the dangerous one. `rocks: None` is that case, and it is also
    /// the state a read-only or not-yet-opened seat is in.
    ///
    /// The empty-list case is asserted too, so that a durable deployment which
    /// declared nothing sees an empty field rather than a figure implying a
    /// policy it never opted into.
    #[test]
    #[cfg(feature = "rocks")]
    fn an_unanswerable_capacity_is_not_zero() {
        use super::{EVICTABLE_NS, evictable_ns_bytes_joined};

        // Nothing declared: the default, and the common case.
        assert_eq!(evictable_ns_bytes_joined(&None), "");

        if let Ok(mut g) = EVICTABLE_NS.write() {
            *g = vec!["cache".to_string(), "scratch".to_string()];
        }
        // No engine to ask. Not "0 bytes used" — "not known".
        let out = evictable_ns_bytes_joined(&None);
        assert_eq!(
            out, "cache=?,scratch=?",
            "unknown must not render as a size"
        );
        assert!(
            !out.contains("=0"),
            "an unanswered capacity rendered as zero: {out}"
        );
        if let Ok(mut g) = EVICTABLE_NS.write() {
            g.clear();
        }
    }

    /// Two seats given the same namespaces in a different order must compare
    /// EQUAL, or the pair-agreement check reports a mismatch that is not one —
    /// a false alarm about divergent policy is worse than no alarm, because it
    /// trains an operator to ignore the real one.
    #[test]
    fn the_canonical_form_is_order_and_whitespace_independent() {
        assert_eq!(
            parse_evictable_ns(" beta, alpha ,beta,"),
            parse_evictable_ns("alpha,beta")
        );
        assert_eq!(parse_evictable_ns("alpha,beta"), vec!["alpha", "beta"]);
        assert!(parse_evictable_ns("").is_empty());
        assert!(parse_evictable_ns("  , ,").is_empty());
    }

    /// ABSENT and EMPTY are different answers. A peer that predates this field
    /// cannot be compared; a peer that answered with no evictable namespaces
    /// can. Collapsing them would report agreement with a seat that never
    /// answered the question.
    #[test]
    #[cfg(feature = "rocks")]
    fn an_absent_field_is_not_an_empty_one() {
        let with = b"role:master\r\nevictable_ns:cache\r\nuptime_ms:5\r\n";
        let empty = b"role:master\r\nevictable_ns:\r\nuptime_ms:5\r\n";
        let without = b"role:master\r\nuptime_ms:5\r\n";
        assert_eq!(
            find_info_field(with, "evictable_ns").as_deref(),
            Some("cache")
        );
        assert_eq!(find_info_field(empty, "evictable_ns").as_deref(), Some(""));
        assert_eq!(find_info_field(without, "evictable_ns"), None);
    }

    /// The substring trap, as a test. FLINTINFO carries `writes_shed_lag`,
    /// `lag_ms` and `lag_hard_ms`; an unanchored search for `lag` would match
    /// inside the first. Four separate defects in this repository have been
    /// exactly this shape — argv matching its own pattern, `i-` matching
    /// inside `ami-`, `$B` matching inside `$BK`, `flint-server` matching
    /// inside `flint-server/rocks`.
    #[test]
    #[cfg(feature = "rocks")]
    fn a_field_name_is_not_matched_as_a_substring_of_another() {
        let info = b"writes_shed_lag:9\r\nlag_ms:3\r\nlag_hard_ms:1000\r\n";
        assert_eq!(find_info_field(info, "lag_ms").as_deref(), Some("3"));
        assert_eq!(
            find_info_field(info, "lag_hard_ms").as_deref(),
            Some("1000")
        );
        assert_eq!(
            find_info_field(info, "writes_shed_lag").as_deref(),
            Some("9")
        );
        // `shed_lag` is a suffix of a real field and must NOT resolve.
        assert_eq!(find_info_field(info, "shed_lag"), None);
    }
}

/// BUG-0062. The boot picks a rewind candidate by reading a directory, so the
/// only thing that can stop it re-picking a snapshot it just died on is that
/// snapshot no longer being in the directory's candidate set. These hold that,
/// and the count assertions are what distinguish "one boot to a re-seed" from
/// "one boot per snapshot", which is the difference that mattered in the soak.
#[cfg(all(test, feature = "rocks"))]
mod quarantine_tests {
    use super::*;

    fn scratch(name: &str) -> std::path::PathBuf {
        let d =
            std::env::temp_dir().join(format!("flint-quarantine-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).expect("scratch dir");
        d
    }

    fn touch_snap(dir: &std::path::Path, ms: u64, seq: u64, generation: u64, ctr: u64) -> String {
        let n = format!("snap-{ms}-seq{seq}-e{generation}.{ctr}");
        // Snapshots are directories on a real node; rename must handle that.
        std::fs::create_dir_all(dir.join(&n)).expect("snap dir");
        n
    }

    fn candidates(dir: &std::path::Path) -> Vec<u64> {
        let mut v: Vec<u64> = std::fs::read_dir(dir)
            .expect("read scratch")
            .flatten()
            .filter_map(|e| parse_rewind_candidate(e.file_name().to_str()?).map(|(s, _)| s))
            .collect();
        v.sort_unstable();
        v
    }

    /// The soak's own directory: three snapshots, the newest at the cursor the
    /// master could no longer reach. All three must leave the candidate set in
    /// ONE pass — quarantining only the one that failed would leave 196517 for
    /// the next boot to fail on, then 130042, three deadlines to reach the
    /// re-seed that was available immediately.
    #[test]
    fn every_snapshot_the_cursor_cannot_reach_stops_being_a_candidate() {
        let d = scratch("soak-shape");
        touch_snap(&d, 1787809837104, 130042, 0, 4);
        touch_snap(&d, 1787809867201, 196517, 0, 6);
        touch_snap(&d, 1787809927380, 207196, 0, 7);
        assert_eq!(
            candidates(&d),
            vec![130042, 196517, 207196],
            "control: all three are candidates before the quarantine"
        );

        let n = quarantine_unresumable(d.to_str().expect("test setup"), 207196);

        assert_eq!(n, 3, "all three are at or below the unresumable cursor");
        assert!(
            candidates(&d).is_empty(),
            "the next boot must find NO rewind candidate, got {:?}",
            candidates(&d)
        );
        // Renamed, not deleted: still three directories on disk.
        assert_eq!(std::fs::read_dir(&d).expect("scratch readable").count(), 3);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// A snapshot the master can still reach is untouched — the quarantine is
    /// bounded by the cursor, not a blanket wipe of the snapshot dir.
    #[test]
    fn a_snapshot_above_the_cursor_survives() {
        let d = scratch("above");
        touch_snap(&d, 1, 100, 0, 1);
        touch_snap(&d, 2, 300, 0, 1);
        let n = quarantine_unresumable(d.to_str().expect("test setup"), 200);
        assert_eq!(n, 1);
        assert_eq!(
            candidates(&d),
            vec![300],
            "the reachable snapshot is still a candidate"
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    /// A second pass must be a no-op, not `unresumable-unresumable-…`: the
    /// tailer can hit this path again before the boot re-seeds.
    /// BUG-0071. The quarantined NAME is now a contract between two
    /// functions: `quarantine_unresumable` writes the cursor it disqualified
    /// against, and `try_rewind` reads it to decide whether a later, lower
    /// fence may reconsider the snapshot. A round trip pins both halves —
    /// asserting the parser against a hand-written string would pass while the
    /// writer drifted.
    #[test]
    fn a_quarantined_name_round_trips_its_cursor() {
        let d = std::env::temp_dir().join(format!("flint-q-rt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).expect("scratch");
        std::fs::write(d.join("snap-111-seq500-e0.7"), b"x").expect("write");
        assert_eq!(
            quarantine_unresumable(d.to_str().expect("utf8"), 900),
            1,
            "fixture: the snapshot must actually be quarantined"
        );
        let name = std::fs::read_dir(&d)
            .expect("read")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .find(|n| n.starts_with(UNRESUMABLE_PREFIX))
            .expect("a quarantined name");
        let (cursor, seq, epoch) =
            parse_quarantined_candidate(&name).expect("the writer's own name must parse");
        assert_eq!(cursor, 900, "the disqualifying cursor was lost: {name}");
        assert_eq!(seq, 500, "the snapshot's own seq was lost: {name}");
        assert_eq!(epoch.generation, 0);
        assert_eq!(epoch.counter, 7);
        // And it must NOT read as an ordinary candidate, or the fence would
        // reconsider it unconditionally.
        assert!(
            parse_rewind_candidate(&name).is_none(),
            "a quarantined name still parses as a live candidate: {name}"
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    /// Names written before BUG-0071 carry no cursor. They must stay
    /// quarantined rather than being re-admitted on an upgrade, because
    /// nothing records what they were disqualified against.
    #[test]
    fn a_pre_cursor_quarantined_name_is_not_reconsidered() {
        assert!(
            parse_quarantined_candidate("unresumable-snap-111-seq500-e0.7").is_none(),
            "a legacy quarantined name was re-admitted; nothing says what it failed against"
        );
        // Nor may a malformed cursor be read as one.
        assert!(parse_quarantined_candidate("unresumable-cxx-snap-111-seq500-e0.7").is_none());
        assert!(parse_quarantined_candidate("unresumable-c-snap-111-seq500-e0.7").is_none());
    }

    #[test]
    fn quarantine_is_idempotent() {
        let d = scratch("idempotent");
        touch_snap(&d, 1, 100, 0, 1);
        assert_eq!(
            quarantine_unresumable(d.to_str().expect("test setup"), 200),
            1
        );
        assert_eq!(
            quarantine_unresumable(d.to_str().expect("test setup"), 200),
            0,
            "nothing left that parses as a candidate, so nothing to rename"
        );
        let names: Vec<String> = std::fs::read_dir(&d)
            .expect("test setup")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names.len(), 1);
        assert!(
            !names[0].starts_with("unresumable-unresumable-"),
            "double-prefixed: {}",
            names[0]
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    /// The property the rename relies on, asserted directly rather than
    /// inferred: the prefix is what disqualifies the name.
    #[test]
    fn a_quarantined_name_does_not_parse_as_a_candidate() {
        let good = "snap-123-seq456-e0.7";
        assert!(parse_rewind_candidate(good).is_some(), "control");
        assert!(
            parse_rewind_candidate(&format!("{UNRESUMABLE_PREFIX}{good}")).is_none(),
            "the prefix must remove it from the candidate set"
        );
    }

    /// Anything that is not an epoch-labeled snapshot is left alone — the
    /// snapshot dir is not exclusively ours to rewrite.
    #[test]
    fn unrelated_entries_are_untouched() {
        let d = scratch("unrelated");
        std::fs::create_dir_all(d.join("in-progress")).expect("test setup");
        std::fs::write(d.join("README"), b"x").expect("test setup");
        touch_snap(&d, 1, 50, 0, 1);
        assert_eq!(
            quarantine_unresumable(d.to_str().expect("test setup"), 100),
            1
        );
        assert!(d.join("in-progress").exists(), "unrelated dir renamed");
        assert!(d.join("README").exists(), "unrelated file renamed");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// A missing snapshot dir is a normal state (a node that never took one),
    /// and must not panic on the path that is already handling a failure.
    #[test]
    fn a_missing_snapshot_dir_is_not_fatal() {
        let d =
            std::env::temp_dir().join(format!("flint-quarantine-{}-absent", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        assert_eq!(
            quarantine_unresumable(d.to_str().expect("test setup"), 1),
            0
        );
    }
}

/// The distinction that cost soak 2026-08-27 cycle 3 ninety-five seconds: a
/// master's refusal and a socket's failure arrived as the same `Err`, so a
/// fence-vouched snapshot was discarded over `EAGAIN` and the rejoin became a
/// 93-file full re-seed while the promoted master shed every write.
#[cfg(all(test, feature = "rocks"))]
mod probe_verdict_tests {
    use super::{ProbeVerdict, classify_probe};
    use flint_resp::Value;
    use std::io::{Error, ErrorKind};

    #[test]
    fn an_ok_handshake_is_resumable() {
        assert_eq!(
            classify_probe(Ok(Value::Simple("FLINTSYNC-OK 42 e0.7".into()))),
            ProbeVerdict::Resumable
        );
    }

    /// The master spoke. Asking again gets the same answer, so this must not
    /// be retried — retrying a WALGAP is the livelock BUG-0062 filed.
    #[test]
    fn a_master_error_is_a_verdict_not_a_hiccup() {
        for msg in [
            "WALGAP cursor 207196 is no longer reachable from this WAL: full sync required",
            "cursor 999 is past the promotion fence 500 for epoch (0,7)",
        ] {
            assert_eq!(
                classify_probe(Ok(Value::Error(msg.into()))),
                ProbeVerdict::Refused(msg.into()),
                "a master's answer must classify as Refused: {msg}"
            );
        }
    }

    /// THE REGRESSION. EAGAIN is the exact errno the soak hit ("Resource
    /// temporarily unavailable (os error 11)"). The master never answered, so
    /// this says nothing about the copy and must not read as a refusal.
    #[test]
    fn eagain_is_transport_not_refusal() {
        let v = classify_probe(Err(Error::from_raw_os_error(11)));
        assert!(
            matches!(v, ProbeVerdict::Transport(_)),
            "EAGAIN must be Transport, got {v:?} — classifying it as Refused discards a \
             fence-vouched snapshot and turns a bounded tail into a full re-seed"
        );
    }

    /// Every transport failure, not just the one that happened to bite.
    #[test]
    fn the_usual_transport_failures_are_all_retryable() {
        for kind in [
            ErrorKind::WouldBlock,
            ErrorKind::TimedOut,
            ErrorKind::ConnectionRefused,
            ErrorKind::ConnectionReset,
            ErrorKind::BrokenPipe,
        ] {
            let v = classify_probe(Err(Error::new(kind, "x")));
            assert!(
                matches!(v, ProbeVerdict::Transport(_)),
                "{kind:?} must be Transport, got {v:?}"
            );
        }
    }

    /// A reply we cannot parse is a protocol fault, and retrying it spins.
    /// Deliberately a refusal so the boot falls back to the path that works.
    #[test]
    fn an_unparseable_reply_is_refused_so_the_boot_falls_back() {
        assert!(matches!(
            classify_probe(Ok(Value::Integer(7))),
            ProbeVerdict::Refused(_)
        ));
    }
}

#[cfg(test)]
mod pure_write_predicate {
    use super::is_pure_write;

    fn args(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.as_bytes().to_vec()).collect()
    }

    /// The predicate decides whether a write may take its stripe SHARED, so
    /// admitting anything that reads its old value is a lost update, not a
    /// slow path (ADR-0027). Every option that makes SET read adds a token,
    /// which is why the test is arity and not a name list.
    #[test]
    fn only_a_bare_three_token_set_is_pure() {
        assert!(is_pure_write(b"SET", &args(&["SET", "k", "v"])));

        for cmd in [
            vec!["SET", "k", "v", "NX"],
            vec!["SET", "k", "v", "XX"],
            vec!["SET", "k", "v", "GET"],
            vec!["SET", "k", "v", "KEEPTTL"],
            vec!["SET", "k", "v", "EX", "10"],
            vec!["SET", "k", "v", "PX", "10000"],
            vec!["SET", "k", "v", "EXAT", "99999"],
        ] {
            assert!(
                !is_pure_write(b"SET", &args(&cmd)),
                "SET with options reads its old header: {cmd:?}"
            );
        }

        // The read-modify-write commands, which is exactly the set
        // `write_queue::is_batchable` would have let through.
        for cmd in [
            vec!["INCR", "k"],
            vec!["DECR", "k"],
            vec!["INCRBY", "k", "5"],
            vec!["APPEND", "k", "v"],
            vec!["SETNX", "k", "v"],
            vec!["SETEX", "k", "10", "v"],
            vec!["LPUSH", "k", "v"],
        ] {
            let name = cmd[0].as_bytes().to_ascii_uppercase();
            assert!(
                !is_pure_write(&name, &args(&cmd)),
                "read-modify-write must take the stripe exclusively: {cmd:?}"
            );
        }
    }
}
