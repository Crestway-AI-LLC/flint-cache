// SPDX-License-Identifier: Elastic-2.0
//! Refusing writes while there is still room to recover.
//!
//! Per-tenant quotas bound each namespace; nothing bounds the HOST. The sum
//! of quotas is meant to exceed the disk — that oversubscription is the
//! packing economics — so a full node is a normal consequence of the
//! business model, not operator error, and something has to notice.
//!
//! The gate deliberately fires EARLY, with gigabytes still free. An LSM
//! needs headroom to compact (new SSTs are written before old ones are
//! dropped), and the cure for a full disk is a trap without it: freeing
//! space means deleting, a delete is a write, and reclaiming the bytes
//! needs the compaction that has no room to run. Waiting for the disk to
//! actually fill produces a node that cannot dig itself out.
//!
//! ONE level, deliberately. The gate sheds ordinary writes with `-QUOTA`;
//! reads still serve and space-REDUCING commands still run, which is what
//! lets the condition clear itself.
//!
//! A second, deeper level was drafted and removed: it was to stop the async
//! write queue draining, but the gate already sits upstream of the queue's
//! submit, so shedding refuses batched writes before they are ever queued.
//! The only thing "stop draining" could still do is strand writes the
//! server already accepted, leaving those clients waiting on acks that
//! never come — strictly worse than letting a bounded batch finish. A knob
//! with no defensible action is worse than no knob.
//!
//! The gap that second level was reaching for is real and remains open: a
//! REPLICA applying its master's WAL keeps writing regardless of any
//! client-facing gate. Fixing that means declining to apply, which stops
//! acking and pushes back through the lag cap — worth doing, and bigger
//! than this change.
//!
//! `-QUOTA` is reused rather than inventing `-DISKFULL`: every client, SDK
//! and runbook already handles it, and the required client behaviour is
//! identical (stop writing, delete to recover, reads still work). The
//! MESSAGE says plainly that this is the server and not the tenant's cap,
//! so nobody goes hunting through a console that shows them well under
//! quota.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Thresholds, as configured.
#[derive(Debug, Clone, Copy)]
pub struct Thresholds {
    /// Shed below this share of the filesystem, 0 disables the percentage
    /// test.
    pub min_free_pct: u64,
    /// Shed below this many free bytes, whichever binds first. A percentage
    /// alone is useless on a very large disk (10% of 16 TB is 1.6 TB of
    /// headroom nobody wants to hold) and a byte floor alone is useless on
    /// a small one, so both apply and the stricter wins.
    pub min_free_bytes: u64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            min_free_pct: 10,
            min_free_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}

/// What the sampler decided this tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Ok,
    /// Ordinary writes refused; reads and space-reducing writes continue.
    Shed,
}

/// Hysteresis: once shedding, require this much MORE than the threshold
/// before reopening. Without it a node parked exactly at the line flaps
/// between accepting and refusing writes every sample, which is worse for
/// a caller than either steady state.
const REOPEN_MARGIN: u64 = 5; // percent, relative

/// Decide from one sample. Pure, so the interesting cases are testable
/// without a real filesystem in a real state of distress.
///
/// `currently` is the last verdict, and only ever relaxes the test — the
/// gate closes the moment a sample says so, and opens only once the sample
/// clears the threshold plus a margin.
pub fn verdict(
    usage: Option<flint_storage::disk::Usage>,
    t: Thresholds,
    currently: Verdict,
) -> Verdict {
    // No reading is NOT evidence of fullness. A stat that fails (a path
    // that vanished, an exotic filesystem, a container quirk) must never
    // shed writes on its own — that turns a monitoring gap into an outage.
    let Some(u) = usage else {
        return Verdict::Ok;
    };
    let shedding = currently != Verdict::Ok;
    // Rounds UP: `10 * 105 / 100` truncates back to 10, which silently
    // erased the margin for exactly the small percentage thresholds it
    // exists to protect. Ceiling division keeps it real at every scale.
    let scale = |v: u64| {
        if shedding {
            v.saturating_mul(100 + REOPEN_MARGIN).div_ceil(100)
        } else {
            v
        }
    };
    let pct_bad = t.min_free_pct > 0 && u.free_pct() < scale(t.min_free_pct);
    let bytes_bad = t.min_free_bytes > 0 && u.free_bytes < scale(t.min_free_bytes);
    if pct_bad || bytes_bad {
        Verdict::Shed
    } else {
        Verdict::Ok
    }
}

/// Never sample slower than the configured cadence; never faster than this.
/// A `statvfs` is microseconds, so the floor exists to bound the syscall rate
/// under a pathological drain, not because the call is expensive.
const MIN_SAMPLE: std::time::Duration = std::time::Duration::from_millis(50);

/// How close to the shed line one sample is allowed to bring us: the interval
/// is set so at most a quarter of the remaining headroom can be consumed
/// before the next look.
const HEADROOM_DIVISOR: f64 = 4.0;

/// The free-byte level at which [`verdict`] flips to `Shed` on this
/// filesystem. Both thresholds apply and the stricter wins, so this is the
/// higher of the two — the same rule `verdict` uses, expressed as a level
/// instead of a test, because pacing needs the distance to it.
pub fn shed_floor_bytes(u: flint_storage::disk::Usage, t: Thresholds) -> u64 {
    let by_pct = if t.min_free_pct > 0 {
        u.total_bytes.saturating_mul(t.min_free_pct) / 100
    } else {
        0
    };
    by_pct.max(t.min_free_bytes)
}

/// How long to wait before the next sample.
///
/// A FIXED cadence is a promise the guard cannot keep. Whether it holds the
/// threshold depends entirely on how fast the disk is filling, and the write
/// path can cross the whole headroom between two ticks — at which point the
/// guard is not a headroom guard, it is a report of what already happened.
/// Measured on `disk_selffill_drill.sh`: with a 500 ms cadence and a 20%
/// threshold, three runs in five saw the first refusal at 7-10% free, because
/// ~100 MB landed inside one interval.
///
/// So pace against the DRAIN RATE rather than the clock: estimate when free
/// space will reach the shed floor at the rate just observed, and look again
/// at a quarter of that. Bounded both ways — never slower than the configured
/// cadence, never faster than [`MIN_SAMPLE`] — so an idle node costs exactly
/// what it costs today and a filling one cannot outrun the guard by more than
/// a quarter of its remaining headroom.
///
/// Deliberately only reacts to a FALLING sample. Space coming back is not
/// urgent: writes are already being refused, and the reopen has its own
/// hysteresis margin.
pub fn pace(
    prev_free: Option<u64>,
    cur: flint_storage::disk::Usage,
    t: Thresholds,
    elapsed: std::time::Duration,
    ceiling: std::time::Duration,
) -> std::time::Duration {
    let Some(prev) = prev_free else {
        return ceiling;
    };
    let drained = prev.saturating_sub(cur.free_bytes);
    let headroom = cur.free_bytes.saturating_sub(shed_floor_bytes(cur, t));
    // Nothing draining, or already at the line: the ordinary cadence answers
    // both. Below the line the space in play belongs to compaction, and
    // watching it faster changes nothing about who gets refused.
    if drained == 0 || headroom == 0 {
        return ceiling;
    }
    let secs = elapsed.as_secs_f64().max(0.001);
    let eta = headroom as f64 / (drained as f64 / secs);
    let want = std::time::Duration::from_secs_f64((eta / HEADROOM_DIVISOR).max(0.0));
    want.clamp(MIN_SAMPLE.min(ceiling), ceiling)
}

/// The live state every connection reads, and the sampler writes.
#[derive(Debug, Default)]
pub struct DiskGuard {
    free_bytes: AtomicU64,
    total_bytes: AtomicU64,
    shed: AtomicBool,
    /// Samples that produced no reading. Surfaced so an operator can tell
    /// "healthy" from "never actually measured".
    unknown_samples: AtomicU64,
}

impl DiskGuard {
    pub fn apply(&self, usage: Option<flint_storage::disk::Usage>, v: Verdict) {
        match usage {
            Some(u) => {
                self.free_bytes.store(u.free_bytes, Ordering::Relaxed);
                self.total_bytes.store(u.total_bytes, Ordering::Relaxed);
            }
            None => {
                self.unknown_samples.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.shed.store(v != Verdict::Ok, Ordering::Relaxed);
    }

    /// True when ordinary writes must be refused.
    pub fn shedding(&self) -> bool {
        self.shed.load(Ordering::Relaxed)
    }

    // Reported through FLINTINFO, which only carries these fields on the
    // rocks build — the mem engine has no filesystem to run out of and
    // starts no sampler. Same idiom the other rocks-only surfaces use.
    #[cfg_attr(not(feature = "rocks"), allow(dead_code))]
    pub fn current(&self) -> Verdict {
        if self.shed.load(Ordering::Relaxed) {
            Verdict::Shed
        } else {
            Verdict::Ok
        }
    }

    /// `(free, total, unknown_samples)` for FLINTINFO and the exporter.
    #[cfg_attr(not(feature = "rocks"), allow(dead_code))]
    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.free_bytes.load(Ordering::Relaxed),
            self.total_bytes.load(Ordering::Relaxed),
            self.unknown_samples.load(Ordering::Relaxed),
        )
    }
}

/// The reply a shed write gets. See the module docs for why `-QUOTA`.
pub const DISK_FULL_ERROR: &str = "QUOTA server is low on disk space; writes rejected until space is reclaimed \
     (reads still served, and DEL/UNLINK/EXPIRE/FLUSHALL still work)";

#[cfg(test)]
mod tests {
    use super::*;
    use flint_storage::disk::Usage;
    use std::time::Duration;

    fn u(free: u64, total: u64) -> Option<Usage> {
        Some(raw(free, total))
    }
    /// The same reading unwrapped, for the pacing helpers — they take a
    /// Usage rather than an Option, because pacing off a reading you do not
    /// have is inventing one (see the `None` arm at the sampler).
    fn raw(free: u64, total: u64) -> Usage {
        Usage {
            free_bytes: free,
            total_bytes: total,
        }
    }
    const GB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn healthy_disk_passes() {
        let t = Thresholds::default();
        assert_eq!(verdict(u(50 * GB, 100 * GB), t, Verdict::Ok), Verdict::Ok);
    }

    #[test]
    fn either_threshold_can_bind_and_the_stricter_wins() {
        let t = Thresholds {
            min_free_pct: 10,
            min_free_bytes: 2 * GB,
        };
        // Plenty of percent, not enough bytes: a big disk that is fine by
        // ratio and nearly out in absolute terms. 1 GB is 1% of 100 GB, so
        // both tests bind here — the point is that the byte floor alone
        // would have caught it.
        assert_eq!(
            verdict(u(GB, 100 * GB), t, Verdict::Ok),
            Verdict::Shed,
            "1 GB free is under the 2 GB floor"
        );
        // Plenty of bytes, not enough percent: 70 GB free is far past the
        // byte floor, but 7% of a 1 TB disk still trips the ratio test.
        assert_eq!(
            verdict(u(70 * GB, 1000 * GB), t, Verdict::Ok),
            Verdict::Shed,
            "7% free trips the percentage test despite 70 GB free"
        );
        // Neither binds: comfortable on both counts.
        assert_eq!(verdict(u(300 * GB, 1000 * GB), t, Verdict::Ok), Verdict::Ok);
    }

    /// The property that keeps a node parked at the threshold from flapping
    /// between accepting and refusing writes on every sample.
    #[test]
    fn reopening_needs_more_than_closing_did() {
        let t = Thresholds {
            min_free_pct: 10,
            min_free_bytes: 0,
        };
        // Exactly at the line, coming from healthy: still fine.
        assert_eq!(verdict(u(10, 100), t, Verdict::Ok), Verdict::Ok);
        // The same reading while already shedding does NOT reopen.
        assert_eq!(verdict(u(10, 100), t, Verdict::Shed), Verdict::Shed);
        // Clearing the margin does.
        assert_eq!(verdict(u(11, 100), t, Verdict::Shed), Verdict::Ok);
    }

    /// The failure mode that would turn a broken stat into an outage.
    #[test]
    fn an_unreadable_filesystem_is_never_treated_as_full() {
        let t = Thresholds::default();
        assert_eq!(verdict(None, t, Verdict::Ok), Verdict::Ok);
        assert_eq!(
            verdict(None, t, Verdict::Shed),
            Verdict::Ok,
            "losing the reading must release the gate, not hold it shut forever"
        );
    }

    #[test]
    fn guard_tracks_state_and_counts_unknown_samples() {
        let g = DiskGuard::default();
        g.apply(u(GB, 100 * GB), Verdict::Shed);
        assert!(g.shedding());
        assert_eq!(g.current(), Verdict::Shed);
        let (free, total, unknown) = g.snapshot();
        assert_eq!((free, total, unknown), (GB, 100 * GB, 0));
        g.apply(None, Verdict::Ok);
        assert!(!g.shedding());
        assert_eq!(g.snapshot().2, 1, "the unreadable sample was counted");
    }

    #[test]
    fn zero_disables_a_threshold() {
        let off = Thresholds {
            min_free_pct: 0,
            min_free_bytes: 0,
        };
        assert_eq!(verdict(u(0, 100 * GB), off, Verdict::Ok), Verdict::Ok);
    }

    // ---- pacing: the guard must not be outrunnable -----------------------

    /// The two thresholds are a floor each, and the stricter one is the level
    /// pacing has to steer by — the same rule `verdict` tests with.
    #[test]
    fn the_shed_floor_is_the_stricter_of_the_two_thresholds() {
        let t = Thresholds {
            min_free_pct: 10,
            min_free_bytes: 2 * GB,
        };
        // 10% of 100GB = 10GB, which binds above the 2GB byte floor.
        assert_eq!(shed_floor_bytes(raw(50 * GB, 100 * GB), t), 10 * GB);
        // 10% of 10GB = 1GB, so here the byte floor is the stricter one.
        assert_eq!(shed_floor_bytes(raw(5 * GB, 10 * GB), t), 2 * GB);
    }

    #[test]
    fn an_idle_disk_keeps_the_configured_cadence() {
        let t = Thresholds::default();
        let every = Duration::from_secs(2);
        let now = raw(50 * GB, 100 * GB);
        // No previous reading, and nothing draining, both answer `every`.
        assert_eq!(pace(None, now, t, every, every), every);
        assert_eq!(
            pace(Some(50 * GB), now, t, every, every),
            every,
            "a disk that is not filling costs exactly what it costs today"
        );
    }

    /// THE BUG. A fixed cadence lets the write path cross the whole headroom
    /// between two looks: 40GB of headroom against 100GB/s of drain is gone
    /// in under half a second, and the guard would not look again for two.
    #[test]
    fn a_fast_drain_shortens_the_interval_far_below_the_cadence() {
        let t = Thresholds::default(); // 10% -> floor 10GB on a 100GB disk
        let every = Duration::from_secs(2);
        // 10GB consumed in the last 100ms = 100GB/s; 40GB of headroom left.
        let now = raw(50 * GB, 100 * GB);
        let next = pace(Some(60 * GB), now, t, Duration::from_millis(100), every);
        assert!(
            next < Duration::from_millis(150),
            "at 100GB/s the 40GB headroom is 0.4s away; sampling in {next:?} \
             lets a quarter of it go unobserved at most"
        );
        assert!(next >= MIN_SAMPLE, "and never tighter than the floor");
    }

    /// Discrimination: the SAME headroom with a gentle drain must not panic
    /// the sampler into spinning.
    #[test]
    fn a_slow_drain_keeps_the_cadence() {
        let t = Thresholds::default();
        let every = Duration::from_secs(2);
        let now = raw(50 * GB, 100 * GB);
        // 1MB in 100ms = 10MB/s: the 40GB headroom is over an hour away.
        let next = pace(
            Some(50 * GB + 1024 * 1024),
            now,
            t,
            Duration::from_millis(100),
            every,
        );
        assert_eq!(next, every);
    }

    /// Below the line there is nothing left to protect — writes are already
    /// refused, and the space still moving belongs to compaction. Watching it
    /// faster would burn syscalls to change nothing.
    #[test]
    fn at_or_below_the_floor_the_cadence_returns() {
        let t = Thresholds::default();
        let every = Duration::from_secs(2);
        let now = raw(5 * GB, 100 * GB); // under the 10GB floor
        assert_eq!(
            pace(Some(20 * GB), now, t, Duration::from_millis(100), every),
            every
        );
    }

    /// The ceiling is a ceiling in both directions: a caller that configures a
    /// cadence tighter than MIN_SAMPLE gets what it asked for, not a slower
    /// interval imposed by the clamp.
    #[test]
    fn a_cadence_below_the_floor_is_honoured() {
        let t = Thresholds::default();
        let every = Duration::from_millis(10);
        let now = raw(50 * GB, 100 * GB);
        let next = pace(Some(60 * GB), now, t, Duration::from_millis(100), every);
        assert_eq!(next, every);
    }
}
