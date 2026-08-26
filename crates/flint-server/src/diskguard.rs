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

/// Where capacity reclaim engages, as a percentage OF THE SHED FLOOR.
///
/// ADR-0023 D7 requirement 4: reclaim must run above the shed threshold, so an
/// evictable namespace under pressure evicts rather than ever reaching
/// `-QUOTA`, while a non-evictable namespace behaves exactly as it does today.
///
/// Expressing these relative to [`shed_floor_bytes`] rather than as their own
/// independent thresholds is what makes that ordering ARITHMETIC instead of a
/// pair of numbers somebody has to keep consistent. Two free-standing knobs can
/// be configured into the wrong order and the failure would be silent — an
/// evictable namespace shedding writes while sitting on reclaimable data. 150%
/// of a floor cannot be below it.
const RECLAIM_START_PCT_OF_FLOOR: u64 = 150;
/// Reclaim down to here, not merely back to the start line. The gap is the
/// hysteresis: without it a node parked at the mark reclaims a few keys every
/// sample forever, which is the eviction equivalent of the write flapping
/// [`REOPEN_MARGIN`] exists to prevent.
const RECLAIM_TARGET_PCT_OF_FLOOR: u64 = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReclaimAction {
    /// Nothing to do: either there is headroom, or there is no signal.
    Idle,
    /// Reclaim from evictable namespaces until at least this many bytes are
    /// free.
    ReclaimToFreeBytes(u64),
}

/// Should capacity reclaim be running, and down to what?
///
/// `currently` carries the hysteresis: idle engages at the start mark, and a
/// run in progress continues to the (higher) target rather than stopping the
/// moment it crosses back over the line.
///
/// **No signal means Idle, and that is the fail-safe direction here even
/// though it is the opposite of [`verdict`]'s.** `verdict` answers Ok when it
/// cannot measure, because a monitoring gap must not shed writes and turn
/// itself into an outage. This answers Idle for the same reason read the other
/// way round: the action it authorises is DELETION, so an unmeasurable disk
/// must not license reclaiming anything. Both refuse to act on a blind sample;
/// they differ only in which action was the dangerous one.
pub fn reclaim_action(
    usage: Option<flint_storage::disk::Usage>,
    t: Thresholds,
    currently: bool,
) -> ReclaimAction {
    let Some(u) = usage else {
        return ReclaimAction::Idle;
    };
    let floor = shed_floor_bytes(u, t);
    if floor == 0 {
        // No shed line configured at all, so there is nothing to stay above.
        return ReclaimAction::Idle;
    }
    let start = floor.saturating_mul(RECLAIM_START_PCT_OF_FLOOR) / 100;
    let target = floor.saturating_mul(RECLAIM_TARGET_PCT_OF_FLOOR) / 100;
    let engage = if currently {
        u.free_bytes < target
    } else {
        u.free_bytes < start
    };
    if engage {
        ReclaimAction::ReclaimToFreeBytes(target)
    } else {
        ReclaimAction::Idle
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

/// How far out the proximity term starts tightening, in multiples of the shed
/// floor. It has to begin well ABOVE the line, because what it bounds is the
/// FIRST tick of a burst — the one the rate term cannot yet have seen.
///
/// Derived, not picked. On the self-fill drill (768 MB, 20% floor, ~200 MB/s
/// of physical growth) the whole approach from 40% free down to the line was
/// still sampled at the full 500 ms ceiling, so that one unpaced tick cost
/// ~13 points on its own — which is exactly the overshoot tail measured with
/// the proximity term starting at ONE floor. Four floors puts the same tick
/// at ~125 ms and ~3 points. On a real node (3.7 TB, 10% floor) the term
/// costs nothing above 50% free and only reaches the 50 ms floor when the
/// headroom left is a rounding error.
const PROXIMITY_BAND: f64 = 4.0;

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
    let floor = shed_floor_bytes(cur, t);
    let headroom = cur.free_bytes.saturating_sub(floor);
    // Already at the line: the space still in play belongs to compaction, and
    // watching it faster changes nothing about who gets refused.
    if headroom == 0 || floor == 0 {
        return ceiling;
    }
    // PROXIMITY, which does not need a previous reading — and that is the
    // point. The rate term below is REACTIVE: it can only shorten the interval
    // AFTER an interval has been consumed at speed, so the first tick of a
    // burst is always unpaced, and on a burst that eats half the headroom in
    // one tick that single tick IS the overshoot. Measured: with the rate term
    // alone the self-fill drill decided to shed anywhere from 1 to 9 points
    // late against a 10-point bound — passing, with a point to spare, which is
    // not a bound.
    //
    // So tighten with distance as well as speed: full cadence while headroom
    // is still PROXIMITY_BAND floors deep, scaling down linearly from there. A
    // quiet interval is not evidence the next one will be quiet, and the
    // closer the line the more a single interval can cost.
    let by_proximity =
        ceiling.mul_f64((headroom as f64 / (PROXIMITY_BAND * floor as f64)).min(1.0));
    let Some(prev) = prev_free else {
        return by_proximity.clamp(MIN_SAMPLE.min(ceiling), ceiling);
    };
    let drained = prev.saturating_sub(cur.free_bytes);
    let want = if drained == 0 {
        by_proximity
    } else {
        let secs = elapsed.as_secs_f64().max(0.001);
        let eta = headroom as f64 / (drained as f64 / secs);
        let by_rate = std::time::Duration::from_secs_f64((eta / HEADROOM_DIVISOR).max(0.0));
        by_rate.min(by_proximity)
    };
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
        let t = Thresholds::default(); // 10% -> floor 10GB on a 100GB disk
        let every = Duration::from_secs(2);
        // 50GB free is 40GB of headroom over a 10GB floor — four times the
        // floor, so proximity does not bind and an idle disk costs what it
        // costs today.
        let now = raw(50 * GB, 100 * GB);
        assert_eq!(pace(None, now, t, every, every), every);
        assert_eq!(
            pace(Some(50 * GB), now, t, every, every),
            every,
            "a disk that is not filling costs exactly what it costs today"
        );
    }

    /// The rate term is reactive: it cannot shorten the interval until an
    /// interval has already been spent at speed, so on a burst that eats the
    /// headroom in one tick that tick IS the overshoot. Distance has to
    /// tighten the interval on its own, with no previous reading at all.
    #[test]
    fn closing_on_the_line_tightens_the_interval_without_any_rate_evidence() {
        let t = Thresholds::default(); // floor 10GB
        let every = Duration::from_secs(2);
        // 12GB free = 2GB of headroom over the floor: a fifth of it.
        let near = raw(12 * GB, 100 * GB);
        let next = pace(None, near, t, every, every);
        assert!(
            next <= every / 4,
            "close to the line the cadence must tighten on distance alone, got {next:?}"
        );
        // And a QUIET interval near the line must not relax it back: quiet now
        // is not evidence of quiet next.
        assert_eq!(pace(Some(12 * GB), near, t, every, every), next);
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

    /// ADR-0023 D7 requirement 4, as a PROPERTY rather than three examples:
    /// wherever writes would be shed, reclaim was already engaged. An
    /// evictable namespace must evict rather than ever reach `-QUOTA`, and the
    /// way that fails is not dramatically — it is one threshold combination,
    /// somewhere in the configuration space, where the two cross over.
    ///
    /// Swept across disk sizes, both thresholds, and every free level from
    /// empty to full. The positive control matters as much as the assertion:
    /// if no combination ever shed, the loop body would never run and this
    /// would pass while checking nothing.
    #[test]
    fn reclaim_always_engages_before_writes_are_shed() {
        let mut shed_seen = 0u32;
        let mut reclaim_seen = 0u32;
        for total_gb in [1u64, 8, 64, 512] {
            let total = total_gb * GB;
            for pct in [0u64, 5, 10, 25] {
                for min_gb in [0u64, 1, 2, 8] {
                    let t = Thresholds {
                        min_free_pct: pct,
                        min_free_bytes: min_gb * GB,
                    };
                    for step in 0..=100u64 {
                        let u = flint_storage::disk::Usage {
                            free_bytes: total * step / 100,
                            total_bytes: total,
                        };
                        let sheds = verdict(Some(u), t, Verdict::Ok) == Verdict::Shed;
                        let reclaims = reclaim_action(Some(u), t, false) != ReclaimAction::Idle;
                        if reclaims {
                            reclaim_seen += 1;
                        }
                        if sheds {
                            shed_seen += 1;
                            assert!(
                                reclaims,
                                "writes shed while reclaim was idle: total={total_gb}GB \
                                 free={}% min_free_pct={pct} min_free_bytes={min_gb}GB — an \
                                 evictable namespace would reach -QUOTA sitting on \
                                 reclaimable data",
                                step
                            );
                        }
                    }
                }
            }
        }
        assert!(
            shed_seen > 0 && reclaim_seen > 0,
            "positive control: shed_seen={shed_seen} reclaim_seen={reclaim_seen} — the \
             sweep never reached either state, so nothing above was actually checked"
        );
    }

    /// Hysteresis: a run in progress continues past the line it started at,
    /// so a node parked at the mark does not reclaim a few keys every sample
    /// forever. Same reason `REOPEN_MARGIN` exists for writes.
    #[test]
    fn a_reclaim_in_progress_runs_past_the_line_that_started_it() {
        let t = Thresholds {
            min_free_pct: 10,
            min_free_bytes: 0,
        };
        let total = 100 * GB;
        let floor = 10 * GB;
        // Just above the start mark (150% of floor = 15 GB): idle stays idle.
        let above = flint_storage::disk::Usage {
            free_bytes: floor * 160 / 100,
            total_bytes: total,
        };
        assert_eq!(reclaim_action(Some(above), t, false), ReclaimAction::Idle);
        // ... but a run already going keeps going, because the target is 200%.
        assert_ne!(
            reclaim_action(Some(above), t, true),
            ReclaimAction::Idle,
            "reclaim stopped at the start mark instead of its target, which \
             re-engages on the next sample"
        );
        // Past the target, a run in progress finally stops.
        let clear = flint_storage::disk::Usage {
            free_bytes: floor * 210 / 100,
            total_bytes: total,
        };
        assert_eq!(reclaim_action(Some(clear), t, true), ReclaimAction::Idle);
    }

    /// An unmeasurable disk authorises no deletion. Note this is the OPPOSITE
    /// answer to `verdict`, which returns Ok when blind so a monitoring gap
    /// cannot shed writes — both refuse to act, and the dangerous action is
    /// simply different in each case.
    #[test]
    fn a_blind_sample_reclaims_nothing() {
        let t = Thresholds::default();
        assert_eq!(reclaim_action(None, t, false), ReclaimAction::Idle);
        assert_eq!(
            reclaim_action(None, t, true),
            ReclaimAction::Idle,
            "a reclaim in progress kept deleting after the disk signal was lost"
        );
    }
}
