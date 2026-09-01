// SPDX-License-Identifier: Elastic-2.0
//! Master-side replication bookkeeping: replica acknowledgement state and
//! time-based lag, feeding the lag-cap backpressure that enforces the
//! failover RPO bound by construction.
//!
//! Lag definition: the age of the oldest master write not yet acknowledged
//! by the freshest replica. The hub keeps a ring of (seq, timestamp)
//! samples; lag = now - timestamp of the earliest sample whose seq exceeds
//! the acked cursor. No live replica (no ACK within the liveness window)
//! means no backpressure — the degraded window is governed by the snapshot
//! bound, not the lag cap (docs/design.md §2.5).

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// ACKs older than this mean the replica link is down.
pub const LIVENESS_WINDOW_MS: u64 = 2_000;
/// Default soft cap: writes are briefly delayed.
pub const DEFAULT_LAG_SOFT_MS: u64 = 500;
/// Default hard cap: writes are shed with a retriable -THROTTLED error.
/// The hard cap bounds the VOLUME of at-risk writes: past it the master
/// stops accepting new ones. It does NOT bound their age — see
/// `widowed_beyond_grace` for the gate that does.
/// Shed writes once the master runs this far ahead of its slowest live
/// replica. The unit is SEQUENCES, which is a proxy for the retained BYTES
/// RocksDB actually budgets — the conversion depends on value size, so this
/// default assumes roughly 1 KB values and works out near half of
/// `DEFAULT_WAL_SIZE_LIMIT_MB` (8 GiB). Half, not all, so a replica meets
/// backpressure well before it meets a deleted segment.
///
/// An operator whose values are much larger or smaller should retune from
/// the exported `wal_headroom_seq`, which is why that metric exists. 0
/// disables the gate entirely.
pub const DEFAULT_WAL_HEADROOM_SHED_SEQ: u64 = 4_000_000;

/// Bytes per sequence to assume when converting the archive's BYTE budget
/// into this gate's SEQUENCE threshold.
///
/// The old default assumed ~1 KB, reasoning that one sequence carries one
/// value. It does not: a sequence is a WAL BATCH, and a pipelining client
/// puts several writes in each. Measured under a bulk load of 1 KB values at
/// pipeline depth 256: **4,327 bytes per sequence**. The assumption was off
/// by a factor of four, which put the gate at 4,000,000 sequences (~17 GB)
/// against an 8 GiB archive — so deletion happened at HALF the distance
/// where shedding began, and the gate reported full headroom
/// (`writes_shed_headroom: 0`) while the replica was cut off (BUG-0079).
///
/// 16 KiB is deliberately pessimistic — four times what was measured. Being
/// wrong high costs early backpressure, which is a degradation an operator
/// sees; being wrong low costs a dead replica, a widowed master, and a write
/// path that shuts. Those are not symmetric, and the constant should not
/// pretend they are.
/// Bytes per WAL sequence, ASSUMED. Measured 2026-08-31: bytes-per-sequence is
/// exactly the size of one write, because RocksDB numbers sequences per key.
/// At depth 64, batchable `SET`, values of 32 / 1024 / 16384 bytes gave 32 /
/// 1024 / 16384 bytes per sequence — so this constant is right at 16 KiB
/// values and wrong by the ratio at anything else.
///
/// Since `headroom_shed_seq_for_budget` DIVIDES by it, a value larger than
/// reality makes the threshold too small and the gate fires EARLY: 16x early
/// at the ~1 KiB values the call site says to expect, 512x early at 32 B.
/// That is the conservative direction — a replica meets backpressure sooner
/// than intended, never later — which is why nothing has caught it. It is
/// still wrong, and BUG-0078's fix makes it likelier to bite, because a
/// master can now outrun a replica far faster than when this was chosen.
///
/// Left as-is deliberately: correcting it moves a SAFETY gate's firing point
/// later, which needs its own evidence rather than a drive-by edit. Deriving
/// it from observed writes is proposed in ADR-0022; `--wal-headroom-seq`
/// overrides it today.
const ASSUMED_BYTES_PER_SEQ: u64 = 16 * 1024;

/// Shed threshold for an archive of `wal_size_limit_mb`.
///
/// Half the budget, so a replica meets backpressure well before it meets a
/// deleted segment — the intent the old constant described and, at the value
/// size it assumed, achieved. Derived rather than fixed so the relationship
/// survives a change to either term.
///
/// "At the value size it assumed" is load-bearing and was never checked: see
/// `ASSUMED_BYTES_PER_SEQ`. Below 16 KiB values this returns a threshold
/// smaller than the intent, in proportion.
pub fn headroom_shed_seq_for_budget(wal_size_limit_mb: u64) -> u64 {
    let budget_bytes = wal_size_limit_mb.saturating_mul(1024 * 1024);
    (budget_bytes / 2) / ASSUMED_BYTES_PER_SEQ
}

pub const DEFAULT_LAG_HARD_MS: u64 = 1_000;
/// Default widowed grace: 0 = disabled, so a standalone node and every
/// existing deployment keep their current behaviour. flintctl turns it on
/// for pair members, which are the nodes that HAVE a replica to lose.
pub const DEFAULT_WIDOWED_GRACE_MS: u64 = 0;

// Producers (flintsync stream loop, ack reader) are compiled only with
// the rocks feature; the hub itself stays feature-independent.
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
const MAX_SAMPLES: usize = 512;

pub struct ReplHub {
    /// Soft/hard lag caps (ms) and the min-replicas gate — ATOMIC so they
    /// hot-reload live (FLINTCONFIG) without a restart. Read on the write
    /// path via the getters below.
    ///   soft: delay writes beyond this lag.
    ///   hard: shed writes beyond this lag — the RPO bound.
    ///   min_replicas: shed while fewer than this many replicas are LIVE
    ///     (Redis min-replicas-to-write). Closes the widowed-master hole
    ///     the lag cap alone leaves open (no live replica => no lag to
    ///     measure); 0 disables it (a standalone master serves freely).
    ///   widowed_grace: shed writes once NO replica has acked for this long.
    ///     The lag cap cannot fire without a replica to measure against, so
    ///     this is the only gate that bounds how long a master may go on
    ///     accepting writes nothing is copying. 0 disables it.
    lag_soft_ms: AtomicU64,
    lag_hard_ms: AtomicU64,
    min_replicas_to_write: AtomicU32,
    widowed_grace_ms: AtomicU64,
    /// Shed writes once the master is this many sequences ahead of its
    /// SLOWEST live replica (ADR-0022). 0 disables the gate.
    wal_headroom_shed_seq: AtomicU64,
    /// Latch for the write-quorum gate's LAST observed state, so a
    /// transition can be journaled once instead of on every refused write.
    /// `true` = currently refusing. Starts `false`: a master that never had
    /// a replica should announce losing quorum the first time it matters,
    /// not stay silent because it was always below.
    quorum_below: std::sync::atomic::AtomicBool,
    next_id: AtomicU64,
    /// Newest ack timestamp from ANY replica, ever, this process-life.
    /// Deliberately separate from `replicas`, which drops an entry on
    /// connection teardown: a replica that disconnects cleanly would
    /// otherwise erase the evidence that it was ever there and restart the
    /// widowed clock from zero — turning a clean shutdown into a fresh
    /// grace period, which is precisely the case the grace exists to bound.
    last_ack_ms: AtomicU64,
    /// When the widowed clock started for a master that has NEVER seen an
    /// ack (a fresh standalone, or one just promoted and still waiting for
    /// its replacement). Set on first observation rather than at
    /// construction so the node gets the FULL grace to attach a replica,
    /// instead of spending it on startup.
    widow_since_ms: AtomicU64,
    /// Per-replica ack state: id -> (acked_seq, last_ack_ms). The RPO
    /// reference is the freshest LIVE replica: a dead replica's cursor
    /// must not count, because promotion can only choose among the living.
    replicas: Mutex<HashMap<u64, (u64, u64)>>,
    /// (master_seq, wall_ms) samples, ascending in both.
    samples: Mutex<VecDeque<(u64, u64)>>,
}

impl Default for ReplHub {
    fn default() -> Self {
        Self::new(DEFAULT_LAG_SOFT_MS, DEFAULT_LAG_HARD_MS, 0)
    }
}

#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
impl ReplHub {
    pub fn new(lag_soft_ms: u64, lag_hard_ms: u64, min_replicas_to_write: u32) -> Self {
        Self {
            lag_soft_ms: AtomicU64::new(lag_soft_ms),
            lag_hard_ms: AtomicU64::new(lag_hard_ms.max(lag_soft_ms)),
            min_replicas_to_write: AtomicU32::new(min_replicas_to_write),
            widowed_grace_ms: AtomicU64::new(DEFAULT_WIDOWED_GRACE_MS),
            wal_headroom_shed_seq: AtomicU64::new(DEFAULT_WAL_HEADROOM_SHED_SEQ),
            quorum_below: std::sync::atomic::AtomicBool::new(false),
            next_id: AtomicU64::new(1),
            last_ack_ms: AtomicU64::new(0),
            widow_since_ms: AtomicU64::new(0),
            replicas: Mutex::new(HashMap::new()),
            samples: Mutex::new(VecDeque::new()),
        }
    }

    /// Current soft/hard lag caps and min-replicas gate (hot-reloadable).
    pub fn lag_soft_ms(&self) -> u64 {
        self.lag_soft_ms.load(Ordering::Relaxed)
    }
    pub fn lag_hard_ms(&self) -> u64 {
        self.lag_hard_ms.load(Ordering::Relaxed)
    }
    pub fn min_replicas_to_write(&self) -> u32 {
        self.min_replicas_to_write.load(Ordering::Relaxed)
    }

    /// Live retune (FLINTCONFIG). Hard is clamped >= soft so the invariant
    /// the constructor enforces can never be broken at runtime either.
    pub fn set_lag_soft_ms(&self, v: u64) {
        self.lag_soft_ms.store(v, Ordering::Relaxed);
        if self.lag_hard_ms.load(Ordering::Relaxed) < v {
            self.lag_hard_ms.store(v, Ordering::Relaxed);
        }
    }
    pub fn set_lag_hard_ms(&self, v: u64) {
        let clamped = v.max(self.lag_soft_ms.load(Ordering::Relaxed));
        self.lag_hard_ms.store(clamped, Ordering::Relaxed);
    }
    pub fn set_min_replicas_to_write(&self, v: u32) {
        self.min_replicas_to_write.store(v, Ordering::Relaxed);
    }
    pub fn widowed_grace_ms(&self) -> u64 {
        self.widowed_grace_ms.load(Ordering::Relaxed)
    }
    pub fn set_widowed_grace_ms(&self, v: u64) {
        self.widowed_grace_ms.store(v, Ordering::Relaxed);
    }

    /// True when NO replica has acked for longer than the widowed grace —
    /// the master has been accepting writes nothing is copying, for longer
    /// than the operator is willing to risk.
    ///
    /// WHY THIS EXISTS, given the lag cap already sheds. The lag cap needs a
    /// replica to measure against: `lag_ms` returns None when none is live,
    /// and the write path's match falls through to "no backpressure". So the
    /// exact moment the master becomes the only copy of the data is the
    /// moment every bound switches off. Measured on a default pair before
    /// this gate existed: freeze the replica, and the master sheds 88 writes
    /// while the replica is still inside LIVENESS_WINDOW_MS, then accepts
    /// 539 more in ~4s once it ages out, with zero replicas and no throttle.
    ///
    /// WHY A GRACE RATHER THAN min-replicas-to-write, which already sheds at
    /// zero live replicas. That gate cannot distinguish "my peer died" from
    /// "I was promoted five milliseconds ago and my replacement has not
    /// attached yet" — both are a master with no replica. Setting it to 1 on
    /// a pair therefore freezes writes for the whole replacement full-sync
    /// on EVERY failover, trading the published RTO away to buy the RPO. The
    /// grace buys the same bound without that: normal promotions attach a
    /// replacement well inside it and never shed, while a master that is
    /// still alone when the grace expires stops accepting.
    ///
    /// This is the only gate here that bounds the AGE of at-risk writes
    /// rather than their volume, which is what docs/failover.md claims.
    /// LOCK-FREE ON PURPOSE. This runs on every write, and the write path
    /// already takes the `replicas` mutex once (lag_ms -> effective_acked);
    /// asking `live_replica_count` for a liveness answer here would double
    /// that on every write of every pair member in the fleet. It is also
    /// unnecessary: `last_ack_ms` is the newest ack from ANY replica, so
    /// "some replica is live" is exactly `now - last_ack <= LIVENESS_WINDOW`,
    /// and since a sane grace is far longer than that window, a live replica
    /// already fails the comparison below. Two relaxed loads, no lock.
    /// PURE — safe for observers. FLINTINFO renders this on every node,
    /// and the arming used to live inside this check: the controller's
    /// routine status sweeps armed the widow clock on every REPLICA (whose
    /// last_ack_ms is always 0) at bring-up, so a survivor promoted hours
    /// later had already outlived its whole grace and shed its very first
    /// write — writes stayed down for the replacement's entire re-seed,
    /// the exact failover freeze the grace design exists to avoid (60.4 s
    /// measured at 100 GB, scale run 18). The clock is armed only by the
    /// write path (arming variant) and re-armed by FLINTPROMOTE.
    pub fn widowed_beyond_grace(&self, now_ms: u64) -> bool {
        let grace = self.widowed_grace_ms.load(Ordering::Relaxed);
        if grace == 0 {
            return false;
        }
        // The newest of "a replica acked" and "the clock was (re)armed".
        // max() rather than last-wins keeps a re-promoted ex-master from
        // inheriting a stale ack time from its previous mastership.
        let basis = self
            .last_ack_ms
            .load(Ordering::Relaxed)
            .max(self.widow_since_ms.load(Ordering::Relaxed));
        basis != 0 && now_ms.saturating_sub(basis) > grace
    }

    /// Write-path variant: additionally arms the clock on the first gated
    /// write of a never-acked, never-armed master (a bootstrap master whose
    /// configured replica has not attached), so such a node still converges
    /// on shedding after one grace instead of never.
    pub fn widowed_beyond_grace_arming(&self, now_ms: u64) -> bool {
        let grace = self.widowed_grace_ms.load(Ordering::Relaxed);
        if grace == 0 {
            return false;
        }
        let basis = self
            .last_ack_ms
            .load(Ordering::Relaxed)
            .max(self.widow_since_ms.load(Ordering::Relaxed));
        if basis == 0 {
            self.widow_since_ms.store(now_ms, Ordering::Relaxed);
            return false;
        }
        now_ms.saturating_sub(basis) > grace
    }

    /// A just-promoted master gets the WHOLE grace to find a replacement
    /// rather than inheriting a deadline from its replica life or an
    /// earlier mastership. FLINTPROMOTE calls this.
    pub fn rearm_widow_clock(&self, now_ms: u64) {
        self.widow_since_ms.store(now_ms, Ordering::Relaxed);
    }

    /// True when the write path must shed because fewer replicas are live
    /// than the configured minimum. Liveness is "acking recently", not
    /// "converged": a respawned replica lifts the gate as soon as it starts
    /// acking, so the unavailability window after losing a replica is
    /// detect + respawn + first ack, not a full resync.
    /// Report the gate's current state and return `Some(now_below)` ONLY when
    /// it changed, so the caller journals a transition rather than a stream.
    ///
    /// This exists because the write outage is exactly observable here and
    /// nowhere else. `Promoted` is not its start (a promoted master shuts the
    /// gate as a consequence, moments later) and `Supervised` is not its end
    /// (that is a controller OBSERVING convergence, on its own poll cadence).
    /// Bracketing it on the node that does the refusing is what makes an RTO
    /// number attributable instead of inferred from a client's stall.
    pub fn note_quorum_transition(&self, below: bool) -> Option<bool> {
        let prev = self
            .quorum_below
            .swap(below, std::sync::atomic::Ordering::Relaxed);
        (prev != below).then_some(below)
    }

    pub fn below_write_quorum(&self, now_ms: u64) -> bool {
        let min = self.min_replicas_to_write();
        min > 0 && (self.live_replica_count(now_ms) as u32) < min
    }

    /// A replication connection announces itself; ACKs carry its id.
    pub fn register_replica(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Connection teardown. A crashed link that never unregisters is
    /// equally handled by the liveness window.
    pub fn unregister_replica(&self, id: u64) {
        self.replicas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
    }

    /// Record "master was at `seq` at time `now_ms`" (stream loop cadence).
    pub fn record_sample(&self, seq: u64, now_ms: u64) {
        let mut samples = self.samples.lock().unwrap_or_else(|e| e.into_inner());
        if samples.back().is_some_and(|&(s, _)| s >= seq) {
            return; // nothing new since the last sample
        }
        samples.push_back((seq, now_ms));
        if samples.len() > MAX_SAMPLES {
            samples.pop_front();
        }
    }

    pub fn record_ack(&self, id: u64, seq: u64, now_ms: u64) {
        let mut replicas = self.replicas.lock().unwrap_or_else(|e| e.into_inner());
        let entry = replicas.entry(id).or_insert((0, 0));
        entry.0 = entry.0.max(seq);
        entry.1 = entry.1.max(now_ms);
        // Survives unregister_replica, which the per-replica map does not.
        self.last_ack_ms.fetch_max(now_ms, Ordering::Relaxed);
    }

    /// Freshest cursor among LIVE replicas — the promotion candidate's
    /// cursor, and therefore the RPO reference. None = no live replica.
    pub fn effective_acked(&self, now_ms: u64) -> Option<u64> {
        self.replicas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|(_, last)| *last != 0 && now_ms.saturating_sub(*last) <= LIVENESS_WINDOW_MS)
            .map(|(acked, _)| *acked)
            .max()
    }

    /// Cursor of the SLOWEST live replica — the mirror of `effective_acked`,
    /// and the one WAL retention has to respect.
    ///
    /// The two differ on purpose. RPO asks "if I promote now, what is the
    /// best copy I can promote?", so it takes the MAX: promotion can only
    /// choose among the living, and the freshest survivor bounds the loss.
    /// Retention asks "what may I still delete?", so it takes the MIN: a
    /// segment is only safe to drop once EVERY live consumer is past it.
    /// Using the freshest here would delete out from under the slowest
    /// replica, which is exactly the failure ADR-0022 exists for.
    ///
    /// LIVE, not all: a replica that has stopped acking drops out of the
    /// window and stops pinning retention. Without that a dead seat would
    /// hold the WAL open forever and turn a replication problem into a disk
    /// problem — the naive min is worse than no min at all.
    pub fn wal_headroom_shed_seq(&self) -> u64 {
        self.wal_headroom_shed_seq.load(Ordering::Relaxed)
    }

    pub fn set_wal_headroom_shed_seq(&self, v: u64) {
        self.wal_headroom_shed_seq.store(v, Ordering::Relaxed);
    }

    /// True when the master has outrun its slowest live replica far enough
    /// that the WAL it still needs is at risk of being recycled.
    ///
    /// Returns false with NO live replica: retention is pinned by nobody
    /// then, and `widowed_beyond_grace_arming` is the gate that bounds that
    /// state. Two gates firing on one cause would report the wrong one.
    pub fn wal_headroom_exhausted(&self, latest_seq: u64, now_ms: u64) -> bool {
        let limit = self.wal_headroom_shed_seq();
        if limit == 0 {
            return false;
        }
        self.wal_headroom_seq(latest_seq, now_ms)
            .is_some_and(|h| h >= limit)
    }

    pub fn min_acked_live(&self, now_ms: u64) -> Option<u64> {
        self.replicas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|(_, last)| *last != 0 && now_ms.saturating_sub(*last) <= LIVENESS_WINDOW_MS)
            .map(|(acked, _)| *acked)
            .min()
    }

    /// How far the master has run ahead of its slowest live replica, in
    /// sequences. `None` = no live replica, where retention cannot be
    /// pinned by anyone and the widowed-grace gate governs instead.
    ///
    /// Sequences are a PROXY for retained bytes, which is what RocksDB
    /// actually budgets (`wal_size_limit_mb`). The mapping depends on value
    /// size, so a threshold set on this is approximate by construction —
    /// which is why the headroom is exported as a metric rather than only
    /// consulted: an operator has to be able to see the real curve for their
    /// workload and tune against it.
    pub fn wal_headroom_seq(&self, latest_seq: u64, now_ms: u64) -> Option<u64> {
        self.min_acked_live(now_ms)
            .map(|acked| latest_seq.saturating_sub(acked))
    }

    pub fn live_replica_count(&self, now_ms: u64) -> usize {
        self.replicas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|(_, last)| *last != 0 && now_ms.saturating_sub(*last) <= LIVENESS_WINDOW_MS)
            .count()
    }

    /// Used by tests and (soon) the trio's health view.
    #[allow(dead_code)]
    pub fn has_live_replica(&self, now_ms: u64) -> bool {
        self.effective_acked(now_ms).is_some()
    }

    /// Time-lag of the freshest LIVE replica (the smallest gap). None = no
    /// live replica (no cap; the degraded window is snapshot-bounded).
    pub fn lag_ms(&self, now_ms: u64) -> Option<u64> {
        let acked = self.effective_acked(now_ms)?;
        let samples = self.samples.lock().unwrap_or_else(|e| e.into_inner());
        // Earliest sample the freshest replica has NOT confirmed yet.
        match samples.iter().find(|&&(seq, _)| seq > acked) {
            Some(&(_, ts)) => Some(now_ms.saturating_sub(ts)),
            None => Some(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_tracks_the_freshest_live_replica() {
        let hub = ReplHub::default();
        hub.record_sample(10, 1_000);
        hub.record_sample(20, 1_400);
        hub.record_sample(30, 1_800);
        let fast = hub.register_replica();
        let slow = hub.register_replica();
        hub.record_ack(slow, 10, 1_900);
        hub.record_ack(fast, 20, 1_900);
        // Freshest (fast, acked 20) governs: oldest unacked is seq 30 @1800.
        assert_eq!(hub.effective_acked(2_000), Some(20));
        assert_eq!(hub.lag_ms(2_000), Some(200));
        hub.record_ack(fast, 30, 2_000);
        assert_eq!(hub.lag_ms(2_050), Some(0), "freshest fully caught up");
        assert_eq!(hub.live_replica_count(2_050), 2);
    }

    #[test]
    fn retention_follows_the_slowest_live_replica_not_the_freshest() {
        let hub = ReplHub::default();
        let fast = hub.register_replica();
        let slow = hub.register_replica();
        hub.record_ack(fast, 500, 1_000);
        hub.record_ack(slow, 100, 1_000);
        // RPO looks at the best copy it could promote...
        assert_eq!(hub.effective_acked(1_100), Some(500));
        // ...retention at the one that would lose data if we deleted.
        assert_eq!(hub.min_acked_live(1_100), Some(100));
        // Headroom is measured against the SLOW one, so it is the larger,
        // more conservative number. Taking the freshest here would report
        // 100 sequences of headroom while the slow replica is 900 behind.
        assert_eq!(hub.wal_headroom_seq(1_000, 1_100), Some(900));
    }

    #[test]
    fn a_dead_slow_replica_stops_pinning_retention() {
        let hub = ReplHub::default();
        let fast = hub.register_replica();
        let slow = hub.register_replica();
        hub.record_ack(fast, 500, 1_000);
        hub.record_ack(slow, 100, 1_000);
        assert_eq!(hub.min_acked_live(1_100), Some(100));
        // The slow one stops acking; the fast one keeps going. A naive
        // min-across-ALL would hold the WAL at 100 forever and turn a
        // replication problem into a disk-exhaustion one.
        let later = 1_000 + LIVENESS_WINDOW_MS + 1;
        hub.record_ack(fast, 900, later);
        assert_eq!(
            hub.min_acked_live(later),
            Some(900),
            "a replica outside the liveness window must not pin retention"
        );
        assert_eq!(hub.wal_headroom_seq(1_000, later), Some(100));
    }

    #[test]
    fn no_live_replica_means_no_headroom_bound() {
        let hub = ReplHub::default();
        // Nobody has ever acked: retention is pinned by nobody, and the
        // widowed-grace gate — not this one — is what bounds the risk.
        assert_eq!(hub.min_acked_live(5_000), None);
        assert_eq!(hub.wal_headroom_seq(9_999, 5_000), None);
    }

    #[test]
    fn headroom_never_underflows_when_a_replica_reports_ahead() {
        let hub = ReplHub::default();
        let r = hub.register_replica();
        // A replica acking past the master's latest is not expected, but a
        // saturating gap is the difference between "0" and a number near
        // u64::MAX being fed to a shed comparison.
        hub.record_ack(r, 100, 1_000);
        assert_eq!(hub.wal_headroom_seq(50, 1_100), Some(0));
    }

    #[test]
    fn headroom_gate_fires_on_the_slowest_replica_not_the_freshest() {
        let hub = ReplHub::default();
        hub.set_wal_headroom_shed_seq(1_000);
        let fast = hub.register_replica();
        let slow = hub.register_replica();
        hub.record_ack(fast, 9_500, 1_000);
        hub.record_ack(slow, 8_000, 1_000);
        // Freshest is 500 behind; slowest is 2_000 behind. The gate must
        // read the slowest, or it protects nobody.
        assert!(hub.wal_headroom_exhausted(10_000, 1_100));
        // Slow one catches up: back under the limit.
        hub.record_ack(slow, 9_800, 1_100);
        assert!(!hub.wal_headroom_exhausted(10_000, 1_150));
    }

    #[test]
    fn headroom_gate_is_silent_without_a_live_replica() {
        let hub = ReplHub::default();
        hub.set_wal_headroom_shed_seq(1);
        // Nobody live: retention is pinned by nobody, and the widowed-grace
        // gate owns this state. Firing here would report the wrong cause for
        // a condition another gate already names.
        assert!(!hub.wal_headroom_exhausted(u64::MAX, 5_000));

        let r = hub.register_replica();
        hub.record_ack(r, 0, 1_000);
        let dead = 1_000 + LIVENESS_WINDOW_MS + 1;
        assert!(
            !hub.wal_headroom_exhausted(u64::MAX, dead),
            "a replica outside the liveness window must not hold the gate open"
        );
    }

    #[test]
    fn headroom_gate_disabled_at_zero() {
        let hub = ReplHub::default();
        hub.set_wal_headroom_shed_seq(0);
        let r = hub.register_replica();
        hub.record_ack(r, 0, 1_000);
        assert!(
            !hub.wal_headroom_exhausted(u64::MAX, 1_100),
            "0 must disable the gate, not shed everything"
        );
    }

    #[test]
    fn headroom_gate_fires_exactly_at_the_limit() {
        let hub = ReplHub::default();
        hub.set_wal_headroom_shed_seq(100);
        let r = hub.register_replica();
        hub.record_ack(r, 900, 1_000);
        assert!(!hub.wal_headroom_exhausted(999, 1_100), "99 behind: under");
        assert!(hub.wal_headroom_exhausted(1_000, 1_100), "100 behind: at");
        assert!(hub.wal_headroom_exhausted(1_001, 1_100), "101 behind: over");
    }

    #[test]
    fn dead_freshest_replica_stops_counting() {
        let hub = ReplHub::default();
        hub.record_sample(10, 1_000);
        hub.record_sample(30, 1_500);
        let fast = hub.register_replica();
        let slow = hub.register_replica();
        hub.record_ack(fast, 30, 1_600);
        hub.record_ack(slow, 10, 1_600);
        assert_eq!(hub.effective_acked(1_700), Some(30));
        // The fast replica dies; the slow one keeps acking its old cursor.
        let later = 1_600 + LIVENESS_WINDOW_MS + 500;
        hub.record_ack(slow, 10, later);
        // RPO reference falls back to the best LIVE replica (acked 10):
        // lag is now measured against seq 30 written at t=1500.
        assert_eq!(hub.effective_acked(later), Some(10));
        assert_eq!(hub.lag_ms(later), Some(later - 1_500));
        assert_eq!(hub.live_replica_count(later), 1);
    }

    #[test]
    fn no_live_replica_means_no_cap() {
        let hub = ReplHub::default();
        hub.record_sample(10, 1_000);
        assert_eq!(hub.lag_ms(1_500), None, "never acked = not live");
        let r = hub.register_replica();
        hub.record_ack(r, 5, 1_500);
        assert!(hub.lag_ms(1_600).is_some());
        // Liveness window expires.
        assert_eq!(hub.lag_ms(1_500 + LIVENESS_WINDOW_MS + 1), None);
        // Unregister removes immediately.
        hub.record_ack(r, 6, 5_000);
        assert!(hub.has_live_replica(5_001));
        hub.unregister_replica(r);
        assert!(!hub.has_live_replica(5_001));
    }

    #[test]
    fn widowed_grace_bounds_what_the_lag_cap_cannot() {
        let hub = ReplHub::default();
        hub.set_widowed_grace_ms(10_000);
        let r = hub.register_replica();
        hub.record_ack(r, 5, 1_000);

        // Live replica: the lag cap governs, the grace stays out of the way.
        assert!(!hub.widowed_beyond_grace(1_500));
        // Replica stops acking. Past the liveness window `lag_ms` goes None —
        // the exact point the lag cap stops being able to shed anything.
        let dead = 1_000 + LIVENESS_WINDOW_MS + 1;
        assert_eq!(hub.lag_ms(dead), None, "no live replica = no lag cap");
        assert!(!hub.widowed_beyond_grace(dead), "still inside the grace");
        // ... and the grace is what eventually stops the writes.
        assert!(hub.widowed_beyond_grace(1_000 + 10_001));

        // A returning replica lifts it again, without a restart.
        hub.record_ack(r, 9, 30_000);
        assert!(!hub.widowed_beyond_grace(30_100));
    }

    /// The scale-run-18 regression: FLINTINFO polls a REPLICA (last_ack 0)
    /// for hours, then the node is promoted. The observer must not have
    /// armed the widow clock — the promoted master gets its WHOLE grace,
    /// not an instant shed followed by a re-seed-long write outage.
    #[test]
    fn an_observer_cannot_spend_a_future_masters_grace() {
        let hub = ReplHub::default();
        hub.set_widowed_grace_ms(10_000);

        // Hours of status sweeps against the replica. Pure: never arms.
        for t in (0..7_200_000).step_by(30_000) {
            assert!(!hub.widowed_beyond_grace(t), "observer armed the clock");
        }

        // Promotion at t=7_200_000 re-arms; the first gated write moments
        // later is inside a FRESH grace even though the process is hours old.
        hub.rearm_widow_clock(7_200_000);
        assert!(!hub.widowed_beyond_grace_arming(7_200_500));
        assert!(!hub.widowed_beyond_grace(7_200_500));
        // Still alone one whole grace later: NOW the age gate sheds.
        assert!(hub.widowed_beyond_grace_arming(7_210_001));

        // A replacement replica's first ack lifts it without a restart.
        let r = hub.register_replica();
        hub.record_ack(r, 42, 7_215_000);
        assert!(!hub.widowed_beyond_grace_arming(7_215_100));
    }

    /// A never-promoted bootstrap master (configured replica absent) still
    /// converges on shedding: the WRITE path arms the clock lazily.
    #[test]
    fn the_write_path_still_arms_a_bootstrap_master() {
        let hub = ReplHub::default();
        hub.set_widowed_grace_ms(10_000);
        assert!(!hub.widowed_beyond_grace_arming(1_000)); // arms here
        assert!(!hub.widowed_beyond_grace_arming(5_000)); // inside grace
        assert!(hub.widowed_beyond_grace_arming(11_001)); // beyond it
    }

    #[test]
    fn widowed_grace_never_takes_the_replicas_lock() {
        // The gate runs on every write, alongside a lag check that already
        // locks `replicas`. Holding that lock here would double the
        // contention on every write of every pair member, so the gate reads
        // only atomics — proven by answering correctly while the lock is
        // held by this thread, which would deadlock or hang if it grabbed it.
        let hub = ReplHub::default();
        hub.set_widowed_grace_ms(1_000);
        let r = hub.register_replica();
        hub.record_ack(r, 1, 5_000);
        let guard = hub.replicas.lock().unwrap_or_else(|e| e.into_inner());
        assert!(!hub.widowed_beyond_grace(5_500));
        assert!(hub.widowed_beyond_grace(6_001));
        drop(guard);
    }

    #[test]
    fn a_clean_disconnect_does_not_restart_the_widowed_clock() {
        // unregister_replica drops the per-replica entry, so a hub that read
        // liveness only from that map would see "never had a replica" after a
        // graceful teardown and hand out a whole fresh grace — converting an
        // orderly shutdown into the longest possible unbounded window.
        let hub = ReplHub::default();
        hub.set_widowed_grace_ms(5_000);
        let r = hub.register_replica();
        hub.record_ack(r, 1, 1_000);
        hub.unregister_replica(r);
        assert!(
            hub.widowed_beyond_grace(6_002),
            "grace must run from the last ack, not from the disconnect"
        );
    }

    #[test]
    fn a_promoted_master_gets_the_whole_grace_to_find_a_replacement() {
        // The case that rules out min-replicas-to-write as the default: a
        // master that has never been acked is either a fresh standalone or
        // one promoted moments ago. Neither should shed instantly. The clock
        // starts on the first GATED WRITE — "first look" was the run-18 bug:
        // an INFO observer's look must never start it.
        let hub = ReplHub::default();
        hub.set_widowed_grace_ms(10_000);
        assert!(
            !hub.widowed_beyond_grace_arming(50_000),
            "clock starts on the first gated write"
        );
        assert!(
            !hub.widowed_beyond_grace_arming(59_000),
            "still inside the grace"
        );
        assert!(hub.widowed_beyond_grace_arming(60_001), "and then it bites");
    }

    #[test]
    fn widowed_grace_is_off_unless_configured() {
        let hub = ReplHub::default();
        assert_eq!(hub.widowed_grace_ms(), DEFAULT_WIDOWED_GRACE_MS);
        assert!(
            !hub.widowed_beyond_grace(u64::MAX / 2),
            "0 disables: a standalone node serves freely forever"
        );
    }

    #[test]
    fn samples_dedupe_and_cap() {
        let hub = ReplHub::default();
        hub.record_sample(7, 100);
        hub.record_sample(7, 200); // same seq: dropped
        for i in 0..2 * MAX_SAMPLES as u64 {
            hub.record_sample(100 + i, 300 + i);
        }
        let n = hub.samples.lock().unwrap_or_else(|e| e.into_inner()).len();
        assert!(n <= MAX_SAMPLES);
    }
}

#[cfg(test)]
mod cap_tests {
    use super::*;

    #[test]
    fn caps_are_configurable_and_ordered() {
        let hub = ReplHub::new(200, 800, 0);
        assert_eq!((hub.lag_soft_ms(), hub.lag_hard_ms()), (200, 800));
        // Live retune keeps the hard >= soft invariant.
        hub.set_lag_hard_ms(100);
        assert_eq!(hub.lag_hard_ms(), 200);
        hub.set_lag_soft_ms(500);
        assert_eq!((hub.lag_soft_ms(), hub.lag_hard_ms()), (500, 500));
        // Hard cap can never sit below the soft cap.
        let clamped = ReplHub::new(900, 300, 0);
        assert_eq!(clamped.lag_hard_ms(), 900);
        let d = ReplHub::default();
        assert_eq!(
            (d.lag_soft_ms(), d.lag_hard_ms()),
            (DEFAULT_LAG_SOFT_MS, DEFAULT_LAG_HARD_MS)
        );
    }

    #[test]
    fn min_replicas_gate() {
        // Default 0: standalone masters are never gated.
        let open = ReplHub::default();
        assert!(!open.below_write_quorum(1_000));

        // min=1: gated until a replica acks; open while it is live;
        // gated again the moment it unregisters (connection teardown)
        // or falls out of the liveness window (silent death).
        let hub = ReplHub::new(500, 1_000, 1);
        assert!(hub.below_write_quorum(1_000), "no replica yet");
        let r = hub.register_replica();
        assert!(hub.below_write_quorum(1_000), "registered but never acked");
        hub.record_ack(r, 5, 1_000);
        assert!(
            !hub.below_write_quorum(1_001),
            "acking replica lifts the gate"
        );
        assert!(
            hub.below_write_quorum(1_000 + LIVENESS_WINDOW_MS + 1),
            "silent replica death re-engages the gate after the window"
        );
        hub.record_ack(r, 6, 5_000);
        assert!(!hub.below_write_quorum(5_001));
        hub.unregister_replica(r);
        assert!(
            hub.below_write_quorum(5_001),
            "teardown re-engages immediately"
        );

        // min=2 with only one live replica: still gated.
        let two = ReplHub::new(500, 1_000, 2);
        let a = two.register_replica();
        two.record_ack(a, 5, 1_000);
        assert!(two.below_write_quorum(1_001));
        let b = two.register_replica();
        two.record_ack(b, 5, 1_002);
        assert!(!two.below_write_quorum(1_003));
    }
}

#[cfg(test)]
mod quorum_transition_tests {
    use super::ReplHub;

    fn hub(min: u32) -> ReplHub {
        ReplHub::new(0, 0, min)
    }

    /// Edges only. Inside one outage the gate is consulted on every write —
    /// thousands of times — and journaling each would bury the two instants
    /// that actually bound it.
    #[test]
    fn only_the_change_is_reported() {
        let h = hub(1);
        assert_eq!(h.note_quorum_transition(true), Some(true), "first loss");
        assert_eq!(h.note_quorum_transition(true), None, "still below");
        assert_eq!(h.note_quorum_transition(true), None, "still below");
        assert_eq!(h.note_quorum_transition(false), Some(false), "restored");
        assert_eq!(h.note_quorum_transition(false), None, "still fine");
        assert_eq!(h.note_quorum_transition(true), Some(true), "lost again");
    }

    /// A healthy master must not announce a RESTORE it never lost. The latch
    /// starts "not below", so the steady state emits nothing — otherwise every
    /// node would journal a recovery on its first write and the timeline would
    /// show an outage that never happened.
    #[test]
    fn a_healthy_master_says_nothing() {
        let h = hub(1);
        for _ in 0..100 {
            assert_eq!(
                h.note_quorum_transition(false),
                None,
                "a master that never lost quorum must stay silent"
            );
        }
    }

    /// The latch is state, not a predicate: it must not be confused with
    /// `below_write_quorum`, which is recomputed per write. Asking twice with
    /// the same answer is not an event even though the underlying condition
    /// is continuously true.
    #[test]
    fn the_latch_is_not_the_condition() {
        let h = hub(2);
        assert_eq!(h.note_quorum_transition(true), Some(true));
        // The condition is still true here; the EVENT is not.
        assert_eq!(h.note_quorum_transition(true), None);
    }
}
