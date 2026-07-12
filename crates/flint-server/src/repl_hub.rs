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
use std::sync::atomic::{AtomicU64, Ordering};

/// ACKs older than this mean the replica link is down.
pub const LIVENESS_WINDOW_MS: u64 = 2_000;
/// Default soft cap: writes are briefly delayed.
pub const DEFAULT_LAG_SOFT_MS: u64 = 500;
/// Default hard cap: writes are shed with a retriable -THROTTLED error.
/// The hard cap IS the advertised worst-case failover RPO.
pub const DEFAULT_LAG_HARD_MS: u64 = 1_000;

// Producers (flintsync stream loop, ack reader) are compiled only with
// the rocks feature; the hub itself stays feature-independent.
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
const MAX_SAMPLES: usize = 512;

pub struct ReplHub {
    /// Soft cap (ms): delay writes beyond this lag.
    pub lag_soft_ms: u64,
    /// Hard cap (ms): shed writes beyond this lag — the RPO bound.
    pub lag_hard_ms: u64,
    next_id: AtomicU64,
    /// Per-replica ack state: id -> (acked_seq, last_ack_ms). The RPO
    /// reference is the freshest LIVE replica: a dead replica's cursor
    /// must not count, because promotion can only choose among the living.
    replicas: Mutex<HashMap<u64, (u64, u64)>>,
    /// (master_seq, wall_ms) samples, ascending in both.
    samples: Mutex<VecDeque<(u64, u64)>>,
}

impl Default for ReplHub {
    fn default() -> Self {
        Self::new(DEFAULT_LAG_SOFT_MS, DEFAULT_LAG_HARD_MS)
    }
}

#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
impl ReplHub {
    pub fn new(lag_soft_ms: u64, lag_hard_ms: u64) -> Self {
        Self {
            lag_soft_ms,
            lag_hard_ms: lag_hard_ms.max(lag_soft_ms),
            next_id: AtomicU64::new(1),
            replicas: Mutex::new(HashMap::new()),
            samples: Mutex::new(VecDeque::new()),
        }
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
        let hub = ReplHub::new(200, 800);
        assert_eq!((hub.lag_soft_ms, hub.lag_hard_ms), (200, 800));
        // Hard cap can never sit below the soft cap.
        let clamped = ReplHub::new(900, 300);
        assert_eq!(clamped.lag_hard_ms, 900);
        let d = ReplHub::default();
        assert_eq!(
            (d.lag_soft_ms, d.lag_hard_ms),
            (DEFAULT_LAG_SOFT_MS, DEFAULT_LAG_HARD_MS)
        );
    }
}
