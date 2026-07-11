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

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// ACKs older than this mean the replica link is down.
pub const LIVENESS_WINDOW_MS: u64 = 2_000;
/// Soft cap: writes are briefly delayed.
pub const LAG_SOFT_MS: u64 = 500;
/// Hard cap: writes are shed with a retriable -THROTTLED error.
pub const LAG_HARD_MS: u64 = 1_000;

// Producers (flintsync stream loop, ack reader) are compiled only with
// the rocks feature; the hub itself stays feature-independent.
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
const MAX_SAMPLES: usize = 512;

#[derive(Default)]
pub struct ReplHub {
    /// Freshest replica-acknowledged master sequence.
    acked: AtomicU64,
    /// Wall time of the most recent ACK.
    last_ack_ms: AtomicU64,
    /// (master_seq, wall_ms) samples, ascending in both.
    samples: Mutex<VecDeque<(u64, u64)>>,
}

#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
impl ReplHub {
    pub fn new() -> Self {
        Self::default()
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

    pub fn record_ack(&self, seq: u64, now_ms: u64) {
        self.acked.fetch_max(seq, Ordering::Relaxed);
        self.last_ack_ms.fetch_max(now_ms, Ordering::Relaxed);
    }

    pub fn acked(&self) -> u64 {
        self.acked.load(Ordering::Relaxed)
    }

    pub fn has_live_replica(&self, now_ms: u64) -> bool {
        let last = self.last_ack_ms.load(Ordering::Relaxed);
        last != 0 && now_ms.saturating_sub(last) <= LIVENESS_WINDOW_MS
    }

    /// Time-lag of the freshest replica. None = no live replica (no cap).
    pub fn lag_ms(&self, now_ms: u64) -> Option<u64> {
        if !self.has_live_replica(now_ms) {
            return None;
        }
        let acked = self.acked();
        let samples = self.samples.lock().unwrap_or_else(|e| e.into_inner());
        // Earliest sample the replica has NOT confirmed yet.
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
    fn lag_is_age_of_oldest_unacked_write() {
        let hub = ReplHub::new();
        hub.record_sample(10, 1_000);
        hub.record_sample(20, 1_400);
        hub.record_sample(30, 1_800);
        hub.record_ack(10, 1_900);
        // Oldest unacked sample is seq 20 at t=1400.
        assert_eq!(hub.lag_ms(2_000), Some(600));
        hub.record_ack(20, 2_000);
        assert_eq!(hub.lag_ms(2_050), Some(250));
        hub.record_ack(30, 2_100);
        assert_eq!(hub.lag_ms(2_100), Some(0), "fully caught up");
    }

    #[test]
    fn no_live_replica_means_no_cap() {
        let hub = ReplHub::new();
        hub.record_sample(10, 1_000);
        assert_eq!(hub.lag_ms(1_500), None, "never acked = not live");
        hub.record_ack(5, 1_500);
        assert!(hub.lag_ms(1_600).is_some());
        // Liveness window expires.
        assert_eq!(hub.lag_ms(1_500 + LIVENESS_WINDOW_MS + 1), None);
    }

    #[test]
    fn samples_dedupe_and_cap() {
        let hub = ReplHub::new();
        hub.record_sample(7, 100);
        hub.record_sample(7, 200); // same seq: dropped
        for i in 0..2 * MAX_SAMPLES as u64 {
            hub.record_sample(100 + i, 300 + i);
        }
        let n = hub.samples.lock().unwrap_or_else(|e| e.into_inner()).len();
        assert!(n <= MAX_SAMPLES);
    }
}
