// SPDX-License-Identifier: Elastic-2.0
//! Per-slot op counter — the "heat" signal the traffic-balance policy reads.
//!
//! A fixed `[AtomicU64; 16384]` indexed by slot (summed across namespaces),
//! bumped once per keyed command on the dispatch hot path. Cost per op is a
//! CRC16 over the key plus one relaxed atomic add — cheap enough to leave
//! always-on. Counters are cumulative (like a Prometheus counter): a consumer
//! reads FLINTSLOTHEAT twice and divides the delta by the elapsed wall time
//! to get ops/second. `uptime_ms` lets a single scrape estimate an average.
//!
//! Summed-across-ns is deliberate: per-(ns, slot) counting would need a
//! concurrent map on the hottest path. A slot is normally owned by one pair
//! across all namespaces (Option B default ranges), so per-slot heat
//! attributes cleanly to that pair; the rare exception-run case is a small
//! inaccuracy in a load *heuristic*, not a correctness issue. The consumer
//! crosses per-slot heat with FLINTSLOTSTATS (per (ns, slot)) to pick the
//! actual migration unit.
//!
//! This lives in the OPEN server as instrumentation, mirroring FLINTNSBYTES:
//! the raw signal is open; the traffic-balance *policy* that consumes it is
//! the managed plane's.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const SLOTS: usize = 16384;

static SLOT_HEAT: [AtomicU64; SLOTS] = [const { AtomicU64::new(0) }; SLOTS];

/// Process-start wall clock (ms), set on first use, so a single FLINTSLOTHEAT
/// scrape can estimate an average rate without a prior sample.
static STARTED_MS: AtomicU64 = AtomicU64::new(0);

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Record one op against `slot`. Relaxed: the counter is a statistical load
/// signal, not a synchronization point — exact ordering across threads is
/// irrelevant to a rate estimate.
#[inline]
pub fn record_slot(slot: u16) {
    STARTED_MS
        .compare_exchange(0, now_ms(), Ordering::Relaxed, Ordering::Relaxed)
        .ok();
    SLOT_HEAT[slot as usize].fetch_add(1, Ordering::Relaxed);
}

/// Record one op for `key` (computes its slot, hash-tag aware).
#[inline]
pub fn record_key(key: &[u8]) {
    record_slot(flint_slot::slot_for_key(key));
}

/// Milliseconds since the first recorded op (0 before any op).
pub fn uptime_ms() -> u64 {
    match STARTED_MS.load(Ordering::Relaxed) {
        0 => 0,
        started => now_ms().saturating_sub(started),
    }
}

/// Wall clock (ms) at which this PROCESS started. Distinct from
/// `STARTED_MS` above, which is the first-op clock: a node that has served
/// no traffic reports zero there, which is right for a rate estimate and
/// wrong for "how long has this seat been up".
///
/// The difference matters for ADR-0014 D2's config-drift check. Drift
/// between a master and its replica is legitimate mid-roll, so the check
/// suppresses it while either seat is young — and using the first-op clock
/// would have made an IDLE seat look permanently young, silently
/// suppressing real drift forever. Wrong in the direction that hides
/// things.
static PROC_STARTED_MS: AtomicU64 = AtomicU64::new(0);

/// Call once, early in main. Idempotent.
pub fn mark_process_start() {
    PROC_STARTED_MS
        .compare_exchange(0, now_ms(), Ordering::Relaxed, Ordering::Relaxed)
        .ok();
}

/// Milliseconds this process has been running, or 0 if never marked.
///
/// Only read from the rocks-gated FLINTINFO block; the mem-only build
/// still marks the start, it just has no info response to report it in.
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
pub fn process_uptime_ms() -> u64 {
    match PROC_STARTED_MS.load(Ordering::Relaxed) {
        0 => 0,
        started => now_ms().saturating_sub(started),
    }
}

/// Non-zero (slot, cumulative-ops) rows, ascending by slot. Bounded by the
/// number of slots that have seen traffic.
pub fn snapshot() -> Vec<(u16, u64)> {
    let mut rows = Vec::new();
    for (slot, cell) in SLOT_HEAT.iter().enumerate() {
        let n = cell.load(Ordering::Relaxed);
        if n > 0 {
            rows.push((slot as u16, n));
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_snapshots_by_slot() {
        // Uses a couple of high slots unlikely to collide with other tests'
        // keys in the same process; asserts on deltas, not absolutes.
        let before: std::collections::HashMap<u16, u64> = snapshot().into_iter().collect();
        record_slot(12345);
        record_slot(12345);
        record_slot(12346);
        let after: std::collections::HashMap<u16, u64> = snapshot().into_iter().collect();
        let d = |s: u16| after.get(&s).copied().unwrap_or(0) - before.get(&s).copied().unwrap_or(0);
        assert_eq!(d(12345), 2);
        assert_eq!(d(12346), 1);
    }

    #[test]
    fn record_key_hashes_to_a_slot() {
        let before: std::collections::HashMap<u16, u64> = snapshot().into_iter().collect();
        let slot = flint_slot::slot_for_key(b"heat-probe-key");
        record_key(b"heat-probe-key");
        let after: std::collections::HashMap<u16, u64> = snapshot().into_iter().collect();
        let d = after.get(&slot).copied().unwrap_or(0) - before.get(&slot).copied().unwrap_or(0);
        assert_eq!(d, 1);
        assert!(uptime_ms() < 60_000, "uptime is a small recent delta");
    }
}
