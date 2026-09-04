// SPDX-License-Identifier: Elastic-2.0
//! Bounding the SUM of concurrent collection reads, which is BUG-0060's title.
//!
//! Every unit is already bounded: `max-value-bytes` caps one collection at
//! 512 MiB. Nothing bounds their sum, and the sum is what reaches physical
//! memory -- five concurrent max-size reads is ~8.1 GB, at a `max-conns` of
//! 2048.
//!
//! # Why a memory sample alone is not enough
//!
//! Sampling `mem_avail_bytes` per read is nearly right: a read that has already
//! allocated is visible in the next sample. But reads admitted CONCURRENTLY
//! inside one sampling window all see the same headroom and all pass. So this
//! keeps its own exact count of what it has admitted and not yet released --
//! the sample bounds the budget, the counter bounds the sum.
//!
//! # The multiplier, and why it is not 1.0
//!
//! A collection read costs multiples of the collection, so the estimate is
//! `k x ComplexMeta.bytes`. k was measured on the platform that ships
//! (`tools/collection_read_peak.py`, Linux x86_64): it CLIMBS with collection
//! size and again with item count before saturating -- 1.96 at 50 MB, 3.03 at
//! the 512 MiB cap. k is increasing, so k(cap) bounds every smaller read and a
//! mid-range sample does not. 3.5 clears the Linux maximum of 3.025 by 16% and
//! the macOS maximum of 3.189 by 10%, and every measurement is a lower bound
//! because an RSS peak understates demand whenever the host is reclaiming.
//!
//! # Two deliberate choices that would otherwise look like bugs
//!
//! **An unreadable budget ADMITS**, and says so. `mem::sample()` returns `None`
//! on macOS and on any unreadable `/proc/meminfo`. Refusing everything would
//! take a node down over a missing file; admitting silently would let this
//! claim a check it never made. So it admits and counts, and the count is in
//! FLINTINFO -- the rule BUG-0079 broke, applied to an admission rather than a
//! refusal: never assert a fact you failed to look up.
//!
//! **The budget double-counts our own in-flight reads.** `mem_avail` already
//! reflects what in-flight reads have allocated, and they are counted again in
//! `in_flight`. That makes admission progressively stricter as load rises,
//! which is the conservative direction and the behaviour a stampede valve
//! wants. Stated because it is not an accident.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::mem;

/// Peak bytes a collection read costs, per byte of collection, in TENTHS --
/// integer arithmetic so the estimate cannot drift with float formatting.
/// See the module docs for how 3.5 was chosen and measured.
pub const READ_PEAK_MULTIPLIER_TENTHS: u64 = 35;

/// Opt-in. 0 disables admission entirely, and is the default: a new refusal
/// path does not get switched on for existing workloads by inference.
pub const DEFAULT_COLLECTION_READ_BUDGET_PCT: u8 = 0;

/// How long a memory sample is reused. The fast-moving term -- what THIS node
/// has admitted -- is counted exactly, so staleness only lags memory pressure
/// arriving from outside the process.
const SAMPLE_TTL_MS: u64 = 250;

/// Packed `avail_mib:u32 | taken_ms:u32`, one atomic so the two halves cannot
/// tear: a torn read could pair a fresh timestamp with a stale figure and then
/// never refresh. `taken_ms` is milliseconds since this struct was built.
/// 0 is the never-sampled sentinel.
fn pack(avail_mib: u32, taken_ms: u32) -> u64 {
    (u64::from(avail_mib) << 32) | u64::from(taken_ms)
}

fn unpack(v: u64) -> (u32, u32) {
    ((v >> 32) as u32, v as u32)
}

/// What admission decided, and everything needed to explain it to the client.
#[derive(Debug, PartialEq, Eq)]
pub enum Admit {
    /// Admission is switched off (`pct == 0`). No reservation is held.
    Disabled,
    /// Admitted, holding `est` bytes of reservation until `release`.
    Admitted { est: u64 },
    /// Admitted WITHOUT a budget, because node memory could not be read.
    /// Distinct from `Admitted` so the count is visible rather than silent.
    AdmittedUnmeasured { est: u64 },
    /// Refused. Carries its own terms, so the message names what it saw
    /// rather than only its verdict.
    Refused {
        est: u64,
        in_flight: u64,
        budget: u64,
        avail: u64,
    },
}

impl Admit {
    /// The client-facing refusal, naming every term it saw rather than only
    /// its verdict -- the shape the write-deadline refusal established, and
    /// what makes a THROTTLED reply diagnosable without server logs.
    /// `None` for any outcome that admitted.
    pub fn refusal(&self, bytes: u64, pct: u8) -> Option<String> {
        let Admit::Refused {
            est,
            in_flight,
            budget,
            avail,
        } = self
        else {
            return None;
        };
        Some(format!(
            "THROTTLED collection read needs ~{est} bytes ({bytes} x {}.{} peak), in-flight \
             {in_flight}, past --collection-read-budget-pct {pct} of {avail} available \
             ({budget}), retry with backoff",
            READ_PEAK_MULTIPLIER_TENTHS / 10,
            READ_PEAK_MULTIPLIER_TENTHS % 10,
        ))
    }
}

/// Node-level admission for collection reads.
#[derive(Debug)]
pub struct ReadAdmission {
    pct: u8,
    in_flight: AtomicU64,
    sample: AtomicU64,
    started: Instant,
    refused: AtomicU64,
    unmeasured: AtomicU64,
    /// Test seam: a fixed figure standing in for node memory, so the refusal
    /// arithmetic can be PROVEN on a host whose real memory is unreadable.
    /// macOS has no `/proc/meminfo`, so without this every local test would
    /// take the `AdmittedUnmeasured` path and a broken bound would still pass.
    /// Production is always `None` and reads the node.
    avail_override: Option<u64>,
}

impl ReadAdmission {
    pub fn new(pct: u8) -> Self {
        Self {
            pct: pct.min(100),
            in_flight: AtomicU64::new(0),
            sample: AtomicU64::new(0),
            started: Instant::now(),
            refused: AtomicU64::new(0),
            unmeasured: AtomicU64::new(0),
            avail_override: None,
        }
    }

    /// A `ReadAdmission` that believes the node has exactly `avail` bytes
    /// free. Tests only -- see `avail_override`.
    pub fn with_fixed_avail(pct: u8, avail: u64) -> Self {
        Self {
            avail_override: Some(avail),
            ..Self::new(pct)
        }
    }

    pub fn enabled(&self) -> bool {
        self.pct > 0
    }

    pub fn pct(&self) -> u8 {
        self.pct
    }

    pub fn in_flight_bytes(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    pub fn refused_total(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    /// Reads admitted while node memory was unreadable -- admitted WITHOUT a
    /// budget having been checked. A non-zero value here means the bound is
    /// not in force, which is exactly the thing that must not be silent.
    pub fn unmeasured_total(&self) -> u64 {
        self.unmeasured.load(Ordering::Relaxed)
    }

    /// `k x bytes`, in u128 so a large collection cannot wrap the product.
    pub fn estimate(bytes: u64) -> u64 {
        let est = u128::from(bytes) * u128::from(READ_PEAK_MULTIPLIER_TENTHS) / 10;
        u64::try_from(est).unwrap_or(u64::MAX)
    }

    /// Available node memory, cached for `SAMPLE_TTL_MS`. `None` means it
    /// could not be read -- NOT that no memory is available.
    fn avail_bytes(&self) -> Option<u64> {
        if let Some(fixed) = self.avail_override {
            return Some(fixed);
        }
        let now_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let (cached_mib, taken_ms) = unpack(self.sample.load(Ordering::Relaxed));
        if cached_mib > 0 && now_ms.saturating_sub(u64::from(taken_ms)) < SAMPLE_TTL_MS {
            return Some(u64::from(cached_mib) * 1024 * 1024);
        }
        let usage = mem::sample()?;
        let mib = u32::try_from(usage.avail_bytes / (1024 * 1024)).unwrap_or(u32::MAX);
        // A real reading below 1 MiB packs as the never-sampled sentinel, so
        // it simply is not cached -- correctness over one avoided syscall on a
        // node that has already run out of memory.
        if mib > 0 {
            let stamp = u32::try_from(now_ms).unwrap_or(u32::MAX);
            self.sample.store(pack(mib, stamp), Ordering::Relaxed);
        }
        Some(usage.avail_bytes)
    }

    /// Decide, and reserve on success. Every `Admitted*` MUST be paired with
    /// `release(est)`; the caller holds it across dispatch AND encoding,
    /// because the materialised reply owns the collection until then.
    pub fn admit(&self, bytes: u64) -> Admit {
        if !self.enabled() {
            return Admit::Disabled;
        }
        let est = Self::estimate(bytes);
        let Some(avail) = self.avail_bytes() else {
            self.in_flight.fetch_add(est, Ordering::AcqRel);
            self.unmeasured.fetch_add(1, Ordering::Relaxed);
            return Admit::AdmittedUnmeasured { est };
        };
        let budget =
            u64::try_from(u128::from(avail) * u128::from(self.pct) / 100).unwrap_or(u64::MAX);
        let in_flight = self.in_flight.load(Ordering::Acquire);
        if in_flight.saturating_add(est) > budget {
            self.refused.fetch_add(1, Ordering::Relaxed);
            return Admit::Refused {
                est,
                in_flight,
                budget,
                avail,
            };
        }
        self.in_flight.fetch_add(est, Ordering::AcqRel);
        Admit::Admitted { est }
    }

    /// Release a reservation. Saturating: a double release must not wrap the
    /// counter to near-u64::MAX and refuse every subsequent read forever.
    pub fn release(&self, est: u64) {
        let mut cur = self.in_flight.load(Ordering::Acquire);
        loop {
            let next = cur.saturating_sub(est);
            match self.in_flight.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => cur = observed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_by_default_admits_without_reserving() {
        let a = ReadAdmission::new(DEFAULT_COLLECTION_READ_BUDGET_PCT);
        assert!(!a.enabled());
        assert_eq!(a.admit(512 * 1024 * 1024), Admit::Disabled);
        assert_eq!(a.in_flight_bytes(), 0, "disabled must not reserve");
    }

    #[test]
    fn the_estimate_is_the_measured_multiplier() {
        // 3.5x, and the tenths arithmetic must not drift on a big collection.
        assert_eq!(ReadAdmission::estimate(1000), 3500);
        assert_eq!(
            ReadAdmission::estimate(512 * 1024 * 1024),
            1_879_048_192,
            "512 MiB x 3.5"
        );
        assert_eq!(
            ReadAdmission::estimate(u64::MAX),
            u64::MAX,
            "saturates rather than wrapping to a tiny estimate that admits"
        );
    }

    #[test]
    fn a_release_of_more_than_is_held_cannot_wrap_the_counter() {
        // Wrapping here would leave in_flight near u64::MAX and refuse every
        // read for the life of the process -- a far worse failure than the
        // double release that caused it.
        let a = ReadAdmission::new(50);
        a.release(1_000_000);
        assert_eq!(a.in_flight_bytes(), 0);
    }

    #[test]
    fn packing_round_trips_and_does_not_tear() {
        for (mib, ms) in [(0u32, 0u32), (1, 1), (u32::MAX, u32::MAX), (4096, 250)] {
            assert_eq!(unpack(pack(mib, ms)), (mib, ms));
        }
    }

    #[test]
    fn a_read_past_the_budget_is_refused_and_reserves_nothing() {
        // 1 GiB free, 25% budget = 268435456. A 512 MiB collection estimates
        // at 1.8 GB, which does not fit.
        let a = ReadAdmission::with_fixed_avail(25, 1024 * 1024 * 1024);
        let bytes = 512 * 1024 * 1024;
        let d = a.admit(bytes);
        assert!(
            matches!(d, Admit::Refused { .. }),
            "expected a refusal, got {d:?}"
        );
        assert_eq!(a.refused_total(), 1);
        assert_eq!(
            a.in_flight_bytes(),
            0,
            "a refused read must reserve nothing -- otherwise one refusal \
             permanently shrinks the budget"
        );
        let msg = d
            .refusal(bytes, a.pct())
            .expect("a refusal must explain itself");
        for term in [
            "THROTTLED",
            "1879048192",
            "3.5 peak",
            "268435456",
            "retry with backoff",
        ] {
            assert!(msg.contains(term), "refusal omits {term}: {msg}");
        }
    }

    #[test]
    fn the_sum_is_what_is_bounded_not_the_individual_read() {
        // This is the bug's title. Each read fits on its own; the third is
        // refused because the two before it are still in flight.
        let a = ReadAdmission::with_fixed_avail(100, 1_000_000_000);
        let bytes = 100_000_000; // est 350 MB each
        let mut held = Vec::new();
        for i in 0..2 {
            match a.admit(bytes) {
                Admit::Admitted { est } => held.push(est),
                other => panic!("read {i} should fit alone: {other:?}"),
            }
        }
        assert_eq!(a.in_flight_bytes(), 700_000_000);
        assert!(
            matches!(a.admit(bytes), Admit::Refused { .. }),
            "the third read fits the budget alone but not alongside the two in flight"
        );
        // Positive control: the refusal is caused by the reservations, not by
        // something that would refuse regardless. Release and it admits.
        for est in held {
            a.release(est);
        }
        assert_eq!(a.in_flight_bytes(), 0);
        assert!(
            matches!(a.admit(bytes), Admit::Admitted { .. }),
            "with nothing in flight the same read must be admitted -- otherwise \
             the test above proves nothing about the SUM"
        );
    }

    #[test]
    fn an_unreadable_budget_admits_and_is_counted_separately() {
        // On macOS `mem::sample()` is None, which is the same shape as an
        // unreadable /proc/meminfo on Linux. Either way the read is admitted
        // -- a missing file must not take the node down -- and the fact that
        // no budget was checked has to be visible.
        let a = ReadAdmission::new(25);
        match a.admit(1024) {
            Admit::AdmittedUnmeasured { est } => {
                assert_eq!(est, 3584);
                assert_eq!(a.unmeasured_total(), 1);
                assert_eq!(a.in_flight_bytes(), 3584, "still reserved");
                a.release(est);
                assert_eq!(a.in_flight_bytes(), 0);
            }
            // On Linux the budget IS readable, so this path is the live one.
            Admit::Admitted { est } => {
                assert_eq!(a.unmeasured_total(), 0);
                a.release(est);
                assert_eq!(a.in_flight_bytes(), 0);
            }
            other => panic!("a 1 KiB read must not be refused: {other:?}"),
        }
    }
}
