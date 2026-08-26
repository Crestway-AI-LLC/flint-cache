//! S3-FIFO admission and eviction (ADR-0023 D7, requirement 3).
//!
//! **Why not LRU.** The accelerator's normal workload is a training job walking
//! a dataset larger than the cache, epoch after epoch. Under LRU a cyclic scan
//! evicts every object just before it comes round again, so the hit rate is not
//! `cache/dataset` — it is approximately ZERO. Worse, a single scan walks the
//! whole cache out of residency, so a hot working set that was serving well
//! before the scan is gone after it. That is not underperformance, it is a
//! cache that stops being one at the exact moment the workload gets big.
//!
//! **The shape.** Three queues, all FIFO, no recency list to maintain:
//!
//! - `small` — where every new key enters, holding a small share of capacity.
//! - `main` — where keys go once they prove they are reused.
//! - `ghost` — KEYS ONLY, no data. Remembers what left `small` unused.
//!
//! A key evicted from `small` without a second access leaves a ghost entry. If
//! it is admitted again while that ghost is still remembered, it enters `main`
//! directly: one-hit wonders are filtered by `small`, and genuinely reused keys
//! are recognised on their second appearance rather than their first.
//!
//! That is what makes it scan-resistant. Scanned keys are touched once, so they
//! flow through `small` and out, and `main` — which is most of the capacity and
//! holds the working set — is never disturbed by them. The property is asserted
//! in the tests against an LRU baseline built in the same file, because "we
//! chose a scan-resistant policy" is a claim, and the workload that breaks LRU
//! is cheap to simulate.
//!
//! **This structure decides only WHICH keys to reclaim.** It does not delete
//! anything. Its output is fed to [`crate::eviction::EvictionState::mark`], and
//! the compaction filter's guard still independently refuses any key whose
//! namespace was not declared evictable. A bug here cannot cost data, only hit
//! rate — which is the reason the two are separate types.

use std::collections::{HashMap, HashSet, VecDeque};

/// How much of capacity `small` may hold before it is the queue evicted from.
/// 10% is the figure from the S3-FIFO paper and the one the reference
/// implementations use; it is the knob most worth revisiting with a measured
/// workload, and least worth guessing at now.
const SMALL_SHARE_PCT: u64 = 10;

/// Access counter ceiling. S3-FIFO caps frequency deliberately low: the point
/// is to separate "reused" from "touched once", not to rank hot keys against
/// each other, and a high ceiling makes an object that was briefly popular
/// long ago expensive to displace.
const FREQ_MAX: u8 = 3;

/// The smallest ghost queue worth keeping. The bound below scales with the
/// resident object count, which is zero on an empty cache — without a floor,
/// the first keys admitted could never be remembered and the policy would
/// degrade to plain FIFO exactly while it is warming up.
const GHOST_MIN: usize = 16;

#[derive(Default)]
pub struct S3Fifo {
    capacity_bytes: u64,
    small: VecDeque<Vec<u8>>,
    main: VecDeque<Vec<u8>>,
    ghost: VecDeque<Vec<u8>>,
    ghost_set: HashSet<Vec<u8>>,
    /// Resident keys and their sizes. Also the membership test.
    sizes: HashMap<Vec<u8>, u64>,
    freq: HashMap<Vec<u8>, u8>,
    small_bytes: u64,
    main_bytes: u64,
}

impl S3Fifo {
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            capacity_bytes,
            ..Default::default()
        }
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Resize. Does not evict — the caller drives eviction through
    /// [`Self::reclaim_to`] so that the decision to delete is always explicit
    /// and always at a moment the caller chose.
    pub fn set_capacity_bytes(&mut self, bytes: u64) {
        self.capacity_bytes = bytes;
    }

    pub fn resident_bytes(&self) -> u64 {
        self.small_bytes + self.main_bytes
    }

    pub fn resident_keys(&self) -> usize {
        self.sizes.len()
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.sizes.contains_key(key)
    }

    /// A read hit. Raises the key's frequency, which is what lets it survive
    /// `small` and, once in `main`, survive a pass of the eviction hand.
    pub fn on_access(&mut self, key: &[u8]) {
        if let Some(f) = self.freq.get_mut(key) {
            *f = (*f + 1).min(FREQ_MAX);
        }
    }

    /// A key was written or fetched into the cache.
    ///
    /// A key still remembered by the ghost queue enters `main` directly: it has
    /// now been seen twice, which is the evidence `small` exists to collect.
    pub fn on_admit(&mut self, key: &[u8], bytes: u64) {
        if self.sizes.contains_key(key) {
            self.on_access(key);
            return;
        }
        self.sizes.insert(key.to_vec(), bytes);
        self.freq.insert(key.to_vec(), 0);
        if self.ghost_set.remove(key) {
            if let Some(p) = self.ghost.iter().position(|g| g == key) {
                self.ghost.remove(p);
            }
            self.main.push_back(key.to_vec());
            self.main_bytes += bytes;
        } else {
            self.small.push_back(key.to_vec());
            self.small_bytes += bytes;
        }
    }

    /// A key left the cache by some other route (explicit delete, expiry).
    pub fn forget(&mut self, key: &[u8]) {
        if let Some(bytes) = self.sizes.remove(key) {
            if let Some(p) = self.small.iter().position(|k| k == key) {
                self.small.remove(p);
                self.small_bytes = self.small_bytes.saturating_sub(bytes);
            } else if let Some(p) = self.main.iter().position(|k| k == key) {
                self.main.remove(p);
                self.main_bytes = self.main_bytes.saturating_sub(bytes);
            }
            self.freq.remove(key);
        }
    }

    /// Choose keys to reclaim until resident bytes fall to `target_bytes`.
    ///
    /// Returns the keys, in eviction order. Nothing is deleted here — see the
    /// module header: the caller marks them and the compaction filter's guard
    /// gets the final say.
    ///
    /// Bounded by construction: every iteration either returns a key or breaks,
    /// so a caller cannot spin this on an empty cache or on one whose queues
    /// disagree with the byte counters.
    pub fn reclaim_to(&mut self, target_bytes: u64) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        while self.resident_bytes() > target_bytes {
            match self.evict_one() {
                Some(k) => out.push(k),
                None => break,
            }
        }
        out
    }

    fn small_limit(&self) -> u64 {
        self.capacity_bytes * SMALL_SHARE_PCT / 100
    }

    /// One eviction step. `None` means nothing is evictable, which is the
    /// termination condition rather than an error.
    fn evict_one(&mut self) -> Option<Vec<u8>> {
        // A full pass of both queues without evicting is possible in principle
        // (every key promoted or demoted rather than dropped), so the loop is
        // bounded by the number of resident keys rather than by `while true`.
        let budget = self.small.len() + self.main.len() + 1;
        for _ in 0..budget {
            let from_small = self.small_bytes > self.small_limit() && !self.small.is_empty();
            let evicted = if from_small {
                self.step_small()
            } else if !self.main.is_empty() {
                self.step_main()
            } else if !self.small.is_empty() {
                self.step_small()
            } else {
                return None;
            };
            if let Some(k) = evicted {
                return Some(k);
            }
        }
        None
    }

    /// Head of `small`: promote if it was reused, otherwise evict and remember
    /// it as a ghost so a second sighting skips `small` next time.
    fn step_small(&mut self) -> Option<Vec<u8>> {
        let key = self.small.pop_front()?;
        let bytes = *self.sizes.get(&key)?;
        self.small_bytes = self.small_bytes.saturating_sub(bytes);
        if self.freq.get(&key).copied().unwrap_or(0) > 0 {
            self.freq.insert(key.clone(), 0);
            self.main.push_back(key);
            self.main_bytes += bytes;
            return None;
        }
        self.sizes.remove(&key);
        self.freq.remove(&key);
        self.push_ghost(key.clone());
        Some(key)
    }

    /// Head of `main`: one reprieve per accumulated access, then out.
    fn step_main(&mut self) -> Option<Vec<u8>> {
        let key = self.main.pop_front()?;
        let bytes = *self.sizes.get(&key)?;
        let f = self.freq.get(&key).copied().unwrap_or(0);
        if f > 0 {
            self.freq.insert(key.clone(), f - 1);
            self.main.push_back(key);
            return None;
        }
        self.main_bytes = self.main_bytes.saturating_sub(bytes);
        self.sizes.remove(&key);
        self.freq.remove(&key);
        Some(key)
    }

    /// Ghost entries are keys without data, so the bound is on COUNT. Scaled to
    /// the resident object count — the population the ghost queue is trying to
    /// remember alternatives for — with a floor so a warming cache still has a
    /// memory. See [`GHOST_MIN`].
    fn push_ghost(&mut self, key: Vec<u8>) {
        let limit = self.sizes.len().max(GHOST_MIN);
        self.ghost_set.insert(key.clone());
        self.ghost.push_back(key);
        while self.ghost.len() > limit {
            if let Some(old) = self.ghost.pop_front() {
                self.ghost_set.remove(&old);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(n: usize) -> Vec<u8> {
        format!("k{n}").into_bytes()
    }

    /// The baseline the policy choice is justified against.
    ///
    /// "We picked a scan-resistant policy" is a claim about a COMPARISON, and a
    /// comparison needs both sides present. Textbook LRU, built here so the
    /// failure mode D7 describes is demonstrated rather than cited.
    struct Lru {
        cap: usize,
        order: VecDeque<Vec<u8>>,
        set: HashSet<Vec<u8>>,
    }

    impl Lru {
        fn new(cap: usize) -> Self {
            Self {
                cap,
                order: VecDeque::new(),
                set: HashSet::new(),
            }
        }
        fn touch(&mut self, key: &[u8]) {
            if let Some(p) = self.order.iter().position(|x| x == key) {
                self.order.remove(p);
            } else {
                self.set.insert(key.to_vec());
            }
            self.order.push_back(key.to_vec());
            while self.order.len() > self.cap {
                if let Some(old) = self.order.pop_front() {
                    self.set.remove(&old);
                }
            }
        }
        fn contains(&self, key: &[u8]) -> bool {
            self.set.contains(key)
        }
    }

    /// THE REQUIREMENT (D7 req 3), as the property that actually matters.
    ///
    /// Not "a cyclic scan gets hits" — under a scan over a dataset larger than
    /// the cache, NO policy gets hits, and a test asserting otherwise would be
    /// asserting something false. The property is that a scan must not DESTROY
    /// a working set that was serving well before it started. Under LRU one
    /// scan walks the entire cache out of residency; that is what makes a
    /// training job's epoch pass hostile rather than merely large.
    ///
    /// 100 units of capacity, a 50-key hot set, then a 1000-key scan.
    #[test]
    fn a_full_dataset_scan_does_not_evict_the_working_set() {
        const CAP: u64 = 100;
        const HOT: usize = 50;
        const SCAN: usize = 1000;

        let mut s3 = S3Fifo::new(CAP);
        let mut lru = Lru::new(CAP as usize);

        // Establish the working set: admitted, then genuinely reused.
        for round in 0..3 {
            for i in 0..HOT {
                s3.on_admit(&k(i), 1);
                if round > 0 {
                    s3.on_access(&k(i));
                }
                s3.reclaim_to(CAP);
                lru.touch(&k(i));
            }
        }
        let hot_before = (0..HOT).filter(|&i| s3.contains(&k(i))).count();
        // POSITIVE CONTROL: there is a working set to lose. Without this, "the
        // scan did not evict it" is also true of a cache that never held it.
        assert_eq!(
            hot_before, HOT,
            "the working set was not resident before the scan, so this test \
             cannot show anything about surviving one"
        );

        // The epoch pass: 1000 distinct keys, each touched exactly once.
        for i in 0..SCAN {
            let key = k(10_000 + i);
            s3.on_admit(&key, 1);
            s3.reclaim_to(CAP);
            lru.touch(&key);
        }

        let s3_survivors = (0..HOT).filter(|&i| s3.contains(&k(i))).count();
        let lru_survivors = (0..HOT).filter(|&i| lru.contains(&k(i))).count();

        // The failure mode, MEASURED rather than assumed. If LRU somehow kept
        // the working set here, the workload would not be the hostile one this
        // policy was chosen for and the comparison below would prove nothing.
        assert_eq!(
            lru_survivors, 0,
            "LRU kept {lru_survivors} of {HOT} hot keys — the scan is not \
             actually hostile, so this test is not exercising the requirement"
        );
        assert!(
            s3_survivors >= HOT * 8 / 10,
            "S3-FIFO kept only {s3_survivors} of {HOT} hot keys through a \
             {SCAN}-key scan; the working set is not protected"
        );
    }

    /// The mechanism behind that result: a key seen once flows through `small`
    /// and out, and only a key seen AGAIN reaches `main`. Asserted directly, so
    /// a scan-resistance regression is attributable rather than just visible.
    #[test]
    fn a_second_sighting_promotes_past_small() {
        let mut c = S3Fifo::new(100);
        let target = k(1);

        c.on_admit(&target, 1);
        // Push it out with unreused traffic. Two constraints, and an earlier
        // version of this test violated both in turn.
        //
        // It has to EXCEED capacity, not merely approach it: 60 one-byte
        // admits into a 100-byte cache evict nothing at all.
        //
        // And it must not run so long that the ghost queue forgets the key.
        // Ghost memory is BOUNDED (see `push_ghost`), so a key re-admitted
        // long enough after its eviction is indistinguishable from a new one
        // and correctly enters `small` again. 300 admits did that and the
        // test read it as the promotion failing. 150 leaves the eviction
        // comfortably inside the window.
        for i in 0..150 {
            c.on_admit(&k(1_000 + i), 1);
            c.reclaim_to(100);
        }
        assert!(!c.contains(&target), "one-hit key stayed resident");

        // Re-admitted while still remembered as a ghost: straight into main.
        c.on_admit(&target, 1);
        assert!(c.contains(&target));
        for i in 0..300 {
            c.on_admit(&k(2_000 + i), 1);
            c.reclaim_to(100);
        }
        assert!(
            c.contains(&target),
            "a twice-seen key was evicted as readily as a once-seen one, so \
             the ghost queue is not promoting"
        );
    }

    /// `reclaim_to` must reach its target and must terminate. The loop can see
    /// queues that promote rather than evict, so "it always returns something"
    /// is not free.
    #[test]
    fn reclaim_reaches_its_target_and_terminates() {
        let mut c = S3Fifo::new(100);
        for i in 0..500 {
            c.on_admit(&k(i), 1);
            c.on_access(&k(i));
        }
        let evicted = c.reclaim_to(100);
        assert!(!evicted.is_empty());
        assert!(
            c.resident_bytes() <= 100,
            "resident {} above target after reclaim",
            c.resident_bytes()
        );
        // An empty cache terminates rather than spinning.
        let mut empty = S3Fifo::new(10);
        assert!(empty.reclaim_to(0).is_empty());
    }

    /// Bytes must be tracked, not object counts: the accelerator's values are
    /// 8 MiB chunks and a policy counting objects would hold 8 MiB and 8 B
    /// equally.
    #[test]
    fn capacity_is_bytes_not_objects() {
        let mut c = S3Fifo::new(1000);
        c.on_admit(&k(1), 900);
        c.on_admit(&k(2), 900);
        c.reclaim_to(1000);
        assert!(
            c.resident_bytes() <= 1000,
            "two 900-byte values both stayed under a 1000-byte cap"
        );
    }

    /// A key removed by another path leaves no phantom bytes behind.
    #[test]
    fn forget_releases_the_bytes_it_accounted() {
        let mut c = S3Fifo::new(100);
        c.on_admit(&k(1), 40);
        c.on_admit(&k(2), 40);
        assert_eq!(c.resident_bytes(), 80);
        c.forget(&k(1));
        assert_eq!(c.resident_bytes(), 40);
        assert!(!c.contains(&k(1)));
        c.forget(&k(1));
        assert_eq!(c.resident_bytes(), 40, "double forget double-counted");
    }
}
