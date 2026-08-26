//! Capacity-eviction state for one DB (ADR-0023 D7).
//!
//! Flint's position is that it never silently drops what a user put there, and
//! D7.3's mechanism sits uncomfortably close to the opposite: the compaction
//! filter returns `Decision::Remove` for marked rows, so eviction is a SILENT
//! DELETE performed by a background thread. That is the right mechanism — a
//! tombstone-based evictor adds writes to the tier BUG-0013 shows is already
//! saturating compaction — but it means a bug in the marking policy does not
//! surface as an error. It surfaces as data that is simply gone, in a
//! namespace whose whole contract was that it would not be.
//!
//! So the namespace check is NOT part of the policy, and does not trust it.
//! [`EvictionState::should_drop`] re-derives the namespace FROM THE KEY ITSELF
//! and requires it to be declared evictable, whatever the mark set believes. A
//! policy that marks the wrong key cannot reach a durable namespace, because
//! the policy is not consulted on that question. A mark is a request; the
//! guard is the authority.
//!
//! A mark that the guard refuses is a POLICY BUG, and it is counted rather
//! than merely prevented — a defence that silently works leaves the bug in
//! place and running. [`EvictionState::refused`] is that counter.
//!
//! **The state is deliberately non-durable and per-DB.** Non-durable because
//! persisting it would add exactly the write path D7.3 exists to avoid, and
//! losing it is benign: forgotten marks mean keys stay resident, which is a
//! capacity question and never a correctness one. Per-DB rather than a
//! process-wide static because a global could not be a per-test fixture, and
//! two DBs in one process would share one another's marks.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

/// Counters for tuning the batching floors. See [`EvictionState::metrics`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EvictionMetrics {
    pub marked_total: u64,
    pub marked_now: u64,
    pub dropped: u64,
    pub refused: u64,
    pub mark_overflow: u64,
    pub forced_passes: u64,
    pub forced_skipped_cooldown: u64,
    pub forced_skipped_small: u64,
    pub reclaim_cycles: u64,
    pub bytes_requested: u64,
    pub marks_at_last_pass: u64,
}

#[derive(Default)]
pub struct EvictionState {
    /// Namespaces the operator declared re-derivable. The guard's authority.
    evictable: RwLock<HashSet<Vec<u8>>>,
    /// Full keys the policy has asked to reclaim.
    marks: RwLock<HashSet<Vec<u8>>>,
    /// Marks outstanding, so the filter's hot path can skip both locks. The
    /// filter runs per key on every compaction, on a tier whose compaction
    /// headroom is the subject of BUG-0013, so "costs nothing when unused" is
    /// a requirement rather than an optimisation.
    marks_len: AtomicUsize,
    /// Declared-evictable namespaces, as a count, so the request-path hooks can
    /// decide in one relaxed load. Zero on every durable deployment, which is
    /// almost all of them.
    evictable_count: AtomicUsize,
    /// Per-namespace admission policy. ONE mutex, taken with `try_lock` and
    /// never blocking: recording an access is an optimisation, and stalling a
    /// GET to write one down would spend the thing being protected on the
    /// thing protecting it. Sharding this is the obvious next step and is
    /// deliberately not done yet — it should follow a measurement of real
    /// contention, not a guess about it.
    policies: Mutex<HashMap<Vec<u8>, crate::s3fifo::S3Fifo>>,
    dropped: AtomicU64,
    refused: AtomicU64,
    /// Marks refused because the set was full. Nonzero means reclaim is
    /// batching (expected under heavy pressure), not that anything is wrong.
    mark_overflow: AtomicU64,
    /// Byte capacity for the policies, set from the DISK sample the trigger
    /// already takes rather than guessed at admission time. It shapes the
    /// small/main split (and so scan resistance), which is a property of the
    /// device, not of whichever request happened to create the policy.
    ///
    /// Zero means "not yet sampled", and admissions are skipped until it is
    /// set — a policy shaped by a capacity of nothing evicts out of `main`
    /// immediately and is worse than no policy. The disk guard samples at
    /// least every two seconds, so the window is small and self-closing.
    capacity_hint: AtomicU64,
    /// When each namespace last had a compaction pass FORCED, unix-ms.
    last_forced_ms: Mutex<HashMap<Vec<u8>, u64>>,
    // ---- tuning counters. See `metrics()`. ----
    marked_total: AtomicU64,
    forced_passes: AtomicU64,
    forced_skipped_cooldown: AtomicU64,
    forced_skipped_small: AtomicU64,
    reclaim_cycles: AtomicU64,
    bytes_requested: AtomicU64,
    marks_at_last_pass: AtomicU64,
}

/// Minimum gap between FORCED compaction passes on one namespace.
///
/// This is the control that makes batching structural instead of a convention
/// somebody has to remember. `compact_ns` reads and rewrites the SURVIVING
/// rows, so its cost is proportional to NAMESPACE SIZE and not to how many
/// keys were marked: reclaiming a thousand keys from a 400 GB namespace costs
/// the same I/O as reclaiming four hundred million. A forced pass per eviction
/// would therefore be close to the most expensive way to run this feature, and
/// nothing about the call site makes that obvious.
///
/// Waiting is always safe. Marked rows are dropped by ORDINARY compaction
/// whenever it next rewrites them, at zero additional cost; forcing only
/// accelerates that. So the cooldown can never lose data or leak space, it can
/// only delay reclamation — which is why it is a hard floor rather than a
/// heuristic with an override for urgency.
const MIN_FORCED_PASS_INTERVAL_MS: u64 = 60_000;

/// And a pass is not worth its I/O for a handful of rows. Same arithmetic from
/// the other side: if the pass costs the same regardless, wait until enough is
/// riding on it. Marks keep accumulating meanwhile and natural compaction
/// keeps draining them.
const MIN_MARKS_TO_FORCE: usize = 1_000;

/// Ceiling on outstanding marks.
///
/// Marks are FULL KEYS in memory, so this is the one part of the design whose
/// cost scales with the number of rows evicted rather than with the bytes
/// compaction rewrites. At ~50-byte keys plus hash-set overhead, call it
/// ~100 B a mark: a billion marks would be ~100 GB of RAM, which is not a
/// bound so much as an outage. A node's keyspace does not fit here and is not
/// meant to.
///
/// So reclaim BATCHES: mark up to this many, let compaction drop them, clear,
/// go again if the trigger still says so. That converts an impossible memory
/// requirement into more iterations, and it costs nothing extra in I/O because
/// the expensive part — `compact_ns` rewriting the surviving rows — is
/// proportional to NAMESPACE SIZE, not to how many keys were marked. Evicting
/// a thousand keys from a 400 GB namespace and evicting a million cost the
/// same pass.
///
/// 100k marks is ~10 MB and is comfortably more than one compaction pass needs
/// to be worth forcing.
pub const MAX_MARKS: usize = 100_000;

impl EvictionState {
    /// Replace the declared-evictable set. Hot-reloadable: `--evictable-ns` is
    /// re-read through FLINTCONFIG, so this is called again at runtime.
    ///
    /// **A CHANGE DISCARDS EVERY OUTSTANDING MARK.** The guard already makes
    /// stale marks harmless while a namespace is not declared, so this is not
    /// what keeps a durable namespace safe. It is about the sequence
    /// declare -> revoke -> declare: marks taken by the first policy would
    /// otherwise still be armed when the third step re-opens the namespace,
    /// and rows chosen as cold minutes ago by a policy that has since been
    /// turned off would be deleted on the next compaction. Nobody asked for
    /// that, and "surprising deletion" is the whole thing this module is
    /// arranged to prevent. A mark is only valid under the policy that made
    /// it; changing the policy invalidates it.
    ///
    /// Losing marks is free — see the module header. They are a capacity
    /// optimisation, and the cost of dropping them is that some cold keys stay
    /// resident until the policy marks them again.
    pub fn set_evictable<I, S>(&self, names: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        let next: HashSet<Vec<u8>> = names.into_iter().map(|n| n.as_ref().to_vec()).collect();
        // A poisoned lock reads as "changed": re-marking costs a little hit
        // rate, and the alternative is keeping marks whose licence cannot be
        // confirmed.
        let changed = self.evictable.read().map_or(true, |g| *g != next);
        if let Ok(mut g) = self.evictable.write() {
            self.evictable_count.store(next.len(), Ordering::Relaxed);
            *g = next;
        }
        if changed {
            self.clear_marks();
            // Policies belong to the declaration too: a namespace that is no
            // longer evictable should not go on accumulating state, and one
            // being re-declared starts clean rather than resuming a policy
            // whose marks were just discarded.
            if let Ok(mut g) = self.policies.lock() {
                g.clear();
            }
        }
    }

    /// Is this namespace declared evictable?
    pub fn is_evictable_ns(&self, ns: &[u8]) -> bool {
        self.evictable.read().is_ok_and(|g| g.contains(ns))
    }

    /// Ask for a key to be reclaimed at the next compaction that rewrites it.
    ///
    /// A REQUEST, not an instruction: `should_drop` still re-derives the
    /// namespace from the key and refuses if it is not evictable. Marking is
    /// therefore safe to call from a policy that is wrong.
    pub fn mark(&self, key: &[u8]) {
        if let Ok(mut g) = self.marks.write() {
            if g.len() >= MAX_MARKS && !g.contains(key) {
                self.mark_overflow.fetch_add(1, Ordering::Relaxed);
                return;
            }
            if g.insert(key.to_vec()) {
                self.marks_len.store(g.len(), Ordering::Relaxed);
                self.marked_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Forget every mark. Used when a namespace stops being evictable, so
    /// marks taken under the old policy cannot outlive it.
    pub fn clear_marks(&self) {
        if let Ok(mut g) = self.marks.write() {
            g.clear();
            self.marks_len.store(0, Ordering::Relaxed);
        }
    }

    /// THE GUARD. Called by the compaction filter for every row it rewrites.
    ///
    /// Two independent conditions, and the namespace one is not delegated to
    /// the caller that did the marking:
    ///
    /// 1. the key is marked, and
    /// 2. the key's OWN namespace, parsed out of the key here, is declared
    ///    evictable.
    ///
    /// Fails closed at every step. An unparseable key, a poisoned lock, a key
    /// in a namespace nobody declared — all return false, because the cost of
    /// wrongly keeping a row is a little capacity and the cost of wrongly
    /// dropping one is data loss in a store sold on not doing that.
    pub fn should_drop(&self, key: &[u8]) -> bool {
        // Hot path: nothing marked, no locks taken. This is every row of every
        // compaction on every durable deployment, which is almost all of them.
        if self.marks_len.load(Ordering::Relaxed) == 0 {
            return false;
        }
        let Ok(marks) = self.marks.read() else {
            return false;
        };
        if !marks.contains(key) {
            return false;
        }
        drop(marks);

        // Marked. Now the guard, which the mark does not get a vote in.
        let Some(ns) = crate::encoding::ns_of_envelope(key) else {
            self.refused.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        if self.is_evictable_ns(ns) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            // A mark reached a namespace that never opted in. Prevented, and
            // COUNTED: a guard that silently absorbs the bug leaves it running.
            self.refused.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// Rows the filter has actually dropped.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Marks the guard REFUSED. Nonzero means a policy bug, not a tuning
    /// question: something asked to evict a row it had no licence to evict.
    pub fn refused(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    /// Outstanding marks.
    pub fn marked(&self) -> usize {
        self.marks_len.load(Ordering::Relaxed)
    }

    /// A read hit on a user key, from the request path.
    ///
    /// BEST-EFFORT BY CONTRACT. Returns without recording if another thread
    /// holds the policy, because the alternative is blocking a GET to write
    /// down that a GET happened. A dropped access costs a little accuracy in
    /// the frequency signal, which costs a little hit rate; nothing here can
    /// affect correctness, and the guard means it cannot affect durability
    /// either.
    ///
    /// Costs one relaxed load when nothing is declared evictable.
    pub fn note_read(&self, key: &[u8]) {
        if self.evictable_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        let Some(ns) = crate::encoding::ns_of_envelope(key) else {
            return;
        };
        if !self.is_evictable_ns(ns) {
            return;
        }
        if let Ok(mut g) = self.policies.try_lock()
            && let Some(p) = g.get_mut(ns)
        {
            p.on_access(key);
        }
    }

    /// A key was written or admitted, with an APPROXIMATE size.
    ///
    /// Approximate is sufficient and worth being explicit about: the policy's
    /// byte accounting only orders and counts candidates, while the decision to
    /// reclaim at all comes from the disk, which is measured. A size that is a
    /// little wrong changes how many keys are offered, never whether the node
    /// acts.
    ///
    /// Best-effort on the same terms as [`Self::note_read`]. A missed admission
    /// means the policy does not know the key exists and so will never offer
    /// it, which leaves it resident — a capacity question that the next trigger
    /// sample re-raises.
    pub fn note_write(&self, key: &[u8], approx_bytes: u64) {
        if self.evictable_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        let capacity = self.capacity_hint.load(Ordering::Relaxed);
        if capacity == 0 {
            return;
        }
        let Some(ns) = crate::encoding::ns_of_envelope(key) else {
            return;
        };
        if !self.is_evictable_ns(ns) {
            return;
        }
        if let Ok(mut g) = self.policies.try_lock() {
            g.entry(ns.to_vec())
                .or_insert_with(|| crate::s3fifo::S3Fifo::new(capacity))
                .on_admit(key, approx_bytes);
        }
    }

    /// Ask the policy for up to `want_bytes` of the coldest keys and mark them.
    /// Returns how many were marked.
    ///
    /// Driven by a BYTE QUANTITY rather than a target level, because the target
    /// belongs to the disk and the policy only knows about the keys it was told
    /// about. If the policy's view undercounts — a missed admission, a restart
    /// — it simply offers fewer candidates, and the trigger asks again on the
    /// next sample. It cannot ask for something incoherent.
    ///
    /// Bounded by [`MAX_MARKS`]; see there for why this batches rather than
    /// marking everything at once.
    pub fn reclaim(&self, want_bytes: u64) -> usize {
        if self.evictable_count.load(Ordering::Relaxed) == 0 {
            return 0;
        }
        self.reclaim_cycles.fetch_add(1, Ordering::Relaxed);
        self.bytes_requested
            .fetch_add(want_bytes, Ordering::Relaxed);
        let mut chosen: Vec<Vec<u8>> = Vec::new();
        if let Ok(mut g) = self.policies.lock() {
            for policy in g.values_mut() {
                let resident = policy.resident_bytes();
                let target = resident.saturating_sub(want_bytes);
                chosen.extend(policy.reclaim_to(target));
                if chosen.len() >= MAX_MARKS {
                    break;
                }
            }
        }
        let mut marked = 0;
        for k in chosen {
            self.mark(&k);
            marked += 1;
        }
        marked
    }

    /// Set the byte capacity the policies are shaped against, from the disk
    /// sample. Applied to existing policies too, so a resize is not something
    /// only new namespaces hear about.
    pub fn set_capacity_hint(&self, bytes: u64) {
        self.capacity_hint.store(bytes, Ordering::Relaxed);
        if let Ok(mut g) = self.policies.try_lock() {
            for p in g.values_mut() {
                p.set_capacity_bytes(bytes);
            }
        }
    }

    /// May a compaction pass be FORCED on this namespace now?
    ///
    /// Both floors have to clear: enough marked to be worth a full pass, and
    /// long enough since the last one. Refusals are counted separately, because
    /// which floor is binding is the first thing anyone tuning this needs to
    /// know — "always cooling down" and "never enough marks" call for opposite
    /// changes and are indistinguishable from the pass count alone.
    pub fn should_force_pass(&self, ns: &[u8], now_ms: u64) -> bool {
        if self.marked() < MIN_MARKS_TO_FORCE {
            self.forced_skipped_small.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let Ok(mut g) = self.last_forced_ms.lock() else {
            return false;
        };
        let last = g.get(ns).copied().unwrap_or(0);
        if now_ms.saturating_sub(last) < MIN_FORCED_PASS_INTERVAL_MS {
            self.forced_skipped_cooldown.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        g.insert(ns.to_vec(), now_ms);
        self.forced_passes.fetch_add(1, Ordering::Relaxed);
        self.marks_at_last_pass
            .store(self.marked() as u64, Ordering::Relaxed);
        true
    }

    /// Counters, for tuning the two floors above and nothing else.
    ///
    /// The number that matters is MARKS PER FORCED PASS — `marks_at_last_pass`
    /// against `forced_passes`. A full-namespace rewrite costs the same whether
    /// it reclaims a thousand rows or a million, so a low figure here means the
    /// node is paying full price for a fraction of the benefit, which is the
    /// exact failure the floors exist to prevent. It is the first thing to look
    /// at, and the reason these are exported at all.
    ///
    /// The two skip counters say WHICH floor is binding. Cooldown-dominated
    /// means pressure outruns the interval; small-batch-dominated means marks
    /// arrive too slowly to be worth forcing and natural compaction is likely
    /// doing the work already.
    ///
    /// `mark_overflow` against `marked_total` says whether MAX_MARKS is the
    /// constraint, i.e. whether reclaim is having to batch within a cycle.
    pub fn metrics(&self) -> EvictionMetrics {
        EvictionMetrics {
            marked_total: self.marked_total.load(Ordering::Relaxed),
            marked_now: self.marked() as u64,
            dropped: self.dropped(),
            refused: self.refused(),
            mark_overflow: self.mark_overflow(),
            forced_passes: self.forced_passes.load(Ordering::Relaxed),
            forced_skipped_cooldown: self.forced_skipped_cooldown.load(Ordering::Relaxed),
            forced_skipped_small: self.forced_skipped_small.load(Ordering::Relaxed),
            reclaim_cycles: self.reclaim_cycles.load(Ordering::Relaxed),
            bytes_requested: self.bytes_requested.load(Ordering::Relaxed),
            marks_at_last_pass: self.marks_at_last_pass.load(Ordering::Relaxed),
        }
    }

    /// Marks refused for want of room. See [`MAX_MARKS`].
    pub fn mark_overflow(&self) -> u64 {
        self.mark_overflow.load(Ordering::Relaxed)
    }

    /// Namespaces currently declared evictable.
    pub fn evictable_count(&self) -> usize {
        self.evictable_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::encoding::Cf;

    /// `cf | ns_len | ns | slot(2B BE) | user_key`
    fn envelope(cf: Cf, ns: &[u8], user_key: &[u8]) -> Vec<u8> {
        let mut k = vec![cf as u8, ns.len() as u8];
        k.extend_from_slice(ns);
        k.extend_from_slice(&0u16.to_be_bytes());
        k.extend_from_slice(user_key);
        k
    }

    /// THE GUARD, stated as the property that matters: marking is not
    /// sufficient. A policy can be wrong about any key it likes and still not
    /// reach a namespace that never opted in.
    #[test]
    fn a_mark_on_a_non_evictable_namespace_is_refused_and_counted() {
        let ev = EvictionState::default();
        ev.set_evictable(["cache"]);

        let doomed = envelope(Cf::Metadata, b"cache", b"k1");
        let safe = envelope(Cf::Metadata, b"durable", b"k1");
        ev.mark(&doomed);
        ev.mark(&safe);

        // POSITIVE CONTROL: the mechanism works at all. Without this, the
        // refusal below could just mean nothing is ever dropped.
        assert!(ev.should_drop(&doomed), "an evictable marked row must drop");

        // The guard.
        assert!(
            !ev.should_drop(&safe),
            "a marked row in a namespace nobody declared evictable was dropped"
        );
        assert_eq!(ev.dropped(), 1);
        assert_eq!(
            ev.refused(),
            1,
            "the refusal must be COUNTED — a guard that silently absorbs a \
             policy bug leaves it running"
        );
    }

    /// declare -> revoke -> re-declare must NOT resurrect the first policy's
    /// marks. The guard alone would allow it: the namespace is evictable again
    /// by then, so a mark taken minutes earlier under a policy since switched
    /// off would delete a row on the next compaction, with nothing having
    /// asked for it.
    #[test]
    fn re_declaring_does_not_resurrect_marks_from_an_earlier_policy() {
        let ev = EvictionState::default();
        ev.set_evictable(["cache"]);
        let k = envelope(Cf::Metadata, b"cache", b"k1");
        ev.mark(&k);
        assert!(ev.should_drop(&k), "positive control: the mark is armed");

        ev.set_evictable(Vec::<&str>::new());
        ev.set_evictable(["cache"]);
        assert!(
            !ev.should_drop(&k),
            "a mark from a retired policy fired after the namespace was \
             re-declared"
        );
        assert_eq!(ev.marked(), 0, "marks survived a policy change");
    }

    /// Revoking the declaration must disarm marks already taken, not leave
    /// them armed against a namespace that is now durable.
    #[test]
    fn revoking_evictability_disarms_existing_marks() {
        let ev = EvictionState::default();
        ev.set_evictable(["cache"]);
        let k = envelope(Cf::Metadata, b"cache", b"k1");
        ev.mark(&k);
        assert!(ev.should_drop(&k));

        ev.set_evictable(Vec::<&str>::new());
        assert!(
            !ev.should_drop(&k),
            "a mark outlived the declaration that licensed it"
        );
    }

    /// A key the guard cannot attribute is never dropped. Truncated keys, and
    /// keys in a CF that is not namespaced this way, both fail closed.
    #[test]
    fn an_unattributable_key_is_never_dropped() {
        let ev = EvictionState::default();
        ev.set_evictable(["cache"]);

        // Claims a 200-byte namespace it does not contain.
        let truncated = vec![Cf::Metadata as u8, 200, b'c', b'a'];
        // A CF tag that carries no namespace at all.
        let foreign = vec![b'Q', 5, b'c', b'a', b'c', b'h', b'e', 0, 0];
        ev.mark(&truncated);
        ev.mark(&foreign);

        assert!(!ev.should_drop(&truncated));
        assert!(!ev.should_drop(&foreign));
        assert_eq!(ev.refused(), 2);
        assert_eq!(ev.dropped(), 0);
    }

    /// The mark set is BOUNDED, and the bound is the one part of this design
    /// whose cost scales with rows evicted rather than bytes rewritten. Without
    /// it, a large reclaim is a memory-exhaustion bug wearing a capacity
    /// feature's clothes.
    ///
    /// Overflow is counted rather than silent, because "reclaim marked fewer
    /// keys than it chose" is something an operator watching a cache fill needs
    /// to be able to see.
    #[test]
    fn the_mark_set_is_bounded_and_overflow_is_counted() {
        let ev = EvictionState::default();
        ev.set_evictable(["cache"]);
        for i in 0..(MAX_MARKS + 500) {
            ev.mark(&envelope(
                Cf::Metadata,
                b"cache",
                format!("k{i}").as_bytes(),
            ));
        }
        assert_eq!(ev.marked(), MAX_MARKS, "the mark set grew past its bound");
        assert!(
            ev.mark_overflow() >= 500,
            "overflow was not counted: {}",
            ev.mark_overflow()
        );
    }

    /// BATCHING IS STRUCTURAL. `compact_ns` rewrites the surviving rows, so a
    /// forced pass costs the same whether it reclaims a thousand rows or a
    /// million — which makes "force a pass per eviction" close to the most
    /// expensive way to run this feature, and nothing at the call site would
    /// make that obvious. Both floors are enforced here rather than left to
    /// whoever writes the next caller.
    ///
    /// Also asserts WHICH floor refused, because "always cooling down" and
    /// "never enough marks" want opposite tuning and look identical in a pass
    /// count.
    #[test]
    fn a_forced_pass_needs_a_batch_and_a_cooldown() {
        let ev = EvictionState::default();
        ev.set_evictable(["cache"]);
        let ns = b"cache";

        // Too few marks: refused, and attributed to the batch floor.
        ev.mark(&envelope(Cf::Metadata, ns, b"only-one"));
        assert!(!ev.should_force_pass(ns, 1_000_000));
        assert_eq!(ev.metrics().forced_skipped_small, 1);
        assert_eq!(ev.metrics().forced_passes, 0);

        // Enough marks: allowed. POSITIVE CONTROL — without this, the
        // refusals below would also hold for a function that never says yes.
        for i in 0..MIN_MARKS_TO_FORCE {
            ev.mark(&envelope(Cf::Metadata, ns, format!("k{i}").as_bytes()));
        }
        assert!(
            ev.should_force_pass(ns, 1_000_000),
            "a full batch was refused"
        );
        assert_eq!(ev.metrics().forced_passes, 1);
        assert!(ev.metrics().marks_at_last_pass >= MIN_MARKS_TO_FORCE as u64);

        // Immediately again: refused by the cooldown, not the batch floor.
        let before = ev.metrics();
        assert!(
            !ev.should_force_pass(ns, 1_000_001),
            "a second pass was allowed one millisecond later — this is exactly \
             the per-eviction pass the floors exist to prevent"
        );
        assert_eq!(
            ev.metrics().forced_skipped_cooldown,
            before.forced_skipped_cooldown + 1
        );
        assert_eq!(
            ev.metrics().forced_skipped_small,
            before.forced_skipped_small
        );

        // Past the interval: allowed again.
        assert!(ev.should_force_pass(ns, 1_000_000 + MIN_FORCED_PASS_INTERVAL_MS));
        assert_eq!(ev.metrics().forced_passes, 2);

        // A different namespace has its own cooldown; one busy namespace must
        // not starve another.
        ev.set_evictable(["cache", "other"]);
        for i in 0..MIN_MARKS_TO_FORCE {
            ev.mark(&envelope(
                Cf::Metadata,
                b"other",
                format!("o{i}").as_bytes(),
            ));
        }
        assert!(ev.should_force_pass(b"other", 1_000_002));
    }

    /// The request-path hooks must cost nothing, and do nothing, on a durable
    /// deployment. Asserted through behaviour: no policy state appears for a
    /// namespace nobody declared, so nothing can later be offered for eviction
    /// from one.
    #[test]
    fn the_hooks_ignore_undeclared_namespaces() {
        let ev = EvictionState::default();
        ev.set_evictable(["cache"]);
        let durable = envelope(Cf::Metadata, b"durable", b"k1");
        let cached = envelope(Cf::Metadata, b"cache", b"k1");

        ev.set_capacity_hint(10_000);
        ev.note_write(&durable, 1000);
        ev.note_read(&durable);
        // POSITIVE CONTROL: the hooks work at all, so "nothing happened for
        // durable" is not just "nothing happens ever".
        ev.note_write(&cached, 1000);
        assert!(
            ev.reclaim(10_000) > 0,
            "the hooks recorded nothing at all, so this test cannot show that \
             they ignored the durable namespace specifically"
        );
        // Nothing from the undeclared namespace was ever a candidate.
        assert!(
            !ev.should_drop(&durable),
            "a durable key became evictable through the request-path hooks"
        );
    }

    /// With nothing declared, the hooks are a single relaxed load and record
    /// nothing whatsoever — the state a durable deployment is always in.
    #[test]
    fn with_nothing_declared_the_hooks_are_inert() {
        let ev = EvictionState::default();
        assert_eq!(ev.evictable_count(), 0);
        ev.note_write(&envelope(Cf::Metadata, b"anything", b"k"), 999);
        ev.note_read(&envelope(Cf::Metadata, b"anything", b"k"));
        assert_eq!(ev.reclaim(1_000_000), 0);
        assert_eq!(ev.marked(), 0);
    }

    /// The hot path: an unmarked row costs one relaxed load and touches no
    /// lock. Asserted through behaviour rather than instrumentation — nothing
    /// is dropped and nothing is counted, in either direction.
    #[test]
    fn an_unmarked_row_is_kept_and_counts_nothing() {
        let ev = EvictionState::default();
        ev.set_evictable(["cache"]);
        let k = envelope(Cf::Metadata, b"cache", b"never-marked");
        assert!(!ev.should_drop(&k));
        assert_eq!(ev.dropped(), 0);
        assert_eq!(ev.refused(), 0);
        assert_eq!(ev.marked(), 0);
    }
}
