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

use std::collections::HashSet;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

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
    dropped: AtomicU64,
    refused: AtomicU64,
}

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
            *g = next;
        }
        if changed {
            self.clear_marks();
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
        if let Ok(mut g) = self.marks.write()
            && g.insert(key.to_vec())
        {
            self.marks_len.store(g.len(), Ordering::Relaxed);
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
