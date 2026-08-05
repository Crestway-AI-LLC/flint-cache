// SPDX-License-Identifier: Elastic-2.0
//! Modification tracking for WATCH (ADR-0012 D5).
//!
//! WATCH has to answer one question at EXEC time: was this key touched
//! since I watched it? The answer must never be a wrong "no" — a missed
//! modification is a lost update, which is the whole failure WATCH exists
//! to prevent. A wrong "yes" is only a retry.
//!
//! So this is a fixed array of counters, each write bumping the stripe its
//! key hashes to. Memory is constant no matter how many keys exist or how
//! many are watched, and there is no per-write allocation. Two different
//! keys can share a stripe, in which case a write to one aborts a
//! transaction watching the other — a spurious retry, on the safe side of
//! the trade.
//!
//! THE REJECTED ALTERNATIVE was fingerprinting each watched key's row and
//! comparing at EXEC. It reads better until you notice it misses the ABA
//! case — a key changed and changed back is indistinguishable from one
//! never touched — and that miss is exactly a lost update. It also costs a
//! read per watched key at EXEC, where this costs an atomic load.
//!
//! WHAT GETS WATCHED. A key's METADATA row, because every mutation of every
//! type writes it: the string types keep their value there, and the
//! collection types update size/bytes on each change. Lazy expiry and the
//! GC sweeper delete it too, so a key expiring counts as the modification
//! it is. `watch_invariant_holds_for_every_type` in the tests is what keeps
//! that true rather than merely believed.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::Kv;

/// Number of stripes. Large enough that unrelated keys rarely collide,
/// small enough to stay in cache; 32 KiB of counters.
const STRIPES: usize = 4096;

pub struct WatchTable {
    stripes: Vec<AtomicU64>,
}

impl Default for WatchTable {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchTable {
    pub fn new() -> Self {
        Self {
            stripes: (0..STRIPES).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// FNV-1a. Chosen for having no dependency and no per-call state — the
    /// table only needs stripes to be spread, not to resist an adversary,
    /// and a watch never outlives the process that made it.
    fn index(key: &[u8]) -> usize {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in key {
            h ^= *b as u64;
            h = h.wrapping_mul(0x1000_0000_01b3);
        }
        (h as usize) % STRIPES
    }

    /// Record that `key` changed.
    pub fn bump(&self, key: &[u8]) {
        self.stripes[Self::index(key)].fetch_add(1, Ordering::Relaxed);
    }

    /// The current value for `key`'s stripe, to be compared later.
    pub fn version(&self, key: &[u8]) -> u64 {
        self.stripes[Self::index(key)].load(Ordering::Relaxed)
    }

    /// Invalidate EVERY watch — FLUSHALL removed the whole namespace, so no
    /// watched key can be claimed unchanged.
    pub fn bump_all(&self) {
        for s in &self.stripes {
            s.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// A store that reports every mutation to a `WatchTable`.
///
/// Wrapping at the `Kv` layer rather than at the command layer is what
/// makes the coverage total: the GC sweeper's deletes, a read path's lazy
/// expiry, a transaction's commit and the async queue's commit all reach
/// the store through here, and none of them passes the command layer.
pub struct WatchedKv {
    under: Arc<dyn Kv>,
    table: Arc<WatchTable>,
}

impl WatchedKv {
    pub fn new(under: Arc<dyn Kv>, table: Arc<WatchTable>) -> Self {
        Self { under, table }
    }
}

impl Kv for WatchedKv {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.under.get(key)
    }

    fn put(&self, key: &[u8], value: &[u8]) {
        self.table.bump(key);
        self.under.put(key, value);
    }

    fn delete(&self, key: &[u8]) -> bool {
        self.table.bump(key);
        self.under.delete(key)
    }

    fn for_each_prefix(&self, prefix: &[u8], visit: &mut dyn FnMut(&[u8], &[u8]) -> bool) {
        self.under.for_each_prefix(prefix, visit);
    }

    // Forwarded rather than left to the trait defaults: the underlying
    // stores override these with a real seek and a single-lock snapshot,
    // and routing them through the default bodies would silently give that
    // up for every read on the server.
    fn for_each_from(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) {
        self.under.for_each_from(prefix, start_after, visit);
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.under.scan_prefix(prefix)
    }

    fn count_prefix(&self, prefix: &[u8]) -> usize {
        self.under.count_prefix(prefix)
    }

    fn clear(&self) {
        self.table.bump_all();
        self.under.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;
    use crate::encoding::{Cf, envelope};
    use crate::strings::system_clock;

    fn watched() -> (Arc<WatchTable>, Arc<dyn Kv>) {
        let table = Arc::new(WatchTable::new());
        let store: Arc<dyn Kv> = Arc::new(WatchedKv::new(
            Arc::new(MemKv::new()) as Arc<dyn Kv>,
            Arc::clone(&table),
        ));
        (table, store)
    }

    /// The metadata row of a user key — what a watch actually tracks.
    fn meta(ns: &[u8], slot: u16, key: &[u8]) -> Vec<u8> {
        envelope(Cf::Metadata, ns, slot, key)
    }

    #[test]
    fn a_write_moves_only_its_own_stripe() {
        let t = WatchTable::new();
        let before_a = t.version(b"a");
        let before_b = t.version(b"b");
        t.bump(b"a");
        assert_ne!(t.version(b"a"), before_a);
        // Not a guarantee for ALL keys — collisions are allowed and
        // deliberate — but two short distinct keys must not collide, or the
        // table is not spreading at all.
        assert_eq!(t.version(b"b"), before_b);
    }

    #[test]
    fn reads_do_not_move_a_stripe() {
        let (t, s) = watched();
        s.put(b"k", b"v");
        let after_write = t.version(b"k");
        assert_eq!(s.get(b"k").as_deref(), Some(b"v".as_slice()));
        s.scan_prefix(b"k");
        s.count_prefix(b"k");
        s.for_each_prefix(b"k", &mut |_, _| true);
        assert_eq!(
            t.version(b"k"),
            after_write,
            "a read must never invalidate a watch"
        );
    }

    #[test]
    fn a_delete_and_a_flush_both_invalidate() {
        let (t, s) = watched();
        s.put(b"k", b"v");
        let v = t.version(b"k");
        s.delete(b"k");
        assert_ne!(t.version(b"k"), v, "delete is a modification");
        // FLUSHALL removes everything, so no watch can survive it.
        let v = t.version(b"other");
        s.clear();
        assert_ne!(
            t.version(b"other"),
            v,
            "FLUSHALL must invalidate every watch"
        );
    }

    /// EVERY value type must move its key's METADATA stripe when mutated.
    ///
    /// This is the invariant the whole design rests on: a watch tracks one
    /// row, so a mutation that reached only a subkey row would be a missed
    /// modification — a lost update, the one failure mode D5 forbids. The
    /// collection cases are the ones worth having, since their payload
    /// lives in rows the watch does NOT track.
    #[test]
    fn watch_invariant_holds_for_every_type() {
        use crate::hashes::HashStore;
        use crate::json::JsonStore;
        use crate::lists::ListStore;
        use crate::sets::SetStore;
        use crate::strings::{SetOptions, StringStore};
        use crate::zsets::ZSetStore;

        let (t, s) = watched();
        let ns = b"0";
        let slot = 5u16;
        let mut checked = 0;
        let check = |name: &str, key: &[u8], before: u64, t: &WatchTable| {
            assert_ne!(
                t.version(&meta(ns, slot, key)),
                before,
                "{name}: mutation did not move the watched key's stripe"
            );
        };

        let v = t.version(&meta(ns, slot, b"str"));
        StringStore::new(s.as_ref(), ns, system_clock)
            .set(slot, b"str", b"v", SetOptions::default())
            .expect("set");
        check("SET", b"str", v, &t);
        checked += 1;

        let v = t.version(&meta(ns, slot, b"h"));
        HashStore::new(s.as_ref(), ns, system_clock)
            .hset(slot, b"h", &[(b"f".to_vec(), b"v".to_vec())])
            .expect("hset");
        check("HSET", b"h", v, &t);
        checked += 1;

        let v = t.version(&meta(ns, slot, b"set"));
        SetStore::new(s.as_ref(), ns, system_clock)
            .sadd(slot, b"set", &[b"m".to_vec()])
            .expect("sadd");
        check("SADD", b"set", v, &t);
        checked += 1;

        let v = t.version(&meta(ns, slot, b"l"));
        ListStore::new(s.as_ref(), ns, system_clock)
            .push(slot, b"l", &[b"e".to_vec()], false)
            .expect("rpush");
        check("RPUSH", b"l", v, &t);
        checked += 1;

        let v = t.version(&meta(ns, slot, b"z"));
        ZSetStore::new(s.as_ref(), ns, system_clock)
            .zadd(slot, b"z", &[(1.0, b"m".to_vec())])
            .expect("zadd");
        check("ZADD", b"z", v, &t);
        checked += 1;

        let v = t.version(&meta(ns, slot, b"j"));
        JsonStore::new(s.as_ref(), ns, system_clock)
            .set(slot, b"j", b"1")
            .expect("json.set");
        check("JSON.SET", b"j", v, &t);
        checked += 1;

        assert_eq!(checked, 6, "every value type must be covered here");

        // And the second mutation of an EXISTING collection must move it
        // too — the first write creates metadata, which is the easy case.
        let v = t.version(&meta(ns, slot, b"set"));
        SetStore::new(s.as_ref(), ns, system_clock)
            .sadd(slot, b"set", &[b"m2".to_vec()])
            .expect("sadd again");
        check("SADD (existing key)", b"set", v, &t);

        let v = t.version(&meta(ns, slot, b"set"));
        SetStore::new(s.as_ref(), ns, system_clock)
            .srem(slot, b"set", &[b"m2".to_vec()])
            .expect("srem");
        check("SREM", b"set", v, &t);
    }
}
