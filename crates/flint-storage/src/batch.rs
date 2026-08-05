// SPDX-License-Identifier: Elastic-2.0
//! Write-batching KV overlay (ADR-0005 D4). Wraps the real store; buffers
//! writes and overlays them on reads, so the async-write consumer can run a
//! batch of commands through the normal `Dispatcher` — each computing its
//! exact reply against the accumulating state (an INCR burst on one key sees
//! its own prior increments) — then commit the whole buffer as ONE engine
//! WriteBatch.
//!
//! The overlay covers SCANS as well as point reads (ADR-0012 D4), so a
//! collection command reads back what an earlier command in the same batch
//! wrote. It did not always: while only string writes were batched,
//! `for_each_prefix` delegated straight to the underlying store, and the
//! queue's correctness rested on the command set never containing a scan.
//! That is a restriction the async queue still applies, but it is no longer
//! what keeps the overlay honest — transactions (ADR-0012) run arbitrary
//! same-slot commands through this same buffer, and every collection type
//! reads through a prefix scan.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::Kv;

pub struct BatchingKv<'a> {
    under: &'a dyn Kv,
    // key -> Some(value) (put) | None (delete). Final state per key; the
    // intermediate values were already observed via the read overlay during
    // dispatch, so only the last op per key needs to reach the store.
    buf: Mutex<HashMap<Vec<u8>, Option<Vec<u8>>>>,
}

impl<'a> BatchingKv<'a> {
    pub fn new(under: &'a dyn Kv) -> Self {
        Self {
            under,
            buf: Mutex::new(HashMap::new()),
        }
    }

    /// The buffered mutations, ready for `RocksKv::apply_writes`.
    pub fn into_ops(self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.buf
            .into_inner()
            .unwrap_or_default()
            .into_iter()
            .collect()
    }
}

impl Kv for BatchingKv<'_> {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Ok(buf) = self.buf.lock()
            && let Some(op) = buf.get(key)
        {
            // Overlay hit: Some(v) = written, None = deleted this batch.
            return op.clone();
        }
        self.under.get(key)
    }

    fn put(&self, key: &[u8], value: &[u8]) {
        if let Ok(mut buf) = self.buf.lock() {
            buf.insert(key.to_vec(), Some(value.to_vec()));
        }
    }

    fn delete(&self, key: &[u8]) -> bool {
        let existed = self.get(key).is_some();
        if let Ok(mut buf) = self.buf.lock() {
            buf.insert(key.to_vec(), None);
        }
        existed
    }

    /// Ordered merge of the buffer over the underlying range (ADR-0012 D4).
    ///
    /// `scan_prefix` and `count_prefix` both default to this one method, and
    /// every collection type reads through them — SMEMBERS, HGETALL, LRANGE
    /// and the zset index scan are all prefix scans. Delegating past the
    /// buffer, as this did while only string writes were batched, means a
    /// transaction cannot read its own collection writes: SADD then SMEMBERS
    /// would report the set without the member just added. Wrong, and
    /// wrong quietly.
    ///
    /// TWO PROPERTIES THIS MUST NOT LOSE:
    ///
    ///   1. The underlying range is never materialized. Only the buffered
    ///      rows under this prefix are, and those are bounded by the batch,
    ///      not by the store — the whole point of a disk-first engine is
    ///      that a collection may be far larger than RAM.
    ///   2. The buffer lock is released before the first `visit`. The `Kv`
    ///      contract requires it (a visitor may call back in, and the GC
    ///      sweeper does exactly that), and here it is also a deadlock the
    ///      overlay would inflict on itself, since a visitor that writes
    ///      takes this same lock.
    fn for_each_prefix(&self, prefix: &[u8], visit: &mut dyn FnMut(&[u8], &[u8]) -> bool) {
        let pending: Vec<(Vec<u8>, Option<Vec<u8>>)> = {
            let Ok(buf) = self.buf.lock() else {
                return self.under.for_each_prefix(prefix, visit);
            };
            let mut rows: Vec<(Vec<u8>, Option<Vec<u8>>)> = buf
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            // The underlying store yields ascending keys; the buffer is a
            // HashMap and yields none. Sorting here is what lets the two be
            // merged in one pass instead of collected and re-sorted.
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            rows
        };
        if pending.is_empty() {
            return self.under.for_each_prefix(prefix, visit);
        }

        let mut next = 0usize;
        let mut stopped = false;
        self.under.for_each_prefix(prefix, &mut |k, v| {
            // Buffered keys that sort before this one are inserts the store
            // has never seen; they belong here, not appended at the end.
            while next < pending.len() && pending[next].0.as_slice() < k {
                let (bk, bv) = &pending[next];
                next += 1;
                if let Some(val) = bv
                    && !visit(bk, val)
                {
                    stopped = true;
                    return false;
                }
            }
            // Same key in both: the buffer wins, and a buffered delete
            // means the row is gone as far as this scan is concerned.
            if next < pending.len() && pending[next].0.as_slice() == k {
                let (bk, bv) = &pending[next];
                next += 1;
                if let Some(val) = bv
                    && !visit(bk, val)
                {
                    stopped = true;
                    return false;
                }
                return true;
            }
            if !visit(k, v) {
                stopped = true;
                return false;
            }
            true
        });
        if stopped {
            return;
        }
        // Whatever sorts past the end of the underlying range.
        while next < pending.len() {
            let (bk, bv) = &pending[next];
            next += 1;
            if let Some(val) = bv
                && !visit(bk, val)
            {
                return;
            }
        }
    }

    fn clear(&self) {
        // FLUSHALL is not batchable (never enqueued), so this is unreachable
        // in the queue path; delegate for completeness.
        self.under.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;

    #[test]
    fn overlay_reads_see_buffered_writes() {
        let under = MemKv::new();
        under.put(b"a", b"0");
        let b = BatchingKv::new(&under);
        // Buffered put is visible to a later get (INCR-on-same-key semantics).
        assert_eq!(b.get(b"a").as_deref(), Some(b"0".as_slice()));
        b.put(b"a", b"1");
        assert_eq!(b.get(b"a").as_deref(), Some(b"1".as_slice()));
        b.put(b"a", b"2");
        assert_eq!(b.get(b"a").as_deref(), Some(b"2".as_slice()));
        // Delete hides the underlying value.
        b.put(b"c", b"z");
        assert!(b.delete(b"c"));
        assert_eq!(b.get(b"c"), None);
        // Underlying is untouched until commit.
        assert_eq!(under.get(b"a").as_deref(), Some(b"0".as_slice()));
        // The buffer is the FINAL state per key.
        let ops = b.into_ops();
        let map: std::collections::HashMap<_, _> = ops.into_iter().collect();
        assert_eq!(map.get(b"a".as_slice()), Some(&Some(b"2".to_vec())));
        assert_eq!(map.get(b"c".as_slice()), Some(&None));
    }

    /// Every (key, value) a prefix scan yields, in the order it yielded them.
    fn scan(kv: &dyn Kv, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        kv.for_each_prefix(prefix, &mut |k, v| {
            out.push((k.to_vec(), v.to_vec()));
            true
        });
        out
    }

    fn keys(rows: &[(Vec<u8>, Vec<u8>)]) -> Vec<&[u8]> {
        rows.iter().map(|(k, _)| k.as_slice()).collect()
    }

    #[test]
    fn a_scan_merges_buffered_rows_in_key_order() {
        let under = MemKv::new();
        for k in [b"p:b".as_slice(), b"p:d", b"p:f"] {
            under.put(k, b"under");
        }
        let b = BatchingKv::new(&under);
        // One insert before everything, one between, one past the end —
        // the three positions a naive "append the buffer" would get wrong.
        b.put(b"p:a", b"new");
        b.put(b"p:e", b"new");
        b.put(b"p:z", b"new");
        // An overwrite of an existing row, and a delete of another.
        b.put(b"p:d", b"over");
        b.delete(b"p:f");

        let rows = scan(&b, b"p:");
        assert_eq!(
            keys(&rows),
            vec![b"p:a".as_slice(), b"p:b", b"p:d", b"p:e", b"p:z"],
            "ascending order with inserts placed, delete hidden"
        );
        // The buffer wins where both have the key.
        let map: std::collections::HashMap<_, _> = rows.into_iter().collect();
        assert_eq!(
            map.get(b"p:d".as_slice()).map(|v| v.as_slice()),
            Some(b"over".as_slice())
        );
        assert_eq!(
            map.get(b"p:b".as_slice()).map(|v| v.as_slice()),
            Some(b"under".as_slice())
        );

        // scan_prefix and count_prefix default to for_each_prefix, so they
        // inherit the overlay rather than needing their own.
        assert_eq!(b.count_prefix(b"p:"), 5);
        assert_eq!(b.scan_prefix(b"p:").len(), 5);
        // The underlying store is still untouched until commit.
        assert_eq!(under.count_prefix(b"p:"), 3);
    }

    #[test]
    fn a_scan_ignores_buffered_rows_outside_the_prefix() {
        let under = MemKv::new();
        under.put(b"p:a", b"1");
        let b = BatchingKv::new(&under);
        b.put(b"q:a", b"2");
        b.put(b"p:b", b"3");
        assert_eq!(keys(&scan(&b, b"p:")), vec![b"p:a".as_slice(), b"p:b"]);
        assert_eq!(keys(&scan(&b, b"q:")), vec![b"q:a".as_slice()]);
    }

    #[test]
    fn a_visitor_can_stop_the_merged_scan_early() {
        let under = MemKv::new();
        for k in [b"p:b".as_slice(), b"p:c", b"p:d"] {
            under.put(k, b"u");
        }
        let b = BatchingKv::new(&under);
        b.put(b"p:a", b"buffered");
        b.put(b"p:e", b"buffered");
        // Stopping on the FIRST row proves the early exit is honoured on the
        // buffered branch, which runs before the underlying store is reached.
        let mut seen = Vec::new();
        b.for_each_prefix(b"p:", &mut |k, _| {
            seen.push(k.to_vec());
            false
        });
        assert_eq!(seen, vec![b"p:a".to_vec()]);
        // And stopping midway leaves the tail unvisited, including the
        // buffered row that sorts past the end of the store.
        let mut seen = Vec::new();
        b.for_each_prefix(b"p:", &mut |k, _| {
            seen.push(k.to_vec());
            seen.len() < 3
        });
        assert_eq!(
            seen,
            vec![b"p:a".to_vec(), b"p:b".to_vec(), b"p:c".to_vec()]
        );
    }

    #[test]
    fn a_visitor_that_writes_does_not_deadlock_the_overlay() {
        // The Kv contract lets a visitor call back into the store, and the
        // GC sweeper does. Here the callback takes the same buffer lock the
        // scan reads, so holding it across visit would hang rather than
        // fail — the reason the merge snapshots and releases first.
        let under = MemKv::new();
        under.put(b"p:a", b"1");
        under.put(b"p:b", b"2");
        let b = BatchingKv::new(&under);
        b.put(b"p:c", b"3");
        let mut n = 0;
        b.for_each_prefix(b"p:", &mut |k, _| {
            n += 1;
            b.put(&[b"seen:", k].concat(), b"x");
            true
        });
        assert_eq!(n, 3);
        assert_eq!(b.get(b"seen:p:c").as_deref(), Some(b"x".as_slice()));
    }

    /// The end-to-end property the overlay exists for: a COLLECTION command
    /// reading back what an earlier command in the same batch wrote. This is
    /// what delegating past the buffer got wrong, and it is invisible from
    /// any string-only test.
    #[test]
    fn a_set_reads_back_members_added_in_the_same_batch() {
        use crate::sets::SetStore;
        use crate::strings::system_clock;

        let under = MemKv::new();
        let seeded = SetStore::new(&under, b"0", system_clock);
        seeded.sadd(7, b"k", &[b"one".to_vec()]).expect("seed sadd");

        let b = BatchingKv::new(&under);
        let s = SetStore::new(&b, b"0", system_clock);
        s.sadd(7, b"k", &[b"two".to_vec(), b"three".to_vec()])
            .expect("batched sadd");

        let mut members = s.smembers(7, b"k").expect("smembers");
        members.sort();
        assert_eq!(
            members,
            vec![b"one".to_vec(), b"three".to_vec(), b"two".to_vec()],
            "SMEMBERS must see members SADDed earlier in the same batch"
        );
        assert_eq!(s.scard(7, b"k").expect("scard"), 3);

        // Removing one of them takes effect for the same reason.
        s.srem(7, b"k", &[b"one".to_vec()]).expect("srem");
        let mut members = s.smembers(7, b"k").expect("smembers");
        members.sort();
        assert_eq!(members, vec![b"three".to_vec(), b"two".to_vec()]);

        // And none of it has reached the store yet.
        let outside = SetStore::new(&under, b"0", system_clock);
        assert_eq!(
            outside.smembers(7, b"k").expect("smembers"),
            vec![b"one".to_vec()]
        );
    }
}
