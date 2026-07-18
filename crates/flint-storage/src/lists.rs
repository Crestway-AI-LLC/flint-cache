//! ListStore: Redis lists as a deque of index-addressed subkey rows.
//!
//! The metadata carries head/tail counters (elements at head..tail); the
//! element at index i lives at `subkey_envelope(…, biased_be(i))`, so LPUSH
//! is head-1 and RPUSH is tail — both O(1), exactly the Kvrocks scheme.
//!
//! Interior mutation (LREM/LINSERT) keeps the index space DENSE by
//! collect-and-rewrite: read the elements, edit in memory, lay them back
//! down from the old head. That is O(n) row writes per call — the accepted
//! price for keeping LINDEX/LSET O(1) point lookups. The alternative
//! (tolerating gaps in the index space) was rejected: every
//! index-addressed op would degrade to a scan, taxing the common
//! queue-shaped workload to subsidize the rare interior edit.

use crate::Kv;
use crate::encoding::{
    Cf, ListMeta, MetaHeader, ValueType, VersionGen, bias_index, envelope, subkey_envelope,
};
use crate::strings::{Clock, StoreError};

/// LSET's three-way result: the command answers "ERR no such key" and
/// "ERR index out of range" differently, so the store must say which.
#[derive(Debug, PartialEq, Eq)]
pub enum LsetOutcome {
    NoKey,
    OutOfRange,
    Set,
}

pub struct ListStore<'a> {
    kv: &'a dyn Kv,
    ns: Vec<u8>,
    clock: Clock,
    max_value_bytes: u64,
}

impl<'a> ListStore<'a> {
    pub fn new(kv: &'a dyn Kv, ns: &[u8], clock: Clock) -> Self {
        Self::with_max_value_bytes(kv, ns, clock, crate::DEFAULT_MAX_VALUE_BYTES)
    }

    /// `max_value_bytes` = 0 disables the cap.
    pub fn with_max_value_bytes(kv: &'a dyn Kv, ns: &[u8], clock: Clock, max: u64) -> Self {
        Self {
            kv,
            ns: ns.to_vec(),
            clock,
            max_value_bytes: if max == 0 { u64::MAX } else { max },
        }
    }

    fn meta_key(&self, slot: u16, key: &[u8]) -> Vec<u8> {
        envelope(Cf::Metadata, &self.ns, slot, key)
    }

    fn elem_key(&self, slot: u16, key: &[u8], version: u64, index: i64) -> Vec<u8> {
        subkey_envelope(
            &self.ns,
            slot,
            key,
            version,
            &bias_index(index).to_be_bytes(),
        )
    }

    fn read_meta(&self, slot: u16, key: &[u8]) -> Result<Option<ListMeta>, StoreError> {
        let mk = self.meta_key(slot, key);
        let Some(row) = self.kv.get(&mk) else {
            return Ok(None);
        };
        let Some(header) = MetaHeader::decode(&row) else {
            return Ok(None);
        };
        if header.is_expired((self.clock)()) {
            self.kv.delete(&mk);
            return Ok(None);
        }
        if header.value_type() != Some(ValueType::List) {
            return Err(StoreError::WrongType);
        }
        ListMeta::decode(&row)
            .ok_or(StoreError::WrongType)
            .map(Some)
    }

    fn write_meta(&self, slot: u16, key: &[u8], meta: &ListMeta) {
        if meta.head == meta.tail {
            self.kv.delete(&self.meta_key(slot, key));
        } else {
            let mut m = *meta;
            m.base.size = (meta.tail - meta.head) as u32;
            self.kv.put(&self.meta_key(slot, key), &m.encode());
        }
    }

    /// LPUSH/RPUSH (multi-value): returns the new length.
    pub fn push(
        &self,
        slot: u16,
        key: &[u8],
        values: &[Vec<u8>],
        left: bool,
    ) -> Result<u64, StoreError> {
        let mut meta = match self.read_meta(slot, key)? {
            Some(m) => m,
            None => ListMeta::new(VersionGen::next((self.clock)())),
        };
        // Check max-value-bytes before any write: a violation must leave
        // the list untouched.
        let bytes = meta.base.bytes + values.iter().map(|v| v.len() as u64).sum::<u64>();
        if bytes > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        meta.base.bytes = bytes;
        for v in values {
            let idx = if left {
                meta.head -= 1;
                meta.head
            } else {
                let i = meta.tail;
                meta.tail += 1;
                i
            };
            self.kv
                .put(&self.elem_key(slot, key, meta.base.version, idx), v);
        }
        self.write_meta(slot, key, &meta);
        Ok((meta.tail - meta.head) as u64)
    }

    /// LPOP/RPOP (single).
    pub fn pop(&self, slot: u16, key: &[u8], left: bool) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(mut meta) = self.read_meta(slot, key)? else {
            return Ok(None);
        };
        let idx = if left {
            let i = meta.head;
            meta.head += 1;
            i
        } else {
            meta.tail -= 1;
            meta.tail
        };
        let ek = self.elem_key(slot, key, meta.base.version, idx);
        let val = self.kv.get(&ek);
        self.kv.delete(&ek);
        if let Some(v) = &val {
            meta.base.bytes = meta.base.bytes.saturating_sub(v.len() as u64);
        }
        self.write_meta(slot, key, &meta);
        Ok(val)
    }

    pub fn llen(&self, slot: u16, key: &[u8]) -> Result<u64, StoreError> {
        Ok(self
            .read_meta(slot, key)?
            .map_or(0, |m| (m.tail - m.head) as u64))
    }

    /// LINDEX: rank with negatives from end; None when out of range.
    pub fn lindex(&self, slot: u16, key: &[u8], rank: i64) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(None);
        };
        let len = meta.tail - meta.head;
        let rank = if rank < 0 { len + rank } else { rank };
        if rank < 0 || rank >= len {
            return Ok(None);
        }
        Ok(self
            .kv
            .get(&self.elem_key(slot, key, meta.base.version, meta.head + rank)))
    }

    /// LSET: overwrite the element at `rank`. The caller must distinguish a
    /// missing key from a bad index (different Redis errors), hence the enum.
    pub fn lset(
        &self,
        slot: u16,
        key: &[u8],
        rank: i64,
        value: &[u8],
    ) -> Result<LsetOutcome, StoreError> {
        let Some(mut meta) = self.read_meta(slot, key)? else {
            return Ok(LsetOutcome::NoKey);
        };
        let len = meta.tail - meta.head;
        let rank = if rank < 0 { len + rank } else { rank };
        if rank < 0 || rank >= len {
            return Ok(LsetOutcome::OutOfRange);
        }
        let ek = self.elem_key(slot, key, meta.base.version, meta.head + rank);
        let old = self.kv.get(&ek).map_or(0, |v| v.len() as u64);
        let bytes = meta.base.bytes.saturating_sub(old) + value.len() as u64;
        if bytes > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        meta.base.bytes = bytes;
        self.kv.put(&ek, value);
        self.write_meta(slot, key, &meta);
        Ok(LsetOutcome::Set)
    }

    /// LTRIM: keep `[start, stop]` (Redis inclusive/negative semantics). The
    /// kept run stays where it is and head/tail move inward — trimming is
    /// end-based by construction, so no renumbering. An empty keep-range
    /// deletes the key (write_meta's head==tail rule).
    pub fn ltrim(&self, slot: u16, key: &[u8], start: i64, stop: i64) -> Result<(), StoreError> {
        let Some(mut meta) = self.read_meta(slot, key)? else {
            return Ok(());
        };
        let len = meta.tail - meta.head;
        let norm = |i: i64| if i < 0 { len + i } else { i };
        let from = norm(start).max(0);
        let to = norm(stop).min(len - 1);
        let (new_head, new_tail) = if from > to {
            (meta.head, meta.head)
        } else {
            (meta.head + from, meta.head + to + 1)
        };
        for idx in (meta.head..new_head).chain(new_tail..meta.tail) {
            let ek = self.elem_key(slot, key, meta.base.version, idx);
            if let Some(v) = self.kv.get(&ek) {
                meta.base.bytes = meta.base.bytes.saturating_sub(v.len() as u64);
            }
            self.kv.delete(&ek);
        }
        meta.head = new_head;
        meta.tail = new_tail;
        self.write_meta(slot, key, &meta);
        Ok(())
    }

    /// LPOS: head-based indices of elements equal to `element`, in scan
    /// order. `rank` (caller guarantees non-zero) picks the direction and the
    /// starting match: positive scans head→tail skipping `rank-1` matches,
    /// negative scans tail→head. `count` caps the hits (0 = unlimited);
    /// `maxlen` caps the comparisons (0 = unlimited).
    pub fn lpos(
        &self,
        slot: u16,
        key: &[u8],
        element: &[u8],
        rank: i64,
        count: u64,
        maxlen: u64,
    ) -> Result<Vec<i64>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(Vec::new());
        };
        let len = meta.tail - meta.head;
        let ranks: Box<dyn Iterator<Item = i64>> = if rank > 0 {
            Box::new(0..len)
        } else {
            Box::new((0..len).rev())
        };
        // `maxlen` caps how many elements get COMPARED, not how many match.
        let ranks: Box<dyn Iterator<Item = i64>> = if maxlen != 0 {
            Box::new(ranks.take(maxlen as usize))
        } else {
            ranks
        };
        let mut skip = rank.unsigned_abs().saturating_sub(1);
        let mut out = Vec::new();
        for r in ranks {
            let v = self
                .kv
                .get(&self.elem_key(slot, key, meta.base.version, meta.head + r));
            if v.as_deref() == Some(element) {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                out.push(r);
                if count != 0 && out.len() as u64 >= count {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// All live values head→tail (the collect half of collect-and-rewrite).
    fn collect(&self, slot: u16, key: &[u8], meta: &ListMeta) -> Vec<Vec<u8>> {
        (meta.head..meta.tail)
            .filter_map(|i| self.kv.get(&self.elem_key(slot, key, meta.base.version, i)))
            .collect()
    }

    /// The rewrite half: drop every old row, lay `vals` down densely from
    /// the old head, refresh counters and bytes. Callers mutate `vals` in
    /// memory between collect and rewrite.
    fn rewrite(&self, slot: u16, key: &[u8], meta: &mut ListMeta, vals: Vec<Vec<u8>>) {
        for i in meta.head..meta.tail {
            self.kv
                .delete(&self.elem_key(slot, key, meta.base.version, i));
        }
        meta.base.bytes = vals.iter().map(|v| v.len() as u64).sum();
        meta.tail = meta.head + vals.len() as i64;
        for (off, v) in vals.iter().enumerate() {
            self.kv.put(
                &self.elem_key(slot, key, meta.base.version, meta.head + off as i64),
                v,
            );
        }
        self.write_meta(slot, key, meta);
    }

    /// LREM: remove matches of `element` — `count > 0` removes the first
    /// `count` head→tail, `count < 0` the first `|count|` tail→head, `0`
    /// removes all. Returns how many went.
    pub fn lrem(
        &self,
        slot: u16,
        key: &[u8],
        count: i64,
        element: &[u8],
    ) -> Result<u64, StoreError> {
        let Some(mut meta) = self.read_meta(slot, key)? else {
            return Ok(0);
        };
        let vals = self.collect(slot, key, &meta);
        let limit = if count == 0 {
            u64::MAX
        } else {
            count.unsigned_abs()
        };
        let mut removed = 0u64;
        let mut matches = |v: &Vec<u8>| {
            if removed < limit && v == element {
                removed += 1;
                false
            } else {
                true
            }
        };
        let keep: Vec<Vec<u8>> = if count >= 0 {
            vals.into_iter().filter(|v| matches(v)).collect()
        } else {
            let mut kept: Vec<Vec<u8>> = vals.into_iter().rev().filter(|v| matches(v)).collect();
            kept.reverse();
            kept
        };
        if removed > 0 {
            self.rewrite(slot, key, &mut meta, keep);
        }
        Ok(removed)
    }

    /// LINSERT before/after the FIRST occurrence of `pivot` (head→tail
    /// search, as in Redis). Returns the new length, `-1` when the pivot is
    /// absent, `0` when the key does not exist — the command's exact wire
    /// values.
    pub fn linsert(
        &self,
        slot: u16,
        key: &[u8],
        before: bool,
        pivot: &[u8],
        element: &[u8],
    ) -> Result<i64, StoreError> {
        let Some(mut meta) = self.read_meta(slot, key)? else {
            return Ok(0);
        };
        if meta.base.bytes + element.len() as u64 > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        let mut vals = self.collect(slot, key, &meta);
        let Some(pos) = vals.iter().position(|v| v == pivot) else {
            return Ok(-1);
        };
        vals.insert(if before { pos } else { pos + 1 }, element.to_vec());
        let len = vals.len() as i64;
        self.rewrite(slot, key, &mut meta, vals);
        Ok(len)
    }

    /// LRANGE with Redis index semantics (inclusive, negatives from end).
    pub fn lrange(
        &self,
        slot: u16,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<Vec<u8>>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(Vec::new());
        };
        let len = meta.tail - meta.head;
        let norm = |i: i64| if i < 0 { len + i } else { i };
        let from = norm(start).max(0);
        let to = norm(stop).min(len - 1);
        if from > to {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity((to - from + 1) as usize);
        for rank in from..=to {
            let idx = meta.head + rank;
            if let Some(v) = self
                .kv
                .get(&self.elem_key(slot, key, meta.base.version, idx))
            {
                out.push(v);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;

    fn now() -> u64 {
        1_000_000
    }

    fn vs(v: &[&[u8]]) -> Vec<Vec<u8>> {
        v.iter().map(|m| m.to_vec()).collect()
    }

    #[test]
    fn push_pop_order() {
        let kv = MemKv::new();
        let l = ListStore::new(&kv, b"t", now);
        // RPUSH a b, LPUSH c → c a b
        assert_eq!(l.push(1, b"l", &vs(&[b"a", b"b"]), false), Ok(2));
        assert_eq!(l.push(1, b"l", &vs(&[b"c"]), true), Ok(3));
        assert_eq!(l.lrange(1, b"l", 0, -1), Ok(vs(&[b"c", b"a", b"b"])));
        assert_eq!(l.pop(1, b"l", true), Ok(Some(b"c".to_vec())));
        assert_eq!(l.pop(1, b"l", false), Ok(Some(b"b".to_vec())));
        assert_eq!(l.llen(1, b"l"), Ok(1));
        assert_eq!(l.pop(1, b"l", true), Ok(Some(b"a".to_vec())));
        // Empty list deletes the key.
        assert_eq!(l.llen(1, b"l"), Ok(0));
        assert_eq!(l.pop(1, b"l", true), Ok(None));
    }

    #[test]
    fn lrange_negative_and_clamping() {
        let kv = MemKv::new();
        let l = ListStore::new(&kv, b"t", now);
        l.push(1, b"l", &vs(&[b"a", b"b", b"c", b"d"]), false)
            .expect("push");
        assert_eq!(l.lrange(1, b"l", 1, 2), Ok(vs(&[b"b", b"c"])));
        assert_eq!(l.lrange(1, b"l", -2, -1), Ok(vs(&[b"c", b"d"])));
        assert_eq!(l.lrange(1, b"l", 0, 99), Ok(vs(&[b"a", b"b", b"c", b"d"])));
        assert_eq!(l.lrange(1, b"l", 3, 1), Ok(vec![]));
        assert_eq!(l.lrange(1, b"l", -99, 0), Ok(vs(&[b"a"])));
    }

    #[test]
    fn lset_overwrites_and_distinguishes_errors() {
        let kv = MemKv::new();
        let l = ListStore::new(&kv, b"t", now);
        assert_eq!(l.lset(1, b"nope", 0, b"x"), Ok(LsetOutcome::NoKey));
        l.push(1, b"l", &vs(&[b"a", b"b", b"c"]), false)
            .expect("push");
        assert_eq!(l.lset(1, b"l", 1, b"B"), Ok(LsetOutcome::Set));
        assert_eq!(l.lset(1, b"l", -1, b"C"), Ok(LsetOutcome::Set));
        assert_eq!(l.lset(1, b"l", 3, b"x"), Ok(LsetOutcome::OutOfRange));
        assert_eq!(l.lset(1, b"l", -4, b"x"), Ok(LsetOutcome::OutOfRange));
        assert_eq!(l.lrange(1, b"l", 0, -1), Ok(vs(&[b"a", b"B", b"C"])));
    }

    #[test]
    fn ltrim_keeps_range_and_deletes_empty() {
        let kv = MemKv::new();
        let l = ListStore::new(&kv, b"t", now);
        l.push(1, b"l", &vs(&[b"a", b"b", b"c", b"d", b"e"]), false)
            .expect("push");
        l.ltrim(1, b"l", 1, -2).expect("trim");
        assert_eq!(l.lrange(1, b"l", 0, -1), Ok(vs(&[b"b", b"c", b"d"])));
        // Pushing after a trim keeps working (head/tail moved, no renumber).
        l.push(1, b"l", &vs(&[b"z"]), true).expect("push");
        assert_eq!(l.lrange(1, b"l", 0, -1), Ok(vs(&[b"z", b"b", b"c", b"d"])));
        // Empty keep-range deletes the key entirely.
        l.ltrim(1, b"l", 5, 1).expect("trim");
        assert_eq!(l.llen(1, b"l"), Ok(0));
        // Missing key is a no-op OK.
        assert_eq!(l.ltrim(1, b"nope", 0, -1), Ok(()));
    }

    #[test]
    fn lpos_rank_count_maxlen() {
        let kv = MemKv::new();
        let l = ListStore::new(&kv, b"t", now);
        // a b c a b c a  → "a" at 0, 3, 6
        l.push(
            1,
            b"l",
            &vs(&[b"a", b"b", b"c", b"a", b"b", b"c", b"a"]),
            false,
        )
        .expect("push");
        assert_eq!(l.lpos(1, b"l", b"a", 1, 1, 0), Ok(vec![0]));
        assert_eq!(l.lpos(1, b"l", b"a", 2, 1, 0), Ok(vec![3]));
        assert_eq!(l.lpos(1, b"l", b"a", 1, 0, 0), Ok(vec![0, 3, 6]));
        assert_eq!(l.lpos(1, b"l", b"a", -1, 2, 0), Ok(vec![6, 3]));
        // MAXLEN caps comparisons: only the first 2 elements are inspected.
        assert_eq!(l.lpos(1, b"l", b"a", 1, 0, 2), Ok(vec![0]));
        assert_eq!(l.lpos(1, b"l", b"missing", 1, 1, 0), Ok(vec![]));
        assert_eq!(l.lpos(1, b"nope", b"a", 1, 1, 0), Ok(vec![]));
    }

    #[test]
    fn lrem_directions_and_counts() {
        let kv = MemKv::new();
        let l = ListStore::new(&kv, b"t", now);
        l.push(1, b"l", &vs(&[b"a", b"b", b"a", b"c", b"a"]), false)
            .expect("push");
        assert_eq!(l.lrem(1, b"l", 1, b"a"), Ok(1));
        assert_eq!(l.lrange(1, b"l", 0, -1), Ok(vs(&[b"b", b"a", b"c", b"a"])));
        assert_eq!(l.lrem(1, b"l", -1, b"a"), Ok(1));
        assert_eq!(l.lrange(1, b"l", 0, -1), Ok(vs(&[b"b", b"a", b"c"])));
        assert_eq!(l.lrem(1, b"l", 0, b"a"), Ok(1));
        assert_eq!(l.lrange(1, b"l", 0, -1), Ok(vs(&[b"b", b"c"])));
        assert_eq!(l.lrem(1, b"l", 0, b"zz"), Ok(0));
        assert_eq!(l.lrem(1, b"nope", 0, b"a"), Ok(0));
        // Removing everything deletes the key.
        assert_eq!(l.lrem(1, b"l", 0, b"b"), Ok(1));
        assert_eq!(l.lrem(1, b"l", 0, b"c"), Ok(1));
        assert_eq!(l.llen(1, b"l"), Ok(0));
    }

    #[test]
    fn linsert_before_after_and_misses() {
        let kv = MemKv::new();
        let l = ListStore::new(&kv, b"t", now);
        l.push(1, b"l", &vs(&[b"a", b"c"]), false).expect("push");
        assert_eq!(l.linsert(1, b"l", true, b"c", b"b"), Ok(3));
        assert_eq!(l.linsert(1, b"l", false, b"c", b"d"), Ok(4));
        assert_eq!(l.lrange(1, b"l", 0, -1), Ok(vs(&[b"a", b"b", b"c", b"d"])));
        assert_eq!(l.linsert(1, b"l", true, b"zz", b"x"), Ok(-1));
        assert_eq!(l.linsert(1, b"nope", true, b"a", b"x"), Ok(0));
        // Ends still behave after an interior rewrite.
        l.push(1, b"l", &vs(&[b"e"]), false).expect("push");
        assert_eq!(l.pop(1, b"l", true), Ok(Some(b"a".to_vec())));
        assert_eq!(l.lrange(1, b"l", 0, -1), Ok(vs(&[b"b", b"c", b"d", b"e"])));
    }

    #[test]
    fn lpush_multi_reverses() {
        let kv = MemKv::new();
        let l = ListStore::new(&kv, b"t", now);
        // LPUSH a b c pushes one at a time → c b a
        assert_eq!(l.push(1, b"l", &vs(&[b"a", b"b", b"c"]), true), Ok(3));
        assert_eq!(l.lrange(1, b"l", 0, -1), Ok(vs(&[b"c", b"b", b"a"])));
    }
}
