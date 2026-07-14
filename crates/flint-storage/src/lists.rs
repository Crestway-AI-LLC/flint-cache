//! ListStore: Redis lists as a deque of index-addressed subkey rows.
//!
//! The metadata carries head/tail counters (elements at head..tail); the
//! element at index i lives at `subkey_envelope(…, biased_be(i))`, so LPUSH
//! is head-1 and RPUSH is tail — both O(1), exactly the Kvrocks scheme.

use crate::Kv;
use crate::encoding::{
    Cf, ListMeta, MetaHeader, ValueType, VersionGen, bias_index, envelope, subkey_envelope,
};
use crate::strings::{Clock, StoreError};

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
    fn lpush_multi_reverses() {
        let kv = MemKv::new();
        let l = ListStore::new(&kv, b"t", now);
        // LPUSH a b c pushes one at a time → c b a
        assert_eq!(l.push(1, b"l", &vs(&[b"a", b"b", b"c"]), true), Ok(3));
        assert_eq!(l.lrange(1, b"l", 0, -1), Ok(vs(&[b"c", b"b", b"a"])));
    }
}
