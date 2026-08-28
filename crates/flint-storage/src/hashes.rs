// SPDX-License-Identifier: Elastic-2.0
//! HashStore: Redis hashes on versioned subkey rows.
//!
//! Metadata row: `header | version | size`. Each field is its own row at
//! `subkey_envelope(ns, slot, key, version, field)`. Deleting or expiring
//! the hash only touches the metadata row; a recreated hash gets a fresh
//! version, so old rows are unreachable orphans awaiting compaction GC.

use crate::Kv;
use crate::encoding::{
    Cf, ComplexMeta, ValueType, VersionGen, envelope, subkey_envelope, subkey_prefix,
};
use crate::strings::{Clock, StoreError};

/// (field, value) pairs.
pub type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

pub struct HashStore<'a> {
    kv: &'a dyn Kv,
    ns: Vec<u8>,
    clock: Clock,
    max_value_bytes: u64,
}

impl<'a> HashStore<'a> {
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

    /// Live hash metadata; `Ok(None)` if missing/expired, WRONGTYPE if the
    /// key holds another type.
    fn read_meta(&self, slot: u16, key: &[u8]) -> Result<Option<ComplexMeta>, StoreError> {
        let mk = self.meta_key(slot, key);
        let Some(row) = self.kv.get(&mk) else {
            return Ok(None);
        };
        let Some(header) = crate::encoding::MetaHeader::decode(&row) else {
            return Ok(None);
        };
        if header.is_expired((self.clock)()) {
            self.kv.delete(&mk);
            return Ok(None);
        }
        if header.value_type() != Some(ValueType::Hash) {
            return Err(StoreError::WrongType);
        }
        ComplexMeta::decode(&row)
            .ok_or(StoreError::WrongType)
            .map(Some)
    }

    /// HSET: returns the number of NEW fields added.
    pub fn hset(
        &self,
        slot: u16,
        key: &[u8],
        pairs: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<u64, StoreError> {
        let mut meta = match self.read_meta(slot, key)? {
            Some(m) => m,
            None => ComplexMeta::new(ValueType::Hash, VersionGen::next((self.clock)())),
        };
        // Duplicate fields in one call: last write wins (Redis semantics);
        // dedupe first or the size/byte deltas would double-count.
        let mut unique: std::collections::HashMap<&[u8], &[u8]> = Default::default();
        for (field, value) in pairs {
            unique.insert(field, value);
        }
        // Accounting pass before any write: a max-value-bytes violation
        // must leave the hash untouched.
        let mut added = 0u64;
        let mut bytes = meta.bytes;
        for (field, value) in &unique {
            match self
                .kv
                .get(&subkey_envelope(&self.ns, slot, key, meta.version, field))
            {
                Some(old) => bytes = bytes.saturating_sub(old.len() as u64),
                None => {
                    added += 1;
                    bytes += field.len() as u64;
                }
            }
            bytes += value.len() as u64;
        }
        if bytes > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        for (field, value) in &unique {
            self.kv.put(
                &subkey_envelope(&self.ns, slot, key, meta.version, field),
                value,
            );
        }
        meta.size += added as u32;
        meta.bytes = bytes;
        meta.touch((self.clock)());
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        Ok(added)
    }

    /// HSETNX: set the field only if it does not exist. Returns true if set.
    pub fn hsetnx(
        &self,
        slot: u16,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> Result<bool, StoreError> {
        if self.hget(slot, key, field)?.is_some() {
            return Ok(false);
        }
        self.hset(slot, key, &[(field.to_vec(), value.to_vec())])?;
        Ok(true)
    }

    /// HSTRLEN: length of the field's value (0 if the field/key is missing).
    pub fn hstrlen(&self, slot: u16, key: &[u8], field: &[u8]) -> Result<usize, StoreError> {
        Ok(self.hget(slot, key, field)?.map(|v| v.len()).unwrap_or(0))
    }

    pub fn hget(&self, slot: u16, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(None);
        };
        Ok(self
            .kv
            .get(&subkey_envelope(&self.ns, slot, key, meta.version, field)))
    }

    /// HDEL: returns removed count; removing the last field deletes the key.
    pub fn hdel(&self, slot: u16, key: &[u8], fields: &[Vec<u8>]) -> Result<u64, StoreError> {
        let Some(mut meta) = self.read_meta(slot, key)? else {
            return Ok(0);
        };
        let mut removed = 0u64;
        for field in fields {
            let sk = subkey_envelope(&self.ns, slot, key, meta.version, field);
            // Read before delete: the byte accounting needs the freed size.
            if let Some(old) = self.kv.get(&sk) {
                self.kv.delete(&sk);
                removed += 1;
                meta.bytes = meta.bytes.saturating_sub((field.len() + old.len()) as u64);
            }
        }
        meta.size = meta.size.saturating_sub(removed as u32);
        if meta.size == 0 {
            self.kv.delete(&self.meta_key(slot, key));
        } else {
            meta.touch((self.clock)());
            self.kv.put(&self.meta_key(slot, key), &meta.encode());
        }
        Ok(removed)
    }

    /// HINCRBY: field must hold an integer string; creates key/field at 0.
    pub fn hincr_by(
        &self,
        slot: u16,
        key: &[u8],
        field: &[u8],
        delta: i64,
    ) -> Result<i64, StoreError> {
        let current = match self.hget(slot, key, field)? {
            None => 0i64,
            Some(raw) => std::str::from_utf8(&raw)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or(StoreError::NotInteger)?,
        };
        let next = current.checked_add(delta).ok_or(StoreError::Overflow)?;
        self.hset(
            slot,
            key,
            &[(field.to_vec(), next.to_string().into_bytes())],
        )?;
        Ok(next)
    }

    pub fn hlen(&self, slot: u16, key: &[u8]) -> Result<u64, StoreError> {
        Ok(self.read_meta(slot, key)?.map_or(0, |m| m.size as u64))
    }

    pub fn hexists(&self, slot: u16, key: &[u8], field: &[u8]) -> Result<bool, StoreError> {
        Ok(self.hget(slot, key, field)?.is_some())
    }

    pub fn hmget(
        &self,
        slot: u16,
        key: &[u8],
        fields: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, StoreError> {
        let meta = self.read_meta(slot, key)?;
        Ok(fields
            .iter()
            .map(|f| {
                meta.as_ref().and_then(|m| {
                    self.kv
                        .get(&subkey_envelope(&self.ns, slot, key, m.version, f))
                })
            })
            .collect())
    }

    /// All (field, value) pairs in lexicographic field order.
    pub fn hgetall(&self, slot: u16, key: &[u8]) -> Result<Pairs, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(Vec::new());
        };
        let prefix = subkey_prefix(&self.ns, slot, key, meta.version);
        // Smaller win than the SET case, and worth being precise about: the
        // VALUE is copied exactly once either way (`scan_prefix` owns it out
        // of the iterator, and the old map then MOVED it). What streaming the
        // visit removes here is the full-key copies and one whole Vec, not a
        // dataset-sized allocation. The dataset-sized one that remains is the
        // returned `Pairs` itself, which only a streaming reply can bound.
        let mut out = Pairs::new();
        self.kv.for_each_prefix(&prefix, &mut |k, v| {
            out.push((k[prefix.len()..].to_vec(), v.to_vec()));
            true
        });
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;
    use std::sync::atomic::{AtomicU64, Ordering};

    macro_rules! test_clock {
        ($static_name:ident, $fn_name:ident, $initial:expr) => {
            static $static_name: AtomicU64 = AtomicU64::new($initial);
            fn $fn_name() -> u64 {
                $static_name.load(Ordering::Relaxed)
            }
        };
    }

    fn pair(f: &[u8], v: &[u8]) -> (Vec<u8>, Vec<u8>) {
        (f.to_vec(), v.to_vec())
    }

    #[test]
    fn hset_hget_lifecycle() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let h = HashStore::new(&kv, b"t", now);
        assert_eq!(
            h.hset(1, b"h", &[pair(b"a", b"1"), pair(b"b", b"2")]),
            Ok(2)
        );
        assert_eq!(
            h.hset(1, b"h", &[pair(b"a", b"9"), pair(b"c", b"3")]),
            Ok(1)
        );
        assert_eq!(h.hget(1, b"h", b"a"), Ok(Some(b"9".to_vec())));
        assert_eq!(h.hget(1, b"h", b"nope"), Ok(None));
        assert_eq!(h.hlen(1, b"h"), Ok(3));
        assert_eq!(h.hexists(1, b"h", b"b"), Ok(true));
        assert_eq!(
            h.hgetall(1, b"h"),
            Ok(vec![pair(b"a", b"9"), pair(b"b", b"2"), pair(b"c", b"3")])
        );
        assert_eq!(
            h.hmget(1, b"h", &[b"c".to_vec(), b"nope".to_vec()]),
            Ok(vec![Some(b"3".to_vec()), None])
        );
    }

    #[test]
    fn hdel_to_empty_removes_key() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let h = HashStore::new(&kv, b"t", now);
        h.hset(1, b"h", &[pair(b"a", b"1"), pair(b"b", b"2")])
            .expect("hset");
        assert_eq!(h.hdel(1, b"h", &[b"a".to_vec(), b"nope".to_vec()]), Ok(1));
        assert_eq!(h.hlen(1, b"h"), Ok(1));
        assert_eq!(h.hdel(1, b"h", &[b"b".to_vec()]), Ok(1));
        assert_eq!(h.hlen(1, b"h"), Ok(0));
        assert_eq!(h.hget(1, b"h", b"b"), Ok(None));
        // Meta row is gone entirely (TYPE would say none).
        assert!(kv.get(&envelope(Cf::Metadata, b"t", 1, b"h")).is_none());
    }

    #[test]
    fn recreate_after_delete_gets_fresh_version() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let h = HashStore::new(&kv, b"t", now);
        h.hset(1, b"h", &[pair(b"old", b"x")]).expect("hset");
        // O(1) delete: only the meta row.
        kv.delete(&envelope(Cf::Metadata, b"t", 1, b"h"));
        // Old subkey row still physically present (orphan)…
        assert_eq!(h.hlen(1, b"h"), Ok(0));
        h.hset(1, b"h", &[pair(b"new", b"y")]).expect("hset");
        // …but invisible to the recreated hash.
        assert_eq!(h.hget(1, b"h", b"old"), Ok(None));
        assert_eq!(h.hgetall(1, b"h"), Ok(vec![pair(b"new", b"y")]));
    }

    #[test]
    fn wrongtype_against_string() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let h = HashStore::new(&kv, b"t", now);
        let s = crate::strings::StringStore::new(&kv, b"t", now);
        s.set(1, b"str", b"v", crate::strings::SetOptions::default())
            .expect("set");
        assert_eq!(h.hget(1, b"str", b"f"), Err(StoreError::WrongType));
        assert_eq!(
            h.hset(1, b"str", &[pair(b"f", b"v")]),
            Err(StoreError::WrongType)
        );
        assert_eq!(h.hlen(1, b"str"), Err(StoreError::WrongType));
    }
}
