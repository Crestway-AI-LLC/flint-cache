//! SetStore: Redis sets — members are empty-valued subkey rows.

use crate::Kv;
use crate::encoding::{
    Cf, ComplexMeta, MetaHeader, ValueType, VersionGen, envelope, subkey_envelope, subkey_prefix,
};
use crate::strings::{Clock, StoreError};

pub struct SetStore<'a> {
    kv: &'a dyn Kv,
    ns: Vec<u8>,
    clock: Clock,
    max_value_bytes: u64,
}

impl<'a> SetStore<'a> {
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

    fn read_meta(&self, slot: u16, key: &[u8]) -> Result<Option<ComplexMeta>, StoreError> {
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
        if header.value_type() != Some(ValueType::Set) {
            return Err(StoreError::WrongType);
        }
        ComplexMeta::decode(&row)
            .ok_or(StoreError::WrongType)
            .map(Some)
    }

    pub fn sadd(&self, slot: u16, key: &[u8], members: &[Vec<u8>]) -> Result<u64, StoreError> {
        let mut meta = match self.read_meta(slot, key)? {
            Some(m) => m,
            None => ComplexMeta::new(ValueType::Set, VersionGen::next((self.clock)())),
        };
        // Dedupe within the call, then account before any write: a
        // max-value-bytes violation must leave the set untouched.
        let mut fresh: Vec<&[u8]> = Vec::new();
        {
            let mut seen: std::collections::HashSet<&[u8]> = Default::default();
            for m in members {
                let sk = subkey_envelope(&self.ns, slot, key, meta.version, m);
                if seen.insert(m) && self.kv.get(&sk).is_none() {
                    fresh.push(m);
                }
            }
        }
        let bytes = meta.bytes + fresh.iter().map(|m| m.len() as u64).sum::<u64>();
        if bytes > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        for m in &fresh {
            self.kv
                .put(&subkey_envelope(&self.ns, slot, key, meta.version, m), b"");
        }
        meta.size += fresh.len() as u32;
        meta.bytes = bytes;
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        Ok(fresh.len() as u64)
    }

    pub fn srem(&self, slot: u16, key: &[u8], members: &[Vec<u8>]) -> Result<u64, StoreError> {
        let Some(mut meta) = self.read_meta(slot, key)? else {
            return Ok(0);
        };
        let mut removed = 0u64;
        for m in members {
            if self
                .kv
                .delete(&subkey_envelope(&self.ns, slot, key, meta.version, m))
            {
                removed += 1;
                meta.bytes = meta.bytes.saturating_sub(m.len() as u64);
            }
        }
        meta.size = meta.size.saturating_sub(removed as u32);
        if meta.size == 0 {
            self.kv.delete(&self.meta_key(slot, key));
        } else {
            self.kv.put(&self.meta_key(slot, key), &meta.encode());
        }
        Ok(removed)
    }

    pub fn sismember(&self, slot: u16, key: &[u8], member: &[u8]) -> Result<bool, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(false);
        };
        Ok(self
            .kv
            .get(&subkey_envelope(&self.ns, slot, key, meta.version, member))
            .is_some())
    }

    pub fn smembers(&self, slot: u16, key: &[u8]) -> Result<Vec<Vec<u8>>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(Vec::new());
        };
        let prefix = subkey_prefix(&self.ns, slot, key, meta.version);
        Ok(self
            .kv
            .scan_prefix(&prefix)
            .into_iter()
            .map(|(k, _)| k[prefix.len()..].to_vec())
            .collect())
    }

    pub fn scard(&self, slot: u16, key: &[u8]) -> Result<u64, StoreError> {
        Ok(self.read_meta(slot, key)?.map_or(0, |m| m.size as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;

    fn now() -> u64 {
        1_000_000
    }

    #[test]
    fn sadd_srem_lifecycle() {
        let kv = MemKv::new();
        let s = SetStore::new(&kv, b"t", now);
        let ms = |v: &[&[u8]]| v.iter().map(|m| m.to_vec()).collect::<Vec<_>>();
        assert_eq!(s.sadd(1, b"s", &ms(&[b"a", b"b", b"a"])), Ok(2));
        assert_eq!(s.sadd(1, b"s", &ms(&[b"b", b"c"])), Ok(1));
        assert_eq!(s.scard(1, b"s"), Ok(3));
        assert_eq!(s.sismember(1, b"s", b"a"), Ok(true));
        assert_eq!(s.sismember(1, b"s", b"z"), Ok(false));
        assert_eq!(s.smembers(1, b"s"), Ok(ms(&[b"a", b"b", b"c"])));
        assert_eq!(s.srem(1, b"s", &ms(&[b"a", b"z"])), Ok(1));
        assert_eq!(s.srem(1, b"s", &ms(&[b"b", b"c"])), Ok(2));
        assert_eq!(s.scard(1, b"s"), Ok(0));
        assert_eq!(s.smembers(1, b"s"), Ok(vec![]));
    }
}
