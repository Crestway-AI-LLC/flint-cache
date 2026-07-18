// SPDX-License-Identifier: Elastic-2.0
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

    /// SMISMEMBER: membership for several members in one pass.
    pub fn smismember(
        &self,
        slot: u16,
        key: &[u8],
        members: &[Vec<u8>],
    ) -> Result<Vec<bool>, StoreError> {
        members
            .iter()
            .map(|m| self.sismember(slot, key, m))
            .collect()
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

    /// SPOP: remove and return up to `count` random members. Randomness is
    /// process-local — replication carries the resulting KV deletes, so a
    /// replica never re-rolls and can't diverge.
    pub fn spop(&self, slot: u16, key: &[u8], count: u64) -> Result<Vec<Vec<u8>>, StoreError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut members = self.smembers(slot, key)?;
        if members.is_empty() {
            return Ok(Vec::new());
        }
        let take = (count.min(members.len() as u64)) as usize;
        partial_shuffle(&mut members, take);
        members.truncate(take);
        self.srem(slot, key, &members)?;
        Ok(members)
    }

    /// SRANDMEMBER: `count >= 0` → up to `count` DISTINCT members;
    /// `count < 0` → exactly `|count|` picks WITH repetition (Redis
    /// semantics). Read-only.
    pub fn srandmember(
        &self,
        slot: u16,
        key: &[u8],
        count: i64,
    ) -> Result<Vec<Vec<u8>>, StoreError> {
        let mut members = self.smembers(slot, key)?;
        if members.is_empty() {
            return Ok(Vec::new());
        }
        if count >= 0 {
            let take = (count as usize).min(members.len());
            partial_shuffle(&mut members, take);
            members.truncate(take);
            Ok(members)
        } else {
            Ok((0..count.unsigned_abs())
                .map(|_| members[(rand_u64() as usize) % members.len()].clone())
                .collect())
        }
    }
}

/// Fisher–Yates over the first `take` positions — enough shuffle for a
/// bounded pick, no full-vector cost.
fn partial_shuffle(v: &mut [Vec<u8>], take: usize) {
    for i in 0..take.min(v.len().saturating_sub(1)) {
        let j = i + (rand_u64() as usize) % (v.len() - i);
        v.swap(i, j);
    }
}

/// Non-crypto randomness without a new dependency: std's per-instance
/// hasher seed. Fresh entropy per call; never used for secrets (tokens
/// come from flint-tls's ring CSPRNG).
fn rand_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;

    fn now() -> u64 {
        1_000_000
    }

    fn ms(v: &[&[u8]]) -> Vec<Vec<u8>> {
        v.iter().map(|m| m.to_vec()).collect()
    }

    #[test]
    fn sadd_srem_lifecycle() {
        let kv = MemKv::new();
        let s = SetStore::new(&kv, b"t", now);
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

    #[test]
    fn spop_removes_random_members() {
        let kv = MemKv::new();
        let s = SetStore::new(&kv, b"t", now);
        s.sadd(1, b"s", &ms(&[b"a", b"b", b"c", b"d"]))
            .expect("sadd");
        let popped = s.spop(1, b"s", 3).expect("spop");
        assert_eq!(popped.len(), 3);
        assert_eq!(s.scard(1, b"s"), Ok(1));
        // Popped ∪ remaining reassembles the original set exactly.
        let mut all = popped;
        all.extend(s.smembers(1, b"s").expect("smembers"));
        all.sort();
        assert_eq!(all, ms(&[b"a", b"b", b"c", b"d"]));
        // Over-count pops the rest and deletes the key; empties answer empty.
        assert_eq!(s.spop(1, b"s", 99).expect("spop").len(), 1);
        assert_eq!(s.scard(1, b"s"), Ok(0));
        assert_eq!(s.spop(1, b"s", 1), Ok(vec![]));
        assert_eq!(s.spop(1, b"nope", 1), Ok(vec![]));
    }

    #[test]
    fn srandmember_distinct_and_repeating() {
        let kv = MemKv::new();
        let s = SetStore::new(&kv, b"t", now);
        s.sadd(1, b"s", &ms(&[b"a", b"b", b"c"])).expect("sadd");
        // Positive count: distinct members, clamped to the cardinality.
        let picks = s.srandmember(1, b"s", 2).expect("srand");
        assert_eq!(picks.len(), 2);
        let mut dedup = picks.clone();
        dedup.sort();
        dedup.dedup();
        assert_eq!(dedup.len(), 2);
        assert_eq!(s.srandmember(1, b"s", 99).expect("srand").len(), 3);
        // Negative count: exactly |count| picks, repetition allowed.
        let many = s.srandmember(1, b"s", -10).expect("srand");
        assert_eq!(many.len(), 10);
        assert!(many.iter().all(|m| m == b"a" || m == b"b" || m == b"c"));
        // The set itself is untouched.
        assert_eq!(s.scard(1, b"s"), Ok(3));
        assert_eq!(s.srandmember(1, b"nope", 5), Ok(vec![]));
    }
}
