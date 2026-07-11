//! ZSetStore: sorted sets with the dual-index scheme.
//!
//! Member row (Subkey CF): member → score bytes (f64 LE), for O(1) ZSCORE.
//! Index row (ZScore CF): `…|version|encoded_score|member` → empty, whose
//! lexicographic order IS (score, member) order, so rank queries are a
//! prefix scan. Score updates delete the old index row and write both anew.

use crate::Kv;
use crate::encoding::{
    Cf, ComplexMeta, MetaHeader, ValueType, VersionGen, decode_score, encode_score, envelope,
    subkey_envelope, subkey_prefix, zscore_envelope, zscore_prefix,
};
use crate::strings::{Clock, StoreError};

pub struct ZSetStore<'a> {
    kv: &'a dyn Kv,
    ns: Vec<u8>,
    clock: Clock,
}

impl<'a> ZSetStore<'a> {
    pub fn new(kv: &'a dyn Kv, ns: &[u8], clock: Clock) -> Self {
        Self {
            kv,
            ns: ns.to_vec(),
            clock,
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
        if header.value_type() != Some(ValueType::ZSet) {
            return Err(StoreError::WrongType);
        }
        ComplexMeta::decode(&row)
            .ok_or(StoreError::WrongType)
            .map(Some)
    }

    fn member_key(&self, slot: u16, key: &[u8], version: u64, member: &[u8]) -> Vec<u8> {
        subkey_envelope(&self.ns, slot, key, version, member)
    }

    /// ZADD (plain): returns count of NEW members.
    pub fn zadd(&self, slot: u16, key: &[u8], pairs: &[(f64, Vec<u8>)]) -> Result<u64, StoreError> {
        let mut meta = match self.read_meta(slot, key)? {
            Some(m) => m,
            None => ComplexMeta::new(ValueType::ZSet, VersionGen::next((self.clock)())),
        };
        let mut added = 0u64;
        for (score, member) in pairs {
            let mk = self.member_key(slot, key, meta.version, member);
            match self.kv.get(&mk) {
                Some(old) => {
                    let old_score = f64::from_le_bytes(old.try_into().unwrap_or([0; 8]));
                    if old_score != *score {
                        self.kv.delete(&zscore_envelope(
                            &self.ns,
                            slot,
                            key,
                            meta.version,
                            old_score,
                            member,
                        ));
                    }
                }
                None => added += 1,
            }
            self.kv.put(&mk, &score.to_le_bytes());
            self.kv.put(
                &zscore_envelope(&self.ns, slot, key, meta.version, *score, member),
                b"",
            );
        }
        meta.size += added as u32;
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        Ok(added)
    }

    pub fn zscore(&self, slot: u16, key: &[u8], member: &[u8]) -> Result<Option<f64>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(None);
        };
        Ok(self
            .kv
            .get(&self.member_key(slot, key, meta.version, member))
            .map(|b| f64::from_le_bytes(b.try_into().unwrap_or([0; 8]))))
    }

    pub fn zrem(&self, slot: u16, key: &[u8], members: &[Vec<u8>]) -> Result<u64, StoreError> {
        let Some(mut meta) = self.read_meta(slot, key)? else {
            return Ok(0);
        };
        let mut removed = 0u64;
        for member in members {
            let mk = self.member_key(slot, key, meta.version, member);
            if let Some(old) = self.kv.get(&mk) {
                let old_score = f64::from_le_bytes(old.try_into().unwrap_or([0; 8]));
                self.kv.delete(&mk);
                self.kv.delete(&zscore_envelope(
                    &self.ns,
                    slot,
                    key,
                    meta.version,
                    old_score,
                    member,
                ));
                removed += 1;
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

    pub fn zcard(&self, slot: u16, key: &[u8]) -> Result<u64, StoreError> {
        Ok(self.read_meta(slot, key)?.map_or(0, |m| m.size as u64))
    }

    /// ZRANGE by rank (inclusive, negatives from end): (member, score) in
    /// (score, member) order.
    pub fn zrange(
        &self,
        slot: u16,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<(Vec<u8>, f64)>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(Vec::new());
        };
        let prefix = zscore_prefix(&self.ns, slot, key, meta.version);
        let all: Vec<(Vec<u8>, f64)> = self
            .kv
            .scan_prefix(&prefix)
            .into_iter()
            .map(|(k, _)| {
                let rest = &k[prefix.len()..];
                let score =
                    decode_score(u64::from_be_bytes(rest[..8].try_into().unwrap_or([0; 8])));
                (rest[8..].to_vec(), score)
            })
            .collect();
        let len = all.len() as i64;
        let norm = |i: i64| if i < 0 { len + i } else { i };
        let from = norm(start).max(0);
        let to = norm(stop).min(len - 1);
        if from > to {
            return Ok(Vec::new());
        }
        Ok(all[from as usize..=to as usize].to_vec())
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
    fn zadd_zscore_zrange_order() {
        let kv = MemKv::new();
        let z = ZSetStore::new(&kv, b"t", now);
        let pairs = vec![
            (2.0, b"b".to_vec()),
            (1.0, b"a".to_vec()),
            (3.0, b"c".to_vec()),
        ];
        assert_eq!(z.zadd(1, b"z", &pairs), Ok(3));
        assert_eq!(z.zscore(1, b"z", b"b"), Ok(Some(2.0)));
        assert_eq!(z.zscore(1, b"z", b"missing"), Ok(None));
        let ranked = z.zrange(1, b"z", 0, -1).expect("zrange");
        let members: Vec<&[u8]> = ranked.iter().map(|(m, _)| m.as_slice()).collect();
        assert_eq!(members, vec![b"a".as_slice(), b"b", b"c"]);
        assert_eq!(
            z.zrange(1, b"z", -1, -1).expect("zrange")[0].0,
            b"c".to_vec()
        );
    }

    #[test]
    fn score_update_reorders_and_does_not_double_count() {
        let kv = MemKv::new();
        let z = ZSetStore::new(&kv, b"t", now);
        z.zadd(1, b"z", &[(1.0, b"a".to_vec()), (2.0, b"b".to_vec())])
            .expect("zadd");
        // Move a above b: update, not add.
        assert_eq!(z.zadd(1, b"z", &[(5.0, b"a".to_vec())]), Ok(0));
        assert_eq!(z.zcard(1, b"z"), Ok(2));
        let ranked = z.zrange(1, b"z", 0, -1).expect("zrange");
        assert_eq!(ranked[0].0, b"b".to_vec());
        assert_eq!(ranked[1], (b"a".to_vec(), 5.0));
    }

    #[test]
    fn zrem_to_empty_removes_key() {
        let kv = MemKv::new();
        let z = ZSetStore::new(&kv, b"t", now);
        z.zadd(1, b"z", &[(1.0, b"a".to_vec())]).expect("zadd");
        assert_eq!(z.zrem(1, b"z", &[b"a".to_vec(), b"x".to_vec()]), Ok(1));
        assert_eq!(z.zcard(1, b"z"), Ok(0));
        assert_eq!(z.zrange(1, b"z", 0, -1), Ok(vec![]));
    }
}
