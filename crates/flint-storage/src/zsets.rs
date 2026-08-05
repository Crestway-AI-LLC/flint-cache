// SPDX-License-Identifier: Elastic-2.0
//! ZSetStore: sorted sets with the dual-index scheme.
//!
//! Member row (Subkey CF): member → score bytes (f64 LE), for O(1) ZSCORE.
//! Index row (ZScore CF): `…|version|encoded_score|member` → empty, whose
//! lexicographic order IS (score, member) order, so rank queries are a
//! prefix scan. Score updates delete the old index row and write both anew.

use crate::Kv;
use crate::encoding::{
    Cf, ComplexMeta, MetaHeader, ValueType, VersionGen, decode_score, envelope, subkey_envelope,
    zscore_envelope, zscore_prefix,
};
use crate::strings::{Clock, StoreError};

pub struct ZSetStore<'a> {
    kv: &'a dyn Kv,
    ns: Vec<u8>,
    clock: Clock,
    max_value_bytes: u64,
}

/// What one member contributes to the zset's byte total: the member
/// payload plus its 8-byte score.
/// A ZRANGEBYSCORE / ZCOUNT bound: a value with inclusive/exclusive flag.
/// `-inf`/`+inf` parse to ±infinity (always satisfied on that side).
#[derive(Clone, Copy)]
pub struct ScoreBound {
    pub value: f64,
    pub inclusive: bool,
}

impl ScoreBound {
    /// Parse a Redis score-range token: "5" (inclusive), "(5" (exclusive),
    /// "-inf"/"+inf"/"inf". None = malformed.
    pub fn parse(raw: &[u8]) -> Option<ScoreBound> {
        let (inclusive, body) = match raw.first() {
            Some(b'(') => (false, &raw[1..]),
            _ => (true, raw),
        };
        let text = std::str::from_utf8(body).ok()?.trim();
        let value = match text.to_ascii_lowercase().as_str() {
            "-inf" | "-infinity" => f64::NEG_INFINITY,
            "+inf" | "inf" | "+infinity" | "infinity" => f64::INFINITY,
            other => other.parse::<f64>().ok()?,
        };
        Some(ScoreBound { value, inclusive })
    }

    /// Is `score` above this LOWER bound?
    fn ge_lower(&self, score: f64) -> bool {
        if self.inclusive {
            score >= self.value
        } else {
            score > self.value
        }
    }

    /// Is `score` below this UPPER bound?
    fn le_upper(&self, score: f64) -> bool {
        if self.inclusive {
            score <= self.value
        } else {
            score < self.value
        }
    }
}

/// A ZRANGEBYLEX bound. Lexicographic ranges are only meaningful when every
/// member shares one score — then the index's (score, member) order collapses
/// to plain member order. Redis leaves the mixed-score case undefined; see
/// `zrange_by_lex` for what we do about that.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LexBound {
    /// `-`: below every member.
    NegInf,
    /// `+`: above every member.
    PosInf,
    /// `[value`
    Incl(Vec<u8>),
    /// `(value`
    Excl(Vec<u8>),
}

impl LexBound {
    /// Parse a Redis lex-range token. None = malformed, which the caller
    /// must report rather than coerce: a token with no `[`/`(` prefix is a
    /// mistake worth surfacing, not an implied inclusive bound.
    pub fn parse(raw: &[u8]) -> Option<LexBound> {
        match raw.first()? {
            // `-` and `+` are infinities only when they are the WHOLE token.
            // "(-" is an exclusive bound on the member "-", and "[+x" is an
            // inclusive bound on "+x".
            b'-' if raw.len() == 1 => Some(LexBound::NegInf),
            b'+' if raw.len() == 1 => Some(LexBound::PosInf),
            // An empty body is legal: `[` is the inclusive empty string,
            // which sorts below every non-empty member.
            b'[' => Some(LexBound::Incl(raw[1..].to_vec())),
            b'(' => Some(LexBound::Excl(raw[1..].to_vec())),
            _ => None,
        }
    }

    /// Is `member` at or above this bound read as the range's LOWER end?
    fn ge_lower(&self, member: &[u8]) -> bool {
        match self {
            LexBound::NegInf => true,
            LexBound::PosInf => false,
            LexBound::Incl(v) => member >= v.as_slice(),
            LexBound::Excl(v) => member > v.as_slice(),
        }
    }

    /// Is `member` at or below this bound read as the range's UPPER end?
    fn le_upper(&self, member: &[u8]) -> bool {
        match self {
            LexBound::NegInf => false,
            LexBound::PosInf => true,
            LexBound::Incl(v) => member <= v.as_slice(),
            LexBound::Excl(v) => member < v.as_slice(),
        }
    }
}

fn member_cost(member: &[u8]) -> u64 {
    member.len() as u64 + 8
}

impl<'a> ZSetStore<'a> {
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
        // Duplicate members in one call: last score wins; dedupe before
        // accounting. Score updates leave the byte total unchanged, so
        // only genuinely-new members count toward max-value-bytes — and
        // the check happens before any write.
        let mut unique: std::collections::HashMap<&[u8], f64> = Default::default();
        for (score, member) in pairs {
            unique.insert(member, *score);
        }
        let mut added = 0u64;
        let mut bytes = meta.bytes;
        for member in unique.keys() {
            if self
                .kv
                .get(&self.member_key(slot, key, meta.version, member))
                .is_none()
            {
                added += 1;
                bytes += member_cost(member);
            }
        }
        if bytes > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        for (member, score) in &unique {
            let mk = self.member_key(slot, key, meta.version, member);
            if let Some(old) = self.kv.get(&mk) {
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
            self.kv.put(&mk, &score.to_le_bytes());
            self.kv.put(
                &zscore_envelope(&self.ns, slot, key, meta.version, *score, member),
                b"",
            );
        }
        meta.size += added as u32;
        meta.bytes = bytes;
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
                meta.bytes = meta.bytes.saturating_sub(member_cost(member));
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

    /// ZINCRBY: returns the new score; missing member starts at 0.
    pub fn zincr_by(
        &self,
        slot: u16,
        key: &[u8],
        delta: f64,
        member: &[u8],
    ) -> Result<f64, StoreError> {
        let current = self.zscore(slot, key, member)?.unwrap_or(0.0);
        let next = current + delta;
        self.zadd(slot, key, &[(next, member.to_vec())])?;
        Ok(next)
    }

    pub fn zcard(&self, slot: u16, key: &[u8]) -> Result<u64, StoreError> {
        Ok(self.read_meta(slot, key)?.map_or(0, |m| m.size as u64))
    }

    /// ZRANGE by rank (inclusive, negatives from end): (member, score) in
    /// (score, member) order.
    /// All (member, score) in ascending (score, member) order — the ZScore
    /// CF is already ordered that way, so this is a single prefix scan. A
    /// zset lives in ONE key, so this is bounded by the zset's cardinality.
    fn all_ordered(&self, slot: u16, key: &[u8]) -> Result<Vec<(Vec<u8>, f64)>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(Vec::new());
        };
        let prefix = zscore_prefix(&self.ns, slot, key, meta.version);
        Ok(self
            .kv
            .scan_prefix(&prefix)
            .into_iter()
            .map(|(k, _)| {
                let rest = &k[prefix.len()..];
                let score =
                    decode_score(u64::from_be_bytes(rest[..8].try_into().unwrap_or([0; 8])));
                (rest[8..].to_vec(), score)
            })
            .collect())
    }

    /// ZRANGEBYSCORE / ZREVRANGEBYSCORE: members whose score is in
    /// [min,max] (bounds may be exclusive), optionally reversed, then
    /// LIMIT offset/count applied. Ascending output unless `rev`.
    #[allow(clippy::too_many_arguments)]
    pub fn zrange_by_score(
        &self,
        slot: u16,
        key: &[u8],
        min: ScoreBound,
        max: ScoreBound,
        rev: bool,
        offset: i64,
        count: i64,
    ) -> Result<Vec<(Vec<u8>, f64)>, StoreError> {
        let mut hits: Vec<(Vec<u8>, f64)> = self
            .all_ordered(slot, key)?
            .into_iter()
            .filter(|(_, sc)| min.ge_lower(*sc) && max.le_upper(*sc))
            .collect();
        if rev {
            hits.reverse();
        }
        // LIMIT offset count (count < 0 = to the end); no LIMIT => all.
        if offset > 0 {
            let off = (offset as usize).min(hits.len());
            hits.drain(..off);
        }
        if count >= 0 {
            hits.truncate(count as usize);
        }
        Ok(hits)
    }

    /// ZRANGEBYLEX / ZREVRANGEBYLEX: members inside a lexicographic range,
    /// then LIMIT offset/count. Ascending output unless `rev`.
    ///
    /// WHY THIS WALKS AND STOPS INSTEAD OF FILTERING. The obvious body is
    /// `all_ordered().filter(in range)`, and it is subtly not Redis. Redis
    /// seeks to the first member in range and walks until one falls out,
    /// which differs from a filter exactly when scores are NOT uniform:
    /// the index is ordered by (score, member), so with mixed scores the
    /// member sequence is not monotonic and a filter would collect members
    /// that lie beyond the point Redis stops at.
    ///
    /// That case is documented as undefined, which is precisely why it must
    /// not be improvised — the conformance suite compares us against real
    /// Valkey, and "undefined" is only undefined until a corpus case lands
    /// on it. Matching the walk keeps us bit-identical wherever a client
    /// might stray, not merely wherever the docs promise.
    #[allow(clippy::too_many_arguments)]
    pub fn zrange_by_lex(
        &self,
        slot: u16,
        key: &[u8],
        min: &LexBound,
        max: &LexBound,
        rev: bool,
        offset: i64,
        count: i64,
    ) -> Result<Vec<Vec<u8>>, StoreError> {
        let mut ordered: Vec<Vec<u8>> = self
            .all_ordered(slot, key)?
            .into_iter()
            .map(|(m, _)| m)
            .collect();
        if rev {
            ordered.reverse();
        }
        // Seek to the first member inside the range, then walk until one
        // falls out. "Inside" needs no sense of direction, so reversing the
        // sequence is the ONLY thing `rev` changes — the reversed form seeks
        // the last member in range and walks back, which is what this is
        // once the order is flipped.
        let in_range = |m: &[u8]| min.ge_lower(m) && max.le_upper(m);
        let mut hits: Vec<Vec<u8>> = ordered
            .into_iter()
            .skip_while(|m| !in_range(m))
            .take_while(|m| in_range(m))
            .collect();
        if offset > 0 {
            let off = (offset as usize).min(hits.len());
            hits.drain(..off);
        }
        if count >= 0 {
            hits.truncate(count as usize);
        }
        Ok(hits)
    }

    /// The destination side of ZUNIONSTORE / ZINTERSTORE: replace `key`
    /// wholesale with `pairs`, returning the resulting cardinality.
    ///
    /// The old key goes unconditionally, whatever its type — the
    /// destination is overwritten rather than merged, and it need not have
    /// been a sorted set at all. Dropping only the metadata row is the same
    /// O(1) retirement DEL performs; the displaced rows are orphans under a
    /// version no live metadata claims.
    ///
    /// An EMPTY result removes the key instead of leaving an empty sorted
    /// set behind. Redis has no empty collections, and a destination left
    /// as one would answer EXISTS 1 and TYPE zset for something with no
    /// members — a difference a client would see immediately.
    pub fn zreplace(
        &self,
        slot: u16,
        key: &[u8],
        pairs: &[(f64, Vec<u8>)],
    ) -> Result<u64, StoreError> {
        self.kv.delete(&self.meta_key(slot, key));
        if pairs.is_empty() {
            return Ok(0);
        }
        self.zadd(slot, key, pairs)?;
        // Read the cardinality back rather than trusting `pairs.len()`:
        // zadd folds duplicate members, so the two agree only while the
        // caller's input is distinct. This is O(1) — it is a metadata field.
        self.zcard(slot, key)
    }

    /// ZCOUNT: members with score in [min,max].
    pub fn zcount(
        &self,
        slot: u16,
        key: &[u8],
        min: ScoreBound,
        max: ScoreBound,
    ) -> Result<u64, StoreError> {
        Ok(self
            .all_ordered(slot, key)?
            .into_iter()
            .filter(|(_, sc)| min.ge_lower(*sc) && max.le_upper(*sc))
            .count() as u64)
    }

    /// ZRANK / ZREVRANK: 0-based position of `member` in ascending (rev:
    /// descending) order; None if absent.
    pub fn zrank(
        &self,
        slot: u16,
        key: &[u8],
        member: &[u8],
        rev: bool,
    ) -> Result<Option<u64>, StoreError> {
        let all = self.all_ordered(slot, key)?;
        let Some(idx) = all.iter().position(|(m, _)| m.as_slice() == member) else {
            return Ok(None);
        };
        Ok(Some(if rev {
            (all.len() - 1 - idx) as u64
        } else {
            idx as u64
        }))
    }

    /// ZMSCORE: score of each member (None per missing).
    pub fn zmscore(
        &self,
        slot: u16,
        key: &[u8],
        members: &[Vec<u8>],
    ) -> Result<Vec<Option<f64>>, StoreError> {
        members.iter().map(|m| self.zscore(slot, key, m)).collect()
    }

    /// ZPOPMIN / ZPOPMAX: remove and return up to `count` members from the
    /// low (min) or high (max) end, in pop order.
    pub fn zpop(
        &self,
        slot: u16,
        key: &[u8],
        count: usize,
        max_end: bool,
    ) -> Result<Vec<(Vec<u8>, f64)>, StoreError> {
        let mut all = self.all_ordered(slot, key)?;
        if max_end {
            all.reverse();
        }
        let popped: Vec<(Vec<u8>, f64)> = all.into_iter().take(count).collect();
        let members: Vec<Vec<u8>> = popped.iter().map(|(m, _)| m.clone()).collect();
        self.zrem(slot, key, &members)?;
        Ok(popped)
    }

    /// ZREMRANGEBYSCORE: remove members with score in [min,max]; count.
    pub fn zremrangebyscore(
        &self,
        slot: u16,
        key: &[u8],
        min: ScoreBound,
        max: ScoreBound,
    ) -> Result<u64, StoreError> {
        let doomed: Vec<Vec<u8>> = self
            .all_ordered(slot, key)?
            .into_iter()
            .filter(|(_, sc)| min.ge_lower(*sc) && max.le_upper(*sc))
            .map(|(m, _)| m)
            .collect();
        self.zrem(slot, key, &doomed)
    }

    /// ZREMRANGEBYRANK: remove members in the [start,stop] rank window
    /// (negatives from the end); count.
    pub fn zremrangebyrank(
        &self,
        slot: u16,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<u64, StoreError> {
        let all = self.all_ordered(slot, key)?;
        let len = all.len() as i64;
        let norm = |i: i64| if i < 0 { len + i } else { i };
        let from = norm(start).max(0);
        let to = norm(stop).min(len - 1);
        if from > to || all.is_empty() {
            return Ok(0);
        }
        let doomed: Vec<Vec<u8>> = all[from as usize..=to as usize]
            .iter()
            .map(|(m, _)| m.clone())
            .collect();
        self.zrem(slot, key, &doomed)
    }

    /// ZRANGE by rank (inclusive, negatives from end), optionally reversed
    /// (ZREVRANGE): (member, score) in (score, member) order.
    pub fn zrange_rev(
        &self,
        slot: u16,
        key: &[u8],
        start: i64,
        stop: i64,
        rev: bool,
    ) -> Result<Vec<(Vec<u8>, f64)>, StoreError> {
        let mut all = self.all_ordered(slot, key)?;
        if rev {
            all.reverse();
        }
        let len = all.len() as i64;
        let norm = |i: i64| if i < 0 { len + i } else { i };
        let from = norm(start).max(0);
        let to = norm(stop).min(len - 1);
        if from > to {
            return Ok(Vec::new());
        }
        Ok(all[from as usize..=to as usize].to_vec())
    }

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
