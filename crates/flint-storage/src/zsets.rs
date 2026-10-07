// SPDX-License-Identifier: Elastic-2.0
//! ZSetStore: sorted sets with the dual-index scheme.
//!
//! Member row (Subkey CF): member → score bytes (f64 LE), for O(1) ZSCORE.
//! Index row (ZScore CF): `…|version|encoded_score|member` → empty, whose
//! lexicographic order IS (score, member) order, so rank queries are a
//! prefix scan. Score updates delete the old index row and write both anew.

use crate::Kv;
use crate::encoding::{
    Cf, ComplexMeta, MetaHeader, ValueType, VersionGen, decode_score, encode_score, envelope,
    subkey_envelope, zscore_envelope, zscore_prefix,
};
use crate::strings::{Clock, StoreError};

/// ZADD's flags (BUG-0215). CH is not here: it changes only the reply.
#[derive(Debug, Default, Clone, Copy)]
pub struct ZaddFlags {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
    pub incr: bool,
}

/// How many rows a sorted-set read can return, for `read_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZsetRows {
    /// A range with no LIMIT: possibly the whole set.
    All,
    /// A LIMIT's count, or a pop's.
    AtMost(u64),
    /// A rank window, negatives from the end.
    Ranks(i64, i64),
}

/// What a `zadd_with` did: members added, members whose score changed, and
/// the last score it set, which is INCR's reply (`None`, a nil, when the
/// flags left the member alone).
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct ZaddOutcome {
    pub added: u64,
    pub updated: u64,
    pub score: Option<f64>,
}

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
    ///
    /// As Redis 8.2 and Valkey 9.1 read one (found with BUG-0215): an empty
    /// number, `""` or a bare `(`, is 0, as C's `strtod` reads it; NaN and
    /// surrounding spaces are malformed. This trimmed spaces and took `nan`
    /// as a bound nothing satisfies.
    pub fn parse(raw: &[u8]) -> Option<ScoreBound> {
        let (inclusive, body) = match raw.first() {
            Some(b'(') => (false, &raw[1..]),
            _ => (true, raw),
        };
        let text = std::str::from_utf8(body).ok()?;
        let value = match text.to_ascii_lowercase().as_str() {
            "" => 0.0,
            "-inf" | "-infinity" => f64::NEG_INFINITY,
            "+inf" | "inf" | "+infinity" | "infinity" => f64::INFINITY,
            other => other.parse::<f64>().ok().filter(|v| !v.is_nan())?,
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
        meta.touch((self.clock)());
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        Ok(added)
    }

    /// `ZADD key [NX|XX] [GT|LT] [INCR] score member ...` (BUG-0215). The
    /// pairs apply in order, as Redis's `zsetAdd` applies them, so a member
    /// named twice sees its first pair's result: `GT 5 a 3 a` leaves 5, and
    /// with `CH` both pairs count. Every outcome is decided before anything
    /// is written, so a refusal (NaN, max-value-bytes) changes nothing.
    ///
    /// A missing key with XX is left missing. The parser has already refused
    /// NX with XX, GT or LT, and GT with LT.
    pub fn zadd_with(
        &self,
        slot: u16,
        key: &[u8],
        pairs: &[(f64, Vec<u8>)],
        flags: ZaddFlags,
    ) -> Result<ZaddOutcome, StoreError> {
        let mut outcome = ZaddOutcome::default();
        let meta = match self.read_meta(slot, key)? {
            Some(m) => m,
            None if flags.xx => return Ok(outcome),
            None => ComplexMeta::new(ValueType::ZSet, VersionGen::next((self.clock)())),
        };
        // Each touched member's score as stored, and as this call leaves it.
        let mut stored: std::collections::HashMap<&[u8], Option<f64>> = Default::default();
        let mut now: std::collections::HashMap<&[u8], f64> = Default::default();
        for (score, member) in pairs {
            let before = *stored.entry(member.as_slice()).or_insert_with(|| {
                self.kv
                    .get(&self.member_key(slot, key, meta.version, member))
                    .map(|b| f64::from_le_bytes(b.try_into().unwrap_or([0; 8])))
            });
            match now.get(member.as_slice()).copied().or(before) {
                Some(current) => {
                    if flags.nx {
                        continue;
                    }
                    let next = if flags.incr { current + score } else { *score };
                    if next.is_nan() {
                        return Err(StoreError::NanScore);
                    }
                    if (flags.lt && next >= current) || (flags.gt && next <= current) {
                        continue;
                    }
                    outcome.score = Some(next);
                    if next != current {
                        outcome.updated += 1;
                        now.insert(member.as_slice(), next);
                    }
                }
                None if flags.xx => {}
                None => {
                    outcome.added += 1;
                    outcome.score = Some(*score);
                    now.insert(member.as_slice(), *score);
                }
            }
        }
        if now.is_empty() {
            return Ok(outcome);
        }
        let mut meta = meta;
        let new_members = now.keys().filter(|m| stored[*m].is_none());
        let bytes = meta.bytes + new_members.map(|m| member_cost(m)).sum::<u64>();
        if bytes > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        for (member, score) in &now {
            if let Some(old) = stored[member] {
                if old == *score {
                    continue;
                }
                self.kv.delete(&zscore_envelope(
                    &self.ns,
                    slot,
                    key,
                    meta.version,
                    old,
                    member,
                ));
            }
            self.kv.put(
                &self.member_key(slot, key, meta.version, member),
                &score.to_le_bytes(),
            );
            self.kv.put(
                &zscore_envelope(&self.ns, slot, key, meta.version, *score, member),
                b"",
            );
        }
        meta.size += outcome.added as u32;
        meta.bytes = bytes;
        meta.touch((self.clock)());
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        Ok(outcome)
    }

    /// The collection's accounted size (`ComplexMeta.bytes`), from ONE cheap
    /// metadata read and without materialising anything. This is the quantity
    /// BUG-0060's admission divides by, so it must stay the SAME number the
    /// `max-value-bytes` accounting maintains.
    ///
    /// For a ZSET that is `member_cost` summed -- each member's length PLUS 8
    /// for its score -- not `field.len() + value.len()`, which is the HASH
    /// accounting and was copied into this comment by hand. The eight bytes
    /// matter at scale: a zset of a million short members carries 8 MB of
    /// score that a member-lengths-only reading would not see. `None` when the
    /// key does not exist.
    pub fn stored_bytes(&self, slot: u16, key: &[u8]) -> Result<Option<u64>, StoreError> {
        Ok(self.read_meta(slot, key)?.map(|m| m.bytes))
    }

    /// Bytes a read returning at most `rows` members (None = all of them)
    /// would build, from ONE metadata read: the input BUG-0060's admission
    /// sizes a sorted-set read by. The mean member cost times the rows, as
    /// `ListStore::range_bytes` estimates; `None` when the key is missing.
    /// Reads stop where they are told to since BUG-0216, so a narrow one is
    /// charged what it returns rather than the set.
    pub fn read_bytes(
        &self,
        slot: u16,
        key: &[u8],
        rows: ZsetRows,
    ) -> Result<Option<u64>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(None);
        };
        let len = u64::from(meta.size);
        if len == 0 {
            return Ok(Some(0));
        }
        let wanted = match rows {
            ZsetRows::All => return Ok(Some(meta.bytes)),
            ZsetRows::AtMost(n) => n.min(len),
            ZsetRows::Ranks(start, stop) => {
                let len = len as i64;
                let norm = |i: i64| if i < 0 { len.saturating_add(i) } else { i };
                let (from, to) = (norm(start).max(0), norm(stop).min(len - 1));
                if from > to {
                    return Ok(Some(0));
                }
                (to - from + 1) as u64
            }
        };
        Ok(Some(wanted.saturating_mul(meta.bytes / len)))
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
            meta.touch((self.clock)());
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
        // `+inf` plus `-inf` (BUG-0212). Stored, a NaN score has no place
        // in (score, member) order and prints as `NaN`.
        if next.is_nan() {
            return Err(StoreError::NanScore);
        }
        self.zadd(slot, key, &[(next, member.to_vec())])?;
        Ok(next)
    }

    pub fn zcard(&self, slot: u16, key: &[u8]) -> Result<u64, StoreError> {
        Ok(self.read_meta(slot, key)?.map_or(0, |m| m.size as u64))
    }

    /// The suffix, after a set's row prefix, that a forward walk starts
    /// strictly after so it begins at the first row scored `from` or more.
    /// Rows encoded one below `from` are visited too and the caller's bound
    /// check skips them: no finite key sorts right below
    /// `prefix || enc(from)` when a member may be any bytes.
    fn after_score(from: f64) -> Vec<u8> {
        encode_score(from)
            .checked_sub(1)
            .map_or_else(Vec::new, |e| e.to_be_bytes().to_vec())
    }

    /// The suffix a reverse walk starts strictly before, so it begins at
    /// the last row scored `from` or less. Exact: every such row sorts below
    /// `prefix || enc(from) + 1`.
    fn before_score(from: f64) -> Vec<u8> {
        encode_score(from)
            .checked_add(1)
            .map_or_else(Vec::new, |e| e.to_be_bytes().to_vec())
    }

    /// Walk a set's rows in score order, descending when `rev`, handing
    /// `visit` each member and score until it returns false. `from` is a
    /// suffix after the row prefix to start strictly after (forward) or
    /// strictly before (reverse); empty starts at that end of the set.
    ///
    /// Every sorted-set read goes through this, and reads only the rows it
    /// is handed (BUG-0216). It replaced `all_ordered`, which built the
    /// whole set before any read answered: ZRANK, ZCOUNT, ZPOPMIN and
    /// `ZRANGE k 0 0` each cost about 80 ms on a 1M-member set, where Redis
    /// answers in 0.12 ms, and each allocated the set outside BUG-0060's
    /// admission. The rows are `prefix || score (8B, order-preserving) ||
    /// member`, so key order is (score, member) order and a seek on a score
    /// lands where the range starts.
    fn walk(
        &self,
        slot: u16,
        key: &[u8],
        rev: bool,
        from: &[u8],
        visit: &mut dyn FnMut(&[u8], f64) -> bool,
    ) -> Result<(), StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(());
        };
        let prefix = zscore_prefix(&self.ns, slot, key, meta.version);
        let start = if from.is_empty() {
            Vec::new()
        } else {
            [prefix.as_slice(), from].concat()
        };
        let mut each = |k: &[u8], _: &[u8]| {
            let rest = &k[prefix.len()..];
            let score = decode_score(u64::from_be_bytes(rest[..8].try_into().unwrap_or([0; 8])));
            visit(&rest[8..], score)
        };
        if rev {
            self.kv.for_each_before(&prefix, &start, &mut each);
        } else {
            self.kv.for_each_from(&prefix, &start, &mut each);
        }
        Ok(())
    }

    /// The rank window `[start, stop]` (negatives from the end), ascending,
    /// or descending when `rev` (ZREVRANGE). Read from whichever end of the
    /// set is nearer the window, so the top or the bottom of a large set
    /// costs what is returned (BUG-0216).
    fn rank_window(
        &self,
        slot: u16,
        key: &[u8],
        start: i64,
        stop: i64,
        rev: bool,
    ) -> Result<Vec<(Vec<u8>, f64)>, StoreError> {
        let len = self.zcard(slot, key)? as i64;
        let norm = |i: i64| if i < 0 { len.saturating_add(i) } else { i };
        let from = norm(start).max(0);
        let to = norm(stop).min(len - 1);
        if from > to {
            return Ok(Vec::new());
        }
        // The window in ascending ranks, and the nearer end to read from.
        let (lo, hi) = if rev {
            (len - 1 - to, len - 1 - from)
        } else {
            (from, to)
        };
        let want = (hi - lo + 1) as usize;
        let from_low = lo <= len - 1 - hi;
        let mut skip = if from_low { lo } else { len - 1 - hi };
        let mut rows = Vec::with_capacity(want.min(1024));
        self.walk(slot, key, !from_low, b"", &mut |m, s| {
            if skip > 0 {
                skip -= 1;
                return true;
            }
            rows.push((m.to_vec(), s));
            rows.len() < want
        })?;
        // Read from the low end, the rows are ascending; from the high end,
        // descending. Turn them to the order asked for.
        if from_low == rev {
            rows.reverse();
        }
        Ok(rows)
    }

    /// ZRANGEBYSCORE / ZREVRANGEBYSCORE: members whose score is in
    /// `[min,max]` (bounds may be exclusive), optionally reversed, then
    /// LIMIT offset/count applied. Ascending output unless `rev`. Seeks to
    /// the near bound and stops at the far one or at `count` (BUG-0216).
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
        // A negative offset answers nothing, as Redis's skip loop does; it
        // was read as 0 here (found with BUG-0215). The type is checked
        // first: another type's key is WRONGTYPE, whatever the LIMIT.
        if self.read_meta(slot, key)?.is_none() || offset < 0 || count == 0 {
            return Ok(Vec::new());
        }
        let limit = usize::try_from(count).unwrap_or(usize::MAX);
        let mut skip = offset;
        let mut hits = Vec::new();
        let (near, far) = if rev { (&max, &min) } else { (&min, &max) };
        let inside_near = |s: f64| {
            if rev {
                near.le_upper(s)
            } else {
                near.ge_lower(s)
            }
        };
        let inside_far = |s: f64| {
            if rev {
                far.ge_lower(s)
            } else {
                far.le_upper(s)
            }
        };
        let from = if rev {
            Self::before_score(max.value)
        } else {
            Self::after_score(min.value)
        };
        self.walk(slot, key, rev, &from, &mut |m, s| {
            if !inside_near(s) {
                return true;
            }
            if !inside_far(s) {
                return false;
            }
            if skip > 0 {
                skip -= 1;
                return true;
            }
            hits.push((m.to_vec(), s));
            hits.len() < limit
        })?;
        Ok(hits)
    }

    /// ZRANGEBYLEX / ZREVRANGEBYLEX: members inside a lexicographic range,
    /// then LIMIT offset/count. Ascending output unless `rev`.
    ///
    /// WHY THIS WALKS AND STOPS INSTEAD OF FILTERING. The obvious body is
    /// a filter over the whole set, and it is subtly not Redis. Redis
    /// seeks the first member past one bound and walks until one falls past
    /// the other, which differs from a filter exactly when scores are NOT
    /// uniform: the index is ordered by (score, member), so with mixed
    /// scores the member sequence is not monotonic and a filter would
    /// collect members that lie beyond the point Redis stops at.
    ///
    /// That case is documented as undefined, which is precisely why it must
    /// not be improvised — the conformance suite compares us against real
    /// Valkey, and "undefined" is only undefined until a corpus case lands
    /// on it. Matching the walk keeps us bit-identical wherever a client
    /// might stray, not merely wherever the docs promise.
    ///
    /// The walk is Redis's listpack walk, step for step (`zzlIsInLexRange`,
    /// then `zzlFirstInLexRange` or `zzlLastInLexRange`, then the walk;
    /// BUG-0215). When every member shares one score, the defined case,
    /// score order is member order and the walk seeks straight to the bound
    /// (BUG-0216); with mixed scores it reads from the end it starts at.
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
        let mut ends: [Option<(Vec<u8>, f64)>; 2] = [None, None];
        for (i, end) in ends.iter_mut().enumerate() {
            self.walk(slot, key, i == 1, b"", &mut |m, s| {
                *end = Some((m.to_vec(), s));
                false
            })?;
        }
        let [Some((first, low)), Some((last, high))] = ends else {
            return Ok(Vec::new());
        };
        // 1. Empty unless the last member reaches the lower bound and the
        //    first is within the upper one.
        if !min.ge_lower(&last) || !max.le_upper(&first) || offset < 0 || count == 0 {
            return Ok(Vec::new());
        }
        // 2. Seek: forward to the first member at or past `min`, backward to
        //    the last at or below `max`. One score means member order, so
        //    the seek can start at the bound itself.
        let from = if encode_score(low) == encode_score(high) {
            Self::lex_seek(low, if rev { max } else { min }, rev)
        } else {
            Vec::new()
        };
        // 3. That member must be within the other bound. Then skip `offset`
        //    members, in range or not, and walk while the far bound holds.
        let limit = usize::try_from(count).unwrap_or(usize::MAX);
        let mut skip = offset;
        let mut started = false;
        let mut hits = Vec::new();
        let inside_near = |m: &[u8]| {
            if rev {
                max.le_upper(m)
            } else {
                min.ge_lower(m)
            }
        };
        let inside_far = |m: &[u8]| {
            if rev {
                min.ge_lower(m)
            } else {
                max.le_upper(m)
            }
        };
        self.walk(slot, key, rev, &from, &mut |m, _| {
            if !started {
                if !inside_near(m) {
                    return true;
                }
                if !inside_far(m) {
                    return false;
                }
                started = true;
            }
            if skip > 0 {
                skip -= 1;
                return true;
            }
            if !inside_far(m) {
                return false;
            }
            hits.push(m.to_vec());
            hits.len() < limit
        })?;
        Ok(hits)
    }

    /// Where a lex walk over a set whose members all score `score` can
    /// start: the suffix to start strictly after (forward, from `bound` as
    /// a lower bound) or strictly before (reverse, as an upper bound).
    /// Forward from `[v` it starts after `v` less its last byte, so a few
    /// members below `v` may be visited and skipped; the rest are exact.
    fn lex_seek(score: f64, bound: &LexBound, rev: bool) -> Vec<u8> {
        let enc = encode_score(score).to_be_bytes();
        let at = |member: &[u8]| [enc.as_slice(), member].concat();
        match (rev, bound) {
            (false, LexBound::Incl(v)) if !v.is_empty() => at(&v[..v.len() - 1]),
            (false, LexBound::Excl(v)) => at(v),
            (false, _) => Self::after_score(score),
            (true, LexBound::Incl(v)) => at(&[v.as_slice(), &[0]].concat()),
            (true, LexBound::Excl(v)) => at(v),
            (true, _) => Self::before_score(score),
        }
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

    /// ZLEXCOUNT: how many members the lex range covers.
    ///
    /// Deliberately the same walk as `zrange_by_lex` rather than a second
    /// filter written to match it. A count that can disagree with the range
    /// it claims to count is worse than no count at all, and the mixed-score
    /// case is exactly where two implementations would drift apart.
    pub fn zlexcount(
        &self,
        slot: u16,
        key: &[u8],
        min: &LexBound,
        max: &LexBound,
    ) -> Result<u64, StoreError> {
        Ok(self.zrange_by_lex(slot, key, min, max, false, 0, -1)?.len() as u64)
    }

    /// ZREMRANGEBYLEX: drop every member the lex range covers, returning how
    /// many went. Emptying the set removes the key, as zrem already does.
    pub fn zremrangebylex(
        &self,
        slot: u16,
        key: &[u8],
        min: &LexBound,
        max: &LexBound,
    ) -> Result<u64, StoreError> {
        let doomed = self.zrange_by_lex(slot, key, min, max, false, 0, -1)?;
        self.zrem(slot, key, &doomed)
    }

    /// ZCOUNT: members with score in `[min,max]`, counted from a seek on
    /// `min` without building them (BUG-0216).
    pub fn zcount(
        &self,
        slot: u16,
        key: &[u8],
        min: ScoreBound,
        max: ScoreBound,
    ) -> Result<u64, StoreError> {
        let mut n = 0u64;
        self.walk(
            slot,
            key,
            false,
            &Self::after_score(min.value),
            &mut |_, s| {
                if !min.ge_lower(s) {
                    return true;
                }
                if !max.le_upper(s) {
                    return false;
                }
                n += 1;
                true
            },
        )?;
        Ok(n)
    }

    /// ZRANK / ZREVRANK: 0-based position of `member` in ascending (rev:
    /// descending) order; None if absent.
    ///
    /// Counted from the end the rank is measured from, without building
    /// anything (BUG-0216), so the top of a leaderboard ranks cheaply. A
    /// rank still costs its position: Redis's skiplist answers in O(log n),
    /// which needs per-row counts this index does not keep.
    pub fn zrank(
        &self,
        slot: u16,
        key: &[u8],
        member: &[u8],
        rev: bool,
    ) -> Result<Option<u64>, StoreError> {
        let Some(score) = self.zscore(slot, key, member)? else {
            return Ok(None);
        };
        let (mut before, mut found) = (0u64, false);
        self.walk(slot, key, rev, b"", &mut |m, s| {
            if s == score && m == member {
                found = true;
                return false;
            }
            before += 1;
            true
        })?;
        Ok(found.then_some(before))
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
    /// low (min) or high (max) end, in pop order, reading only those
    /// (BUG-0216).
    pub fn zpop(
        &self,
        slot: u16,
        key: &[u8],
        count: usize,
        max_end: bool,
    ) -> Result<Vec<(Vec<u8>, f64)>, StoreError> {
        let mut popped = Vec::new();
        if count > 0 {
            self.walk(slot, key, max_end, b"", &mut |m, s| {
                popped.push((m.to_vec(), s));
                popped.len() < count
            })?;
        }
        let members: Vec<Vec<u8>> = popped.iter().map(|(m, _)| m.clone()).collect();
        self.zrem(slot, key, &members)?;
        Ok(popped)
    }

    /// ZREMRANGEBYSCORE: remove members with score in `[min,max]`; count.
    pub fn zremrangebyscore(
        &self,
        slot: u16,
        key: &[u8],
        min: ScoreBound,
        max: ScoreBound,
    ) -> Result<u64, StoreError> {
        let doomed: Vec<Vec<u8>> = self
            .zrange_by_score(slot, key, min, max, false, 0, -1)?
            .into_iter()
            .map(|(m, _)| m)
            .collect();
        self.zrem(slot, key, &doomed)
    }

    /// ZREMRANGEBYRANK: remove members in the `[start,stop]` rank window
    /// (negatives from the end); count.
    pub fn zremrangebyrank(
        &self,
        slot: u16,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<u64, StoreError> {
        let doomed: Vec<Vec<u8>> = self
            .rank_window(slot, key, start, stop, false)?
            .into_iter()
            .map(|(m, _)| m)
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
        self.rank_window(slot, key, start, stop, rev)
    }

    /// ZRANGE by rank, ascending.
    ///
    /// **`0 -1` is bounded by the zset's cardinality, which is bounded by
    /// nothing.** A tenant builds an arbitrarily large zset with ordinary
    /// ZADDs and then asks for all of it, on any of `max-conns` connections
    /// at once; one key is not a bound, which is what BUG-0060's admission
    /// is for.
    pub fn zrange(
        &self,
        slot: u16,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<(Vec<u8>, f64)>, StoreError> {
        self.rank_window(slot, key, start, stop, false)
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
    fn stored_bytes_counts_the_score_with_each_member() {
        // Pins the DENOMINATOR, and specifically the eight bytes of score.
        // Reading a zset's size as members-only would understate it by 8 per
        // member -- 8 MB on a million short members -- and admission divides
        // by this number, so the bound would be that much too generous.
        let kv = MemKv::new();
        let zs = ZSetStore::new(&kv, b"t", now);
        let pairs: Vec<(f64, Vec<u8>)> = (0..40)
            .map(|i| (i as f64, vec![b'm'; 25]))
            .enumerate()
            .map(|(n, (sc, mut m))| {
                m[0] = b'a' + (n % 26) as u8;
                m[1] = b'a' + (n / 26) as u8;
                (sc, m)
            })
            .collect();
        assert_eq!(zs.zadd(1, b"z", &pairs), Ok(40));
        assert_eq!(
            zs.stored_bytes(1, b"z"),
            Ok(Some(40 * (25 + 8))),
            "a zset's accounted size is member_cost summed: length PLUS 8 for the score"
        );
    }

    #[test]
    fn zadd_with_applies_pairs_in_order_and_writes_nothing_it_refuses() {
        // BUG-0215, each against Redis 8.2's answer.
        let kv = MemKv::new();
        let z = ZSetStore::new(&kv, b"t", now);
        let p = |s: f64, m: &str| (s, m.as_bytes().to_vec());
        let gt = ZaddFlags {
            gt: true,
            ..Default::default()
        };
        // The second pair sees the first's result: GT 5 then GT 3 keeps 5.
        let done = z.zadd_with(1, b"z", &[p(5.0, "a"), p(3.0, "a")], gt);
        assert_eq!(done.map(|d| (d.added, d.updated)), Ok((1, 0)));
        assert_eq!(z.zscore(1, b"z", b"a"), Ok(Some(5.0)));
        // Added, then changed: CH would count both.
        let plain = ZaddFlags::default();
        let done = z.zadd_with(1, b"z", &[p(1.0, "b"), p(2.0, "b")], plain);
        assert_eq!(done.map(|d| (d.added, d.updated)), Ok((1, 1)));
        assert_eq!(z.zcard(1, b"z"), Ok(2));
        // XX on a missing key leaves it missing.
        let xx = ZaddFlags {
            xx: true,
            ..Default::default()
        };
        assert_eq!(
            z.zadd_with(1, b"none", &[p(1.0, "a")], xx),
            Ok(ZaddOutcome::default())
        );
        assert_eq!(z.zcard(1, b"none"), Ok(0));
        // INCR to NaN is refused before anything is written.
        let incr = ZaddFlags {
            incr: true,
            ..Default::default()
        };
        assert_eq!(
            z.zadd_with(1, b"z", &[p(f64::INFINITY, "a")], incr)
                .map(|d| d.score),
            Ok(Some(f64::INFINITY))
        );
        assert_eq!(
            z.zadd_with(1, b"z", &[p(f64::NEG_INFINITY, "a")], incr),
            Err(StoreError::NanScore)
        );
        assert_eq!(z.zscore(1, b"z", b"a"), Ok(Some(f64::INFINITY)));
        // NX on an existing member is a no-op: INCR answers nil.
        let nx_incr = ZaddFlags {
            nx: true,
            incr: true,
            ..Default::default()
        };
        assert_eq!(
            z.zadd_with(1, b"z", &[p(1.0, "b")], nx_incr)
                .map(|d| d.score),
            Ok(None)
        );
    }

    #[test]
    fn zincr_by_refuses_a_nan_score_and_changes_nothing() {
        // BUG-0212: +inf plus -inf.
        let kv = MemKv::new();
        let z = ZSetStore::new(&kv, b"t", now);
        assert_eq!(z.zincr_by(1, b"z", f64::INFINITY, b"m"), Ok(f64::INFINITY));
        assert_eq!(
            z.zincr_by(1, b"z", f64::NEG_INFINITY, b"m"),
            Err(StoreError::NanScore)
        );
        assert_eq!(z.zscore(1, b"z", b"m"), Ok(Some(f64::INFINITY)));
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

    /// A store that counts the rows its scans hand out.
    struct CountingKv {
        inner: MemKv,
        rows: std::sync::atomic::AtomicUsize,
    }

    impl CountingKv {
        fn take(&self) -> usize {
            self.rows.swap(0, std::sync::atomic::Ordering::Relaxed)
        }
        fn counted<'a>(
            &'a self,
            visit: &'a mut dyn FnMut(&[u8], &[u8]) -> bool,
        ) -> impl FnMut(&[u8], &[u8]) -> bool + 'a {
            move |k, v| {
                self.rows.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                visit(k, v)
            }
        }
    }

    impl Kv for CountingKv {
        fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
            self.inner.get(key)
        }
        fn put(&self, key: &[u8], value: &[u8]) {
            self.inner.put(key, value)
        }
        fn delete(&self, key: &[u8]) -> bool {
            self.inner.delete(key)
        }
        fn for_each_prefix(&self, prefix: &[u8], visit: &mut dyn FnMut(&[u8], &[u8]) -> bool) {
            self.inner.for_each_prefix(prefix, &mut self.counted(visit))
        }
        fn for_each_from(
            &self,
            prefix: &[u8],
            start_after: &[u8],
            visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
        ) {
            self.inner
                .for_each_from(prefix, start_after, &mut self.counted(visit))
        }
        fn for_each_before(
            &self,
            prefix: &[u8],
            start_before: &[u8],
            visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
        ) {
            self.inner
                .for_each_before(prefix, start_before, &mut self.counted(visit))
        }
        fn clear(&self) {
            self.inner.clear()
        }
    }

    /// BUG-0216: a one-element read on a large set reads a few rows, not the
    /// set. Each of these read all 5,000 before the fix.
    #[test]
    fn one_element_reads_do_not_read_the_whole_set() {
        let kv = CountingKv {
            inner: MemKv::new(),
            rows: Default::default(),
        };
        let z = ZSetStore::new(&kv, b"t", now);
        let pairs: Vec<(f64, Vec<u8>)> = (0..5_000)
            .map(|i| (f64::from(i), format!("m{i:05}").into_bytes()))
            .collect();
        z.zadd(1, b"z", &pairs).expect("zadd");
        kv.take();
        let bound = |raw: &str| ScoreBound::parse(raw.as_bytes()).expect("bound");
        type Read<'a> = (&'a str, Box<dyn Fn() -> usize + 'a>);
        let cases: Vec<Read> = vec![
            (
                "ZRANGE 0 0",
                Box::new(|| z.zrange(1, b"z", 0, 0).expect("zrange").len()),
            ),
            (
                "ZREVRANGE 0 9",
                Box::new(|| z.zrange_rev(1, b"z", 0, 9, true).expect("zrange").len()),
            ),
            (
                "ZRANGE -1 -1",
                Box::new(|| z.zrange(1, b"z", -1, -1).expect("zrange").len()),
            ),
            (
                "ZRANGEBYSCORE 2500 +inf LIMIT 0 1",
                Box::new(|| {
                    z.zrange_by_score(1, b"z", bound("2500"), bound("+inf"), false, 0, 1)
                        .expect("by score")
                        .len()
                }),
            ),
            (
                "ZREVRANGEBYSCORE 2500 -inf LIMIT 0 1",
                Box::new(|| {
                    z.zrange_by_score(1, b"z", bound("-inf"), bound("2500"), true, 0, 1)
                        .expect("by score")
                        .len()
                }),
            ),
            (
                "ZCOUNT 100 (103",
                Box::new(|| {
                    z.zcount(1, b"z", bound("100"), bound("(103"))
                        .expect("zcount") as usize
                }),
            ),
            (
                "ZREVRANK m04998",
                Box::new(|| {
                    z.zrank(1, b"z", b"m04998", true)
                        .expect("zrank")
                        .map_or(9, |r| r as usize)
                }),
            ),
            (
                "ZPOPMIN",
                Box::new(|| z.zpop(1, b"z", 1, false).expect("zpop").len()),
            ),
            (
                "ZPOPMAX 2",
                Box::new(|| z.zpop(1, b"z", 2, true).expect("zpop").len()),
            ),
        ];
        for (name, run) in cases {
            let got = run();
            let rows = kv.take();
            assert!(got > 0, "{name} answered nothing");
            assert!(rows <= 12, "{name} read {rows} rows");
        }
    }

    /// The seeking reads answer exactly what reading the whole set and
    /// filtering it answers, over random sets of mixed and of uniform
    /// scores, including the lex walk's Redis quirks.
    #[test]
    fn seeking_reads_agree_with_a_whole_set_model() {
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut rand = move |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let letters = [
            b"a".as_slice(),
            b"b",
            b"bb",
            b"c",
            b"d",
            b"",
            b"e\xff",
            b"f",
        ];
        let scores = [-1.0, 0.0, 1.0, 1.5, 2.0, f64::INFINITY, f64::NEG_INFINITY];
        let lex_bounds: Vec<LexBound> = [
            "-", "+", "[a", "(a", "[b", "(bb", "[c", "(d", "[", "(", "[e", "[f",
        ]
        .iter()
        .map(|b| LexBound::parse(b.as_bytes()).expect("lex"))
        .collect();
        let score_bounds: Vec<ScoreBound> = ["-inf", "+inf", "0", "(0", "1", "(1.5", "2", "(-1"]
            .iter()
            .map(|b| ScoreBound::parse(b.as_bytes()).expect("score"))
            .collect();
        for round in 0..300 {
            let kv = MemKv::new();
            let z = ZSetStore::new(&kv, b"t", now);
            let uniform = round % 2 == 0;
            for m in letters {
                if rand(3) > 0 {
                    let s = if uniform {
                        0.0
                    } else {
                        scores[rand(scores.len() as u64) as usize]
                    };
                    z.zadd(1, b"z", &[(s, m.to_vec())]).expect("zadd");
                }
            }
            // The model: every row, in (score, member) order.
            let mut all: Vec<(Vec<u8>, f64)> = Vec::new();
            for m in letters {
                if let Ok(Some(s)) = z.zscore(1, b"z", m) {
                    all.push((m.to_vec(), s));
                }
            }
            all.sort_by(|a, b| {
                encode_score(a.1)
                    .cmp(&encode_score(b.1))
                    .then(a.0.cmp(&b.0))
            });
            let n = all.len() as i64;
            for (start, stop) in [(0, 0), (0, -1), (-1, -1), (1, 3), (-3, -2), (2, 1), (-9, 9)] {
                for rev in [false, true] {
                    let mut model = all.clone();
                    if rev {
                        model.reverse();
                    }
                    let norm = |i: i64| if i < 0 { n + i } else { i };
                    let (from, to) = (norm(start).max(0), norm(stop).min(n - 1));
                    let want = if from > to {
                        vec![]
                    } else {
                        model[from as usize..=to as usize].to_vec()
                    };
                    assert_eq!(
                        z.zrange_rev(1, b"z", start, stop, rev),
                        Ok(want),
                        "rank {start} {stop} {rev}"
                    );
                }
            }
            for min in &score_bounds {
                for max in &score_bounds {
                    let inside: Vec<(Vec<u8>, f64)> = all
                        .iter()
                        .filter(|(_, s)| min.ge_lower(*s) && max.le_upper(*s))
                        .cloned()
                        .collect();
                    assert_eq!(z.zcount(1, b"z", *min, *max), Ok(inside.len() as u64));
                    for (offset, count) in [(0, -1), (1, 1), (0, 2), (-1, 2), (5, -1)] {
                        for rev in [false, true] {
                            let mut want = inside.clone();
                            if rev {
                                want.reverse();
                            }
                            let want: Vec<_> = if offset < 0 {
                                vec![]
                            } else {
                                want.into_iter()
                                    .skip(offset as usize)
                                    .take(usize::try_from(count).unwrap_or(usize::MAX))
                                    .collect()
                            };
                            assert_eq!(
                                z.zrange_by_score(1, b"z", *min, *max, rev, offset, count),
                                Ok(want)
                            );
                        }
                    }
                }
            }
            for (i, (m, _)) in all.iter().enumerate() {
                assert_eq!(z.zrank(1, b"z", m, false), Ok(Some(i as u64)));
                assert_eq!(
                    z.zrank(1, b"z", m, true),
                    Ok(Some((all.len() - 1 - i) as u64))
                );
            }
            // Redis's listpack lex walk, on the model.
            let members: Vec<Vec<u8>> = all.iter().map(|(m, _)| m.clone()).collect();
            for min in &lex_bounds {
                for max in &lex_bounds {
                    for (offset, count) in [(0, -1), (1, 1), (0, 2), (2, -1)] {
                        for rev in [false, true] {
                            let want = lex_model(&members, min, max, rev, offset, count);
                            assert_eq!(
                                z.zrange_by_lex(1, b"z", min, max, rev, offset, count),
                                Ok(want),
                                "lex {min:?} {max:?} rev {rev} limit {offset} {count} on {members:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// `zzlIsInLexRange`, `zzlFirstInLexRange`/`zzlLastInLexRange` and the
    /// walk, over a materialised member list: the BUG-0215 implementation.
    fn lex_model(
        ordered: &[Vec<u8>],
        min: &LexBound,
        max: &LexBound,
        rev: bool,
        offset: i64,
        count: i64,
    ) -> Vec<Vec<u8>> {
        let (Some(first), Some(last)) = (ordered.first(), ordered.last()) else {
            return vec![];
        };
        if !min.ge_lower(last) || !max.le_upper(first) || offset < 0 {
            return vec![];
        }
        let limit = usize::try_from(count).unwrap_or(usize::MAX);
        if rev {
            let Some(start) = ordered.iter().rposition(|m| max.le_upper(m)) else {
                return vec![];
            };
            if !min.ge_lower(&ordered[start]) {
                return vec![];
            }
            ordered[..=start]
                .iter()
                .rev()
                .skip(offset as usize)
                .take_while(|m| min.ge_lower(m))
                .take(limit)
                .cloned()
                .collect()
        } else {
            let Some(start) = ordered.iter().position(|m| min.ge_lower(m)) else {
                return vec![];
            };
            if !max.le_upper(&ordered[start]) {
                return vec![];
            }
            ordered[start..]
                .iter()
                .skip(offset as usize)
                .take_while(|m| max.le_upper(m))
                .take(limit)
                .cloned()
                .collect()
        }
    }
}
