// SPDX-License-Identifier: Elastic-2.0
//! StringStore: the string-type TypeStore over any `Kv`.
//!
//! Owns Redis string semantics: SET options, the integer-string commands,
//! APPEND/STRLEN. Type-agnostic keyspace ops (DEL, EXISTS, TYPE, TTL…)
//! live in `keyspace`. The clock is injected so expiry is testable without
//! sleeping and the replicated apply path stays deterministic (expire-at
//! replicates; wall clocks don't).

use crate::Kv;
use crate::encoding::{
    Cf, ComplexMeta, MetaHeader, StringMeta, ValueType, VersionGen, envelope, subkey_envelope,
    subkey_prefix,
};

pub type Clock = fn() -> u64;

pub fn system_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Outcome of a SET with options.
#[derive(Debug, PartialEq, Eq)]
pub enum SetOutcome {
    Done,
    /// NX/XX condition failed.
    Unchanged,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SetExpiry {
    /// Clear any existing TTL (Redis SET default).
    #[default]
    Clear,
    /// Keep the existing TTL (KEEPTTL).
    Keep,
    /// Set absolute expiry at this unix-ms instant (EX/PX/EXAT/PXAT).
    AtMs(u64),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SetOptions {
    pub nx: bool,
    pub xx: bool,
    pub expiry: SetExpiry,
}

/// An integer as Redis reads one, in an argument or a stored value: an
/// optional `-`, then digits with no leading zero, or `0` alone (Redis's
/// `string2ll`). Rust's `parse` also takes `+1` and `01`, which Redis
/// refuses (BUG-0213): `INCR` on a value of `01` is an error there.
pub fn parse_redis_i64(raw: &[u8]) -> Option<i64> {
    let digits = raw.strip_prefix(b"-").unwrap_or(raw);
    let canonical = match digits {
        [b'0'] => raw.len() == 1,
        [b'1'..=b'9', rest @ ..] => rest.iter().all(u8::is_ascii_digit),
        _ => false,
    };
    if !canonical {
        return None;
    }
    std::str::from_utf8(raw).ok()?.parse().ok()
}

#[derive(Debug, PartialEq, Eq)]
pub enum StoreError {
    NotInteger,
    Overflow,
    NotFloat,
    /// A float op's result left the representable range (Redis refuses to
    /// store NaN/Infinity from INCRBYFLOAT).
    NanOrInfinity,
    /// A sorted-set score would be NaN: `ZINCRBY` adding `-inf` to `+inf`
    /// (BUG-0212). Redis refuses it and changes nothing.
    NanScore,
    WrongType,
    /// The write would grow the value past the max-value-bytes policy
    /// (Valkey's `checkStringLength` analog, extended to collections).
    /// Enforced atomically: the store is untouched when this returns.
    ValueTooLarge,
    /// BF.RESERVE on a key that already holds a filter. Its parameters are
    /// fixed at creation, so the alternatives are to ignore the new ones or
    /// to discard the data — both silent, both wrong.
    KeyExists,
    /// A parameter the store cannot honour: a capacity of zero, an error
    /// rate outside (0, 1).
    BadParameter,
    /// A non-scaling filter is full, or a scaling one hit the chain cap
    /// (ADR-0016 D5). Refusing keeps the promised error rate true.
    FilterFull,
    /// XADD with an ID at or below the stream's last (ADR-0052 D6).
    StreamIdTooSmall,
    /// XADD with the ID 0-0, which no entry may have.
    StreamIdZero,
    /// XADD with `*` on a stream whose last ID is the largest there is.
    StreamExhausted,
    /// A HyperLogLog command on a string that is not one (`hll.rs`).
    NotHll,
    /// An HLL whose registers cannot be read: its opcodes do not cover
    /// exactly 16,384 registers.
    CorruptHll,
}

/// ADR-0056: a string longer than this is stored in chunks, when the seat
/// writes them (`--chunked-strings`).
pub const INLINE_MAX: usize = 64 * 1024;
/// ADR-0056: the bytes one chunk row holds.
pub const CHUNK_BYTES: usize = 32 * 1024;

pub struct StringStore<'a> {
    kv: &'a dyn Kv,
    ns: Vec<u8>,
    clock: Clock,
    max_value_bytes: u64,
    /// Whether a write may store a string in chunks (ADR-0056 D4). A
    /// chunked string is read, and changed in place, either way.
    chunked_writes: bool,
    /// The longest string stored inline when `chunked_writes` is on.
    inline_max: usize,
    /// The bytes per chunk row.
    chunk: usize,
}

/// A live string as stored: inline in its metadata row, or in chunks.
enum Stored {
    Inline(StringMeta),
    Chunked(ComplexMeta),
}

impl Stored {
    fn len(&self) -> u64 {
        match self {
            Stored::Inline(m) => m.payload.len() as u64,
            Stored::Chunked(c) => c.bytes,
        }
    }
}

impl<'a> StringStore<'a> {
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
            chunked_writes: false,
            inline_max: INLINE_MAX,
            chunk: CHUNK_BYTES,
        }
    }

    /// Let writes store a string longer than [`INLINE_MAX`] in chunks
    /// (ADR-0056 D4).
    pub fn chunked(mut self, writes: bool) -> Self {
        self.chunked_writes = writes;
        self
    }

    /// Smaller limits, so tests reach every chunk boundary with short
    /// strings.
    #[cfg(test)]
    fn with_chunks(mut self, inline_max: usize, chunk: usize) -> Self {
        self.chunked_writes = true;
        self.inline_max = inline_max;
        self.chunk = chunk;
        self
    }

    fn meta_key(&self, slot: u16, key: &[u8]) -> Vec<u8> {
        envelope(Cf::Metadata, &self.ns, slot, key)
    }

    fn chunk_key(&self, slot: u16, key: &[u8], version: u64, i: u64) -> Vec<u8> {
        subkey_envelope(&self.ns, slot, key, version, &(i as u32).to_be_bytes())
    }

    /// Live header of ANY type (for SET's existence/KEEPTTL semantics).
    fn read_live_header(&self, slot: u16, key: &[u8]) -> Option<MetaHeader> {
        let mk = self.meta_key(slot, key);
        let row = self.kv.get(&mk)?;
        let header = MetaHeader::decode(&row)?;
        if header.is_expired((self.clock)()) {
            self.kv.delete(&mk);
            return None;
        }
        Some(header)
    }

    /// Live string, inline or chunked; WRONGTYPE if the key holds another
    /// type.
    fn read_live(&self, slot: u16, key: &[u8]) -> Result<Option<Stored>, StoreError> {
        let mk = self.meta_key(slot, key);
        // BORROW THE ROW, COPY ONLY THE PAYLOAD. `kv.get` allocated the whole
        // row and copied it out of the block cache, and then
        // `StringMeta::decode` allocated the payload and copied it AGAIN out
        // of that row — two allocations and two ~1 KB copies where the reader
        // needs one. Decoding against a borrowed row leaves exactly the
        // payload allocation, which is the one the caller actually keeps.
        //
        // The expiry delete happens AFTER the borrow ends: `with_value` may
        // hold a block-cache handle for the closure's lifetime, and calling
        // back into the store while holding one is what its contract forbids.
        enum Row {
            Missing,
            Undecodable,
            Expired,
            WrongType,
            Live(Box<Stored>),
        }
        let mut outcome = Row::Missing;
        let now = (self.clock)();
        self.kv.with_value(&mk, &mut |row| {
            let Some(header) = MetaHeader::decode(row) else {
                outcome = Row::Undecodable;
                return;
            };
            if header.is_expired(now) {
                outcome = Row::Expired;
                return;
            }
            let stored = match header.value_type() {
                Some(ValueType::String) => StringMeta::decode(row).map(Stored::Inline),
                Some(ValueType::ChunkedString) => ComplexMeta::decode(row).map(Stored::Chunked),
                _ => {
                    outcome = Row::WrongType;
                    return;
                }
            };
            outcome = match stored {
                Some(s) => Row::Live(Box::new(s)),
                None => Row::Undecodable,
            };
        });
        match outcome {
            Row::Missing | Row::Undecodable => Ok(None),
            Row::Expired => {
                self.kv.delete(&mk);
                Ok(None)
            }
            Row::WrongType => Err(StoreError::WrongType),
            Row::Live(s) => Ok(Some(*s)),
        }
    }

    /// Bytes `[from, to)` of a chunked string, read from only the chunks
    /// they cover, in one ordered scan; zeros where no chunk row holds them.
    fn read_chunked(&self, slot: u16, key: &[u8], c: &ComplexMeta, from: u64, to: u64) -> Vec<u8> {
        let to = to.min(c.bytes);
        if from >= to {
            return Vec::new();
        }
        let mut out = vec![0u8; (to - from) as usize];
        let size = self.chunk as u64;
        let (first, last) = (from / size, (to - 1) / size);
        let prefix = subkey_prefix(&self.ns, slot, key, c.version);
        let after = if first == 0 {
            Vec::new()
        } else {
            self.chunk_key(slot, key, c.version, first - 1)
        };
        self.kv.for_each_from(&prefix, &after, &mut |k, v| {
            let Some(i) = k
                .get(prefix.len()..)
                .and_then(|f| <[u8; 4]>::try_from(f).ok())
                .map(|f| u64::from(u32::from_be_bytes(f)))
            else {
                return true;
            };
            if i > last {
                return false;
            }
            let base = i * size;
            let (lo, hi) = (base.max(from), (base + v.len() as u64).min(to));
            if lo < hi {
                out[(lo - from) as usize..(hi - from) as usize]
                    .copy_from_slice(&v[(lo - base) as usize..(hi - base) as usize]);
            }
            true
        });
        out
    }

    /// The whole value of a live string.
    /// A live string's whole value and its expiry, for the commands that
    /// read, change and rewrite one (HyperLogLog, `hll.rs`).
    pub(crate) fn value_and_expiry(
        &self,
        slot: u16,
        key: &[u8],
    ) -> Result<Option<(Vec<u8>, u64)>, StoreError> {
        Ok(self.read_live(slot, key)?.map(|s| {
            let expire_ms = match &s {
                Stored::Inline(m) => m.expire_ms,
                Stored::Chunked(c) => c.header.expire_ms,
            };
            (self.value_of(slot, key, s), expire_ms)
        }))
    }

    /// Replace a string's value, keeping `expire_ms`.
    pub(crate) fn store_value(&self, slot: u16, key: &[u8], payload: Vec<u8>, expire_ms: u64) {
        self.put_value(slot, key, payload, expire_ms);
    }

    pub(crate) fn max_value_bytes(&self) -> u64 {
        self.max_value_bytes
    }

    fn value_of(&self, slot: u16, key: &[u8], stored: Stored) -> Vec<u8> {
        match stored {
            Stored::Inline(m) => m.payload,
            Stored::Chunked(c) => self.read_chunked(slot, key, &c, 0, c.bytes),
        }
    }

    /// Store a whole value, replacing the key: inline, or in chunks under a
    /// new version when writes may chunk and it is longer than the inline
    /// limit (ADR-0056 D2). A replaced chunked string's rows are orphans
    /// for the sweeper, as a deleted collection's are.
    fn put_value(&self, slot: u16, key: &[u8], payload: Vec<u8>, expire_ms: u64) {
        let now = (self.clock)();
        if !(self.chunked_writes && payload.len() > self.inline_max) {
            let meta = StringMeta::new(payload, expire_ms, now);
            self.kv.put(&self.meta_key(slot, key), &meta.encode());
            return;
        }
        let mut meta = ComplexMeta::new(ValueType::ChunkedString, VersionGen::next(now));
        meta.header.expire_ms = expire_ms;
        for (i, part) in payload.chunks(self.chunk).enumerate() {
            // A chunk of zeros is the same as no chunk: a sparse bitmap
            // stores only the chunks it has set bits in.
            if part.iter().any(|&b| b != 0) {
                self.kv
                    .put(&self.chunk_key(slot, key, meta.version, i as u64), part);
            }
        }
        meta.bytes = payload.len() as u64;
        meta.size = payload.len().div_ceil(self.chunk) as u32;
        meta.touch(now);
        // Metadata LAST: until it lands the key holds its old value, and a
        // crash leaves unreachable chunks for the sweeper.
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
    }

    /// What an in-place write (APPEND, SETRANGE, SETBIT, BITFIELD) works
    /// on, for a string that will be at least `need` bytes long: the chunks
    /// when it is chunked or is about to be, else the inline value.
    fn target(&self, slot: u16, key: &[u8], need: u64) -> Result<Target<'_, 'a>, StoreError> {
        let existing = self.read_live(slot, key)?;
        let chunk_now = self.chunked_writes && need > self.inline_max as u64;
        Ok(match existing {
            Some(Stored::Chunked(meta)) => Target::Chunked(self.view(slot, key, meta, false)),
            Some(Stored::Inline(m)) if chunk_now => {
                // Rewritten once, into chunks, then changed in place.
                let mut meta =
                    ComplexMeta::new(ValueType::ChunkedString, VersionGen::next((self.clock)()));
                meta.header.expire_ms = m.expire_ms;
                for (i, part) in m.payload.chunks(self.chunk).enumerate() {
                    if part.iter().any(|&b| b != 0) {
                        self.kv
                            .put(&self.chunk_key(slot, key, meta.version, i as u64), part);
                    }
                }
                meta.bytes = m.payload.len() as u64;
                Target::Chunked(self.view(slot, key, meta, true))
            }
            None if chunk_now => {
                let meta =
                    ComplexMeta::new(ValueType::ChunkedString, VersionGen::next((self.clock)()));
                Target::Chunked(self.view(slot, key, meta, true))
            }
            Some(Stored::Inline(m)) => {
                Target::Inline(self.inline(slot, key, m.payload, m.expire_ms, false))
            }
            None => Target::Inline(self.inline(slot, key, Vec::new(), 0, true)),
        })
    }

    fn inline<'s>(
        &'s self,
        slot: u16,
        key: &[u8],
        payload: Vec<u8>,
        expire_ms: u64,
        dirty: bool,
    ) -> InlineView<'s, 'a> {
        InlineView {
            store: self,
            slot,
            key: key.to_vec(),
            payload,
            expire_ms,
            dirty,
        }
    }

    fn view<'s>(
        &'s self,
        slot: u16,
        key: &[u8],
        meta: ComplexMeta,
        dirty: bool,
    ) -> ChunkView<'s, 'a> {
        ChunkView {
            store: self,
            slot,
            key: key.to_vec(),
            meta,
            chunks: std::collections::BTreeMap::new(),
            changed: std::collections::BTreeSet::new(),
            dirty,
        }
    }

    /// Plain SET overwrites any existing type (Redis semantics), so the
    /// NX/XX/KEEPTTL checks read only the type-agnostic header.
    pub fn set(
        &self,
        slot: u16,
        key: &[u8],
        value: &[u8],
        opts: SetOptions,
    ) -> Result<SetOutcome, StoreError> {
        if value.len() as u64 > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        // Read the old header ONLY when something actually consumes it: NX/XX
        // need to know whether the key exists, KEEPTTL needs its expiry.
        // A plain `SET k v` consumed neither, so every unconditional SET paid
        // a point lookup — bloom probe, index block, data block — and then
        // dropped the answer. It showed up as ~3.7% of samples in a
        // WRITE-ONLY profile (2026-08-30), which is also why the write path
        // looked like it was doing reads.
        //
        // The skipped call also lazily deletes an EXPIRED row, and dropping
        // that is safe here and only here: it deletes meta_key(slot, key) and
        // the put below writes that same key, so the overwrite subsumes the
        // delete. Orphaned subkeys of a displaced complex type, or of a
        // displaced chunked string, are the GC sweeper's job either way —
        // SET never cleaned those up.
        let needs_existing = opts.nx || opts.xx || matches!(opts.expiry, SetExpiry::Keep);
        let existing = if needs_existing {
            self.read_live_header(slot, key)
        } else {
            None
        };
        if (opts.nx && existing.is_some()) || (opts.xx && existing.is_none()) {
            return Ok(SetOutcome::Unchanged);
        }
        let expire_ms = match opts.expiry {
            SetExpiry::Clear => 0,
            SetExpiry::Keep => existing.map(|h| h.expire_ms).unwrap_or(0),
            SetExpiry::AtMs(at) => at,
        };
        self.put_value(slot, key, value.to_vec(), expire_ms);
        Ok(SetOutcome::Done)
    }

    pub fn get(&self, slot: u16, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self
            .read_live(slot, key)?
            .map(|s| self.value_of(slot, key, s)))
    }

    /// GETDEL: return the value and delete the key atomically (one node, one
    /// slot). WRONGTYPE if the key holds a non-string (read_live checks type).
    pub fn get_del(&self, slot: u16, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let existing = self
            .read_live(slot, key)?
            .map(|s| self.value_of(slot, key, s));
        if existing.is_some() {
            self.kv.delete(&self.meta_key(slot, key));
        }
        Ok(existing)
    }

    /// GETEX: return the value and, unless `expiry` is Keep, rewrite the
    /// key's TTL in the same pass. WRONGTYPE if the key holds a non-string.
    ///
    /// A past absolute expiry is written verbatim rather than special-cased
    /// into a delete. `read_live` already treats an elapsed expire_ms as
    /// absent, so the key becomes invisible immediately and the GC sweeper
    /// reclaims it — the same path SET ... PXAT in the past takes. Two ways
    /// to retire a key would be two ways to get it wrong.
    pub fn getex(
        &self,
        slot: u16,
        key: &[u8],
        expiry: SetExpiry,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(stored) = self.read_live(slot, key)? else {
            return Ok(None);
        };
        let new_expire = match expiry {
            // GETEX with no option is a plain GET: the TTL is untouched,
            // which is NOT the same as clearing it.
            SetExpiry::Keep => return Ok(Some(self.value_of(slot, key, stored))),
            SetExpiry::AtMs(at) => at,
            // PERSIST.
            SetExpiry::Clear => 0,
        };
        // The TTL changes and the write stamp does not: an expiry is a
        // touch, not a write, for either form.
        match &stored {
            Stored::Inline(m) if new_expire != m.expire_ms => {
                let mut row = m.encode();
                MetaHeader::write_expire(&mut row, new_expire);
                self.kv.put(&self.meta_key(slot, key), &row);
            }
            Stored::Chunked(c) if new_expire != c.header.expire_ms => {
                let mut c = *c;
                c.header.expire_ms = new_expire;
                self.kv.put(&self.meta_key(slot, key), &c.encode());
            }
            _ => {}
        }
        Ok(Some(self.value_of(slot, key, stored)))
    }

    /// INCRBY/DECRBY. Creates the key at 0. Preserves TTL.
    pub fn incr_by(&self, slot: u16, key: &[u8], delta: i64) -> Result<i64, StoreError> {
        let (current, expire_ms) = match self.read_live(slot, key)? {
            None => (0i64, 0u64),
            // Longer than 64 KiB is not an integer, unread (ADR-0056 D3).
            Some(Stored::Chunked(_)) => return Err(StoreError::NotInteger),
            Some(Stored::Inline(m)) => {
                let n = parse_redis_i64(&m.payload).ok_or(StoreError::NotInteger)?;
                (n, m.expire_ms)
            }
        };
        let next = current.checked_add(delta).ok_or(StoreError::Overflow)?;
        let meta = StringMeta::new(next.to_string().into_bytes(), expire_ms, (self.clock)());
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        Ok(next)
    }

    /// INCRBYFLOAT. Creates the key at 0. Preserves TTL. Returns the stored
    /// representation — Redis's LD_STR_HUMAN shape (`%.17f`, trailing zeros
    /// then a bare dot trimmed), which is also what lands in the value.
    pub fn incr_by_float(&self, slot: u16, key: &[u8], delta: f64) -> Result<Vec<u8>, StoreError> {
        let (current, expire_ms) = match self.read_live(slot, key)? {
            None => (0f64, 0u64),
            // Redis reads no float longer than 5 KiB (ADR-0056 D3).
            Some(Stored::Chunked(_)) => return Err(StoreError::NotFloat),
            Some(Stored::Inline(m)) => {
                let s = std::str::from_utf8(&m.payload).map_err(|_| StoreError::NotFloat)?;
                // Redis does not read a stored `nan` as a float, so the
                // value is the error, not the sum (BUG-0219).
                let v: f64 = s
                    .parse()
                    .ok()
                    .filter(|v: &f64| !v.is_nan())
                    .ok_or(StoreError::NotFloat)?;
                (v, m.expire_ms)
            }
        };
        let next = current + delta;
        if !next.is_finite() {
            return Err(StoreError::NanOrInfinity);
        }
        let repr = fmt_float_human(next);
        let meta = StringMeta::new(repr.clone(), expire_ms, (self.clock)());
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        Ok(repr)
    }

    /// APPEND: returns new length. Creates the key. Preserves TTL. A
    /// chunked string writes only its tail.
    pub fn append(&self, slot: u16, key: &[u8], suffix: &[u8]) -> Result<usize, StoreError> {
        let len = self.read_live(slot, key)?.map_or(0, |s| s.len());
        // The incremental hole SET's check can't close: repeated APPENDs
        // must not build a value past the cap (Valkey checkStringLength).
        let end = len + suffix.len() as u64;
        if end > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        let mut target = self.target(slot, key, end)?;
        target.write(len, suffix);
        target.finish();
        Ok(end as usize)
    }

    pub fn strlen(&self, slot: u16, key: &[u8]) -> Result<usize, StoreError> {
        Ok(self.read_live(slot, key)?.map_or(0, |s| s.len() as usize))
    }

    /// GETRANGE: inclusive `[start, end]` with negatives from the end,
    /// clamped; an inverted or fully out-of-range window is the empty string.
    pub fn getrange(
        &self,
        slot: u16,
        key: &[u8],
        start: i64,
        end: i64,
    ) -> Result<Vec<u8>, StoreError> {
        let Some(stored) = self.read_live(slot, key)? else {
            return Ok(Vec::new());
        };
        // Redis's rules (BUG-0214), which are not LRANGE's: two negative
        // indexes in the wrong order answer empty, and an end before the
        // start of the string is clamped to its first byte, not dropped.
        // `GETRANGE k 0 -100` answers the first byte.
        if start < 0 && end < 0 && start > end {
            return Ok(Vec::new());
        }
        let len = stored.len() as i64;
        let norm = |i: i64| if i < 0 { len + i } else { i };
        let from = norm(start).max(0);
        let to = norm(end).max(0).min(len - 1);
        if len == 0 || from > to {
            return Ok(Vec::new());
        }
        Ok(match stored {
            Stored::Inline(m) => m.payload[from as usize..=(to as usize)].to_vec(),
            Stored::Chunked(c) => self.read_chunked(slot, key, &c, from as u64, to as u64 + 1),
        })
    }

    /// SETRANGE: overwrite `patch` at `offset`, zero-padding any gap;
    /// returns the new length. An empty patch is a pure length probe —
    /// Redis never creates the key for it. Preserves TTL (in-place
    /// mutation, like APPEND).
    pub fn setrange(
        &self,
        slot: u16,
        key: &[u8],
        offset: u64,
        patch: &[u8],
    ) -> Result<usize, StoreError> {
        if patch.is_empty() {
            return Ok(self.read_live(slot, key)?.map_or(0, |s| s.len() as usize));
        }
        let end = offset + patch.len() as u64;
        if end > self.max_value_bytes {
            // Valkey checks the type before the size.
            self.read_live(slot, key)?;
            return Err(StoreError::ValueTooLarge);
        }
        let mut target = self.target(slot, key, end)?;
        target.write(offset, patch);
        let len = target.len();
        target.finish();
        Ok(len as usize)
    }

    /// BITFIELD (BUG-0192): run `ops` in order over the string as a bit
    /// array, bit 0 being the high bit of the first byte, and answer one
    /// reply per operation: `None` for a write refused by `OVERFLOW FAIL`.
    ///
    /// Valkey's shape, measured by the conformance corpus: reads past the
    /// end read zeros; with any write among `ops` the string is first grown
    /// with zeros to cover the highest bit written, creating the key if it
    /// was missing, even when every write is then refused by `FAIL`; with
    /// none, nothing is created. The TTL is kept, as by `SETRANGE`.
    pub fn bitfield(
        &self,
        slot: u16,
        key: &[u8],
        ops: &[BitfieldOp],
    ) -> Result<Vec<Option<i64>>, StoreError> {
        let highest = ops
            .iter()
            .filter(|op| !matches!(op.kind, BitfieldKind::Get))
            .map(|op| op.offset + u64::from(op.bits) - 1)
            .max();
        let Some(highest) = highest else {
            // Reads only: nothing is created, and a chunked string reads
            // just the chunks the fields fall in.
            let Some(stored) = self.read_live(slot, key)? else {
                return Ok(ops.iter().map(|_| Some(0)).collect());
            };
            let mut target = match stored {
                Stored::Inline(m) => {
                    Target::Inline(self.inline(slot, key, m.payload, m.expire_ms, false))
                }
                Stored::Chunked(c) => Target::Chunked(self.view(slot, key, c, false)),
            };
            return Ok(ops.iter().map(|op| Some(op.read(&mut target))).collect());
        };
        let need = (highest >> 3) + 1;
        if need > self.max_value_bytes {
            self.read_live(slot, key)?;
            return Err(StoreError::ValueTooLarge);
        }
        let mut target = self.target(slot, key, need)?;
        target.grow(need);
        let mut replies = Vec::with_capacity(ops.len());
        for op in ops {
            let reply = match op.kind {
                BitfieldKind::Get => Some(op.read(&mut target)),
                BitfieldKind::Set(value, overflow) | BitfieldKind::IncrBy(value, overflow) => {
                    let incr = matches!(op.kind, BitfieldKind::IncrBy(..));
                    let old = op.read(&mut target);
                    let (new, overflowed) = if op.signed {
                        let (from, by) = if incr { (old, value) } else { (value, 0) };
                        match signed_overflow(from, by, op.bits, overflow) {
                            Some(limit) => (limit, true),
                            None => (from.wrapping_add(by), false),
                        }
                    } else {
                        let (from, by) = if incr {
                            (old as u64, value)
                        } else {
                            (value as u64, 0)
                        };
                        match unsigned_overflow(from, by, op.bits, overflow) {
                            Some(limit) => (limit as i64, true),
                            None => (from.wrapping_add(by as u64) as i64, false),
                        }
                    };
                    if overflowed && overflow == BitfieldOverflow::Fail {
                        None
                    } else {
                        write_bits(&mut target, op.offset, op.bits, new as u64);
                        Some(if incr { new } else { old })
                    }
                }
            };
            replies.push(reply);
        }
        target.finish();
        Ok(replies)
    }

    /// SETBIT: set bit `offset` (bit 0 is the high bit of the first byte)
    /// and answer its old value. The string grows with zeros to cover the
    /// bit, the key is created if missing, and the TTL is kept. As in
    /// Valkey, a bit that already holds `on` in a string long enough writes
    /// nothing. A chunked string reads and writes one chunk.
    pub fn setbit(&self, slot: u16, key: &[u8], offset: u64, on: bool) -> Result<bool, StoreError> {
        let byte = offset >> 3;
        if byte + 1 > self.max_value_bytes {
            self.read_live(slot, key)?;
            return Err(StoreError::ValueTooLarge);
        }
        let mut target = self.target(slot, key, byte + 1)?;
        target.grow(byte + 1);
        let mask = 1u8 << (7 - (offset & 7));
        let current = target.byte(byte);
        let old = current & mask != 0;
        if old != on {
            target.set_byte(byte, current ^ mask);
        }
        target.finish();
        Ok(old)
    }

    /// GETBIT: the bit at `offset`; zero past the end and for a missing key.
    pub fn getbit(&self, slot: u16, key: &[u8], offset: u64) -> Result<bool, StoreError> {
        let byte = offset >> 3;
        let value = match self.read_live(slot, key)? {
            None => 0,
            Some(Stored::Inline(m)) => m.payload.get(byte as usize).copied().unwrap_or(0),
            Some(Stored::Chunked(c)) => self
                .read_chunked(slot, key, &c, byte, byte + 1)
                .first()
                .copied()
                .unwrap_or(0),
        };
        Ok(value & (1 << (7 - (offset & 7))) != 0)
    }

    /// BITCOUNT: the set bits in the string, or in `range`
    /// (`start`, `end`, in bits when the flag is set, else bytes).
    pub fn bitcount(
        &self,
        slot: u16,
        key: &[u8],
        range: Option<(i64, i64, bool)>,
    ) -> Result<u64, StoreError> {
        Ok(match self.read_live(slot, key)? {
            None => 0,
            Some(Stored::Inline(m)) => bitcount_of(&m.payload, range),
            Some(Stored::Chunked(c)) => bitcount_in(&self.source(slot, key, c), range),
        })
    }

    /// BITPOS: the first bit equal to `bit` in the string, or in `range`
    /// (`start`, an `end` if one was given, in bits when the flag is set).
    pub fn bitpos(
        &self,
        slot: u16,
        key: &[u8],
        bit: bool,
        range: Option<(i64, Option<i64>, bool)>,
    ) -> Result<i64, StoreError> {
        Ok(match self.read_live(slot, key)? {
            // A missing key is an endless run of zeros (Valkey's
            // `bitposCommand`), whatever the range asked.
            None => {
                if bit {
                    -1
                } else {
                    0
                }
            }
            Some(Stored::Inline(m)) => bitpos_of(&m.payload, bit, range),
            Some(Stored::Chunked(c)) => bitpos_in(&self.source(slot, key, c), bit, range),
        })
    }

    fn source(&self, slot: u16, key: &[u8], meta: ComplexMeta) -> ChunkSource<'_, 'a> {
        ChunkSource {
            store: self,
            slot,
            key: key.to_vec(),
            meta,
        }
    }
}

/// A string's bytes as the bit-counting commands read them: its length,
/// and any window of it. A chunked string reads a window from only the
/// chunks it covers (ADR-0056 D3).
trait ByteSource {
    fn len(&self) -> u64;
    /// Bytes `[from, to]`, inclusive, both inside the string.
    fn window(&self, from: u64, to: u64) -> std::borrow::Cow<'_, [u8]>;
}

impl ByteSource for [u8] {
    fn len(&self) -> u64 {
        <[u8]>::len(self) as u64
    }
    fn window(&self, from: u64, to: u64) -> std::borrow::Cow<'_, [u8]> {
        std::borrow::Cow::Borrowed(&self[from as usize..=to as usize])
    }
}

/// A chunked string, as a [`ByteSource`].
struct ChunkSource<'s, 'a> {
    store: &'s StringStore<'a>,
    slot: u16,
    key: Vec<u8>,
    meta: ComplexMeta,
}

impl ByteSource for ChunkSource<'_, '_> {
    fn len(&self) -> u64 {
        self.meta.bytes
    }
    fn window(&self, from: u64, to: u64) -> std::borrow::Cow<'_, [u8]> {
        std::borrow::Cow::Owned(self.store.read_chunked(
            self.slot,
            &self.key,
            &self.meta,
            from,
            to + 1,
        ))
    }
}

/// A string's bytes as the in-place writers change them, a byte at a time:
/// zero past the end, and the string grown by writing past it.
trait BitBuf {
    fn byte(&mut self, i: u64) -> u8;
    fn set_byte(&mut self, i: u64, v: u8);
}

/// What an in-place write changes: an inline value, or a chunked string's
/// chunks.
enum Target<'s, 'a> {
    Inline(InlineView<'s, 'a>),
    Chunked(ChunkView<'s, 'a>),
}

/// An inline string being changed in place.
struct InlineView<'s, 'a> {
    store: &'s StringStore<'a>,
    slot: u16,
    key: Vec<u8>,
    payload: Vec<u8>,
    expire_ms: u64,
    /// The value must be written back: it changed, or it is new.
    dirty: bool,
}

impl InlineView<'_, '_> {
    fn finish(self) {
        if self.dirty {
            let meta = StringMeta::new(self.payload, self.expire_ms, (self.store.clock)());
            self.store
                .kv
                .put(&self.store.meta_key(self.slot, &self.key), &meta.encode());
        }
    }
}

impl BitBuf for InlineView<'_, '_> {
    fn byte(&mut self, i: u64) -> u8 {
        self.payload.get(i as usize).copied().unwrap_or(0)
    }
    fn set_byte(&mut self, i: u64, v: u8) {
        let i = i as usize;
        if self.payload.len() <= i {
            self.payload.resize(i + 1, 0);
            self.dirty = true;
        }
        if self.payload[i] != v {
            self.payload[i] = v;
            self.dirty = true;
        }
    }
}

/// A chunked string being changed in place: the chunks it touches, loaded
/// on first use, written back with its metadata row.
struct ChunkView<'s, 'a> {
    store: &'s StringStore<'a>,
    slot: u16,
    key: Vec<u8>,
    meta: ComplexMeta,
    chunks: std::collections::BTreeMap<u64, Vec<u8>>,
    /// The chunks a write changed.
    changed: std::collections::BTreeSet<u64>,
    /// The metadata row must be written: the string grew or changed, or it
    /// is newly chunked.
    dirty: bool,
}

impl ChunkView<'_, '_> {
    /// Chunk `i`, a full chunk's bytes, zeros where its row has none.
    fn chunk(&mut self, i: u64) -> &mut Vec<u8> {
        let size = self.store.chunk;
        let (store, slot, key, version) = (self.store, self.slot, &self.key, self.meta.version);
        self.chunks.entry(i).or_insert_with(|| {
            let mut c = store
                .kv
                .get(&store.chunk_key(slot, key, version, i))
                .unwrap_or_default();
            c.resize(size, 0);
            c
        })
    }

    /// Write back the changed chunks, each cut at the string's end, and then
    /// the metadata row, stamped, so WATCH sees the change (ADR-0056 D3).
    fn finish(mut self) {
        if !self.dirty && self.changed.is_empty() {
            return;
        }
        let size = self.store.chunk as u64;
        let len = self.meta.bytes;
        for &i in &self.changed {
            let Some(c) = self.chunks.get(&i) else {
                continue;
            };
            let span = len.saturating_sub(i * size).min(size) as usize;
            self.store.kv.put(
                &self
                    .store
                    .chunk_key(self.slot, &self.key, self.meta.version, i),
                &c[..span],
            );
        }
        self.meta.size = len.div_ceil(size) as u32;
        let now = (self.store.clock)();
        self.meta.touch(now);
        self.store.kv.put(
            &self.store.meta_key(self.slot, &self.key),
            &self.meta.encode(),
        );
    }
}

impl BitBuf for ChunkView<'_, '_> {
    fn byte(&mut self, i: u64) -> u8 {
        if i >= self.meta.bytes {
            return 0;
        }
        let size = self.store.chunk as u64;
        self.chunk(i / size)[(i % size) as usize]
    }
    fn set_byte(&mut self, i: u64, v: u8) {
        if i >= self.meta.bytes {
            self.meta.bytes = i + 1;
            self.dirty = true;
        }
        let size = self.store.chunk as u64;
        let byte = &mut self.chunk(i / size)[(i % size) as usize];
        if *byte != v {
            *byte = v;
            self.changed.insert(i / size);
        }
    }
}

impl BitBuf for Target<'_, '_> {
    fn byte(&mut self, i: u64) -> u8 {
        match self {
            Target::Inline(v) => v.byte(i),
            Target::Chunked(v) => v.byte(i),
        }
    }
    fn set_byte(&mut self, i: u64, b: u8) {
        match self {
            Target::Inline(v) => v.set_byte(i, b),
            Target::Chunked(v) => v.set_byte(i, b),
        }
    }
}

impl Target<'_, '_> {
    fn len(&self) -> u64 {
        match self {
            Target::Inline(v) => v.payload.len() as u64,
            Target::Chunked(v) => v.meta.bytes,
        }
    }

    /// Grow to at least `len` bytes with zeros.
    fn grow(&mut self, len: u64) {
        match self {
            Target::Inline(v) => {
                if (v.payload.len() as u64) < len {
                    v.payload.resize(len as usize, 0);
                    v.dirty = true;
                }
            }
            Target::Chunked(v) => {
                if v.meta.bytes < len {
                    v.meta.bytes = len;
                    v.dirty = true;
                }
            }
        }
    }

    /// Write `bytes` at `at`, growing with zeros to reach it. Always a
    /// write, as APPEND and SETRANGE always were, even of the same bytes:
    /// the metadata row is rewritten, though a chunk whose bytes are
    /// unchanged is not.
    fn write(&mut self, at: u64, bytes: &[u8]) {
        match self {
            Target::Inline(v) => {
                let end = at as usize + bytes.len();
                if v.payload.len() < end {
                    v.payload.resize(end, 0);
                }
                v.payload[at as usize..end].copy_from_slice(bytes);
                v.dirty = true;
            }
            Target::Chunked(v) => {
                let size = v.store.chunk as u64;
                let mut done = 0usize;
                while done < bytes.len() {
                    let pos = at + done as u64;
                    let (i, off) = (pos / size, (pos % size) as usize);
                    let n = (size as usize - off).min(bytes.len() - done);
                    let (part, from) = (&bytes[done..done + n], &mut v.chunk(i)[off..off + n]);
                    if from != part {
                        from.copy_from_slice(part);
                        v.changed.insert(i);
                    }
                    done += n;
                }
                let end = at + bytes.len() as u64;
                if end > v.meta.bytes {
                    v.meta.bytes = end;
                }
                v.dirty = true;
            }
        }
    }

    fn finish(self) {
        match self {
            Target::Inline(v) => v.finish(),
            Target::Chunked(v) => v.finish(),
        }
    }
}

/// The largest bit offset Redis takes: one inside a 512 MiB string, its
/// `proto-max-bulk-len`. A larger one is refused as not an offset at all,
/// before any value-size cap is consulted.
pub const MAX_BIT_OFFSET: u64 = 512 * 1024 * 1024 * 8 - 1;

/// Clamp an inclusive `[start, end]` window over `len` units, negatives
/// counting from the end, as Valkey's bit commands do: both clamped below
/// at 0, `end` above at `len - 1`. Empty when `start > end`.
fn bit_window(len: i64, start: i64, end: i64) -> (i64, i64) {
    let start = if start < 0 { len + start } else { start };
    let end = if end < 0 { len + end } else { end };
    let (start, end) = (start.max(0), end.max(0));
    (start, if end >= len { len - 1 } else { end })
}

/// Whether bit `i` of `payload` is set; zero past the end.
fn bit_at(payload: &[u8], i: u64) -> bool {
    payload
        .get((i >> 3) as usize)
        .is_some_and(|b| b & (1 << (7 - (i & 7))) != 0)
}

/// BITCOUNT over a string, as Valkey 9.1 counts: `range` is `start`, `end`
/// and whether they count bits rather than bytes.
pub fn bitcount_of(payload: &[u8], range: Option<(i64, i64, bool)>) -> u64 {
    bitcount_in(payload, range)
}

/// [`bitcount_of`] over any [`ByteSource`], reading only the window it
/// counts.
fn bitcount_in(src: &(impl ByteSource + ?Sized), range: Option<(i64, i64, bool)>) -> u64 {
    let popcount = |bytes: &[u8]| bytes.iter().map(|b| u64::from(b.count_ones())).sum::<u64>();
    let Some((start, end, bits)) = range else {
        return match src.len() {
            0 => 0,
            n => popcount(&src.window(0, n - 1)),
        };
    };
    // Two negative indexes in the wrong order count nothing, BEFORE
    // clamping. Clamped, `-4 -5` on three bytes is `[0, 0]`, the first
    // byte, which is what BITPOS (without this check) does search.
    if start < 0 && end < 0 && start > end {
        return 0;
    }
    let len = src.len() as i64 * if bits { 8 } else { 1 };
    let (start, end) = bit_window(len, start, end);
    if start > end {
        return 0;
    }
    if !bits {
        return popcount(&src.window(start as u64, end as u64));
    }
    // Whole bytes, less the bits of the first before `start` and of the
    // last after `end` (bit 0 being a byte's high bit).
    let (first, last) = ((start >> 3) as u64, (end >> 3) as u64);
    let window = src.window(first, last);
    let before = window[0] & !(0xffu8 >> (start & 7));
    let after = window[window.len() - 1] & (0xffu16 >> ((end & 7) + 1)) as u8;
    popcount(&window) - u64::from(before.count_ones()) - u64::from(after.count_ones())
}

/// BITPOS over a string that exists, as Valkey 9.1 searches: `range` is
/// `start`, the `end` if one was given, and whether they count bits.
pub fn bitpos_of(payload: &[u8], bit: bool, range: Option<(i64, Option<i64>, bool)>) -> i64 {
    bitpos_in(payload, bit, range)
}

/// [`bitpos_of`] over any [`ByteSource`], reading only the window it
/// searches.
fn bitpos_in(
    src: &(impl ByteSource + ?Sized),
    bit: bool,
    range: Option<(i64, Option<i64>, bool)>,
) -> i64 {
    let len = src.len() as i64;
    let (start, end, bits, end_given) = match range {
        None => (0, len - 1, false, false),
        Some((start, end, bits)) => {
            let units = if bits { len * 8 } else { len };
            let (s, e) = bit_window(units, start, end.unwrap_or(units - 1));
            (s, e, bits, end.is_some())
        }
    };
    // An empty window holds neither a 0 nor a 1, and an empty string is
    // one (it answers -1 for either bit, where a missing key answers 0).
    if start > end {
        return -1;
    }
    let (lo, hi) = if bits {
        (start as u64, end as u64)
    } else {
        (start as u64 * 8, end as u64 * 8 + 7)
    };
    // A byte at a time; only the partial first and last bytes need their
    // bits looked at one by one, and a whole byte of the other value is
    // skipped. The window's bytes start at byte `lo >> 3`.
    let base = lo >> 3;
    let window = src.window(base, hi >> 3);
    let skip = if bit { 0x00 } else { 0xff };
    let found = ((lo >> 3)..=(hi >> 3)).find_map(|j| {
        let whole = j * 8 >= lo && j * 8 + 7 <= hi;
        if whole && window[(j - base) as usize] == skip {
            return None;
        }
        (j * 8..j * 8 + 8)
            .filter(|&i| i >= lo && i <= hi)
            .find(|&i| bit_at(&window, i - base * 8) == bit)
    });
    match found {
        Some(i) => i as i64,
        // Looking for a 0 with no end given, the string reads as padded
        // with zeros on the right: the first bit past it.
        None if !bit && !end_given => hi as i64 + 1,
        None => -1,
    }
}

/// BITOP's operators: Valkey's four, and Redis 8.2's `DIFF`, `DIFF1`,
/// `ANDOR` and `ONE`, which Valkey 9.1 does not have.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BitOp {
    And,
    Or,
    Xor,
    Not,
    /// The first source's bits that are in none of the others.
    Diff,
    /// The others' bits that are not in the first.
    Diff1,
    /// The first source's bits that are also in any of the others.
    AndOr,
    /// The bits set in exactly one source.
    One,
}

/// BITOP's result over its sources, a missing one being an empty string:
/// as long as the longest, each shorter one padded with zeros.
pub fn bitop_of(op: BitOp, sources: &[Vec<u8>]) -> Vec<u8> {
    let len = sources.iter().map(Vec::len).max().unwrap_or(0);
    let byte = |s: &Vec<u8>, j: usize| s.get(j).copied().unwrap_or(0);
    (0..len)
        .map(|j| {
            let first = sources.first().map_or(0, |s| byte(s, j));
            let rest = || sources[1..].iter().map(|s| byte(s, j));
            match op {
                BitOp::And => rest().fold(first, |a, b| a & b),
                BitOp::Or => rest().fold(first, |a, b| a | b),
                BitOp::Xor => rest().fold(first, |a, b| a ^ b),
                BitOp::Not => !first,
                BitOp::Diff => first & !rest().fold(0, |a, b| a | b),
                BitOp::Diff1 => !first & rest().fold(0, |a, b| a | b),
                BitOp::AndOr => first & rest().fold(0, |a, b| a | b),
                BitOp::One => {
                    let (mut once, mut more) = (0u8, 0u8);
                    for s in sources {
                        let b = byte(s, j);
                        more |= once & b;
                        once ^= b;
                    }
                    once & !more
                }
            }
        })
        .collect()
}

/// What `OVERFLOW` sets for the `SET` and `INCRBY` after it (BUG-0192).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BitfieldOverflow {
    /// Modular arithmetic, two's complement for a signed field.
    Wrap,
    /// Clamp to the field's minimum or maximum.
    Sat,
    /// Refuse the write, and answer nil for it.
    Fail,
}

/// One `BITFIELD` operation's verb and argument.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BitfieldKind {
    Get,
    /// The value to store; the reply is the field's previous value.
    Set(i64, BitfieldOverflow),
    /// The increment; the reply is the field's new value.
    IncrBy(i64, BitfieldOverflow),
}

/// One `BITFIELD` operation on a field of `bits` bits at bit `offset`:
/// `i1`..`i64` when `signed`, `u1`..`u63` when not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BitfieldOp {
    pub signed: bool,
    pub bits: u32,
    pub offset: u64,
    pub kind: BitfieldKind,
}

impl BitfieldOp {
    /// The field's value in `buf`, sign-extended when signed. Bits past
    /// its end read as zero.
    fn read(&self, buf: &mut dyn BitBuf) -> i64 {
        let mut v = 0u64;
        for i in 0..u64::from(self.bits) {
            let bit = self.offset + i;
            let byte = buf.byte(bit >> 3);
            v = (v << 1) | u64::from((byte >> (7 - (bit & 7))) & 1);
        }
        if self.signed && self.bits < 64 && v & (1 << (self.bits - 1)) != 0 {
            v |= u64::MAX << self.bits;
        }
        v as i64
    }
}

/// Store the low `bits` bits of `value` at bit `offset`; `buf` already
/// covers them.
fn write_bits(buf: &mut dyn BitBuf, offset: u64, bits: u32, value: u64) {
    for i in 0..u64::from(bits) {
        let bit = offset + i;
        let set = (value >> (u64::from(bits) - 1 - i)) & 1 == 1;
        let mask = 1u8 << (7 - (bit & 7));
        let byte = buf.byte(bit >> 3);
        buf.set_byte(bit >> 3, if set { byte | mask } else { byte & !mask });
    }
}

/// Valkey's `checkSignedBitfieldOverflow`: `None` when `value + incr` fits
/// an `i<bits>`, else the value `overflow` stores instead (unused for
/// `Fail`). Its arithmetic, wrapping where C's would.
fn signed_overflow(value: i64, incr: i64, bits: u32, overflow: BitfieldOverflow) -> Option<i64> {
    let max: i64 = if bits == 64 {
        i64::MAX
    } else {
        (1i64 << (bits - 1)) - 1
    };
    let min = -max - 1;
    let max_incr = (max as u64).wrapping_sub(value as u64) as i64;
    let min_incr = min.wrapping_sub(value);
    let wrapped = || {
        let mut c = (value as u64).wrapping_add(incr as u64);
        if bits < 64 {
            let mask = u64::MAX << bits;
            if c & (1u64 << (bits - 1)) != 0 {
                c |= mask;
            } else {
                c &= !mask;
            }
        }
        c as i64
    };
    let limit = |bound: i64| match overflow {
        BitfieldOverflow::Wrap => wrapped(),
        BitfieldOverflow::Sat => bound,
        BitfieldOverflow::Fail => 0,
    };
    if value > max || (bits != 64 && incr > max_incr) || (value >= 0 && incr > 0 && incr > max_incr)
    {
        Some(limit(max))
    } else if value < min
        || (bits != 64 && incr < min_incr)
        || (value < 0 && incr < 0 && incr < min_incr)
    {
        Some(limit(min))
    } else {
        None
    }
}

/// Valkey's `checkUnsignedBitfieldOverflow`, as `signed_overflow` is its
/// signed twin. `bits` is at most 63.
fn unsigned_overflow(value: u64, incr: i64, bits: u32, overflow: BitfieldOverflow) -> Option<u64> {
    let max = (1u64 << bits) - 1;
    let max_incr = max.wrapping_sub(value) as i64;
    let min_incr = value.wrapping_neg() as i64;
    let limit = |bound: u64| match overflow {
        BitfieldOverflow::Wrap => value.wrapping_add(incr as u64) & !(u64::MAX << bits),
        BitfieldOverflow::Sat => bound,
        BitfieldOverflow::Fail => 0,
    };
    if value > max || (incr > 0 && incr > max_incr) {
        Some(limit(max))
    } else if incr < 0 && incr < min_incr {
        Some(limit(0))
    } else {
        None
    }
}

/// Redis's LD_STR_HUMAN float shape: fixed `%.17f`, then trim trailing
/// zeros, then a bare trailing dot. (`10.75` → "10.75", `3.0` → "3".) What
/// is left of a negative zero, or of a negative too small for 17 places, is
/// `-0`, which upstream writes as `0` (BUG-0233). On
/// aarch64 `long double` IS `double`, so f64 reproduces the reference
/// output bit-for-bit on this platform class — the conformance oracle
/// referees.
pub fn fmt_float_human(x: f64) -> Vec<u8> {
    let mut s = format!("{x:.17}");
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s == "-0" {
        s.remove(0);
    }
    s.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;
    use crate::hashes::HashStore;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A live inline string's row.
    fn inline_of(s: &StringStore, slot: u16, key: &[u8]) -> StringMeta {
        match s.read_live(slot, key) {
            Ok(Some(Stored::Inline(m))) => m,
            other => panic!("not a live inline string: {}", other.is_ok()),
        }
    }

    macro_rules! test_clock {
        ($static_name:ident, $fn_name:ident, $initial:expr) => {
            static $static_name: AtomicU64 = AtomicU64::new($initial);
            fn $fn_name() -> u64 {
                $static_name.load(Ordering::Relaxed)
            }
        };
    }

    /// A plain SET no longer reads the old header (it consumed nothing), so
    /// the two behaviours that read WAS incidentally providing are pinned
    /// here: an unconditional overwrite of an EXPIRED row must still land,
    /// and it must not inherit the dead row's expiry.
    #[test]
    fn plain_set_over_an_expired_row_lands_and_does_not_inherit_its_expiry() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        s.set(
            1,
            b"k",
            b"old",
            SetOptions {
                expiry: SetExpiry::AtMs(1_000_500),
                ..Default::default()
            },
        )
        .expect("seed");
        NOW.store(1_001_000, Ordering::Relaxed); // the row is now expired
        assert_eq!(s.get(1, b"k"), Ok(None), "expired row must read as absent");
        // The overwrite subsumes the lazy delete the old read used to do.
        assert_eq!(
            s.set(1, b"k", b"new", SetOptions::default()),
            Ok(SetOutcome::Done)
        );
        assert_eq!(s.get(1, b"k"), Ok(Some(b"new".to_vec())));
        NOW.store(9_999_999, Ordering::Relaxed);
        assert_eq!(
            s.get(1, b"k"),
            Ok(Some(b"new".to_vec())),
            "a plain SET clears expiry; it must not inherit the dead row's"
        );
    }

    /// KEEPTTL still reads, because it is one of the two callers that needs to.
    #[test]
    fn keepttl_still_preserves_expiry_after_the_read_was_made_conditional() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        s.set(
            1,
            b"k",
            b"a",
            SetOptions {
                expiry: SetExpiry::AtMs(1_005_000),
                ..Default::default()
            },
        )
        .expect("seed");
        s.set(
            1,
            b"k",
            b"b",
            SetOptions {
                expiry: SetExpiry::Keep,
                ..Default::default()
            },
        )
        .expect("keepttl");
        assert_eq!(s.get(1, b"k"), Ok(Some(b"b".to_vec())));
        NOW.store(1_006_000, Ordering::Relaxed);
        assert_eq!(
            s.get(1, b"k"),
            Ok(None),
            "KEEPTTL must have kept the expiry"
        );
    }

    #[test]
    fn set_get_with_conditions() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        assert_eq!(
            s.set(1, b"k", b"a", SetOptions::default()),
            Ok(SetOutcome::Done)
        );
        assert_eq!(s.get(1, b"k"), Ok(Some(b"a".to_vec())));
        let nx = SetOptions {
            nx: true,
            ..Default::default()
        };
        assert_eq!(s.set(1, b"k", b"b", nx), Ok(SetOutcome::Unchanged));
        let xx = SetOptions {
            xx: true,
            ..Default::default()
        };
        assert_eq!(s.set(1, b"k", b"c", xx), Ok(SetOutcome::Done));
        assert_eq!(s.get(1, b"k"), Ok(Some(b"c".to_vec())));
        assert_eq!(s.get(2, b"k"), Ok(None), "other slot is another row");
    }

    #[test]
    fn ttl_keep_and_clear_on_set() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        let at = SetOptions {
            expiry: SetExpiry::AtMs(1_000_500),
            ..Default::default()
        };
        s.set(1, b"k", b"v", at).expect("set");
        s.set(
            1,
            b"k",
            b"v2",
            SetOptions {
                expiry: SetExpiry::Keep,
                ..Default::default()
            },
        )
        .expect("set");
        // Expiry survived KEEPTTL: advancing past it kills the key.
        NOW.store(1_000_501, Ordering::Relaxed);
        assert_eq!(s.get(1, b"k"), Ok(None));
        // Plain SET clears TTL.
        NOW.store(1_000_000, Ordering::Relaxed);
        s.set(1, b"k2", b"v", at).expect("set");
        s.set(1, b"k2", b"v2", SetOptions::default()).expect("set");
        NOW.store(1_000_501, Ordering::Relaxed);
        assert_eq!(s.get(1, b"k2"), Ok(Some(b"v2".to_vec())));
    }

    #[test]
    fn incr_family() {
        test_clock!(NOW, now, 3_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        assert_eq!(s.incr_by(1, b"c", 1), Ok(1));
        assert_eq!(s.incr_by(1, b"c", 11), Ok(12));
        assert_eq!(s.incr_by(1, b"c", -6), Ok(6));
        s.set(1, b"s", b"abc", SetOptions::default()).expect("set");
        assert_eq!(s.incr_by(1, b"s", 1), Err(StoreError::NotInteger));
        s.set(
            1,
            b"max",
            i64::MAX.to_string().as_bytes(),
            SetOptions::default(),
        )
        .expect("set");
        assert_eq!(s.incr_by(1, b"max", 1), Err(StoreError::Overflow));
        // BUG-0213: Redis reads only a canonical integer. A leading zero, a
        // plus sign or "-0" is a string to it, and so the value is unchanged.
        for odd in [b"01".as_slice(), b"+1", b"-0", b" 1", b"1 ", b"-", b""] {
            s.set(1, b"odd", odd, SetOptions::default()).expect("set");
            assert_eq!(
                s.incr_by(1, b"odd", 1),
                Err(StoreError::NotInteger),
                "{odd:?}"
            );
            assert_eq!(s.get(1, b"odd"), Ok(Some(odd.to_vec())));
        }
    }

    #[test]
    fn redis_integers_are_canonical() {
        assert_eq!(parse_redis_i64(b"0"), Some(0));
        assert_eq!(parse_redis_i64(b"-7"), Some(-7));
        assert_eq!(parse_redis_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_redis_i64(b"-9223372036854775808"), Some(i64::MIN));
        for odd in [
            b"01".as_slice(),
            b"+1",
            b"-0",
            b"00",
            b"1.0",
            b"9223372036854775808",
            b"-9223372036854775809",
            b"",
            b"-",
        ] {
            assert_eq!(parse_redis_i64(odd), None, "{odd:?}");
        }
    }

    #[test]
    fn append_and_strlen() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        assert_eq!(s.append(1, b"a", b"he"), Ok(2));
        assert_eq!(s.append(1, b"a", b"llo"), Ok(5));
        assert_eq!(s.get(1, b"a"), Ok(Some(b"hello".to_vec())));
        assert_eq!(s.strlen(1, b"a"), Ok(5));
        assert_eq!(s.strlen(1, b"missing"), Ok(0));
    }

    #[test]
    fn incr_by_float_shapes_and_errors() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        // Missing key starts at 0; dyadic values are exact.
        assert_eq!(s.incr_by_float(1, b"f", 10.5), Ok(b"10.5".to_vec()));
        assert_eq!(s.incr_by_float(1, b"f", 0.25), Ok(b"10.75".to_vec()));
        assert_eq!(s.incr_by_float(1, b"f", -0.75), Ok(b"10".to_vec()));
        assert_eq!(s.get(1, b"f"), Ok(Some(b"10".to_vec())));
        // A negative zero, and a negative too small for 17 places, are
        // upstream's `0` (BUG-0233); one just large enough keeps its sign.
        assert_eq!(s.incr_by_float(1, b"z", -0.0), Ok(b"0".to_vec()));
        assert_eq!(s.incr_by_float(1, b"z", -4e-18), Ok(b"0".to_vec()));
        assert_eq!(s.get(1, b"z"), Ok(Some(b"0".to_vec())));
        assert_eq!(
            s.incr_by_float(1, b"z", -6e-18),
            Ok(b"-0.00000000000000001".to_vec())
        );
        // Exponent-form stored values parse; output is always human form.
        s.set(1, b"e", b"3.0e3", SetOptions::default())
            .expect("set");
        assert_eq!(s.incr_by_float(1, b"e", 200.0), Ok(b"3200".to_vec()));
        // Non-float value refuses.
        s.set(1, b"bad", b"hello", SetOptions::default())
            .expect("set");
        assert_eq!(s.incr_by_float(1, b"bad", 1.0), Err(StoreError::NotFloat));
        // Inf result refuses and leaves the value alone.
        s.set(1, b"inf", b"inf", SetOptions::default())
            .expect("set");
        assert_eq!(
            s.incr_by_float(1, b"inf", 1.0),
            Err(StoreError::NanOrInfinity)
        );
        // TTL is preserved.
        s.set(
            1,
            b"t1",
            b"1.5",
            SetOptions {
                expiry: SetExpiry::AtMs(2_000_000),
                ..Default::default()
            },
        )
        .expect("set");
        assert_eq!(s.incr_by_float(1, b"t1", 1.0), Ok(b"2.5".to_vec()));
        let m = inline_of(&s, 1, b"t1");
        assert_eq!(m.expire_ms, 2_000_000);
    }

    /// Bitmaps, against answers read from Valkey 9.1 (and, for BITOP's
    /// four Redis-only operators, Redis 8.2) on the same strings.
    #[test]
    fn bitmaps_answer_as_the_reference_servers_do() {
        let a: &[u8] = &[0xff, 0xf0, 0x00];
        let b: &[u8] = &[0x0f, 0x0f];
        let s6: &[u8] = b"foobar";
        assert_eq!(bitcount_of(a, None), 12);
        assert_eq!(bitcount_of(a, Some((1, 10, true))), 10);
        assert_eq!(bitcount_of(a, Some((-5, -1, true))), 0);
        // Two negative indexes the wrong way round: nothing, though the
        // clamped window would be the first byte.
        assert_eq!(bitcount_of(a, Some((-4, -5, false))), 0);
        assert_eq!(bitcount_of(a, Some((0, -1, false))), 12);
        assert_eq!(bitcount_of(s6, Some((1, 1, false))), 6);
        assert_eq!(bitcount_of(s6, Some((5, 30, true))), 17);
        assert_eq!(bitcount_of(b"", Some((0, -1, true))), 0);

        assert_eq!(bitpos_of(a, false, None), 12);
        assert_eq!(bitpos_of(a, true, None), 0);
        assert_eq!(bitpos_of(a, false, Some((0, Some(-1), true))), 12);
        assert_eq!(bitpos_of(a, true, Some((2, None, false))), -1);
        // No end given: a 0 is found just past the string.
        assert_eq!(bitpos_of(a, false, Some((2, None, false))), 16);
        assert_eq!(bitpos_of(a, false, Some((0, Some(1), false))), 12);
        // An end given: a range of ones holds no 0.
        assert_eq!(bitpos_of(a, false, Some((0, Some(0), false))), -1);
        // An empty string holds neither bit (a missing key is all zeros).
        assert_eq!(bitpos_of(b"", false, None), -1);
        assert_eq!(bitpos_of(b"", true, None), -1);
        // BITPOS has no wrong-way-round check: `-4 -5` clamps to byte 0.
        assert_eq!(bitpos_of(a, true, Some((-4, Some(-5), false))), 0);
        assert_eq!(bitpos_of(a, false, Some((-4, Some(-5), false))), -1);
        assert_eq!(bitpos_of(b, true, Some((3, Some(12), true))), 4);
        assert_eq!(bitpos_of(s6, false, Some((3, Some(12), true))), 3);

        let srcs = vec![a.to_vec(), b.to_vec(), s6.to_vec()];
        let hex = |v: Vec<u8>| v.iter().map(|b| format!("{b:02x}")).collect::<String>();
        assert_eq!(hex(bitop_of(BitOp::And, &srcs)), "060000000000");
        assert_eq!(hex(bitop_of(BitOp::Or, &srcs)), "ffff6f626172");
        assert_eq!(hex(bitop_of(BitOp::Xor, &srcs)), "96906f626172");
        assert_eq!(hex(bitop_of(BitOp::Not, &srcs[..1])), "000fff");
        assert_eq!(hex(bitop_of(BitOp::Diff, &srcs)), "909000000000");
        assert_eq!(hex(bitop_of(BitOp::Diff1, &srcs)), "000f6f626172");
        assert_eq!(hex(bitop_of(BitOp::AndOr, &srcs)), "6f6000000000");
        assert_eq!(hex(bitop_of(BitOp::One, &srcs)), "90906f626172");
        assert!(bitop_of(BitOp::Or, &[vec![], vec![]]).is_empty());
    }

    /// SETBIT grows the string, keeps its TTL, answers the old bit, and
    /// writes nothing when the bit already holds the value.
    #[test]
    fn setbit_grows_keeps_the_ttl_and_skips_a_no_op() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        assert_eq!(s.setbit(1, b"k", 9, true), Ok(false));
        assert_eq!(s.get(1, b"k"), Ok(Some(vec![0x00, 0x40])));
        assert_eq!(s.setbit(1, b"k", 9, true), Ok(true));
        assert_eq!(s.getbit(1, b"k", 9), Ok(true));
        assert_eq!(s.getbit(1, b"k", 1000), Ok(false));
        assert_eq!(s.getbit(1, b"missing", 0), Ok(false));
        s.set(
            1,
            b"t",
            b"\x00",
            SetOptions {
                expiry: SetExpiry::AtMs(2_000_000),
                ..Default::default()
            },
        )
        .expect("set");
        assert_eq!(s.setbit(1, b"t", 0, true), Ok(false));
        let m = inline_of(&s, 1, b"t");
        assert_eq!((m.payload, m.expire_ms), (vec![0x80], 2_000_000));
        // Clearing a clear bit inside the string writes nothing: with the
        // clock moved on, a rewritten row would carry a new write stamp.
        let before = kv.get(&s.meta_key(1, b"k"));
        NOW.store(1_500_000, Ordering::Relaxed);
        assert_eq!(s.setbit(1, b"k", 0, false), Ok(false));
        assert_eq!(kv.get(&s.meta_key(1, b"k")), before);
        assert_eq!(s.setbit(1, b"k", 0, true), Ok(false));
        assert_ne!(
            kv.get(&s.meta_key(1, b"k")),
            before,
            "a real change is written"
        );
        let capped = StringStore::with_max_value_bytes(&kv, b"t", now, 8);
        assert_eq!(
            capped.setbit(1, b"big", 64, true),
            Err(StoreError::ValueTooLarge)
        );
        assert_eq!(capped.setbit(1, b"big", 63, true), Ok(false));
    }

    #[test]
    fn getrange_windows() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        s.set(1, b"k", b"Hello World", SetOptions::default())
            .expect("set");
        assert_eq!(s.getrange(1, b"k", 0, 4), Ok(b"Hello".to_vec()));
        assert_eq!(s.getrange(1, b"k", -5, -1), Ok(b"World".to_vec()));
        assert_eq!(s.getrange(1, b"k", 0, -1), Ok(b"Hello World".to_vec()));
        assert_eq!(s.getrange(1, b"k", 9, 2), Ok(vec![]));
        assert_eq!(s.getrange(1, b"k", 50, 60), Ok(vec![]));
        assert_eq!(s.getrange(1, b"missing", 0, -1), Ok(vec![]));
    }

    /// BUG-0192: Valkey's overflow arithmetic at the edges, where a port of
    /// C to Rust goes wrong: `i64` and `u63` limits, wrap, saturation and
    /// refusal, and the value cap on what a write grows the string to.
    #[test]
    fn bitfield_overflows_as_valkey_does_at_every_edge() {
        use BitfieldKind::{Get, IncrBy, Set};
        use BitfieldOverflow::{Fail, Sat, Wrap};
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        let op = |signed, bits, offset, kind| BitfieldOp {
            signed,
            bits,
            offset,
            kind,
        };
        let run = |ops: &[BitfieldOp]| s.bitfield(1, b"k", ops).expect("bitfield");
        assert_eq!(
            run(&[
                op(true, 64, 0, Set(i64::MAX, Wrap)),
                op(true, 64, 0, IncrBy(1, Wrap)),
                op(true, 64, 0, IncrBy(-1, Sat)),
                op(true, 64, 0, IncrBy(-1, Fail)),
            ]),
            vec![Some(0), Some(i64::MIN), Some(i64::MIN), None]
        );
        // A u63 read over the i64 above: its top 63 bits, so 1 << 62.
        let umax = i64::MAX;
        assert_eq!(
            run(&[
                op(false, 63, 0, Set(umax, Wrap)),
                op(false, 63, 0, IncrBy(1, Wrap)),
                op(false, 63, 0, IncrBy(-1, Sat)),
                op(false, 63, 0, IncrBy(5, Fail)),
                op(false, 63, 0, Get),
            ]),
            vec![Some(1 << 62), Some(0), Some(0), Some(5), Some(5)]
        );
        assert_eq!(
            run(&[
                op(true, 3, 70, Set(3, Wrap)),
                op(true, 3, 70, IncrBy(1, Wrap)),
                op(true, 3, 70, IncrBy(-9, Sat)),
                op(false, 1, 70, Get),
            ]),
            vec![Some(0), Some(-4), Some(-4), Some(1)]
        );
        // Growth is bounded by the value cap before anything is written.
        let capped = StringStore::with_max_value_bytes(&kv, b"t", now, 8);
        let far = [op(false, 8, 64, Set(1, Wrap))];
        assert_eq!(
            capped.bitfield(1, b"c", &far),
            Err(StoreError::ValueTooLarge)
        );
        assert_eq!(capped.get(1, b"c"), Ok(None));
    }

    #[test]
    fn setrange_pads_preserves_ttl() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        // Missing key + offset: zero-padded creation.
        assert_eq!(s.setrange(1, b"k", 5, b"World"), Ok(10));
        assert_eq!(s.get(1, b"k"), Ok(Some(b"\0\0\0\0\0World".to_vec())));
        // Overwrite inside an existing value.
        s.set(1, b"k2", b"Hello World", SetOptions::default())
            .expect("set");
        assert_eq!(s.setrange(1, b"k2", 6, b"Redis"), Ok(11));
        assert_eq!(s.get(1, b"k2"), Ok(Some(b"Hello Redis".to_vec())));
        // Empty patch never creates the key (pure length probe).
        assert_eq!(s.setrange(1, b"nope", 0, b""), Ok(0));
        assert_eq!(s.get(1, b"nope"), Ok(None));
        // TTL survives the in-place mutation.
        s.set(
            1,
            b"k3",
            b"hello",
            SetOptions {
                expiry: SetExpiry::AtMs(2_000_000),
                ..Default::default()
            },
        )
        .expect("set");
        assert_eq!(s.setrange(1, b"k3", 0, b"H"), Ok(5));
        let m = inline_of(&s, 1, b"k3");
        assert_eq!(m.expire_ms, 2_000_000);
        // The cap is enforced on the extended length.
        let capped = StringStore::with_max_value_bytes(&kv, b"t", now, 8);
        assert_eq!(
            capped.setrange(1, b"c", 6, b"abc"),
            Err(StoreError::ValueTooLarge)
        );
    }

    #[test]
    fn wrongtype_and_set_overwrites_hash() {
        test_clock!(NOW, now, 1_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now);
        let h = HashStore::new(&kv, b"t", now);
        h.hset(1, b"h", &[(b"f".to_vec(), b"v".to_vec())])
            .expect("hset");
        // String reads on a hash key are WRONGTYPE…
        assert_eq!(s.get(1, b"h"), Err(StoreError::WrongType));
        assert_eq!(s.incr_by(1, b"h", 1), Err(StoreError::WrongType));
        assert_eq!(s.append(1, b"h", b"x"), Err(StoreError::WrongType));
        assert_eq!(s.strlen(1, b"h"), Err(StoreError::WrongType));
        // …but plain SET overwrites any type (Redis semantics).
        assert_eq!(
            s.set(1, b"h", b"now-a-string", SetOptions::default()),
            Ok(SetOutcome::Done)
        );
        assert_eq!(s.get(1, b"h"), Ok(Some(b"now-a-string".to_vec())));
    }

    test_clock!(CHUNK_NOW, chunk_now, 5_000_000);

    /// ADR-0056: random runs of every string command, on a store that
    /// chunks past 16 bytes in 8-byte chunks and on one that never chunks,
    /// answer the same, reply for reply, and leave the same values, which
    /// the sweeper does not disturb. The small sizes put every chunk edge
    /// within reach of short strings.
    #[test]
    fn chunked_strings_answer_as_inline_ones_do() {
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        let mut rand = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let mut chunked_seen = 0;
        for run in 0..250 {
            let (kv_a, kv_b) = (MemKv::new(), MemKv::new());
            let plain = StringStore::new(&kv_a, b"t", chunk_now);
            let chunked = StringStore::new(&kv_b, b"t", chunk_now).with_chunks(16, 8);
            let (ks_a, ks_b) = (
                crate::keyspace::Keyspace::new(&kv_a, b"t", chunk_now),
                crate::keyspace::Keyspace::new(&kv_b, b"t", chunk_now),
            );
            let keys: [&[u8]; 2] = [b"a", b"b"];
            for step in 0..60 {
                let key = keys[rand(2) as usize];
                let bytes = |rand: &mut dyn FnMut(u64) -> u64, n: u64| -> Vec<u8> {
                    (0..n)
                        .map(|_| [0u8, 0, 0x5a, 0xff, 1, 0x80][rand(6) as usize])
                        .collect()
                };
                let what = rand(17);
                let ctx = format!("run {run} step {step} op {what} key {key:?}");
                match what {
                    0 => {
                        let n = rand(48);
                        let v = bytes(&mut rand, n);
                        assert_eq!(
                            plain.set(1, key, &v, SetOptions::default()),
                            chunked.set(1, key, &v, SetOptions::default()),
                            "{ctx}"
                        );
                    }
                    1 => {
                        let n = 1 + rand(20);
                        let v = bytes(&mut rand, n);
                        assert_eq!(
                            plain.append(1, key, &v),
                            chunked.append(1, key, &v),
                            "{ctx}"
                        );
                    }
                    2 => {
                        let (at, n) = (rand(60), rand(20));
                        let v = bytes(&mut rand, n);
                        assert_eq!(
                            plain.setrange(1, key, at, &v),
                            chunked.setrange(1, key, at, &v),
                            "{ctx}"
                        );
                    }
                    3 => {
                        let (a, b) = (rand(140) as i64 - 70, rand(140) as i64 - 70);
                        assert_eq!(
                            plain.getrange(1, key, a, b),
                            chunked.getrange(1, key, a, b),
                            "{ctx}"
                        );
                    }
                    4 => {
                        let (at, on) = (rand(500), rand(2) == 1);
                        assert_eq!(
                            plain.setbit(1, key, at, on),
                            chunked.setbit(1, key, at, on),
                            "{ctx}"
                        );
                    }
                    5 => {
                        let at = rand(520);
                        assert_eq!(
                            plain.getbit(1, key, at),
                            chunked.getbit(1, key, at),
                            "{ctx}"
                        );
                    }
                    6 => {
                        let range = (rand(3) > 0)
                            .then(|| (rand(140) as i64 - 70, rand(140) as i64 - 70, rand(2) == 1));
                        assert_eq!(
                            plain.bitcount(1, key, range),
                            chunked.bitcount(1, key, range),
                            "{ctx} {range:?}"
                        );
                    }
                    7 => {
                        let bit = rand(2) == 1;
                        let range = (rand(3) > 0).then(|| {
                            let end = (rand(2) == 1).then(|| rand(140) as i64 - 70);
                            (rand(140) as i64 - 70, end, rand(2) == 1)
                        });
                        assert_eq!(
                            plain.bitpos(1, key, bit, range),
                            chunked.bitpos(1, key, bit, range),
                            "{ctx} {bit} {range:?}"
                        );
                    }
                    8 => {
                        let ops: Vec<BitfieldOp> = (0..1 + rand(3))
                            .map(|_| {
                                let signed = rand(2) == 1;
                                let bits = 1 + rand(if signed { 64 } else { 63 }) as u32;
                                let overflow = [
                                    BitfieldOverflow::Wrap,
                                    BitfieldOverflow::Sat,
                                    BitfieldOverflow::Fail,
                                ][rand(3) as usize];
                                let v = rand(1000) as i64 - 500;
                                let kind = match rand(3) {
                                    0 => BitfieldKind::Get,
                                    1 => BitfieldKind::Set(v, overflow),
                                    _ => BitfieldKind::IncrBy(v, overflow),
                                };
                                BitfieldOp {
                                    signed,
                                    bits,
                                    offset: rand(400),
                                    kind,
                                }
                            })
                            .collect();
                        assert_eq!(
                            plain.bitfield(1, key, &ops),
                            chunked.bitfield(1, key, &ops),
                            "{ctx} {ops:?}"
                        );
                    }
                    9 => assert_eq!(plain.strlen(1, key), chunked.strlen(1, key), "{ctx}"),
                    10 => {
                        if rand(4) == 0 {
                            assert_eq!(plain.get_del(1, key), chunked.get_del(1, key), "{ctx}");
                        }
                    }
                    11 => {
                        let e = match rand(3) {
                            0 => SetExpiry::Keep,
                            1 => SetExpiry::Clear,
                            _ => SetExpiry::AtMs(9_000_000),
                        };
                        assert_eq!(plain.getex(1, key, e), chunked.getex(1, key, e), "{ctx}");
                        assert_eq!(
                            ks_a.expire_time_ms(1, key),
                            ks_b.expire_time_ms(1, key),
                            "{ctx}"
                        );
                    }
                    12 => assert_eq!(
                        plain.incr_by(1, key, 3),
                        chunked.incr_by(1, key, 3),
                        "{ctx}"
                    ),
                    13 => {
                        let (src, dst) = (keys[rand(2) as usize], keys[rand(2) as usize]);
                        assert_eq!(
                            ks_a.copy(1, src, dst, true),
                            ks_b.copy(1, src, dst, true),
                            "{ctx}"
                        );
                    }
                    14 => {
                        let (src, dst) = (keys[rand(2) as usize], keys[rand(2) as usize]);
                        assert_eq!(
                            ks_a.rename(1, src, dst, false),
                            ks_b.rename(1, src, dst, false),
                            "{ctx}"
                        );
                    }
                    15 => {
                        if rand(4) == 0 {
                            assert_eq!(ks_a.del(1, key), ks_b.del(1, key), "{ctx}");
                        }
                    }
                    _ => {
                        assert_eq!(
                            ks_a.expire_at(1, key, 8_000_000),
                            ks_b.expire_at(1, key, 8_000_000),
                            "{ctx}"
                        );
                    }
                }
                for k in keys {
                    assert_eq!(plain.get(1, k), chunked.get(1, k), "{ctx} then GET {k:?}");
                    let names =
                        |ks: &crate::keyspace::Keyspace| ks.value_type(1, k).map(|t| t.name());
                    assert_eq!(names(&ks_a), names(&ks_b), "{ctx} then TYPE {k:?}");
                    if ks_b.value_type(1, k) == Some(ValueType::ChunkedString) {
                        chunked_seen += 1;
                    }
                }
            }
            // The sweeper keeps every live chunk, and reclaims the rest.
            crate::gc::sweep(&kv_b, chunk_now(), &crate::gc::unguarded);
            for k in keys {
                assert_eq!(
                    plain.get(1, k),
                    chunked.get(1, k),
                    "run {run} after the sweep"
                );
            }
        }
        assert!(
            chunked_seen > 1000,
            "only {chunked_seen} chunked observations"
        );
    }

    /// The rows a store holds, keys and values.
    fn rows(kv: &MemKv) -> std::collections::BTreeMap<Vec<u8>, Vec<u8>> {
        kv.scan_prefix(b"").into_iter().collect()
    }

    /// A store that records the bytes each write puts and the rows each
    /// read touches.
    struct Recording {
        inner: MemKv,
        puts: std::sync::Mutex<Vec<usize>>,
        read: std::sync::atomic::AtomicUsize,
    }

    impl Recording {
        fn new() -> Self {
            Recording {
                inner: MemKv::new(),
                puts: Default::default(),
                read: Default::default(),
            }
        }
        /// The value sizes put, and the rows read, since the last call.
        fn take(&self) -> (Vec<usize>, usize) {
            let puts = std::mem::take(&mut *self.puts.lock().expect("lock"));
            (
                puts,
                self.read.swap(0, std::sync::atomic::Ordering::Relaxed),
            )
        }
        fn counted<'a>(
            &'a self,
            visit: &'a mut dyn FnMut(&[u8], &[u8]) -> bool,
        ) -> impl FnMut(&[u8], &[u8]) -> bool + 'a {
            move |k, v| {
                self.read.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                visit(k, v)
            }
        }
    }

    impl Kv for Recording {
        fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
            self.read.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.get(key)
        }
        fn put(&self, key: &[u8], value: &[u8]) {
            self.puts.lock().expect("lock").push(value.len());
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

    /// ADR-0056 D3: SETBIT, SETRANGE and APPEND on an 8 MiB chunked string
    /// put one chunk and the metadata row, and GETBIT, GETRANGE and STRLEN
    /// read a chunk or two, where an inline string reads and writes all of
    /// it.
    #[test]
    fn a_write_to_a_chunked_string_writes_its_chunks_only() {
        let kv = Recording::new();
        let s = StringStore::new(&kv, b"t", chunk_now).chunked(true);
        let big = vec![0x5au8; 8 * 1024 * 1024];
        s.set(1, b"bm", &big, SetOptions::default()).expect("set");
        assert_eq!(
            kv.take().0.len(),
            1 + big.len() / CHUNK_BYTES,
            "a row per chunk"
        );
        let small = |puts: &[usize]| puts.iter().all(|&n| n <= CHUNK_BYTES);
        // 0x5a is 0b0101_1010: bit 0 of each byte is clear.
        assert_eq!(s.setbit(1, b"bm", 5_000_000, true), Ok(false));
        let (puts, read) = kv.take();
        assert!(
            puts.len() == 2 && small(&puts),
            "one chunk and the meta row: {puts:?}"
        );
        assert!(read <= 3, "{read} rows read");
        // A SETBIT that changes nothing writes nothing.
        assert_eq!(s.setbit(1, b"bm", 5_000_000, true), Ok(true));
        assert_eq!(kv.take().0, Vec::<usize>::new());
        s.setrange(1, b"bm", 1_000_000, b"hello").expect("setrange");
        let (puts, read) = kv.take();
        assert!(puts.len() == 2 && small(&puts), "{puts:?}");
        assert!(read <= 3, "{read} rows read");
        // The same bytes again: a write to WATCH, so the metadata row is
        // rewritten, and the chunk, unchanged, is not.
        s.setrange(1, b"bm", 1_000_000, b"hello").expect("setrange");
        let (puts, _) = kv.take();
        assert!(
            puts.len() == 1 && small(&puts),
            "the meta row only: {puts:?}"
        );
        // A BITFIELD SET of the value already there changes nothing, and
        // writes nothing, as before chunks.
        let same = BitfieldOp {
            signed: false,
            bits: 8,
            offset: 2_000_000 * 8,
            kind: BitfieldKind::Set(0x5a, BitfieldOverflow::Wrap),
        };
        assert_eq!(s.bitfield(1, b"bm", &[same]), Ok(vec![Some(0x5a)]));
        assert_eq!(kv.take().0, Vec::<usize>::new());
        s.append(1, b"bm", b"!").expect("append");
        let (puts, _) = kv.take();
        assert_eq!(
            puts.len(),
            2,
            "the new tail chunk and the meta row: {puts:?}"
        );
        assert!(
            puts.contains(&1),
            "a chunk ends where the string does: {puts:?}"
        );
        assert_eq!(s.strlen(1, b"bm"), Ok(big.len() + 1));
        assert_eq!(kv.take().1, 1, "STRLEN reads the meta row only");
        assert_eq!(
            s.getrange(1, b"bm", 1_000_000, 1_000_004),
            Ok(b"hello".to_vec())
        );
        assert!(kv.take().1 <= 3);
        assert_eq!(s.getrange(1, b"bm", -2, -1), Ok(b"Z!".to_vec()));
        assert!(kv.take().1 <= 4, "the last two chunks");
        assert_eq!(s.getbit(1, b"bm", 5_000_000), Ok(true));
        assert!(kv.take().1 <= 3);
    }

    /// A sparse bitmap stores only the chunks it has bits in: a bit set at
    /// offset 100 million makes a 12.5 MB string of two rows, not 12.5 MB of
    /// zeros.
    #[test]
    fn a_sparse_bitmap_stores_only_its_set_chunks() {
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", chunk_now).chunked(true);
        assert_eq!(s.setbit(1, b"dau", 100_000_000, true), Ok(false));
        assert_eq!(s.strlen(1, b"dau"), Ok(12_500_001));
        assert_eq!(rows(&kv).len(), 2, "the metadata row and one chunk");
        assert_eq!(s.bitcount(1, b"dau", None), Ok(1));
        assert_eq!(s.bitpos(1, b"dau", true, None), Ok(100_000_000));
        assert_eq!(s.getbit(1, b"dau", 99_999_999), Ok(false));
        // A SET of zeros with one byte set stores one chunk, as SETBIT does.
        let kv3 = MemKv::new();
        let z = StringStore::new(&kv3, b"t", chunk_now).chunked(true);
        let mut zeros = vec![0u8; 1 << 20];
        zeros[(1 << 20) - 1] = 1;
        z.set(1, b"z", &zeros, SetOptions::default()).expect("set");
        assert_eq!(rows(&kv3).len(), 2, "the metadata row and the last chunk");
        assert_eq!(z.get(1, b"z"), Ok(Some(zeros)));
        // Chunked past 64 KiB, and not at it.
        let ks = crate::keyspace::Keyspace::new(&kv, b"t", chunk_now);
        for (key, len, kind) in [
            (b"at".as_slice(), INLINE_MAX, ValueType::String),
            (b"past", INLINE_MAX + 1, ValueType::ChunkedString),
        ] {
            s.set(1, key, &vec![1; len], SetOptions::default())
                .expect("set");
            assert_eq!(ks.value_type(1, key), Some(kind), "SET of {len}");
            s.set(1, key, b"", SetOptions::default()).expect("set");
            s.setrange(1, key, len as u64 - 1, b"x").expect("setrange");
            assert_eq!(ks.value_type(1, key), Some(kind), "SETRANGE to {len}");
        }
        // Off by default: the same write stores the zeros inline.
        let kv2 = MemKv::new();
        let off = StringStore::new(&kv2, b"t", chunk_now);
        off.setbit(1, b"dau", 1_000_000, true).expect("setbit");
        assert_eq!(rows(&kv2).len(), 1, "inline when chunked writes are off");
    }

    /// A chunked string is a string to every client, and the sweeper keeps
    /// its chunks while it lives and reclaims them once it is replaced or
    /// deleted (ADR-0056 D1, D5).
    #[test]
    fn a_chunked_string_is_a_string_and_its_chunks_live_as_long_as_it() {
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", chunk_now).chunked(true);
        let ks = crate::keyspace::Keyspace::new(&kv, b"t", chunk_now);
        let v: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        s.set(1, b"k", &v, SetOptions::default()).expect("set");
        assert_eq!(ks.value_type(1, b"k"), Some(ValueType::ChunkedString));
        assert_eq!(ks.value_type(1, b"k").map(|t| t.name()), Some("string"));
        assert_eq!(ks.key_stat(1, b"k").map(|k| k.size_bytes), Some(200_000));
        let report = crate::gc::sweep(&kv, chunk_now(), &crate::gc::unguarded);
        assert_eq!(report.orphan_rows, 0);
        assert_eq!(s.get(1, b"k"), Ok(Some(v.clone())));
        // Replaced by a short value: inline, and the chunks are orphans.
        s.set(1, b"k", b"short", SetOptions::default())
            .expect("set");
        assert_eq!(ks.value_type(1, b"k"), Some(ValueType::String));
        let report = crate::gc::sweep(&kv, chunk_now(), &crate::gc::unguarded);
        assert_eq!(report.orphan_rows, 7, "200,000 bytes in 32 KiB chunks");
        assert_eq!(s.get(1, b"k"), Ok(Some(b"short".to_vec())));
        // INCR refuses a chunked string unread.
        s.set(1, b"k", &v, SetOptions::default()).expect("set");
        assert_eq!(s.incr_by(1, b"k", 1), Err(StoreError::NotInteger));
        assert_eq!(s.incr_by_float(1, b"k", 1.0), Err(StoreError::NotFloat));
        assert!(ks.del(1, b"k"));
        let report = crate::gc::sweep(&kv, chunk_now(), &crate::gc::unguarded);
        assert_eq!(report.orphan_rows, 7);
        assert_eq!(rows(&kv).len(), 0);
    }
}
