// SPDX-License-Identifier: Elastic-2.0
//! StringStore: the string-type TypeStore over any `Kv`.
//!
//! Owns Redis string semantics: SET options, the integer-string commands,
//! APPEND/STRLEN. Type-agnostic keyspace ops (DEL, EXISTS, TYPE, TTL…)
//! live in `keyspace`. The clock is injected so expiry is testable without
//! sleeping and the replicated apply path stays deterministic (expire-at
//! replicates; wall clocks don't).

use crate::Kv;
use crate::encoding::{Cf, MetaHeader, StringMeta, ValueType, envelope};

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
}

pub struct StringStore<'a> {
    kv: &'a dyn Kv,
    ns: Vec<u8>,
    clock: Clock,
    max_value_bytes: u64,
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
        }
    }

    fn meta_key(&self, slot: u16, key: &[u8]) -> Vec<u8> {
        envelope(Cf::Metadata, &self.ns, slot, key)
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

    /// Live string row; WRONGTYPE if the key holds another type.
    fn read_live(&self, slot: u16, key: &[u8]) -> Result<Option<StringMeta>, StoreError> {
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
            Live(Box<StringMeta>),
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
            if header.value_type() != Some(ValueType::String) {
                outcome = Row::WrongType;
                return;
            }
            outcome = match StringMeta::decode(row) {
                Some(m) => Row::Live(Box::new(m)),
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
            Row::Live(m) => Ok(Some(*m)),
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
        // delete. Orphaned subkeys of a displaced complex type are the GC
        // sweeper's job either way — SET never cleaned those up.
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
        let meta = StringMeta::new(value.to_vec(), expire_ms, (self.clock)());
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        Ok(SetOutcome::Done)
    }

    pub fn get(&self, slot: u16, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.read_live(slot, key)?.map(|m| m.payload))
    }

    /// GETDEL: return the value and delete the key atomically (one node, one
    /// slot). WRONGTYPE if the key holds a non-string (read_live checks type).
    pub fn get_del(&self, slot: u16, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let existing = self.read_live(slot, key)?.map(|m| m.payload);
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
        let Some(mut meta) = self.read_live(slot, key)? else {
            return Ok(None);
        };
        let new_expire = match expiry {
            // GETEX with no option is a plain GET: the TTL is untouched,
            // which is NOT the same as clearing it.
            SetExpiry::Keep => return Ok(Some(meta.payload)),
            SetExpiry::AtMs(at) => at,
            // PERSIST.
            SetExpiry::Clear => 0,
        };
        if new_expire != meta.expire_ms {
            meta.expire_ms = new_expire;
            self.kv.put(&self.meta_key(slot, key), &meta.encode());
        }
        Ok(Some(meta.payload))
    }

    /// INCRBY/DECRBY. Creates the key at 0. Preserves TTL.
    pub fn incr_by(&self, slot: u16, key: &[u8], delta: i64) -> Result<i64, StoreError> {
        let existing = self.read_live(slot, key)?;
        let (current, expire_ms) = match &existing {
            None => (0i64, 0u64),
            Some(m) => {
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
        let existing = self.read_live(slot, key)?;
        let (current, expire_ms) = match &existing {
            None => (0f64, 0u64),
            Some(m) => {
                let s = std::str::from_utf8(&m.payload).map_err(|_| StoreError::NotFloat)?;
                let v: f64 = s.parse().map_err(|_| StoreError::NotFloat)?;
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

    /// APPEND: returns new length. Creates the key. Preserves TTL.
    pub fn append(&self, slot: u16, key: &[u8], suffix: &[u8]) -> Result<usize, StoreError> {
        let existing = self.read_live(slot, key)?;
        let (mut payload, expire_ms) = match existing {
            None => (Vec::new(), 0),
            Some(m) => (m.payload, m.expire_ms),
        };
        // The incremental hole SET's check can't close: repeated APPENDs
        // must not build a value past the cap (Valkey checkStringLength).
        if (payload.len() + suffix.len()) as u64 > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        payload.extend_from_slice(suffix);
        let len = payload.len();
        let meta = StringMeta::new(payload, expire_ms, (self.clock)());
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        Ok(len)
    }

    pub fn strlen(&self, slot: u16, key: &[u8]) -> Result<usize, StoreError> {
        Ok(self.read_live(slot, key)?.map_or(0, |m| m.payload.len()))
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
        let Some(m) = self.read_live(slot, key)? else {
            return Ok(Vec::new());
        };
        // Redis's rules (BUG-0214), which are not LRANGE's: two negative
        // indexes in the wrong order answer empty, and an end before the
        // start of the string is clamped to its first byte, not dropped.
        // `GETRANGE k 0 -100` answers the first byte.
        if start < 0 && end < 0 && start > end {
            return Ok(Vec::new());
        }
        let len = m.payload.len() as i64;
        let norm = |i: i64| if i < 0 { len + i } else { i };
        let from = norm(start).max(0);
        let to = norm(end).max(0).min(len - 1);
        if len == 0 || from > to {
            return Ok(Vec::new());
        }
        Ok(m.payload[from as usize..=(to as usize)].to_vec())
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
        let existing = self.read_live(slot, key)?;
        if patch.is_empty() {
            return Ok(existing.map_or(0, |m| m.payload.len()));
        }
        let (mut payload, expire_ms) = match existing {
            None => (Vec::new(), 0),
            Some(m) => (m.payload, m.expire_ms),
        };
        let end = offset + patch.len() as u64;
        if end > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        let end = end as usize;
        if payload.len() < end {
            payload.resize(end, 0);
        }
        payload[offset as usize..end].copy_from_slice(patch);
        let len = payload.len();
        let meta = StringMeta::new(payload, expire_ms, (self.clock)());
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        Ok(len)
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
        let existing = self.read_live(slot, key)?;
        let highest = ops
            .iter()
            .filter(|op| !matches!(op.kind, BitfieldKind::Get))
            .map(|op| op.offset + u64::from(op.bits) - 1)
            .max();
        let Some(highest) = highest else {
            let payload = existing.map(|m| m.payload).unwrap_or_default();
            return Ok(ops.iter().map(|op| Some(op.read(&payload))).collect());
        };
        let need = (highest >> 3) + 1;
        if need > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        let (mut payload, expire_ms, mut dirty) = match existing {
            None => (Vec::new(), 0, true),
            Some(m) => (m.payload, m.expire_ms, false),
        };
        if (payload.len() as u64) < need {
            payload.resize(need as usize, 0);
            dirty = true;
        }
        let mut replies = Vec::with_capacity(ops.len());
        for op in ops {
            let reply = match op.kind {
                BitfieldKind::Get => Some(op.read(&payload)),
                BitfieldKind::Set(value, overflow) | BitfieldKind::IncrBy(value, overflow) => {
                    let incr = matches!(op.kind, BitfieldKind::IncrBy(..));
                    let old = op.read(&payload);
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
                        write_bits(&mut payload, op.offset, op.bits, new as u64);
                        dirty |= op.read(&payload) != old;
                        Some(if incr { new } else { old })
                    }
                }
            };
            replies.push(reply);
        }
        if dirty {
            let meta = StringMeta::new(payload, expire_ms, (self.clock)());
            self.kv.put(&self.meta_key(slot, key), &meta.encode());
        }
        Ok(replies)
    }
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
    /// The field's value in `payload`, sign-extended when signed. Bits past
    /// the end of `payload` read as zero.
    fn read(&self, payload: &[u8]) -> i64 {
        let mut v = 0u64;
        for i in 0..u64::from(self.bits) {
            let bit = self.offset + i;
            let byte = payload.get((bit >> 3) as usize).copied().unwrap_or(0);
            v = (v << 1) | u64::from((byte >> (7 - (bit & 7))) & 1);
        }
        if self.signed && self.bits < 64 && v & (1 << (self.bits - 1)) != 0 {
            v |= u64::MAX << self.bits;
        }
        v as i64
    }
}

/// Store the low `bits` bits of `value` at bit `offset`; `payload` already
/// covers them.
fn write_bits(payload: &mut [u8], offset: u64, bits: u32, value: u64) {
    for i in 0..u64::from(bits) {
        let bit = offset + i;
        let set = (value >> (u64::from(bits) - 1 - i)) & 1 == 1;
        let mask = 1u8 << (7 - (bit & 7));
        let byte = &mut payload[(bit >> 3) as usize];
        if set {
            *byte |= mask;
        } else {
            *byte &= !mask;
        }
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
/// zeros, then a bare trailing dot. (`10.75` → "10.75", `3.0` → "3".) On
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
    s.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;
    use crate::hashes::HashStore;
    use std::sync::atomic::{AtomicU64, Ordering};

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
        let m = s.read_live(1, b"t1").expect("read").expect("live");
        assert_eq!(m.expire_ms, 2_000_000);
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
        let m = s.read_live(1, b"k3").expect("read").expect("live");
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
}
