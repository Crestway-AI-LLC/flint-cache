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

#[derive(Debug, PartialEq, Eq)]
pub enum StoreError {
    NotInteger,
    Overflow,
    NotFloat,
    /// A float op's result left the representable range (Redis refuses to
    /// store NaN/Infinity from INCRBYFLOAT).
    NanOrInfinity,
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
        let existing = self.read_live_header(slot, key);
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
                let s = std::str::from_utf8(&m.payload).map_err(|_| StoreError::NotInteger)?;
                let n: i64 = s.parse().map_err(|_| StoreError::NotInteger)?;
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
        let len = m.payload.len() as i64;
        let norm = |i: i64| if i < 0 { len + i } else { i };
        let from = norm(start).max(0);
        let to = norm(end).min(len - 1);
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
}

/// Redis's LD_STR_HUMAN float shape: fixed `%.17f`, then trim trailing
/// zeros, then a bare trailing dot. (`10.75` → "10.75", `3.0` → "3".) On
/// aarch64 `long double` IS `double`, so f64 reproduces the reference
/// output bit-for-bit on this platform class — the conformance oracle
/// referees.
fn fmt_float_human(x: f64) -> Vec<u8> {
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
