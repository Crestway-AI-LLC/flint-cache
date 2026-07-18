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
    WrongType,
    /// The write would grow the value past the max-value-bytes policy
    /// (Valkey's `checkStringLength` analog, extended to collections).
    /// Enforced atomically: the store is untouched when this returns.
    ValueTooLarge,
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
        if header.value_type() != Some(ValueType::String) {
            return Err(StoreError::WrongType);
        }
        Ok(StringMeta::decode(&row))
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
        let meta = StringMeta::new(value.to_vec(), expire_ms);
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
        let meta = StringMeta::new(next.to_string().into_bytes(), expire_ms);
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        Ok(next)
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
        let meta = StringMeta::new(payload, expire_ms);
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        Ok(len)
    }

    pub fn strlen(&self, slot: u16, key: &[u8]) -> Result<usize, StoreError> {
        Ok(self.read_live(slot, key)?.map_or(0, |m| m.payload.len()))
    }
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
