//! StringStore: the string-type TypeStore over any `Kv`.
//!
//! Owns Redis string semantics — SET options, TTL behavior (absolute
//! expire-at in the row, lazy deletion on read), and the integer-string
//! commands. The clock is injected so expiry is testable without sleeping;
//! deterministic time is also what the replicated apply path will need
//! (expire-at replicates, wall clocks don't).

use crate::Kv;
use crate::encoding::{Cf, StringMeta, ValueType, envelope};

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
}

/// TTL query result, Redis-shaped.
#[derive(Debug, PartialEq, Eq)]
pub enum Ttl {
    Missing,  // -2
    NoExpiry, // -1
    Ms(u64),  // remaining
}

pub struct StringStore<'a> {
    kv: &'a dyn Kv,
    ns: Vec<u8>,
    clock: Clock,
}

impl<'a> StringStore<'a> {
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

    /// Reads the live (non-expired) metadata row; lazily deletes expired rows.
    fn read_live(&self, slot: u16, key: &[u8]) -> Option<StringMeta> {
        let mk = self.meta_key(slot, key);
        let row = self.kv.get(&mk)?;
        let meta = StringMeta::decode(&row)?;
        if meta.is_expired((self.clock)()) {
            self.kv.delete(&mk);
            return None;
        }
        Some(meta)
    }

    pub fn set(&self, slot: u16, key: &[u8], value: &[u8], opts: SetOptions) -> SetOutcome {
        let existing = self.read_live(slot, key);
        if (opts.nx && existing.is_some()) || (opts.xx && existing.is_none()) {
            return SetOutcome::Unchanged;
        }
        let expire_ms = match opts.expiry {
            SetExpiry::Clear => 0,
            SetExpiry::Keep => existing.map(|m| m.expire_ms).unwrap_or(0),
            SetExpiry::AtMs(at) => at,
        };
        let meta = StringMeta::new(value.to_vec(), expire_ms);
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        SetOutcome::Done
    }

    pub fn get(&self, slot: u16, key: &[u8]) -> Option<Vec<u8>> {
        self.read_live(slot, key).map(|m| m.payload)
    }

    pub fn del(&self, slot: u16, key: &[u8]) -> bool {
        // Deleting an expired-but-present row must not count.
        let live = self.read_live(slot, key).is_some();
        if live {
            self.kv.delete(&self.meta_key(slot, key));
        }
        live
    }

    pub fn exists(&self, slot: u16, key: &[u8]) -> bool {
        self.read_live(slot, key).is_some()
    }

    pub fn value_type(&self, slot: u16, key: &[u8]) -> Option<ValueType> {
        self.read_live(slot, key)
            .and_then(|m| ValueType::from_flags(m.flags))
    }

    /// EXPIRE/PEXPIRE: returns false if the key doesn't exist.
    pub fn expire_at(&self, slot: u16, key: &[u8], at_ms: u64) -> bool {
        let Some(mut meta) = self.read_live(slot, key) else {
            return false;
        };
        if at_ms <= (self.clock)() {
            // Setting an expiry in the past deletes immediately (Redis).
            self.kv.delete(&self.meta_key(slot, key));
            return true;
        }
        meta.expire_ms = at_ms;
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        true
    }

    pub fn ttl(&self, slot: u16, key: &[u8]) -> Ttl {
        match self.read_live(slot, key) {
            None => Ttl::Missing,
            Some(m) if m.expire_ms == 0 => Ttl::NoExpiry,
            Some(m) => Ttl::Ms(m.expire_ms - (self.clock)()),
        }
    }

    /// PERSIST: true if an expiry existed and was removed.
    pub fn persist(&self, slot: u16, key: &[u8]) -> bool {
        let Some(mut meta) = self.read_live(slot, key) else {
            return false;
        };
        if meta.expire_ms == 0 {
            return false;
        }
        meta.expire_ms = 0;
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        true
    }

    /// INCRBY/DECRBY. Creates the key at 0 first. Preserves TTL.
    pub fn incr_by(&self, slot: u16, key: &[u8], delta: i64) -> Result<i64, StoreError> {
        let existing = self.read_live(slot, key);
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
    pub fn append(&self, slot: u16, key: &[u8], suffix: &[u8]) -> usize {
        let existing = self.read_live(slot, key);
        let (mut payload, expire_ms) = match existing {
            None => (Vec::new(), 0),
            Some(m) => (m.payload, m.expire_ms),
        };
        payload.extend_from_slice(suffix);
        let len = payload.len();
        let meta = StringMeta::new(payload, expire_ms);
        self.kv.put(&self.meta_key(slot, key), &meta.encode());
        len
    }

    pub fn strlen(&self, slot: u16, key: &[u8]) -> usize {
        self.read_live(slot, key).map_or(0, |m| m.payload.len())
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

    #[test]
    fn set_get_with_conditions() {
        test_clock!(NOW_A, now_a, 1_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now_a);
        assert_eq!(
            s.set(1, b"k", b"a", SetOptions::default()),
            SetOutcome::Done
        );
        assert_eq!(s.get(1, b"k"), Some(b"a".to_vec()));
        let nx = SetOptions {
            nx: true,
            ..Default::default()
        };
        assert_eq!(s.set(1, b"k", b"b", nx), SetOutcome::Unchanged);
        let xx = SetOptions {
            xx: true,
            ..Default::default()
        };
        assert_eq!(s.set(1, b"k", b"c", xx), SetOutcome::Done);
        assert_eq!(s.get(1, b"k"), Some(b"c".to_vec()));
        // Same user key in a different slot is a different row.
        assert_eq!(s.get(2, b"k"), None);
    }

    #[test]
    fn ttl_lifecycle_with_fake_clock() {
        test_clock!(NOW_B, now_b, 1_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now_b);
        let opts = SetOptions {
            expiry: SetExpiry::AtMs(1_000_500),
            ..Default::default()
        };
        s.set(1, b"k", b"v", opts);
        assert_eq!(s.ttl(1, b"k"), Ttl::Ms(500));
        // Plain SET clears TTL.
        s.set(1, b"k", b"v2", SetOptions::default());
        assert_eq!(s.ttl(1, b"k"), Ttl::NoExpiry);
        // KEEPTTL keeps it.
        s.set(
            1,
            b"k",
            b"v3",
            SetOptions {
                expiry: SetExpiry::AtMs(1_000_800),
                ..Default::default()
            },
        );
        s.set(
            1,
            b"k",
            b"v4",
            SetOptions {
                expiry: SetExpiry::Keep,
                ..Default::default()
            },
        );
        assert_eq!(s.ttl(1, b"k"), Ttl::Ms(800));
        // Time passes; the key expires and reads delete it lazily.
        NOW_B.store(1_000_801, Ordering::Relaxed);
        assert_eq!(s.get(1, b"k"), None);
        assert_eq!(s.ttl(1, b"k"), Ttl::Missing);
        assert!(!s.exists(1, b"k"));
    }

    #[test]
    fn expire_and_persist() {
        test_clock!(NOW_C, now_c, 2_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now_c);
        assert!(!s.expire_at(1, b"nope", 2_000_100));
        s.set(1, b"k", b"v", SetOptions::default());
        assert!(s.expire_at(1, b"k", 2_000_100));
        assert_eq!(s.ttl(1, b"k"), Ttl::Ms(100));
        assert!(s.persist(1, b"k"));
        assert_eq!(s.ttl(1, b"k"), Ttl::NoExpiry);
        assert!(!s.persist(1, b"k"), "no expiry to remove");
        // Expiry in the past deletes immediately.
        assert!(s.expire_at(1, b"k", 1_999_999));
        assert!(!s.exists(1, b"k"));
    }

    #[test]
    fn incr_family() {
        test_clock!(NOW_D, now_d, 3_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now_d);
        assert_eq!(s.incr_by(1, b"c", 1), Ok(1));
        assert_eq!(s.incr_by(1, b"c", 11), Ok(12));
        assert_eq!(s.incr_by(1, b"c", -6), Ok(6));
        s.set(1, b"s", b"abc", SetOptions::default());
        assert_eq!(s.incr_by(1, b"s", 1), Err(StoreError::NotInteger));
        s.set(
            1,
            b"max",
            i64::MAX.to_string().as_bytes(),
            SetOptions::default(),
        );
        assert_eq!(s.incr_by(1, b"max", 1), Err(StoreError::Overflow));
        // INCR preserves TTL.
        s.set(
            1,
            b"tc",
            b"5",
            SetOptions {
                expiry: SetExpiry::AtMs(3_500_000),
                ..Default::default()
            },
        );
        assert_eq!(s.incr_by(1, b"tc", 1), Ok(6));
        assert_eq!(s.ttl(1, b"tc"), Ttl::Ms(500_000));
    }

    #[test]
    fn append_and_strlen() {
        test_clock!(NOW_E, now_e, 1_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now_e);
        assert_eq!(s.append(1, b"a", b"he"), 2);
        assert_eq!(s.append(1, b"a", b"llo"), 5);
        assert_eq!(s.get(1, b"a"), Some(b"hello".to_vec()));
        assert_eq!(s.strlen(1, b"a"), 5);
        assert_eq!(s.strlen(1, b"missing"), 0);
    }

    #[test]
    fn del_of_expired_key_is_not_counted() {
        test_clock!(NOW_F, now_f, 4_000_000);
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", now_f);
        s.set(
            1,
            b"k",
            b"v",
            SetOptions {
                expiry: SetExpiry::AtMs(4_000_001),
                ..Default::default()
            },
        );
        NOW_F.store(4_000_002, Ordering::Relaxed);
        assert!(!s.del(1, b"k"), "expired key must not count as deleted");
    }
}
