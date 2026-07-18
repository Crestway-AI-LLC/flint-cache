//! Type-agnostic keyspace operations.
//!
//! DEL, EXISTS, TYPE, EXPIRE/TTL/PERSIST work on any value type by parsing
//! only the common `MetaHeader` prefix of the metadata row. Deleting a
//! complex key is O(1): drop the metadata row and its subkey rows become
//! unreachable orphans (version-prefixed), garbage-collected by compaction
//! filters later.

use crate::Kv;
use crate::encoding::{Cf, MetaHeader, ValueType, envelope};
use crate::strings::Clock;

/// TTL query result, Redis-shaped.
#[derive(Debug, PartialEq, Eq)]
pub enum Ttl {
    Missing,  // -2
    NoExpiry, // -1
    Ms(u64),  // remaining
}

pub struct Keyspace<'a> {
    kv: &'a dyn Kv,
    ns: Vec<u8>,
    clock: Clock,
}

impl<'a> Keyspace<'a> {
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

    /// Live (non-expired) header + full row; lazily deletes expired rows.
    pub(crate) fn read_live_row(&self, slot: u16, key: &[u8]) -> Option<(MetaHeader, Vec<u8>)> {
        let mk = self.meta_key(slot, key);
        let row = self.kv.get(&mk)?;
        let header = MetaHeader::decode(&row)?;
        if header.is_expired((self.clock)()) {
            self.kv.delete(&mk);
            return None;
        }
        Some((header, row))
    }

    pub fn exists(&self, slot: u16, key: &[u8]) -> bool {
        self.read_live_row(slot, key).is_some()
    }

    pub fn value_type(&self, slot: u16, key: &[u8]) -> Option<ValueType> {
        self.read_live_row(slot, key)
            .and_then(|(h, _)| h.value_type())
    }

    /// O(1) for every type: only the metadata row is removed.
    pub fn del(&self, slot: u16, key: &[u8]) -> bool {
        let live = self.read_live_row(slot, key).is_some();
        if live {
            self.kv.delete(&self.meta_key(slot, key));
        }
        live
    }

    /// EXPIRE/PEXPIRE (absolute). False if the key does not exist.
    /// A past instant deletes immediately (Redis semantics).
    pub fn expire_at(&self, slot: u16, key: &[u8], at_ms: u64) -> bool {
        let Some((_, mut row)) = self.read_live_row(slot, key) else {
            return false;
        };
        if at_ms <= (self.clock)() {
            self.kv.delete(&self.meta_key(slot, key));
            return true;
        }
        MetaHeader::write_expire(&mut row, at_ms);
        self.kv.put(&self.meta_key(slot, key), &row);
        true
    }

    /// EXPIRETIME/PEXPIRETIME: the ABSOLUTE expiry instant in ms.
    /// None = key missing; Some(0) = key exists with no expiry.
    pub fn expire_time_ms(&self, slot: u16, key: &[u8]) -> Option<u64> {
        self.read_live_row(slot, key).map(|(h, _)| h.expire_ms)
    }

    pub fn ttl(&self, slot: u16, key: &[u8]) -> Ttl {
        match self.read_live_row(slot, key) {
            None => Ttl::Missing,
            Some((h, _)) if h.expire_ms == 0 => Ttl::NoExpiry,
            Some((h, _)) => Ttl::Ms(h.expire_ms - (self.clock)()),
        }
    }

    /// PERSIST: true if an expiry existed and was removed.
    pub fn persist(&self, slot: u16, key: &[u8]) -> bool {
        let Some((h, mut row)) = self.read_live_row(slot, key) else {
            return false;
        };
        if h.expire_ms == 0 {
            return false;
        }
        MetaHeader::write_expire(&mut row, 0);
        self.kv.put(&self.meta_key(slot, key), &row);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;
    use crate::hashes::HashStore;
    use crate::strings::{SetOptions, StringStore};
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
    fn generic_ops_work_on_both_layouts() {
        test_clock!(NOW, now, 5_000_000);
        let kv = MemKv::new();
        let ks = Keyspace::new(&kv, b"t", now);
        let strings = StringStore::new(&kv, b"t", now);
        let hashes = HashStore::new(&kv, b"t", now);

        strings
            .set(1, b"s", b"v", SetOptions::default())
            .expect("set");
        hashes
            .hset(1, b"h", &[(b"f".to_vec(), b"v".to_vec())])
            .expect("hset");

        assert_eq!(ks.value_type(1, b"s"), Some(ValueType::String));
        assert_eq!(ks.value_type(1, b"h"), Some(ValueType::Hash));
        assert!(ks.exists(1, b"h"));

        // TTL machinery is layout-independent.
        assert!(ks.expire_at(1, b"h", 5_000_500));
        assert_eq!(ks.ttl(1, b"h"), Ttl::Ms(500));
        assert!(ks.persist(1, b"h"));
        assert_eq!(ks.ttl(1, b"h"), Ttl::NoExpiry);
        // Hash content survived the header rewrites.
        assert_eq!(
            hashes.hget(1, b"h", b"f").expect("hget"),
            Some(b"v".to_vec())
        );

        // O(1) delete works on both.
        assert!(ks.del(1, b"s"));
        assert!(ks.del(1, b"h"));
        assert!(!ks.exists(1, b"h"));
        assert!(!ks.del(1, b"h"));
    }

    #[test]
    fn expiry_lifecycle() {
        test_clock!(NOW, now, 6_000_000);
        let kv = MemKv::new();
        let ks = Keyspace::new(&kv, b"t", now);
        let strings = StringStore::new(&kv, b"t", now);
        strings
            .set(1, b"k", b"v", SetOptions::default())
            .expect("set");
        assert!(ks.expire_at(1, b"k", 6_000_100));
        NOW.store(6_000_101, Ordering::Relaxed);
        assert_eq!(ks.ttl(1, b"k"), Ttl::Missing);
        assert!(!ks.exists(1, b"k"));
        // Past-instant expiry deletes immediately.
        strings
            .set(1, b"k2", b"v", SetOptions::default())
            .expect("set");
        assert!(ks.expire_at(1, b"k2", 6_000_000));
        assert!(!ks.exists(1, b"k2"));
    }
}
