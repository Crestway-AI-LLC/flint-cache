//! Storage layer.
//!
//! Layering (docs/design.md §2.4, encoding-abstraction decision):
//!
//! ```text
//! command handlers → TypeStore (semantic ops, owns row layout + access
//! strategy, tagged per key) → Kv (flat key-value transactions) → engine
//! ```
//!
//! The key *envelope* `namespace | slot | user_key | …` is a system
//! invariant owned by the layers above `Kv` — encodings own only what is
//! inside it. `Kv` is the seam the RocksDB spike validates and the future
//! native-LSM swap point.
//!
//! M0 state: `Kv` trait + in-memory implementation. RocksDB implementation
//! arrives with the storage spike; `TypeStore` arrives with the encoding
//! layer.

pub mod encoding;
pub mod gc;
pub mod hashes;
pub mod keyspace;
pub mod lists;
pub mod manifest;
pub mod migration;
pub mod sets;
pub mod strings;
pub mod zsets;

#[cfg(feature = "rocksdb")]
pub mod repl;
#[cfg(feature = "rocksdb")]
pub mod rocks;

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::RwLock;

/// Minimal flat key-value interface the engine sits on.
///
/// Deliberately synchronous and byte-oriented; ordering (for prefix scans)
/// is part of the contract because slot migration and the subkey encoding
/// depend on it.
pub trait Kv: Send + Sync {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn put(&self, key: &[u8], value: &[u8]);
    /// Returns true if the key existed.
    fn delete(&self, key: &[u8]) -> bool;
    /// All pairs whose key starts with `prefix`, in ascending key order.
    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>;
    /// Remove everything. Test/dev convenience (FLUSHALL v0).
    fn clear(&self);
}

/// A write-suppressing view of another `Kv`, used on replicas.
///
/// A replica rejects mutating commands at dispatch, so the ONLY writes that
/// would otherwise reach its store are the lazy-expiry deletes buried inside
/// read paths (`GET` of an expired key calls `delete`). Those local writes
/// are wrong on a replica: they advance the replica's own sequence space,
/// diverge its physical bytes from the master, and violate the read-only
/// invariant — while the master's replicated `DELETE` (and the compaction
/// filter) already reclaim the row. This view makes reads pass through and
/// every write a no-op, so an expired key still reads as absent (the store
/// returns `None` regardless of the delete's outcome) without any write.
pub struct ReadOnlyKv<'a>(pub &'a dyn Kv);

impl Kv for ReadOnlyKv<'_> {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0.get(key)
    }
    fn put(&self, _key: &[u8], _value: &[u8]) {}
    fn delete(&self, _key: &[u8]) -> bool {
        false
    }
    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.0.scan_prefix(prefix)
    }
    fn clear(&self) {}
}

/// In-memory `Kv` for tests and the v0 server.
#[derive(Default)]
pub struct MemKv {
    map: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemKv {
    pub fn new() -> Self {
        Self::default()
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<Vec<u8>, Vec<u8>>> {
        self.map.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<Vec<u8>, Vec<u8>>> {
        self.map.write().unwrap_or_else(|e| e.into_inner())
    }
}

impl Kv for MemKv {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.read().get(key).cloned()
    }

    fn put(&self, key: &[u8], value: &[u8]) {
        self.write().insert(key.to_vec(), value.to_vec());
    }

    fn delete(&self, key: &[u8]) -> bool {
        self.write().remove(key).is_some()
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.read()
            .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn clear(&self) {
        self.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_delete() {
        let kv = MemKv::new();
        assert_eq!(kv.get(b"k"), None);
        kv.put(b"k", b"v1");
        assert_eq!(kv.get(b"k"), Some(b"v1".to_vec()));
        kv.put(b"k", b"v2");
        assert_eq!(kv.get(b"k"), Some(b"v2".to_vec()));
        assert!(kv.delete(b"k"));
        assert!(!kv.delete(b"k"));
        assert_eq!(kv.get(b"k"), None);
    }

    #[test]
    fn prefix_scan_is_ordered_and_bounded() {
        let kv = MemKv::new();
        kv.put(b"a|2", b"x");
        kv.put(b"a|1", b"y");
        kv.put(b"b|1", b"z");
        kv.put(b"a", b"meta");
        let hits = kv.scan_prefix(b"a|");
        assert_eq!(
            hits,
            vec![
                (b"a|1".to_vec(), b"y".to_vec()),
                (b"a|2".to_vec(), b"x".to_vec()),
            ]
        );
        assert_eq!(kv.scan_prefix(b"a").len(), 3);
        assert_eq!(kv.scan_prefix(b"c").len(), 0);
    }

    #[test]
    fn binary_keys_and_values() {
        let kv = MemKv::new();
        let key = [0u8, 255, 13, 10, 0];
        let val = [1u8, 0, 2, 255];
        kv.put(&key, &val);
        assert_eq!(kv.get(&key), Some(val.to_vec()));
    }

    #[test]
    fn readonly_view_passes_reads_suppresses_writes() {
        let backing = MemKv::new();
        backing.put(b"k", b"v");
        let ro = ReadOnlyKv(&backing);
        assert_eq!(ro.get(b"k"), Some(b"v".to_vec()));
        assert_eq!(ro.scan_prefix(b"k").len(), 1);
        // Writes are silently dropped; the backing store is untouched.
        ro.put(b"k", b"other");
        assert!(!ro.delete(b"k"));
        ro.clear();
        assert_eq!(backing.get(b"k"), Some(b"v".to_vec()));
    }

    #[test]
    fn replica_read_of_expired_key_does_not_write() {
        use crate::strings::{SetExpiry, SetOptions, StringStore};
        use std::sync::atomic::{AtomicU64, Ordering};
        static NOW: AtomicU64 = AtomicU64::new(1_000);
        fn now() -> u64 {
            NOW.load(Ordering::Relaxed)
        }
        let backing = MemKv::new();
        // Master-side: write a key that will expire.
        StringStore::new(&backing, b"0", now)
            .set(
                1,
                b"k",
                b"v",
                SetOptions {
                    expiry: SetExpiry::AtMs(1_100),
                    ..Default::default()
                },
            )
            .expect("set");
        let rows_before = backing.scan_prefix(b"").len();

        // Replica-side: read after expiry through the read-only view.
        NOW.store(1_200, Ordering::Relaxed);
        let ro = ReadOnlyKv(&backing);
        let replica = StringStore::new(&ro, b"0", now);
        assert_eq!(
            replica.get(1, b"k"),
            Ok(None),
            "expired key reads as absent"
        );
        // The physical row was NOT deleted by the replica read.
        assert_eq!(
            backing.scan_prefix(b"").len(),
            rows_before,
            "replica read must not physically delete (master's DELETE / compaction reclaims it)"
        );
        NOW.store(1_000, Ordering::Relaxed);
    }
}
