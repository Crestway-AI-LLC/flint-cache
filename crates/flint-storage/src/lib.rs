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
pub mod hashes;
pub mod keyspace;
pub mod strings;

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
}
