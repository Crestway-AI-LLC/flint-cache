// SPDX-License-Identifier: Elastic-2.0
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

pub mod admission;
pub mod batch;
pub mod bloom;
pub mod disk;
pub mod encoding;
pub mod gc;
pub mod hashes;
pub mod json;
pub mod keyspace;
pub mod lists;
pub mod manifest;
pub mod mem;
pub mod migration;
pub mod sets;
pub mod strings;
pub mod watch;
pub mod zsets;

pub mod eviction;
#[cfg(feature = "rocksdb")]
pub mod repl;
#[cfg(feature = "rocksdb")]
pub mod rocks;
pub mod s3fifo;

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::RwLock;

/// Default cap on any single value's total payload bytes — a string's
/// payload, or a collection's cumulative fields/members/elements. Matches
/// Valkey's `proto-max-bulk-len` default (512MB), which is what bounds
/// value sizes there. On a beyond-RAM engine the cap matters more than in
/// Valkey: a collection can exceed physical memory, at which point any
/// read-all command (HGETALL, SMEMBERS, LRANGE 0 -1) is an OOM, so writes
/// past the cap are rejected instead (`StoreError::ValueTooLarge`).
/// Configure with `--max-value-bytes` (0 = unlimited).
pub const DEFAULT_MAX_VALUE_BYTES: u64 = 512 * 1024 * 1024;

/// Default policy cap on user-key length: 4 KiB, the same ceiling
/// ElastiCache Serverless enforces, so a key that works there works here
/// and a migration does not discover the difference in production.
///
/// This is a POLICY default, well under the structural ceiling below, and
/// deliberately stricter than stock Redis — where a key is just a string
/// and may be up to 512 MB. A multi-megabyte key is never a working cache
/// key; it is a bug or an attack, and every one of them is copied into
/// each subkey envelope. Raise it with `--max-key-bytes` if a real
/// workload needs to, up to the structural ceiling.
pub const DEFAULT_MAX_KEY_BYTES: u64 = 4 * 1024;

/// Structural ceiling on user-key length: the subkey/zscore envelopes
/// frame the key length as 2 bytes (`encoding::subkey_prefix`), so a
/// longer key cannot be represented — the envelope builders assert it and
/// the dispatch layer rejects it first with a clean error. Configure a
/// LOWER policy cap with `--max-key-bytes`; this ceiling cannot be raised
/// without an envelope format change.
pub const MAX_KEY_BYTES: u64 = u16::MAX as u64;

/// Minimal flat key-value interface the engine sits on.
///
/// Deliberately synchronous and byte-oriented; ordering (for prefix scans)
/// is part of the contract because slot migration and the subkey encoding
/// depend on it.
pub trait Kv: Send + Sync {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    /// Borrow a value IN PLACE instead of copying it out. Returns whether the
    /// key existed; `f` runs only when it did.
    ///
    /// `get`'s signature forces an owned `Vec`, so every reader pays an
    /// allocation and a full copy no matter what it intends to do with the
    /// bytes. Under hot-GET load that is not a rounding error: a profile put
    /// malloc/free at 18.7% and memcpy at 14.7% of the server's on-CPU work,
    /// and a single GET allocated and copied ~1 KB THREE times — once out of
    /// the block cache into the row, once out of the row into the payload,
    /// once into the socket buffer.
    ///
    /// A store that can hand back a borrowed slice (RocksDB's `get_pinned`
    /// keeps the block-cache entry alive and hands out a pointer into it)
    /// removes the first of those entirely.
    ///
    /// The default body calls `get`, so every existing implementation stays
    /// correct and only stores that can genuinely borrow override it.
    ///
    /// CONTRACT: `f` must not call back into this store. A borrowed value may
    /// pin an internal resource — a block-cache handle — for its lifetime,
    /// and re-entering while holding one is how a self-deadlock gets written.
    /// Do the work that needs the store AFTER this returns; `read_live`'s
    /// expiry delete is the worked example.
    fn with_value(&self, key: &[u8], f: &mut dyn FnMut(&[u8])) -> bool {
        match self.get(key) {
            Some(v) => {
                f(&v);
                true
            }
            None => false,
        }
    }
    fn put(&self, key: &[u8], value: &[u8]);
    /// Returns true if the key existed.
    fn delete(&self, key: &[u8]) -> bool;
    /// Visit every pair whose key starts with `prefix`, in ascending key
    /// order, without materializing the range: memory stays bounded no
    /// matter how many rows match. Return `false` from `visit` to stop.
    ///
    /// Contract: `visit` may call back into the store (`get`/`put`/`delete`
    /// — the GC sweeper deletes rows mid-scan), so implementations must not
    /// hold internal locks across `visit` invocations. Rows inserted or
    /// removed during the scan may or may not be visited; rows present for
    /// the whole scan are visited exactly once.
    fn for_each_prefix(&self, prefix: &[u8], visit: &mut dyn FnMut(&[u8], &[u8]) -> bool);
    /// Like `for_each_prefix`, but visiting only keys STRICTLY AFTER
    /// `start_after` — the resume primitive for incremental iteration
    /// (keyspace SCAN). `start_after` empty = from the prefix start. The
    /// default body scans the prefix and skips; ordered stores override
    /// with a real seek so resuming deep into a large namespace is O(seek),
    /// not O(position).
    fn for_each_from(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) {
        self.for_each_prefix(prefix, &mut |k, v| {
            if !start_after.is_empty() && k <= start_after {
                return true;
            }
            visit(k, v)
        });
    }
    /// Like `for_each_from`, in DESCENDING key order: the keys under
    /// `prefix` strictly BEFORE `start_before`, the last first, until
    /// `visit` returns false. `start_before` empty = from the prefix's last
    /// key. Same contract as `for_each_prefix`.
    ///
    /// The reverse sorted-set reads (ZREVRANGE, ZPOPMAX, ZREVRANGEBYSCORE)
    /// walk this, so the top of a large set costs what is read, not the set
    /// (BUG-0216). The default body materialises the prefix and walks it
    /// backwards, which keeps every store correct, a buffering one included;
    /// ordered stores override it with a real reverse seek.
    fn for_each_before(
        &self,
        prefix: &[u8],
        start_before: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) {
        for (k, v) in self.scan_prefix(prefix).iter().rev() {
            if !start_before.is_empty() && k.as_slice() >= start_before {
                continue;
            }
            if !visit(k, v) {
                return;
            }
        }
    }
    /// All pairs whose key starts with `prefix`, in ascending key order.
    ///
    /// Materializes the whole range — use only where the range is bounded
    /// by a single value's size (one hash/set/zset, one slot's manifest
    /// rows). CF-wide ranges (DBSIZE, GC) go through `for_each_prefix` /
    /// `count_prefix`; at fleet scale a materialized scan OOMs the process.
    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        self.for_each_prefix(prefix, &mut |k, v| {
            out.push((k.to_vec(), v.to_vec()));
            true
        });
        out
    }
    /// Number of keys with `prefix`, streaming — O(1) memory.
    fn count_prefix(&self, prefix: &[u8]) -> usize {
        let mut n = 0;
        self.for_each_prefix(prefix, &mut |_, _| {
            n += 1;
            true
        });
        n
    }
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
    fn for_each_prefix(&self, prefix: &[u8], visit: &mut dyn FnMut(&[u8], &[u8]) -> bool) {
        self.0.for_each_prefix(prefix, visit)
    }
    fn for_each_from(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) {
        self.0.for_each_from(prefix, start_after, visit)
    }
    fn for_each_before(
        &self,
        prefix: &[u8],
        start_before: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) {
        self.0.for_each_before(prefix, start_before, visit)
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

    /// Chunked cursor scan: the read lock is released before `visit` runs
    /// (the callback may re-enter the store — a write would deadlock
    /// against a held read lock) and memory stays bounded at CHUNK pairs
    /// regardless of range size. `cursor` = resume strictly after this key.
    fn chunked(
        &self,
        prefix: &[u8],
        mut cursor: Option<Vec<u8>>,
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) {
        const CHUNK: usize = 1024;
        loop {
            let chunk: Vec<(Vec<u8>, Vec<u8>)> = {
                let lower = match &cursor {
                    Some(k) => Bound::Excluded(k.as_slice()),
                    None => Bound::Included(prefix),
                };
                self.read()
                    .range::<[u8], _>((lower, Bound::Unbounded))
                    .take_while(|(k, _)| k.starts_with(prefix))
                    .take(CHUNK)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };
            let exhausted = chunk.len() < CHUNK;
            cursor = chunk.last().map(|(k, _)| k.clone());
            for (k, v) in &chunk {
                if !visit(k, v) {
                    return;
                }
            }
            if exhausted {
                return;
            }
        }
    }
}

impl MemKv {
    /// `chunked`, descending: up to CHUNK rows per lock hold, below `cursor`
    /// (exclusive) or from the prefix's last key.
    fn chunked_rev(
        &self,
        prefix: &[u8],
        mut cursor: Option<Vec<u8>>,
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) {
        const CHUNK: usize = 1024;
        let end = prefix_successor(prefix);
        // A cursor past the prefix's last key is the end of the prefix; one
        // at or below the prefix leaves nothing before it, and BTreeMap's
        // `range` panics on a start above its end.
        if let (Some(c), Some(e)) = (&cursor, &end)
            && c > e
        {
            cursor = None;
        }
        if cursor.as_deref().is_some_and(|c| c <= prefix) {
            return;
        }
        loop {
            let chunk: Vec<(Vec<u8>, Vec<u8>)> = {
                let upper = match (&cursor, &end) {
                    (Some(k), _) => Bound::Excluded(k.as_slice()),
                    (None, Some(e)) => Bound::Excluded(e.as_slice()),
                    (None, None) => Bound::Unbounded,
                };
                self.read()
                    .range::<[u8], _>((Bound::Included(prefix), upper))
                    .rev()
                    .take_while(|(k, _)| k.starts_with(prefix))
                    .take(CHUNK)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };
            let exhausted = chunk.len() < CHUNK;
            cursor = chunk.last().map(|(k, _)| k.clone());
            for (k, v) in &chunk {
                if !visit(k, v) {
                    return;
                }
            }
            if exhausted {
                return;
            }
        }
    }
}

/// The least key greater than every key starting with `prefix`: its last
/// byte below 0xFF raised by one, the rest dropped. `None` when every byte
/// is 0xFF, and nothing sorts after the prefix's keys.
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let last = prefix.iter().rposition(|&b| b != 0xFF)?;
    let mut s = prefix[..=last].to_vec();
    s[last] += 1;
    Some(s)
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

    fn for_each_prefix(&self, prefix: &[u8], visit: &mut dyn FnMut(&[u8], &[u8]) -> bool) {
        self.chunked(prefix, None, visit);
    }

    fn for_each_from(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) {
        // The chunked loop's resume cursor IS a strictly-after bound —
        // seeding it with start_after is a real seek, not a scan-and-skip.
        let seed = (!start_after.is_empty()).then(|| start_after.to_vec());
        self.chunked(prefix, seed, visit);
    }

    fn for_each_before(
        &self,
        prefix: &[u8],
        start_before: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) {
        let seed = (!start_before.is_empty()).then(|| start_before.to_vec());
        self.chunked_rev(prefix, seed, visit);
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        // Overrides the default: one lock hold = an atomic snapshot of the
        // range, which the chunked streaming path deliberately gives up.
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

    /// BUG-0216: the descending scan, against the ascending one reversed,
    /// across prefixes that end in 0xFF and keys either side of the prefix.
    #[test]
    fn for_each_before_is_the_ascending_scan_reversed() {
        assert_eq!(prefix_successor(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_successor(b"a\xff\xff"), Some(b"b".to_vec()));
        assert_eq!(prefix_successor(b"\xff"), None);
        let kv = MemKv::new();
        for k in [
            &b"a"[..],
            b"a\xff",
            b"a\xff\x00",
            b"a\xff\xff",
            b"a\xff\xff\x01",
            b"b",
            b"\xff",
            b"\xff\x01",
        ] {
            kv.put(k, b"v");
        }
        for prefix in [&b""[..], b"a", b"a\xff", b"a\xff\xff", b"\xff", b"c"] {
            let mut up: Vec<Vec<u8>> = Vec::new();
            kv.for_each_prefix(prefix, &mut |k, _| {
                up.push(k.to_vec());
                true
            });
            for outside in [&b"\x00"[..], b"a", b"zz", b"\xff\xff"] {
                let want: Vec<Vec<u8>> = up
                    .iter()
                    .rev()
                    .filter(|k| k.as_slice() < outside)
                    .cloned()
                    .collect();
                let mut down: Vec<Vec<u8>> = Vec::new();
                kv.for_each_before(prefix, outside, &mut |k, _| {
                    down.push(k.to_vec());
                    true
                });
                assert_eq!(down, want, "prefix {prefix:?} before {outside:?}");
            }
            for cut in 0..=up.len() {
                let start_before = up.get(cut).cloned().unwrap_or_default();
                let mut want: Vec<Vec<u8>> = up[..cut.min(up.len())].to_vec();
                if start_before.is_empty() {
                    want = up.clone();
                }
                want.reverse();
                let mut down: Vec<Vec<u8>> = Vec::new();
                kv.for_each_before(prefix, &start_before, &mut |k, _| {
                    down.push(k.to_vec());
                    true
                });
                assert_eq!(down, want, "prefix {prefix:?} before {start_before:?}");
                // The trait's default body agrees with the override.
                let mut default: Vec<Vec<u8>> = Vec::new();
                for (k, _) in kv.scan_prefix(prefix).iter().rev() {
                    if start_before.is_empty() || k.as_slice() < start_before.as_slice() {
                        default.push(k.clone());
                    }
                }
                assert_eq!(down, default);
            }
        }
    }

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
    fn for_each_prefix_streams_ordered_bounded_and_stops_early() {
        let kv = MemKv::new();
        kv.put(b"a|2", b"x");
        kv.put(b"a|1", b"y");
        kv.put(b"b|1", b"z");
        kv.put(b"a", b"meta");
        let mut seen = Vec::new();
        kv.for_each_prefix(b"a|", &mut |k, v| {
            seen.push((k.to_vec(), v.to_vec()));
            true
        });
        assert_eq!(
            seen,
            vec![
                (b"a|1".to_vec(), b"y".to_vec()),
                (b"a|2".to_vec(), b"x".to_vec()),
            ]
        );
        assert_eq!(kv.count_prefix(b"a"), 3);
        assert_eq!(kv.count_prefix(b"c"), 0);
        let mut visited = 0;
        kv.for_each_prefix(b"a", &mut |_, _| {
            visited += 1;
            false
        });
        assert_eq!(visited, 1, "returning false stops the scan");
    }

    /// The contract DBSIZE and the GC sweeper lean on: the callback may
    /// write back into the store mid-scan, and every row present for the
    /// whole scan is visited exactly once — including across the chunked
    /// cursor's lock releases, which a range longer than one chunk forces.
    #[test]
    fn for_each_prefix_survives_reentrant_deletes_across_chunks() {
        let kv = MemKv::new();
        // Well past one chunk (1024) so cursor resume is exercised.
        for i in 0..3_000u32 {
            kv.put(format!("p|{i:08}").as_bytes(), &i.to_be_bytes());
        }
        kv.put(b"q", b"other");
        let mut visited = 0;
        kv.for_each_prefix(b"p|", &mut |k, _| {
            assert!(kv.delete(k), "row visited twice or vanished");
            visited += 1;
            true
        });
        assert_eq!(visited, 3_000);
        assert_eq!(
            kv.count_prefix(b"p|"),
            0,
            "sweep-style scan drained the range"
        );
        assert_eq!(kv.get(b"q"), Some(b"other".to_vec()));
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
