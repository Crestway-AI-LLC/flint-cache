//! RocksDB-backed `Kv` + the M0 coverage audit.
//!
//! The audit tests below each prove one API the design depends on
//! (docs/design.md §2.4 and the replication/migration machinery):
//! column families with atomic cross-CF WriteBatch, WAL tailing
//! (`get_updates_since` — replication), compaction filters (expiry/orphan
//! GC), checkpoints (S3 snapshots, spare seeding), range deletes (slot
//! migration cleanup), and ordered prefix iteration (slot scans).

use std::path::Path;

use rocksdb::{DB, Options, WriteBatch};

use crate::Kv;

/// RocksDB-backed `Kv` over the default column family.
///
/// v0: single CF, default options. The real engine opens the CF layout
/// from the encoding layer (metadata / subkey / zscore) — that arrives
/// with `TypeStore`.
pub struct RocksKv {
    db: DB,
}

impl RocksKv {
    /// Crate-internal handle for the replication module.
    pub(crate) fn db(&self) -> &DB {
        &self.db
    }
}

impl RocksKv {
    pub fn open(path: &Path) -> Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // Retain WAL long enough for replicas to tail it (v0: 1h / 1GB;
        // beyond that a replica must full-sync from a checkpoint).
        opts.set_wal_ttl_seconds(3600);
        opts.set_wal_size_limit_mb(1024);
        // Expired metadata rows are dropped organically as compaction
        // rewrites them (subkey orphans are reclaimed by gc::sweep until
        // the filter gains a metadata-lookup handle).
        opts.set_compaction_filter("flint-meta-expiry", |_level, key, value| {
            use crate::encoding::{Cf, MetaHeader};
            use rocksdb::compaction_filter::Decision;
            if key.first() == Some(&(Cf::Metadata as u8))
                && MetaHeader::decode(value)
                    .is_some_and(|h| h.is_expired(crate::strings::system_clock()))
            {
                Decision::Remove
            } else {
                Decision::Keep
            }
        });
        Ok(Self {
            db: DB::open(&opts, path)?,
        })
    }

    /// Create a RocksDB checkpoint (hard-linked consistent copy) at `path`.
    /// The parent directory must exist; `path` itself must not.
    pub fn checkpoint_to(&self, path: &Path) -> Result<(), rocksdb::Error> {
        rocksdb::checkpoint::Checkpoint::new(&self.db)?.create_checkpoint(path)
    }

    /// Force a full compaction (tests, admin).
    pub fn compact_all(&self) {
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);
        // Rows in memtables are not seen by the compaction filter until
        // flushed; flush first for deterministic tests.
    }

    /// Flush memtables to SSTs.
    pub fn flush(&self) {
        let _ = self.db.flush();
    }
}

impl Kv for RocksKv {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.get(key).ok().flatten()
    }

    fn put(&self, key: &[u8], value: &[u8]) {
        let _ = self.db.put(key, value);
    }

    fn delete(&self, key: &[u8]) -> bool {
        let existed = self.get(key).is_some();
        let _ = self.db.delete(key);
        existed
    }

    fn for_each_prefix(&self, prefix: &[u8], visit: &mut dyn FnMut(&[u8], &[u8]) -> bool) {
        // The iterator pins a consistent view, so `visit` may write back
        // into the store (per the trait contract) without disturbing the
        // scan, and nothing is materialized beyond one row at a time.
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            prefix,
            rocksdb::Direction::Forward,
        ));
        for (k, v) in iter.filter_map(Result::ok) {
            if !k.starts_with(prefix) || !visit(&k, &v) {
                return;
            }
        }
    }

    fn clear(&self) {
        let keys: Vec<_> = self
            .db
            .iterator(rocksdb::IteratorMode::Start)
            .filter_map(Result::ok)
            .map(|(k, _)| k)
            .collect();
        let mut batch = WriteBatch::default();
        for k in keys {
            batch.delete(k);
        }
        let _ = self.db.write(batch);
    }
}

#[cfg(test)]
mod audit {
    use super::*;
    use rocksdb::checkpoint::Checkpoint;
    use rocksdb::compaction_filter::Decision;
    use rocksdb::{ColumnFamilyDescriptor, WriteBatchIterator, WriteOptions};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static N: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "flint-rocks-audit-{}-{}-{}",
                tag,
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&path);
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn rockskv_satisfies_kv_contract() {
        let dir = TempDir::new("kv");
        let kv = RocksKv::open(&dir.0).expect("open");
        assert_eq!(kv.get(b"k"), None);
        kv.put(b"k", b"v");
        assert_eq!(kv.get(b"k"), Some(b"v".to_vec()));
        kv.put(b"a|1", b"1");
        kv.put(b"a|2", b"2");
        kv.put(b"b|1", b"3");
        let hits = kv.scan_prefix(b"a|");
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].0, b"a|1");
        assert!(kv.delete(b"k"));
        assert!(!kv.delete(b"k"));
        kv.clear();
        assert_eq!(kv.scan_prefix(b"").len(), 0);
    }

    #[test]
    fn column_families_and_cross_cf_batch_atomicity() {
        let dir = TempDir::new("cf");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cfs = vec![
            ColumnFamilyDescriptor::new("metadata", Options::default()),
            ColumnFamilyDescriptor::new("subkey", Options::default()),
        ];
        let db = DB::open_cf_descriptors(&opts, &dir.0, cfs).expect("open with CFs");
        let meta = db.cf_handle("metadata").expect("metadata cf");
        let sub = db.cf_handle("subkey").expect("subkey cf");
        // The encoding layer's core requirement: metadata row + subkey rows
        // commit atomically.
        let mut batch = WriteBatch::default();
        batch.put_cf(&meta, b"h1", b"type=hash,version=7,size=2");
        batch.put_cf(&sub, b"h1|7|f1", b"v1");
        batch.put_cf(&sub, b"h1|7|f2", b"v2");
        db.write(batch).expect("atomic write");
        assert!(db.get_cf(&meta, b"h1").expect("read").is_some());
        assert_eq!(
            db.get_cf(&sub, b"h1|7|f1").expect("read"),
            Some(b"v1".to_vec())
        );
    }

    /// THE critical audit item: replication = tailing the WAL.
    ///
    /// AUDIT FINDING (rust-rocksdb 0.24 / RocksDB 10.4): `get_updates_since(n)`
    /// positions at the batch containing `n` but the iterator SKIPS that
    /// first batch (advance-before-read). The replication tailer therefore
    /// must be sequence-idempotent: request from `last_applied` (not
    /// `last_applied + 1`) and drop any ops with seq <= last_applied. That
    /// contract is correct under both the buggy and a future-fixed iterator,
    /// and it is required anyway for restart safety. Fresh-replica attach
    /// always full-syncs from a checkpoint first, so last_applied >= 1 by
    /// construction and the skipped batch is always one already applied.
    #[test]
    fn wal_tailing_via_get_updates_since() {
        let dir = TempDir::new("wal");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, &dir.0).expect("open");
        assert_eq!(db.latest_sequence_number(), 0);

        db.put(b"k1", b"v1").expect("put"); // seq 1
        db.put(b"k2", b"v2").expect("put"); // seq 2
        db.put(b"k3", b"v3").expect("put"); // seq 3
        assert_eq!(db.latest_sequence_number(), 3);

        struct Collect {
            floor: u64,
            ops: Vec<Vec<u8>>,
            seq: u64,
        }
        impl WriteBatchIterator for Collect {
            fn put(&mut self, key: &[u8], _value: &[u8]) {
                self.seq += 1;
                if self.seq > self.floor {
                    self.ops.push(key.to_vec());
                }
            }
            fn delete(&mut self, key: &[u8]) {
                self.put(key, b"");
            }
        }

        // Replica state: batch at seq 1 already applied (last_applied = 1).
        // The tailer contract: request from last_applied, dedupe by seq.
        let last_applied = 1u64;
        let mut tail: Vec<Vec<u8>> = Vec::new();
        let iter = db.get_updates_since(last_applied).expect("wal iterator");
        for item in iter {
            let (first_seq, batch) = item.expect("wal entry");
            let mut c = Collect {
                floor: last_applied,
                ops: Vec::new(),
                seq: first_seq - 1,
            };
            batch.iterate(&mut c);
            tail.extend(c.ops);
        }
        assert_eq!(
            tail,
            vec![b"k2".to_vec(), b"k3".to_vec()],
            "tail from last_applied=1 must yield exactly the unapplied ops"
        );
    }

    /// Expiry/orphan GC and per-slot accounting live in compaction filters.
    #[test]
    fn compaction_filter_drops_marked_rows() {
        let dir = TempDir::new("filter");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compaction_filter("flint-expiry", |_level, _key, value| {
            if value == b"expired" {
                Decision::Remove
            } else {
                Decision::Keep
            }
        });
        let db = DB::open(&opts, &dir.0).expect("open");
        db.put(b"live", b"data").expect("put");
        db.put(b"dead", b"expired").expect("put");
        db.flush().expect("flush");
        db.compact_range(None::<&[u8]>, None::<&[u8]>);
        assert_eq!(db.get(b"live").expect("get"), Some(b"data".to_vec()));
        assert_eq!(
            db.get(b"dead").expect("get"),
            None,
            "compaction filter should have dropped the expired row"
        );
    }

    /// S3 snapshots and spare seeding are RocksDB checkpoints.
    #[test]
    fn checkpoint_creates_openable_copy() {
        let dir = TempDir::new("ckpt-src");
        let ckpt_dir = TempDir::new("ckpt-dst");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, &dir.0).expect("open");
        db.put(b"k", b"v").expect("put");
        db.flush().expect("flush");
        // Checkpoint target itself must not exist, but its parent must.
        std::fs::create_dir_all(&ckpt_dir.0).expect("mkdir parent");
        let target = ckpt_dir.0.join("snap");
        Checkpoint::new(&db)
            .expect("checkpoint handle")
            .create_checkpoint(&target)
            .expect("create checkpoint");
        let copy = DB::open_for_read_only(&Options::default(), &target, false).expect("open copy");
        assert_eq!(copy.get(b"k").expect("get"), Some(b"v".to_vec()));
    }

    /// Slot-migration cleanup = DeleteRange over the slot's key envelope.
    #[test]
    fn delete_range_removes_slot_prefix() {
        let dir = TempDir::new("delrange");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cfs = vec![ColumnFamilyDescriptor::new("data", Options::default())];
        let db = DB::open_cf_descriptors(&opts, &dir.0, cfs).expect("open");
        let cf = db.cf_handle("data").expect("cf");
        db.put_cf(&cf, b"ns1|s042|k1", b"a").expect("put");
        db.put_cf(&cf, b"ns1|s042|k2", b"b").expect("put");
        db.put_cf(&cf, b"ns1|s043|k1", b"c").expect("put");
        db.delete_range_cf(&cf, &b"ns1|s042|"[..], &b"ns1|s042|\xff"[..])
            .expect("delete range");
        assert_eq!(db.get_cf(&cf, b"ns1|s042|k1").expect("get"), None);
        assert_eq!(db.get_cf(&cf, b"ns1|s042|k2").expect("get"), None);
        assert_eq!(
            db.get_cf(&cf, b"ns1|s043|k1").expect("get"),
            Some(b"c".to_vec())
        );
    }

    /// Sync writes are the fsync-before-ack path; RocksDB group-commits
    /// concurrent sync writers internally.
    #[test]
    fn sync_write_option_works() {
        let dir = TempDir::new("sync");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, &dir.0).expect("open");
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        let mut batch = WriteBatch::default();
        batch.put(b"durable", b"yes");
        db.write_opt(batch, &wo).expect("sync write");
        assert_eq!(db.get(b"durable").expect("get"), Some(b"yes".to_vec()));
    }
}

#[cfg(test)]
mod gc_integration {
    use super::*;
    use crate::encoding::Cf;
    use crate::strings::{SetExpiry, SetOptions, StringStore};

    #[test]
    fn compaction_filter_drops_expired_metadata_rows() {
        let dir = {
            let p = std::env::temp_dir().join(format!("flint-gcint-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&p);
            p
        };
        let kv = RocksKv::open(&dir).expect("open");
        fn past() -> u64 {
            1 // far in the past relative to system_clock
        }
        let _ = past;
        // Write a string that expired long ago (expire_ms = 1).
        let s = StringStore::new(&kv, b"t", crate::strings::system_clock);
        s.set(
            1,
            b"dead",
            b"v",
            SetOptions {
                expiry: SetExpiry::AtMs(1),
                ..Default::default()
            },
        )
        .expect("set");
        s.set(1, b"live", b"v", SetOptions::default()).expect("set");
        // Physically present before compaction (2 metadata rows).
        assert_eq!(kv.scan_prefix(&[Cf::Metadata as u8]).len(), 2);
        kv.flush();
        kv.compact_all();
        // The expired row is physically gone; the live one remains.
        let remaining = kv.scan_prefix(&[Cf::Metadata as u8]);
        assert_eq!(remaining.len(), 1);
        assert_eq!(s.get(1, b"live"), Ok(Some(b"v".to_vec())));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
