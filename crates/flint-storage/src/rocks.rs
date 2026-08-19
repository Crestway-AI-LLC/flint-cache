// SPDX-License-Identifier: Elastic-2.0
//! RocksDB-backed `Kv` + the M0 coverage audit.
//!
//! The audit tests below each prove one API the design depends on
//! (docs/design.md §2.4 and the replication/migration machinery):
//! column families with atomic cross-CF WriteBatch, WAL tailing
//! (`get_updates_since` — replication), compaction filters (expiry/orphan
//! GC), checkpoints (S3 snapshots, spare seeding), range deletes (slot
//! migration cleanup), and ordered prefix iteration (slot scans).

use std::path::Path;

use rocksdb::{BlockBasedIndexType, BlockBasedOptions, Cache, DB, Options, WriteBatch};

use crate::Kv;

/// RocksDB-backed `Kv` over the default column family.
///
/// v0: single CF, default options. The real engine opens the CF layout
/// from the encoding layer (metadata / subkey / zscore) — that arrives
/// with `TypeStore`.
pub struct RocksKv {
    db: DB,
    /// The directory this DB was opened from. RocksDB does not hand it back,
    /// and callers that need to leave something BESIDE the data (the re-seed
    /// marker) would otherwise have to thread the path separately.
    path: std::path::PathBuf,
    /// Completed WAL fsyncs (the bounded-cadence durability tick).
    wal_fsyncs: std::sync::atomic::AtomicU64,
}

impl RocksKv {
    /// Bytes of live SST data on disk — the capacity-model fill signal
    /// (FLINTINFO `sst_bytes:`; expansion triggers derive fill fractions
    /// and growth ETAs from it). Live files only: obsolete SSTs awaiting
    /// deletion would inflate fill with reclaimable space.
    pub fn sst_bytes(&self) -> u64 {
        self.db
            .property_int_value("rocksdb.live-sst-files-size")
            .ok()
            .flatten()
            .unwrap_or(0)
    }

    /// Apply a set of key mutations as ONE atomic WriteBatch — one grouped
    /// WAL append and one write-lock acquisition for the whole set (RocksDB
    /// group commit). The async write queue (ADR-0005 D4) uses this to
    /// collapse many connections' writes into one engine write. `None` value
    /// = delete. Replication picks the batch up from the WAL as usual.
    pub fn apply_writes(&self, ops: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<(), rocksdb::Error> {
        if ops.is_empty() {
            return Ok(());
        }
        let mut wb = rocksdb::WriteBatch::default();
        for (k, v) in ops {
            match v {
                Some(val) => wb.put(k, val),
                None => wb.delete(k),
            }
        }
        self.db.write(wb)
    }

    /// Engine write-stall signals (W2 back-pressure visibility): RocksDB's
    /// L0/pending-compaction back-pressure that silently slows or stops
    /// writes. `Some((write_stopped, delayed_write_rate_bytes_per_sec))`:
    /// stopped=1 means writes are fully halted; a nonzero rate means writes
    /// are being throttled (soft stall). Both 0 = healthy.
    ///
    /// `None` MEANS "I COULD NOT ANSWER" AND IS NOT THE SAME AS `Some((0, 0))`
    /// (docs/bugs/0022). This used to end in `.unwrap_or(0)`, which folded an
    /// engine that could not answer into the healthiest possible reading — in
    /// the direction an investigator expects, which is the worst direction for
    /// a wrong answer to point. BUG-0013's falsification criterion is "if
    /// write_stopped is zero the hypothesis is wrong", and a zero that means
    /// "never measured" would have acquitted it every time.
    ///
    /// These two are DB PROPERTIES, not statistics tickers, so they are live
    /// on the production open path with `enable_statistics()` off — measured,
    /// see the test below. A genuine ticker (`rocksdb.stall.micros`) reads
    /// `Ok(None)` there, which is precisely the case this signature now keeps
    /// distinguishable.
    pub fn write_stall(&self) -> Option<(u64, u64)> {
        let prop = |name: &str| self.db.property_int_value(name).ok().flatten();
        Some((
            prop("rocksdb.is-write-stopped")?,
            prop("rocksdb.actual-delayed-write-rate")?,
        ))
    }

    /// Approximate resident bytes for one namespace — the storage-metering
    /// signal (M5 quotas). Uses the engine's SST range estimator over the
    /// namespace's three envelope prefixes (Metadata/Subkey/ZScore), so it
    /// is O(file metadata), never a keyspace walk. APPROXIMATE by contract:
    /// it tracks compacted SST bytes (memtable-only recent writes undercount
    /// briefly), which is exactly the honesty level billing/quota sweeps
    /// need — the drill bounds it, the docs say "resident", not "logical".
    pub fn ns_bytes(&self, ns: &[u8]) -> u64 {
        // One (start, end) pair per envelope CF byte. The end bound appends
        // 0xff past the ns prefix: envelope keys continue with a 2-byte BE
        // slot then the user key, all < the 0xff sentinel run, so the range
        // covers exactly this namespace's rows (ns_len byte keeps a longer
        // namespace sharing the prefix out of range).
        let bounds: Vec<(Vec<u8>, Vec<u8>)> = b"MSZ"
            .iter()
            .map(|&cf| {
                let mut start = Vec::with_capacity(2 + ns.len());
                start.push(cf);
                start.push(ns.len() as u8);
                start.extend_from_slice(ns);
                let mut end = start.clone();
                end.extend_from_slice(&[0xff, 0xff, 0xff]);
                (start, end)
            })
            .collect();
        let ranges: Vec<rocksdb::Range> = bounds
            .iter()
            .map(|(s, e)| rocksdb::Range::new(s, e))
            .collect();
        self.db.get_approximate_sizes(&ranges).iter().sum()
    }

    /// Compact one namespace's ranges so tombstones (deleted rows) actually
    /// leave the SSTs — the moment `ns_bytes` sees a delete-driven drop.
    /// Background compaction gets there eventually; this is the on-demand
    /// path (FLINTCOMPACT) for GC pressure and the metering drills.
    pub fn compact_ns(&self, ns: &[u8]) {
        for cf in *b"MSZ" {
            let mut start = Vec::with_capacity(2 + ns.len());
            start.push(cf);
            start.push(ns.len() as u8);
            start.extend_from_slice(ns);
            let mut end = start.clone();
            end.extend_from_slice(&[0xff, 0xff, 0xff]);
            self.db.compact_range(Some(&start), Some(&end));
        }
    }

    /// Crate-internal handle for the replication module.
    pub(crate) fn db(&self) -> &DB {
        &self.db
    }
}

/// Block cache capacity — THE index/filter/data RAM bound at scale (S0.2). A
/// large node should raise this toward a fraction of its RAM (follow-on: a knob
/// threaded from node config); the LRU fills lazily, so small nodes and the
/// drills use only what they touch.
const BLOCK_CACHE_BYTES: usize = 512 * 1024 * 1024;

/// Block-based table options every data SST is written with.
///
/// S0.1 — the whole-key bloom filter is the load-bearing part for point reads:
/// a MISS is ruled out by the per-SST filter rather than seeking a data block
/// on each level. 10 bits/key ≈ 1% false-positive; whole-key filtering (the
/// RocksDB default) is what a `get()` consults.
///
/// S0.2 — keep index + filter RAM BOUNDED as the keyspace grows past RAM.
/// Two-level (partitioned) index + partitioned filters split those blocks so
/// only the small top level is resident; the rest live in the bounded block
/// cache instead of pinned unboundedly per open table. The top level and the
/// hot L0 blocks are pinned in cache so lookups don't pay I/O for them. Without
/// this, index+filter RAM grows linearly with data and a multi-TB node OOMs
/// long before its disk fills (docs/scale-to-100tb.md).
fn table_options() -> BlockBasedOptions {
    let mut bbt = BlockBasedOptions::default();
    bbt.set_bloom_filter(10.0, false);
    bbt.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
    bbt.set_partition_filters(true);
    bbt.set_cache_index_and_filter_blocks(true);
    bbt.set_pin_top_level_index_and_filter(true);
    bbt.set_pin_l0_filter_and_index_blocks_in_cache(true);
    bbt.set_block_cache(&Cache::new_lru_cache(BLOCK_CACHE_BYTES));
    bbt
}

/// WAL retention: how long an obsolete segment stays in `archive/` for a
/// replica to tail. Generous on purpose — the point is that ADR-0022's shed
/// gate, not this window, is what a lagging replica hits first. A window
/// this size makes shedding rare; the shedding is what makes the window safe.
pub const DEFAULT_WAL_TTL_SECONDS: u64 = 21_600; // 6 h, was 1 h
/// Companion byte budget. RocksDB applies whichever bound trips first, so
/// raising only the TTL would have left the 1 GiB limit doing the pruning —
/// which is the term that actually fired in the incident.
pub const DEFAULT_WAL_SIZE_LIMIT_MB: u64 = 8_192; // 8 GiB, was 1 GiB

/// Cap the RocksDB info LOG.
///
/// RocksDB's defaults are "grow without limit, prune nothing", and the LOG is
/// written by background machinery whose volume tracks WAL-archive churn, not
/// stored data. Measured on the playground 2026-08-18: 883 MB of LOG against
/// 248 KB of SSTs — about 3,600:1 — 92% of it one line from wal_manager.cc,
/// at ~6 MB/hour steady state. Since BUG-0012's livelock IS sustained archive
/// churn, leaving this unbounded lets a replication failure turn itself into a
/// disk-capacity failure. See docs/bugs/0017.
///
/// 64 MiB x 5 is a 320 MB ceiling, which at that measured rate is roughly two
/// days of history — deliberately more than one, because the incident that
/// motivated this ran nine hours before anyone looked at it.
pub const DEFAULT_MAX_LOG_FILE_SIZE: usize = 64 * 1024 * 1024;
pub const DEFAULT_KEEP_LOG_FILE_NUM: usize = 5;

/// Apply the LOG bounds to any `Options` that will open a real directory.
///
/// Shared rather than repeated because the read-only open is the easy one to
/// forget: it takes no retention arguments, so it looks like it has nothing to
/// configure, and it still makes RocksDB write a LOG.
fn bound_info_log(opts: &mut Options) {
    opts.set_max_log_file_size(DEFAULT_MAX_LOG_FILE_SIZE);
    opts.set_keep_log_file_num(DEFAULT_KEEP_LOG_FILE_NUM);
}

impl RocksKv {
    /// Open without the ability — or the side effects — of writing.
    ///
    /// A normal `open` MUTATES the directory (a fresh CURRENT, a new WAL,
    /// an updated MANIFEST), which is invisible on a data dir and corrupting
    /// on a backup set: every one of those files is checksummed in the set's
    /// manifest, so reading a set with `open` makes it unrestorable. The
    /// namespace-scoped restore reads checkpoints straight out of a set,
    /// hence this. WAL contents are still visible (replayed in memory).
    pub fn open_read_only(path: &Path) -> Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        bound_info_log(&mut opts);
        Ok(Self {
            db: DB::open_for_read_only(&opts, path, false)?,
            path: path.to_path_buf(),
            wal_fsyncs: std::sync::atomic::AtomicU64::new(0),
        })
    }

    pub fn open(path: &Path) -> Result<Self, rocksdb::Error> {
        Self::open_with_retention(path, DEFAULT_WAL_TTL_SECONDS, DEFAULT_WAL_SIZE_LIMIT_MB)
    }

    /// `open`, with the WAL retention window stated explicitly.
    ///
    /// Retention is what a replica tails, so it is really a replication
    /// parameter wearing a storage parameter's clothes. It is separated here
    /// because ADR-0022 makes the master shed writes when its slowest live
    /// replica approaches the window, and a shed threshold that cannot be
    /// related to the window it protects is untunable.
    ///
    /// Both terms still come from RocksDB and are immutable after open: the
    /// archive is deleted when a segment is older than the TTL **or** the
    /// archive exceeds the size limit, whichever happens first, and neither
    /// consults a replica. That is the reason the shedding in ADR-0022 exists
    /// rather than a cleverer retention rule.
    pub fn open_with_retention(
        path: &Path,
        wal_ttl_seconds: u64,
        wal_size_limit_mb: u64,
    ) -> Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // Retain WAL long enough for replicas to tail it. The v0 values (1 h
        // / 1 GiB) were chosen before anything measured how far a replica
        // actually falls behind, and a replica died against them twice in
        // three weeks — the second time one sequence short of the boundary,
        // on a fleet serving ~50 ops/s. Volume was never the driver: what
        // consumed the window was a replica spending its life full-syncing
        // rather than tailing. See docs/bugs/0012 and ADR-0022.
        opts.set_wal_ttl_seconds(wal_ttl_seconds);
        opts.set_wal_size_limit_mb(wal_size_limit_mb);
        // The info LOG is bounded here too: its dominant writer is the archive
        // manager these two terms drive, so the WAL settings and the log
        // ceiling are the same subsystem seen from two sides.
        bound_info_log(&mut opts);
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
        // S0.1: every data SST carries a bloom filter, so a point-read MISS is
        // answered from the per-SST filter instead of a data-block seek on each
        // level. This is what keeps GET tail latency flat as the dataset grows
        // past RAM — the read-path prerequisite for the 100 TB target
        // (docs/scale-to-100tb.md). RocksDB reads pre-filter SSTs fine, so this
        // is forward-compatible; filters populate as compaction rewrites data.
        opts.set_block_based_table_factory(&table_options());
        Ok(Self {
            db: DB::open(&opts, path)?,
            path: path.to_path_buf(),
            wal_fsyncs: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// The data directory this DB was opened from.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Fsync the WAL — one group commit covering everything appended since
    /// the last call. Ordinary writes go to the WAL unsynced (OS page cache:
    /// zero acked loss across process crash/restart, proven by the chaos
    /// drills); this tick, driven by the server's `--wal-fsync-ms` cadence,
    /// is what bounds the loss window of a HOST failure (power, kernel,
    /// instance loss) to the cadence instead of "whenever the OS flushed".
    pub fn flush_wal_sync(&self) -> Result<(), rocksdb::Error> {
        self.db.flush_wal(true)?;
        self.wal_fsyncs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Completed WAL fsync ticks (FLINTINFO `wal_fsync_total:`).
    pub fn wal_fsync_total(&self) -> u64 {
        self.wal_fsyncs.load(std::sync::atomic::Ordering::Relaxed)
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

    /// Flush, and say whether it worked. The `flush()` above discards the
    /// error, which is tolerable for its callers (tests forcing determinism,
    /// where a failed flush surfaces as a failed assertion one line later)
    /// and not for a caller whose CORRECTNESS is the flush — the restore
    /// scrub reports "system rows scrubbed" on the strength of it, and a
    /// swallowed error there turns into a split-brain hazard that boots.
    pub fn flush_checked(&self) -> Result<(), rocksdb::Error> {
        self.db.flush()
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

    fn for_each_from(
        &self,
        prefix: &[u8],
        start_after: &[u8],
        visit: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) {
        // Real seek: resume deep into a large namespace is O(seek), not a
        // scan-and-skip from the prefix start. `From` is inclusive, so the
        // exact resume key is skipped by comparison.
        let seek = if start_after.is_empty() {
            prefix
        } else {
            start_after
        };
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            seek,
            rocksdb::Direction::Forward,
        ));
        for (k, v) in iter.filter_map(Result::ok) {
            if !k.starts_with(prefix) {
                return;
            }
            if !start_after.is_empty() && k.as_ref() <= start_after {
                continue;
            }
            if !visit(&k, &v) {
                return;
            }
        }
    }

    fn clear(&self) {
        // Chunked delete batches: collecting every key into one Vec plus
        // one giant WriteBatch is FLUSHALL's version of the DBSIZE OOM —
        // two dataset-sized allocations. The scan's pinned view never sees
        // the interleaved deletes, so each key is deleted exactly once.
        const CHUNK: usize = 10_000;
        let mut batch = WriteBatch::default();
        let mut pending = 0;
        self.for_each_prefix(b"", &mut |k, _| {
            batch.delete(k);
            pending += 1;
            if pending == CHUNK {
                let _ = self.db.write(std::mem::take(&mut batch));
                pending = 0;
            }
            true
        });
        if pending > 0 {
            let _ = self.db.write(batch);
        }
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

    /// docs/bugs/0022. Two things are pinned here, and the second rots
    /// silently if it is not.
    ///
    /// 1. POSITIVE CONTROL — the PRODUCTION open path can read the stall
    ///    pair. `enable_statistics()` is called nowhere outside this test
    ///    module, and the write-up that filed 0022 concluded from that alone
    ///    that the stall counters were inert in production. They are not:
    ///    `rocksdb.is-write-stopped` and `rocksdb.actual-delayed-write-rate`
    ///    are DB PROPERTIES, not statistics tickers. If that ever stops being
    ///    true, `write_stall()` starts returning None and this test says so,
    ///    instead of FLINTINFO quietly publishing a healthy-looking zero.
    ///
    /// 2. `Ok(None)` is what "cannot answer" looks like. The whole
    ///    distinction rests on it. A rocksdb upgrade that answered an unknown
    ///    property with `Ok(Some(0))` would fold "cannot answer" back into
    ///    "answered zero" without a line of Flint changing.
    #[test]
    fn write_stall_is_readable_on_the_production_open_path() {
        let dir = TempDir::new("stall");
        let kv = RocksKv::open(&dir.0).expect("open");
        for i in 0..2000u32 {
            kv.put(format!("Mk{i}").as_bytes(), b"v");
        }

        let stall = kv.write_stall();
        assert!(
            stall.is_some(),
            "the production open path could not read the write-stall pair, so \
             every zero FLINTINFO reports for it would mean 'never measured'"
        );
        assert_eq!(
            stall,
            Some((0, 0)),
            "2000 small puts stalled the engine; that is not flake, it is \
             either a real regression in the write path or the stall \
             thresholds have moved (docs/bugs/0013)"
        );

        assert_eq!(
            kv.db
                .property_int_value("rocksdb.no-such-property")
                .expect("reading an unknown property must not error"),
            None,
            "an unavailable property no longer reads as Ok(None), so \
             'cannot answer' and 'answered zero' are one value again and \
             write_stall()'s Option no longer distinguishes anything"
        );
        // A statistics TICKER is genuinely unavailable on this path, and reads
        // as that same Ok(None). This is the case the Option exists for.
        assert_eq!(
            kv.db
                .property_int_value("rocksdb.stall.micros")
                .expect("reading a ticker name as a property must not error"),
            None
        );
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

    /// FLUSHALL must drain datasets larger than one delete chunk without
    /// ever holding all keys in memory (chunked batches, not one Vec).
    #[test]
    fn clear_drains_past_one_chunk() {
        let dir = TempDir::new("clear-chunk");
        let kv = RocksKv::open(&dir.0).expect("open");
        for i in 0..25_000u32 {
            kv.put(format!("k{i:08}").as_bytes(), b"v");
        }
        assert_eq!(kv.count_prefix(b"k"), 25_000);
        kv.clear();
        assert_eq!(kv.count_prefix(b""), 0);
    }

    /// S0.1: a point-read MISS on data that lives in an SST must be answered by
    /// the bloom filter, not a data-block seek on every level. Disk I/O is not
    /// directly observable, but the filter's own `useful` ticker is exactly
    /// "reads the filter short-circuited", so with statistics on it must move
    /// for IN-RANGE absent keys (out-of-range keys are excluded by the SST
    /// key-range check before the filter is even consulted).
    #[test]
    fn bloom_filter_catches_in_range_misses() {
        let dir = TempDir::new("bloom");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.enable_statistics();
        opts.set_block_based_table_factory(&table_options());
        let db = DB::open(&opts, &dir.0).expect("open");
        // Even keys only, zero-padded so they sort as written; flush to an SST
        // (a memtable has no filter).
        for i in (0..10_000u32).step_by(2) {
            db.put(format!("k{i:08}"), b"v").expect("put");
        }
        db.flush().expect("flush to sst");
        // Odd keys are absent but IN RANGE [k00000000, k00009998], so the SST is
        // not ruled out by key range — the bloom filter is what excludes them.
        for i in (1..10_000u32).step_by(2) {
            assert!(db.get(format!("k{i:08}")).expect("get").is_none());
        }
        let stats = opts.get_statistics().expect("statistics enabled");
        assert!(
            stat_count(&stats, "rocksdb.bloom.filter.useful") > 0,
            "bloom filter never fired on ~5000 in-range misses — not configured?\n{stats}"
        );
    }

    /// Pull a COUNT ticker out of RocksDB's statistics dump.
    fn stat_count(stats: &str, name: &str) -> u64 {
        stats
            .lines()
            .find(|l| l.trim_start().starts_with(name))
            .and_then(|l| l.split("COUNT :").nth(1))
            .and_then(|n| n.trim().parse().ok())
            .unwrap_or(0)
    }

    /// S0.2: the production `open()` path must bound index/filter/data RAM to
    /// our block cache rather than let it grow per open table with the dataset.
    /// The configured capacity is observable, so assert open() wired it.
    #[test]
    fn open_bounds_the_block_cache() {
        let dir = TempDir::new("cache");
        let kv = RocksKv::open(&dir.0).expect("open");
        let cap = kv
            .db()
            .property_int_value("rocksdb.block-cache-capacity")
            .expect("property")
            .expect("some");
        assert_eq!(
            cap as usize, BLOCK_CACHE_BYTES,
            "open() must wire the bounded block cache (S0.2)"
        );
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

    /// BUG-0017: the info LOG must be pruned, not merely rotated.
    ///
    /// RocksDB renames LOG to LOG.old.<micros> on EVERY open, so repeated opens
    /// exercise `keep_log_file_num` directly — which is the exact term that
    /// failed on the playground, where two retained LOG.old files held 883 MB
    /// against 248 KB of SSTs. Asserting on reopens rather than on 64 MiB of
    /// writes keeps this in the unit gate instead of a drill, and tests the
    /// pruning rather than the rotation threshold.
    ///
    /// Without the assert this regresses silently: nothing else in the system
    /// notices a large file, which is why it went a week unseen.
    #[test]
    fn info_log_is_pruned_across_reopens() {
        let dir = TempDir::new("infolog");
        let opens = DEFAULT_KEEP_LOG_FILE_NUM + 4;
        for _ in 0..opens {
            let kv = RocksKv::open(&dir.0).expect("open");
            kv.put(b"k", b"v");
            drop(kv);
        }

        let logs: Vec<String> = std::fs::read_dir(&dir.0)
            .expect("read dir")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("LOG"))
            .collect();

        // Positive control: the reopens really did rotate, so a pass cannot be
        // "no logs were ever produced".
        assert!(
            logs.iter().any(|n| n.starts_with("LOG.old.")),
            "expected at least one rotated LOG after {opens} opens, got {logs:?}"
        );
        // The bound itself: live LOG plus at most KEEP retained.
        assert!(
            logs.len() <= DEFAULT_KEEP_LOG_FILE_NUM + 1,
            "info LOG is not pruned: {} files after {} opens (bound {}): {:?}",
            logs.len(),
            opens,
            DEFAULT_KEEP_LOG_FILE_NUM + 1,
            logs
        );
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
