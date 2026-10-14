# BUG-0259: RocksDB deleted a just-archived WAL when its mtime ran ahead of the clock (FIXED 2026-10-12)

**Status:** **FIXED 2026-10-12.** RocksDB no longer applies the WAL TTL. Flint
deletes archived segments past the TTL itself, and a segment stamped ahead
of the clock is not old.
**Severity:** medium. A replica that needed the deleted segment was refused
with `WALGAP`, and recovered by rewinding to a snapshot or by a full
re-seed. The data itself was safe: the deleted segment's writes were
already in the flushed SST. Four such gaps on the playground's pair 0 in
several weeks (ops OPS-0386).

## What happened

Ops (OPS-0386) noticed that each of the four mid-stream gaps lined up with a
snapshot, at exactly the sequence the replica needed plus one. BUG-0252's
re-walk recovered none of them, so it was not a listing race.

**The archive had a hole one WAL long.** On node-7001, read-only:
- `archive/238295.log` ends at 373945900;
- `archive/238300.log` starts at 373946943.

Every other neighbour was contiguous. The 1,042 sequences between them were
`238297.log`, the WAL that was live when the 19:53:21 snapshot's checkpoint
flushed.

**RocksDB's own LOG says what happened to it,** all on the checkpoint's
thread:

    19:53:21.009869  Move log file .../238297.log to .../archive/238297.log -- OK
    19:53:21.009879  Move log file .../238297.log to .../archive/238297.log -- IO error: No such file
    19:53:21.010655  Deleted file .../archive/236928.log immediately     (12 h old: the TTL, as meant)
    19:53:21.011384  Deleted file .../archive/238297.log immediately     (archived 1.5 ms before)

The duplicate move is harmless and happens at every snapshot. The two
deletions are `WalManager::PurgeObsoleteWALFiles`, which runs at most every
600 s. In RocksDB 10.4.2 its age test is:

    if (now_seconds - file_m_time > db_options_.WAL_ttl_seconds)  // both uint64_t

The two sides of that subtraction come from different clocks:
- `now_seconds` comes from `time()`, the kernel's coarse clock. On the
  playground (`CONFIG_HZ=100`, `NO_HZ_FULL`) it lags real time.
- `file_m_time` comes from `stat`. On xfs under Linux 6.18, which has
  fine-grained (multigrain) timestamps, it is exact.

So a segment written a few milliseconds past a second boundary is stamped a
second ahead of `time()`. The subtraction then wraps to about 1.8 × 10¹⁹,
which is past any TTL, and the segment is deleted.

**Measured on the playground,** on the same xfs and kernel: of 4.4 million
writes in 20 s, 55,699 stamped an mtime second ahead of libc `time()`, by up
to about 14 ms.

**Why it lined up with snapshots.** A checkpoint flushes, and the flush
archives the live WAL. The purge that follows runs a few milliseconds after
the WAL's last write. Flint's snapshots fire about 6 ms past a whole second,
which is the window. The 600 s gate on the TTL pass and the width of the
window together explain four incidents in weeks.

Only 236852 (10/9) and 238297 (10/12) were deleted this way in node-7001's
retained LOG. A replica is hurt only if it still needed the segment;
otherwise the archive simply had a hole.

## Fixed

- **RocksDB is opened with `WAL_ttl_seconds` 0** and keeps its size limit,
  whose pass has no such subtraction.
  - RocksDB archives only while one of the two is set. So a TTL with no size
    limit gets a limit too large to reach, 1 PiB, which keeps archiving on.
- **`ArchivePruner`** deletes archived segments older than the TTL: the same
  files, at RocksDB's cadence (`min(600 s, ttl / 2)`, at least every
  second). It uses `SystemTime::duration_since`, so a segment stamped ahead
  of the clock has age 0. It stops when its `RocksKv` is dropped.
- **Unchanged:** the 12 h TTL, the size budget, and how often each applies
  at the shipped settings. self-hosting.md 3b says who applies which.
- **Not unchanged at a short TTL (BUG-0261, 2026-10-14).** With RocksDB's TTL
  off, its size check runs every 600 s whatever the TTL, so at a short TTL
  the size limit went unenforced for up to 600 s, and public CI's
  `snapshot_pressure` failed from this commit on. The pruner now applies the
  size limit at its own cadence too.

## Verification

- `a_segment_stamped_ahead_of_the_clock_survives_the_purge`:
  - it archives a WAL, stamps it ten seconds ahead, and flushes again so
    that RocksDB purges;
  - with RocksDB's own TTL (the control) the segment is deleted at once, on
    the laptop too;
  - with Flint's it stays.
- `age_pruning_deletes_only_what_is_past_the_ttl`: old segments go, while
  fresh ones, future-stamped ones and non-WAL files stay.
- `rocksdb_keeps_archiving_once_its_ttl_is_off`: the size-limit mapping,
  including that 1 PiB stays inside a u64 in RocksDB's arithmetic.
- Two mutants failed: RocksDB's TTL restored, and a future mtime counted
  as old.
- `walgap_quarantine_drill` and `wal_window_drill`, whose short TTLs depend
  on the pruning, pass. So do the storage crate's tests.
