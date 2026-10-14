# BUG-0261: BUG-0259 left the WAL archive's size limit unenforced for up to 600 seconds (FIXED 2026-10-14)

**Status:** **FIXED 2026-10-14.** The archive pruner BUG-0259 added now
enforces `--wal-size-limit-mb` at its own cadence, `min(600, ttl / 2)`
seconds. It deletes the oldest segment first, as RocksDB's size limit did.
**Severity:** low in production, and it blocked a release. Production's 12 h
TTL put RocksDB's check on a 600 s cadence before BUG-0259 and after it, so a
fleet saw no change. But a short TTL no longer kept the archive small. The
public CI gate's `snapshot_pressure` failed on every push from BUG-0259 on,
and `release.sh` refuses to cut on a red public gate, which held rc.83.
**Area:** `crates/flint-storage/src/rocks.rs`, the WAL archive's retention.

## What happened

From 7283769e (BUG-0259) on, `gate (drills)` on main failed in the same way
each time, for five pushes in a row (run 38337033763 is the last):

```
FAIL  snapshot_pressure (9.2s)
      FAIL: 3450 write(s) refused across 30 rounds -- the disk filled behind snapshots
```

The drill runs the server with `--wal-ttl-seconds 2 --wal-size-limit-mb 16` on a
512 MB filesystem. Its header says the archive is pinned small "so it cannot be
what fills the disk". The per-round lines showed it was not pinned:

```
round  1: ... (archive 26M, ...)
round  5: ... (archive 126M, ...)
round 10: ... disk free 5% (archive 251M, ...)
round 11: 400 refused ...
round 12: ... disk free 1% (archive 276M, ...)
```

That is 25 MB a round, against a 16 MB limit.

## Why

RocksDB 10.4.2's `WalManager::PurgeObsoleteWALFiles` runs its TTL test and its
size-limit test at one cadence: every `min(600, WAL_ttl_seconds / 2)` seconds
while a TTL is set, and every 600 seconds while only a size limit is set.
BUG-0259 turned RocksDB's TTL off and kept the size limit, so the size check
moved from every second (TTL 2) to every 600 seconds. Its own pruner took over
the TTL, but only by age. A 2 s TTL deletes nothing younger than 2 s, and the
drill wrote about 750 MB in under ten seconds, so the archive held all of it
until an age pass caught up.

## Fix

`ArchivePruner` takes the size limit too. On each pass, after pruning by age,
`prune_archive_to_size` deletes archived segments oldest first, by their
number, which is creation order, until the archive fits. RocksDB keeps its own
600 s size check as a backstop. A segment that is not a numbered `.log` is
never counted or touched. A segment that is already gone when the pass
reaches it, because RocksDB's own pass got there first, still counts as freed,
so the pass does not delete one more segment to make up for it. The
limit used is the caller's, never the 1 PiB placeholder
`archive_size_limit_mb` hands RocksDB for a TTL with no size limit. A
size-only configuration (TTL 0) starts no pruner and is unchanged: RocksDB
enforces it every 600 s, as before BUG-0259.

`docs/self-hosting.md` 3b and the `open_with_retention` comment described
Flint applying the TTL and RocksDB the size limit. They now describe the new
split.

## Evidence

- **Unit test** `size_pruning_deletes_the_oldest_until_the_archive_fits`:
  - 9 MB of WAL against 5 MB loses the two oldest segments and keeps the
    newest;
  - a second pass inside the limit deletes nothing;
  - `9.log` goes before `10.log`, so the order is numeric, not lexical;
  - a file named `LOG` is never touched.
- **Unit test** `a_segment_already_gone_still_counts_as_freed`: a listing
  goes stale because the oldest segment vanishes, and the pass deletes
  nothing more. Control: without the NotFound branch it fails.
- **`snapshot_pressure`**, the drill that caught it, run on macOS (a 512 MB
  HFS+ image via hdiutil, the internal disk):
  - with the fix, each 1 s pass trims the archive back, so it saws between
    26 and 126 MB (a segment is ~25 MB, and up to five rounds land between
    passes) and 0 writes are refused, in three runs. That is the documented
    `peak = budget + cadence x write rate`, at a 1 s cadence;
  - at origin/main 6d819ad8 the archive grows 25 MB a round to 276 MB and
    1350 writes are refused. That is CI's failure, reproduced. The EC2 gate
    box passed it five times (g577 to g581), so CI and this laptop are where
    the control shows.
