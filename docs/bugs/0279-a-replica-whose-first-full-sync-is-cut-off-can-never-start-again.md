# BUG-0279: a replica whose first full sync is cut off after `CURRENT` arrives can never start again (FIXED)

**Status:** **FIXED 2026-11-05.** Found the same day by the error-handling
review's area B (`docs/error-handling-review.md`: "If a full sync is cut
off, can the replica start serving a partial copy?") and measured on a box.
In public main (`93dcd0c3`) and v0.1.0-rc.86.
**Severity:** medium. No partial copy is ever served: RocksDB refuses it.
But the replica cannot start again until an operator deletes its data
directory, so its pair runs without a replica, and with
`min-replicas-to-write` it sheds writes. A full sync runs at 64 MiB/s by
default, so seeding 1 TB takes about 4 hours, and a restart in that window
(an upgrade's roll, a reboot, an OOM kill) is enough.
**Area:** `flint-server`, the replica's `full_sync_download` and the start
that decides whether a data directory is fresh.

## What happens

- **The download writes straight into the data directory.** Each file the
  master ships is created there as it arrives.
- **The master ships files in `read_dir` order,** which on Linux follows the
  directory's hash, not any order the download relies on. So `CURRENT` can
  arrive before the MANIFEST it names, or before an SST, or while one is
  half-written.
- **A start decides "fresh" by `CURRENT` alone.** With `CURRENT` there, the
  directory is opened as a copy. RocksDB refuses it, and the seat exits 1.
  The supervisor starts it again a minute later, and it exits again.

## Measured, 2026-11-05

Release binaries of public `93dcd0c3` on a gate box (c7i.xlarge). A RocksDB
master holding 100,000 keys of 1 KB of random data, `--fullsync-rate-bytes`
2 MiB/s.

**A replica killed part way through its download,** six times, 1 to 26 s
in: each kill left one or two files and no `CURRENT`, because this
checkpoint's `read_dir` order put the SSTs first. Started again, each saw no
`CURRENT`, took the directory as fresh, and seeded again. That is the safe
case, and it depends on the order.

**The states a cut leaves once `CURRENT` is in,** made from a finished copy
and started as a replica, as a supervisor would:

| state | start |
|---|---|
| the largest SST missing | exit 1: `rocksdb open: Corruption: ... 000009.sst: No such file or directory` |
| the largest SST half its size | exit 1: `rocksdb open: Corruption: Sst file size mismatch ... Expected 64982457, actual size 32491228` |
| the MANIFEST that `CURRENT` names missing | exit 1: `rocksdb open: IO error: ... MANIFEST-000015: No such file or directory` |

Each start ends the same way, every time.

## The fix

**A marker says the copy is not whole.** `full_sync_download` writes
`FULLSYNC_INCOMPLETE` into the directory before the first file, and removes
it at `FULLSYNC-END` only after it has synced every file and the directory.
A crash after that cannot leave a copy that looks whole and is not.

**A start that finds it discards the copy** (`discard_cut_off_full_sync`),
before anything reads the directory. As a replica, the copy never served, so
it is removed and the start seeds again, as a fresh replica does. Without
`--replica-of` there is nothing to seed it from, so the start is refused,
saying so, and the copy is kept, rather than the seat coming up empty as a
master. An attempt retried within one process starts from an empty directory
too, so a file only an earlier checkpoint had is not left behind.

**Not in this fix:** `--restore-from` copies a snapshot into the data
directory file by file the same way, and a crash part way through it would
leave the same kind of copy. It runs once, by hand, on a spare.

## Acceptance

`fullsync_cutoff_drill`, on real seats: a replica killed part way through its
download leaves the marker; with `CURRENT` added, as when it arrives first,
started again as a replica it logs `FULLSYNC_INCOMPLETE present`, seeds again,
and holds every key, sampled equal to the master; started without
`--replica-of` the same copy is refused, naming `--replica-of`, and kept; a
finished sync leaves no marker. Before the fix, the state with `CURRENT` in
exits 1 at `rocksdb open` (the table above). Unit tests:
`cut_off_full_sync_tests::a_copy_with_no_marker_is_kept`,
`a_replicas_cut_off_copy_is_discarded` and
`without_a_master_the_start_is_refused_and_the_copy_kept`.
