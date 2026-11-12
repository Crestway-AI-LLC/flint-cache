# BUG-0285: a seat whose storage engine stopped taking writes stayed master and refused every write (FIXED)

**Status:** **FIXED 2026-11-11.** Found the same day by the error-handling
review's area A question, "After an fsync error from `flush_wal_sync`, does
the node keep serving as if the data were durable?"
(`docs/error-handling-review.md`), and measured on a release binary of public
main `006a103c`.
**Severity:** high for availability, none for data. Every write to the seat is
refused until someone restarts it by hand, and nothing reports why: the seat
stays master, holds its lease, answers reads, and its pair does not fail over.
No acknowledged write is lost: since BUG-0283 each refused write is answered
as an error. It needs one WAL append or sync to fail: a volume that fills
faster than the disk guard samples, or with the guard off, an I/O error, a
file-size limit.
**Area:** `flint-server` (the WAL fsync thread) and `flint-storage`
(`RocksKv::apply_writes`, the refused-write record).

## What happens

- **RocksDB keeps a WAL writer's first error.** Once an append or a sync of
  the WAL fails, `WritableFileWriter` answers every later append and sync
  with it (`Writer has previous error`), and the database refuses every
  write until it is opened again. RocksDB's own recovery did not end it in
  the 90 s measured below, and the C API this crate uses has no `Resume`.
- **The seat did not notice.** The WAL fsync thread logged
  `wal fsync tick failed: IO error: Writer has previous error.` on every
  tick and went on. Each write a client sent was answered with RocksDB's
  error (BUG-0283), and the seat stayed master.
- **Nothing outside the log showed it.** `FLINTINFO` said `role:master` and,
  once space was back, `disk_verdict:ok`. `wal_fsync_total` stopped moving,
  which it also does with `--wal-fsync-ms 0`.

## Measured, 2026-11-11

Release `flint-server` of `006a103c`, `--engine rocks`, its data on a 96 MB
HFS+ volume, the disk guard off (`--disk-min-free-pct 0
--disk-min-free-bytes 0`). A 78 MB file filled most of the volume, then
writes of 256 KiB values: 62 answered `+OK`, then `ERR batch commit failed:
IO error: No space left on device: While appending to file: …`.

| | `SET` | `HSET` | `GET` of a key written before | `FLINTINFO` |
|---|---|---|---|---|
| right after the fault | `ERR … Writer has previous error.` | `ERR write failed: … Writer has previous error.` | the value | |
| the 78 MB file deleted (79,872 KiB free), then 0, 5, 30 and 90 s later | the same error each time | the same error each time | the value | `role:master`, `disk_verdict:ok`, `disk_free_pct:81` |
| after a restart by hand | `+OK` | `:1` | the value | |

After the restart, all 62 acknowledged writes read back at their full
length. So opening the
database again is what ends the state, and nothing in the seat did it.

The file-size limit of `tools/engine_write_fault_drill.sh` (`ulimit -f`,
SIGXFSZ ignored) gives the same state: 15 s after the first refusal the seat
was up and an `HSET` of one byte was refused.

## The fix

**A seat whose engine has stopped taking writes ends itself,** with status
70, as a seat whose vital thread dies does (ADR-0065). The WAL fsync thread
is that thread, and the WAL sync is the call that answers whether the engine
still takes writes:

- **It syncs when a write has been refused,** besides its cadence, and again
  every half second at most until a sync succeeds. `flint_storage` counts
  every write the engine refuses (`Kv::put`, `delete` and `clear` through
  `note_write_fault`, and `apply_writes`), and the thread reads the count.
  So the seat learns of the fault within a second whatever the cadence,
  `--wal-fsync-ms 0` included, and a seat with no write refused syncs on its
  cadence as before.
- **Three syncs refused in a row end the seat:** `the storage engine refused
  3 WAL syncs in a row (IO error: …); RocksDB takes no write after its WAL
  fails until it is opened again, so exiting with status 70 for the seat to
  be started again (BUG-0285)`, through `hard_exit` since the database is
  open. One would do, since RocksDB keeps the error; three rule out a sync
  that failed once and would not again.
- **The supervisor starts the seat again** and opening the database
  recovers every acknowledged write from the WAL, as measured above; a
  master's pair fails over meanwhile, as ADR-0065 describes for any seat
  that stops. A seat whose volume is still full or still failing would fail
  again at its open, or at its next write, and stay down where its pair and
  the operator see it (not measured).

**Measured after the fix** by `tools/engine_write_fault_drill.sh` (CORE, port
6617), which now also waits for the seat to leave: under the 16 MiB limit,
40 rounds of `SET`, `HSET`, `APPEND` and `SET … EX`; `HSET` and `APPEND`
were refused 24 times and `SET … EX` 25, each answered `ERR write failed`;
the seat then exited with status 70 naming BUG-0285; restarted without the
limit, every write answered as a success read back whole and new writes
landed. The same drill on `006a103c`, before the fix: `FAIL (BUG-0285): 15 s
after its engine refused a write the seat is still up, refusing every write`.

On the full 96 MB volume of the first measurement, two runs at the default
`--wal-fsync-ms 500` and one at `--wal-fsync-ms 0`: 62 writes answered `+OK`,
the 63rd `No space left on device`, and the seat exited with status 70 1.2 to
1.4 s after that refusal. Restarted, each read all 62 back at their full
length once it had finished loading, and took new writes.

## What it does not do

- **A failed WAL sync ends the seat the same way,** because RocksDB keeps a
  sync's error as it keeps an append's (`WritableFileWriter::Sync`). That was
  found by reading RocksDB, not measured: a failing fsync needs a block device
  that fails on purpose, which takes root.
- **What a failed fsync did to the bytes is not answered.** After `fsync`
  fails, Linux may mark the pages it could not write as clean, so the WAL the
  restarted seat reads can hold writes that are not on the disk. Those are the
  writes of the `--wal-fsync-ms` window, the same ones a host loss takes.
- **It does not repair the volume.** A seat whose volume still fails or is
  still full exits again at its next open, or cannot open, and stays down.
