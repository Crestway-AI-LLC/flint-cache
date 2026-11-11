# BUG-0284: a key the storage engine could not read was served as absent, and written over (FIXED)

**Status:** **FIXED 2026-11-11.** Found the same day by the error-handling
review's area A question, "`Kv::get` turns a RocksDB read error into
`None`. Can an I/O error be served as a cache miss, or drive a write that
assumes the key is absent (`INCR`, `SETNX`, `SET NX`)?"
(`docs/error-handling-review.md`), and measured on release binaries of
public main `f26be7f4`.
**Severity:** high. A key RocksDB cannot read is answered as missing, and a
write that reads first writes over it: `INCR` stores 1, `SET NX` stores the
new value, and the GC deletes a hash's fields as orphans. Those writes ride
the WAL to the replicas like any other (by design; not measured here), so a
fault in one node's disk becomes lost data on every copy. It needs RocksDB to fail a read: a block whose checksum does not
match (a bad sector, a bit flipped at rest, a torn write), or an I/O error.
**Area:** `flint-storage` (`RocksKv`'s reads and scans, the GC sweep) and
`flint-server` (every command path, slot moves).

## What happens

- **`RocksKv::get` answered `None` for a read RocksDB could not do.** It
  was `self.db.get(key).ok().flatten()`, and `with_value` and `delete`'s
  existence check did the same. The `Kv` trait has no other answer, and no
  caller can tell `None` from a key that is not there.
- **Its scans stopped at the failure as if the range ended.** They read
  RocksDB's iterator through `filter_map(Result::ok)`, and the iterator
  ends at its error. `DBSIZE`, `SCAN`, `HGETALL` and every other prefix scan
  returned what came before the block.
- **A write that reads first wrote over the key.** `INCR` read no value and
  stored 1. `SET NX` read no key and stored. On a hash whose meta row is
  unreadable, `HSET` would start a new hash.
- **The GC deleted the fields of a hash it could not read.** Its orphan
  pass deletes a field row whose hash has no live meta row, and a meta row
  it could not read was no meta row. The field rows were readable, and they
  were deleted.
- **A slot move shipped the rows before the block.** FLINTMIGRATEOUT's
  bulk scan stopped there, the destination took the slot, and the source
  then purged every row of it, the readable ones after the block included.
  Found by reading, not measured: it is the same scan.
- **Nothing was logged.** The server's log had no line about it.

## Measured, 2026-11-11

Release `flint-server` of `f26be7f4`, `--engine rocks`, on a laptop. Data
written, the seat killed and started again so that recovery flushed it to
one SST, killed again, eight bytes of the SST overwritten with `0xff`, then
started again.

| | 2,000 strings `n:i` = 1000+i, one 39,870-byte SST, 8 bytes at offset 12,000 | 500 hashes of four fields, one 47,560-byte SST, 8 bytes at offset 3,000 |
|---|---|---|
| reads | `GET`: 1,896 values, **104 nil**, no error; `DBSIZE` **624** | `HLEN`: 75 hashes **0**; `DBSIZE` **73** |
| writes | `INCR n:41` (1041) answered **1**; `SET n:45 new NX` (1045) answered **OK**, and `GET n:45` then answered `new` | the GC, every second (`FLINTCONFIG gc-sweep-ms 1000`): `gc_swept_orphans` **300**, against 0 in the control run before the SST was touched |
| the SST put back as it was | | every `HLEN` 4 again, and `HGETALL` 1,700 fields of 2,000: 75 hashes (`h:2` among them) have their count and **no fields** |

So the 300 field rows were readable, and the GC deleted them because it
could not read their hashes.

## The fix

**A read the engine cannot do is recorded,** as BUG-0283 records a refused
write. `RocksKv`'s `get`, `with_value` and `delete` still answer `None` or
`false`, as the trait requires, and record the failure
(`flint_storage::note_read_fault`). Its scans go through `rows_until_fault`,
which records the failure and ends the scan.

**Work that acts on what it reads watches the engine** (`watch_engine`, an
`EngineWatch` for the thread until it is dropped). While a watch has seen a
failure, `RocksKv` writes nothing more (`writes_withheld`), so a key read
as absent cannot be written over. `EngineWatch::fault` says what failed,
read or write:

- **Every command `execute` dispatches** is watched from dispatch to reply,
  on a master and on a replica. A read the engine could not do fails any
  command: `ERR read failed: Corruption: block checksum mismatch: …`. A
  refused write fails a write command (BUG-0283).
- **`EXEC`, a script and a queued batch** commit nothing when their watch
  saw a failure: `ERR transaction aborted, nothing applied: …`, `ERR script
  aborted, none of its writes applied: …`, and `ERR batch aborted, nothing
  applied: …` to every command in the batch, which shares one buffer.
- **The GC's orphan pass** watches each judgment, and deletes nothing whose
  hash it could not read. Its expiry pass needs no watch: a re-read that
  fails reads as not expired.
- **A slot move's source** sends the destination an error instead of the
  end of the bulk, and the destination rolls back: `ERR migrate source: ERR
  the source's storage engine read failed: … while reading the slot;
  nothing is handed over`. The destination's own import is watched too, and
  its rollback writes its records unwithheld (`unwithheld`), since they must
  land however the import failed.
- **The server logs it,** one line every 10 s at most: `storage engine read
  failed: … (n engine failure(s) since the last such line; each one
  answered as an error, BUG-0283/BUG-0284)`.

**Measured after the fix** by `tools/engine_read_fault_drill.sh` (CORE,
port 6618): 2,000 strings and 500 hashes in one SST, eight bytes overwritten
at offset 3,000. 76 strings and 20 hashes answered `ERR read failed:
Corruption: block checksum mismatch: …` and none answered nil; `DBSIZE`
answered the same error. `INCR` on the even keys was refused 40 times and
`SET NX` on the odd ones 36 times, none answered as written. The GC, every
second, deleted the 4 fields of a hash deleted on purpose and nothing else.
With the SST put back as it was, every key held exactly what its answered
writes made it, and every hash its four fields. The same drill on the binary
before the fix, run with every check reporting: 76 nil, 20 hashes empty,
`DBSIZE` 97, `INCR` answered 1 40 times and `SET NX` OK 36 times, the GC
deleted 84 rows for 4 orphans, and with the SST put back 76 keys held `1`
or `over` and 20 hashes had lost their fields.

## What it does not do

- **It does not repair the block.** A key in it answers an error until the
  node's copy is replaced from a healthy one. A replica builds its own SSTs
  from the WAL, so a bad block on the master is not in its copy; replacing
  the master's copy is by hand, and was not exercised here.
- **Readers that act outside a watch still take a failed read for an
  absent key:** the role epoch `FLINTPROMOTE` and `FLINTDEMOTE` compare
  against, eviction's choice of rows, and the slot records. None was swept
  here; the review map lists them as open.
