# BUG-0283: a write the storage engine refused was answered as written, unless it was a plain SET (FIXED)

**Status:** **FIXED 2026-11-11.** Found the same day by the error-handling
review's area A question, "Does every write a client is told succeeded go
through `apply_writes`, which returns RocksDB's error?"
(`docs/error-handling-review.md`), and measured on release binaries of
public main `f26be7f4`. v0.1.0-rc.87 has the same `RocksKv::put`. The review
map's area N sweep had said acknowledged writes did not take that path. Of
single commands, only a three-token `SET k v` did not.
**Severity:** high. The client is told a write succeeded, and the key is not
there: not after a restart, and not on the seat that answered. It needs
RocksDB to refuse a write: a volume that fills (the disk guard refuses
writes first, so a seat with the guard on needs a fill faster than the guard
samples, or the guard turned off, and neither was measured), a volume that
fails with an I/O error, or a file-size limit.
**Area:** `flint-storage` (`RocksKv`'s `Kv::put`, `delete` and `clear`) and
`flint-server` (`execute`'s inline write path, slot-migration import,
`FLINTINDEXED`).

## What happens

- **Only a plain `SET` reaches the engine through a call that can fail.**
  ADR-0027 batches a pure write, `SET key value` with no option, and commits
  the batch through `apply_writes`, whose RocksDB error becomes every reply
  in it. `EXEC`, scripts and the async write queue commit the same way.
- **Every other write runs on the store row by row.** `HSET`, `APPEND`,
  `INCR`, `SET … EX`, `LPUSH`, `ZADD` and the rest call `Kv::put` and
  `Kv::delete`, which return nothing. `RocksKv` implemented them as
  `let _ = self.db.put(key, value)`, so a refused row was dropped there, and
  the command answered from what it had computed: `:1` for a new hash field,
  the new length for an `APPEND`.
- **After a refused WAL write, RocksDB refuses every write** until the seat
  restarts (`Writer has previous error`). From the first refusal on, every
  write that was not a plain `SET` was answered as a success and stored
  nothing.
- **A slot move imported its rows the same way.** `migrate_in` applies each
  row from the source with `kv.put`, then freezes the source and flips the
  slot to itself, and the source purges its copy. A row the destination's
  engine refused was in neither copy afterwards. That path was found by
  reading and was not measured; it is the same call.

## Measured, 2026-11-11

Release `flint-server` of `f26be7f4`, `--engine rocks`, on a laptop. Each
round wrote four keys with 256 KiB values, one command at a time on one
connection: `SET s:i v`, `HSET h:i f v`, `APPEND a:i v`, and in the second run
also `SET e:i v EX 3600`. Then every key was read back.

| | 96 MB volume (HFS+ image, disk guard off), 40 rounds | 16 MiB file-size limit (`ulimit -f`, SIGXFSZ ignored), 40 rounds |
|---|---|---|
| `SET` | 18 `+OK`, 22 `ERR batch commit failed: IO error: No space left on device` | 21 `+OK`, 19 `ERR batch commit failed: IO error: While appending to file` |
| `HSET` | 40 `:1` | 40 `:1` |
| `APPEND` | 40 `:262144` | 40 `:262144` |
| read back on the same seat | rounds 0-17 whole; rounds 18-39: all three keys absent | rounds 0-20 whole; rounds 21-39: all three keys absent |

The second seat was then restarted without the limit. It opened cleanly,
served rounds 0-20 and none of the rest, and took new writes. So the 38
HSET and APPEND writes that were answered as successes after the fault had
never been stored. The seat's log said `wal fsync tick failed: IO error:
Writer has previous error.` once a second; nothing reached a client.

## The fix

**A row the engine refuses is recorded on the thread that wrote it.**
`RocksKv`'s `put`, `delete` and `clear` keep RocksDB's error with
`flint_storage::note_write_fault`, since their signature cannot return it.
A command runs on one thread from start to reply, with no await between its
rows, so the work that watches the engine (`flint_storage::watch_engine`,
which BUG-0284 extends to reads) learns of the first failure from
`EngineWatch::fault`.

- **A write command** is watched from dispatch to reply. A refusal replaces
  the reply with `ERR write failed: <RocksDB's message>`. A read whose
  lazy-expiry delete was refused keeps its reply, which that delete does
  not change.
- **A slot import** is watched from its start and reads the watch at each
  caught-up point. A refusal rolls the import back, unfreezing the source if
  it was frozen, and answers `ERR migrate: this node's storage engine write
  failed: … during the import; the slot was not taken`. The source keeps the
  slot and its rows.
- **`FLINTINDEXED`** answers an error when its prefix row was not stored.
  The prefixes guard the node from then on either way. Only a restart
  forgets a row that did not land.

**Measured after the fix,** on the same file-size limit, by
`tools/engine_write_fault_drill.sh` (CORE, port 6617): of 40 rounds, `SET`
was refused 24 times (`ERR batch commit failed`), `HSET` and `APPEND` 24
times and `SET … EX` 25 times, each `ERR write failed: IO error: While
appending to file: …`. Every write answered as a success read back whole,
and again after a restart without the limit, and the restarted seat took
new writes. The same drill on the binary before the fix answered all 40 of
each kind as written, and 73 of them read back absent: `FAIL (BUG-0283)`.

## What it does not do

- **A command that writes several rows is not all-or-nothing on this
  path.** The rows written before the refused one stay. `HSET` writes its
  field rows and then the hash's meta row, so a refusal between them on an
  existing hash leaves a field that `HGETALL` shows and `HLEN` does not
  count; on a new hash, rows no meta row reaches, which the GC sweeps. The
  client is now told the command failed, and a refusal starts part way
  through a command only when the fault arrives between two of its rows.
  `EXEC` and scripts are all-or-nothing because they commit one
  `WriteBatch`. Committing every inline write that way is a change to the
  write path's design (a buffer on every write, and `FLUSHALL`'s deletes are
  unbounded), proposed in the review map, not built.
- **What still calls `put` and `delete` without a reader of the record** is
  best-effort, and no client is told it succeeded: the GC sweep, lazy
  expiry, the purge of a slot this node has disowned (a refused delete
  leaves rows nothing serves, as a crash part way through the purge does)
  and `repl.rs`'s upstream index row.
- **The role write** (`commit_role`, area C) is a `Kv::put` too. Its
  refusal is not read here: it changes the running role as well as the row,
  so it needs the synced, fallible write area C proposes, not a reply
  rewritten after the fact.
