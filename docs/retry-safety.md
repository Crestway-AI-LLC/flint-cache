# Command retry safety

There are two independent notions of idempotence in Flint. Don't conflate
them.

## 1. Replication / WAL replay — idempotent by construction

A replica applying the master's log is safe to replay because **replication
ships effects, not commands**: the only replicated operations are physical
`Put` and `Delete` (see `ReplOp`). The master resolves every logical command
to concrete row writes *before* they enter the WAL — `INCR` becomes
`Put key=6`, `LPUSH` becomes a `Put` at a computed index — so re-applying a
batch converges to the same bytes. The apply path additionally guards
against stale and out-of-order batches (a batch at or below the cursor is a
no-op; a non-contiguous batch is a `SequenceGap`). Determinism holds because
version numbers and absolute TTLs are computed once on the master and shipped
inside the row. **Nothing further is needed here.**

## 2. Client retry across failover — NOT automatically safe

When a client's connection drops mid-command (e.g. during a failover) it
cannot know whether the write applied. Whether re-sending is safe depends on
the command. This matches Redis semantics: the application, or an SDK layer,
owns the risk. A server-side idempotency layer (client-supplied command
token, deduped on the master) is the eventual fix and is deferred past v0.

### The rule that decides it

Before the lists, the principle they follow — because a table is always one
command behind the server, and this is not:

**A retry is safe when the command names WHAT to change. It is unsafe when
the command names WHERE, HOW MANY, or HOW MUCH.** A second delivery of
"set key k to v" or "remove member m" reaches the same state. A second
delivery of "remove the first element", "trim to positions 1..2", "remove 1
occurrence" or "add 5" does not, because the retry addresses a different
element or applies a second time.

Three corollaries, each measured rather than assumed:

- **Position- and rank-addressed operations are unsafe even when they look
  like plain deletes.** `LTRIM key 1 2` on `a b c d` leaves `b c`; sending it
  again leaves `c`. `ZREMRANGEBYRANK key 0 0` removes a second member,
  because a new member now has rank 0.
- **The value-addressed twins of those same commands are safe.**
  `ZREMRANGEBYSCORE`, `ZREMRANGEBYLEX` and `LREM key 0 m` (count 0 = all
  occurrences) converge. `LREM key 1 m` does not.
- **A count or a delta makes an otherwise safe command unsafe.** `LREM` with
  a non-zero count, and every `INCR`-shaped command.

### Safe to retry (state converges; the reply may differ)

**Strings / keyspace**: `GET` · `SET` (plain) · `MSET` · `SETRANGE`
(absolute offset) · `GETSET` · `DEL` · `UNLINK` · `EXISTS` · `TYPE` ·
`FLUSHALL` · `PERSIST` · `COPY … REPLACE`

**Absolute expiry**: `SETEX` · `SET … EXAT`/`PXAT` · `EXPIREAT` ·
`PEXPIREAT` · `GETEX EXAT`/`PXAT`/`PERSIST`

**Collections**: `HSET` · `HDEL` · `SADD` · `SREM` · `ZADD` · `ZREM` ·
`LSET` (absolute index, absolute value) · `LREM key 0 m` ·
`ZREMRANGEBYSCORE` · `ZREMRANGEBYLEX` · the `STORE` variants
(`ZUNIONSTORE`, `ZINTERSTORE`, `SINTERSTORE`, `SUNIONSTORE`, `SDIFFSTORE`),
which overwrite their destination

**Documents and filters**: `JSON.SET` · `JSON.DEL` · `JSON.FORGET` ·
`BF.ADD` · `BF.MADD` · `BF.INSERT`

Re-applying reaches the same end state. **The reply may not match the
first one**, and that is the trap inside this list rather than a footnote to
it: `DEL`/`HDEL`/`SREM`/`ZREM` return a smaller count, and `BF.ADD` returns
0 where the first returned 1. If your code reads that reply as "was this new"
— a dedupe check, a first-writer-wins election — the STATE is right and your
CONCLUSION is wrong, which is the hazard in the next table, not this one.

### NOT safe to retry — application must guard

| Command | Hazard on retry |
|---|---|
| `INCR` `DECR` `INCRBY` `DECRBY` `INCRBYFLOAT` `HINCRBY` `ZINCRBY` `JSON.NUMINCRBY` | Double-counts. |
| `APPEND` `JSON.ARRAPPEND` | Double-appends. |
| `LPUSH` `RPUSH` | Double-pushes. |
| `LINSERT` | Double-inserts: `a b` becomes `a x x b`. |
| `LPOP` `RPOP` `SPOP` `ZPOPMIN` `ZPOPMAX` | Destroys an EXTRA element — silent data loss. `SPOP` on `{a,b,c}` returns `c`, then the retry returns `b` and two members are gone. |
| `LTRIM` `ZREMRANGEBYRANK` `LREM key <n≠0> m` | Position- or count-addressed, so the retry cuts a DIFFERENT set. `LTRIM 1 2` twice on `a b c d` leaves `c`. Also silent data loss. |
| `SET … NX` `SETNX` `HSETNX` | If the first succeeded but the ack was lost, the retry sees the key present and returns 0/nil, so the caller wrongly believes it failed. The classic lock hazard. |
| `GETDEL` | The first returns the value and the retry returns nil, so a retrying reader loses the only copy it was handed. |
| `COPY` (without `REPLACE`) | Returns 0 on the retry: the copy exists, and the caller is told it does not. |
| `RENAME` `RENAMENX` | The retry answers `ERR no such key` — the source moved on the first attempt. The rename SUCCEEDED and the caller sees an error. |
| `BF.RESERVE` | The retry answers `ERR item exists`, same shape: created, reported as failed. |
| `EXPIRE` `PEXPIRE` `SET … EX/PX` `GETEX EX/PX` (relative TTL) | Retry recomputes from a later clock, extending the TTL. Use the absolute `EXPIREAT`/`PEXPIREAT`/`EXAT`/`PXAT` forms for retry safety. |

Note the shape shared by the last five rows: **the write landed and the retry
reports failure.** That is more dangerous than a visible error, because the
natural response — retry again, or fall back — is wrong in both directions.

### Guidance

- For counters that must survive ambiguous failures, prefer an idempotent
  pattern (write an absolute value with `SET`, or reconstruct from a
  source of truth) rather than blind `INCR` retries.
- For locks, use a unique token value with `SET NX` and verify ownership by
  reading the token back rather than trusting the `SET NX` reply alone.
- Prefer absolute-time TTL commands (`EXPIREAT`, `PEXPIREAT`, `SET … EXAT`)
  wherever a client may retry.
- For pops and trims, do not retry blind. Either make the operation
  value-addressed (`LREM key 0 m` instead of `LPOP`, `ZREMRANGEBYSCORE`
  instead of `ZREMRANGEBYRANK`), or treat an ambiguous failure as "the
  element may be gone" and reconcile, because a retry will take another one.
- Where the hazard is a MISLEADING REPLY rather than wrong state — `SETNX`,
  `COPY`, `RENAME`, `BF.RESERVE`, `GETDEL` — verify by reading, not by
  trusting the reply. That is the same advice as the lock guidance above and
  it generalises: after an ambiguous failure, ask the server what is true.

`tools/gates.sh` checks that every command `flint_commands::is_write_command`
classifies as a write appears in one of the two tables above, so this page
cannot silently fall behind the server (BUG-0107). The check enforces
PRESENCE, not correctness: putting a command in the wrong table is still a
judgement nothing but review will catch.

## Replica read semantics (related)

Replicas never write to their own store. A read of a logically-expired key
returns nil (correct), but the physical row is reclaimed by the master's
replicated `DELETE` and by the compaction filter — not by a local delete on
the replica. This keeps a replica a faithful, write-free copy of its master
(see `ReadOnlyKv`).
