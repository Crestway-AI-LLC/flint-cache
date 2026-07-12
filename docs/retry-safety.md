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

### Safe to retry (state converges; only the integer reply may differ)

`GET` · `SET` (plain) · `MSET` · `DEL` · `EXISTS` · `TYPE` · `HSET` ·
`HDEL` · `SADD` · `SREM` · `ZADD` · `ZREM` · `SETEX`/`SET … EXAT`/`PEXPIREAT`
(absolute expiry) · `EXPIREAT`

Re-applying reaches the same end state. `DEL`/`HDEL`/`SREM`/`ZREM` may return
a smaller count on the retry (already removed), but the state is correct.

### NOT safe to retry — application must guard

| Command | Hazard on retry |
|---|---|
| `INCR` `DECR` `INCRBY` `DECRBY` `HINCRBY` `ZINCRBY` | Double-counts. |
| `APPEND` | Double-appends. |
| `LPUSH` `RPUSH` | Double-pushes. |
| `LPOP` `RPOP` | Pops an EXTRA element — silent data loss on retry. |
| `SET … NX` | If the first succeeded but the ack was lost, the retry sees the key present and returns nil, so the caller wrongly believes it failed. The classic lock hazard. |
| `EXPIRE` `PEXPIRE` `SET … EX/PX` (relative TTL) | Retry recomputes from a later clock, extending the TTL. Use the absolute `EXPIREAT`/`PEXPIREAT`/`EXAT`/`PXAT` forms for retry safety. |

### Guidance

- For counters that must survive ambiguous failures, prefer an idempotent
  pattern (write an absolute value with `SET`, or reconstruct from a
  source of truth) rather than blind `INCR` retries.
- For locks, use a unique token value with `SET NX` and verify ownership by
  reading the token back rather than trusting the `SET NX` reply alone.
- Prefer absolute-time TTL commands (`EXPIREAT`, `PEXPIREAT`, `SET … EXAT`)
  wherever a client may retry.

## Replica read semantics (related)

Replicas never write to their own store. A read of a logically-expired key
returns nil (correct), but the physical row is reclaimed by the master's
replicated `DELETE` and by the compaction filter — not by a local delete on
the replica. This keeps a replica a faithful, write-free copy of its master
(see `ReadOnlyKv`).
