# Command support

Every supported command is gated by the conformance oracle: a corpus case
run against both Flint engines (mem, rocks). For every family with a
counterpart in the reference implementation, the same case also runs
against a real Valkey — so a green run proves two independent things: the
case encodes real Redis behavior, and Flint matches it.

**One exception, stated plainly: the JSON family has no reference.** Stock
Redis/Valkey have no JSON type (it is the separate RedisJSON module), so
those cases assert the contract we chose — modelled on RedisJSON, and
documented under "Semantics worth knowing" where we deliberately diverge —
and prove our two engines agree on it. They do not prove bug-for-bug
RedisJSON compatibility. `flint-conformance --reference` skips them rather
than reporting a failure that would say nothing about either side.

## Supported

**Connection / server**: PING, ECHO, AUTH (at the proxy), COMMAND, SELECT,
HELLO, QUIT (minimal, compatibility-shaped), DBSIZE, FLUSHALL (both scoped
to the tenant namespace).

**Keyspace**: DEL, UNLINK, EXISTS, TYPE, EXPIRE, PEXPIRE, EXPIREAT,
PEXPIREAT, TTL, PTTL, EXPIRETIME, PEXPIRETIME, PERSIST.

**JSON documents**: JSON.SET (NX, XX), JSON.GET, JSON.DEL / JSON.FORGET,
JSON.TYPE, JSON.NUMINCRBY, JSON.ARRAPPEND, JSON.ARRLEN.

**Keyspace iteration**: SCAN (MATCH, COUNT, TYPE) — incremental, works
through the proxy across all shard pairs as one cursor stream (redis-cli
`--scan`, RedisInsight, and client iterators work as-is).

**Strings**: SET (NX, XX, EX, PX, EXAT, PXAT, KEEPTTL, GET), SETNX, SETEX,
GET, GETDEL, GETSET, MSET, MGET, APPEND, STRLEN, GETRANGE, SETRANGE,
INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT.

**Hashes**: HSET, HSETNX, HGET, HMGET, HGETALL, HDEL, HLEN, HEXISTS,
HINCRBY, HSTRLEN, HSCAN (MATCH, COUNT, NOVALUES).

**Sets**: SADD, SREM, SISMEMBER, SMISMEMBER, SMEMBERS, SCARD, SPOP,
SRANDMEMBER, SSCAN (MATCH, COUNT).

**Lists**: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LTRIM,
LREM, LINSERT, LPOS (RANK, COUNT, MAXLEN).

**Sorted sets**: ZADD, ZSCORE, ZMSCORE, ZINCRBY, ZREM, ZCARD, ZRANGE,
ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE (WITHSCORES, LIMIT, exclusive
bounds, ±inf), ZRANK, ZREVRANK, ZCOUNT, ZPOPMIN, ZPOPMAX,
ZREMRANGEBYSCORE, ZREMRANGEBYRANK, ZSCAN (MATCH, COUNT).

## Semantics worth knowing

- **Collection scans are single-shot.** HSCAN/SSCAN/ZSCAN return the whole
  (filtered) collection with cursor `0` in one iteration — exactly Redis's
  own behavior for listpack/intset encodings, and a valid SCAN contract
  (every element once, terminating). COUNT is accepted as the hint it is.
- **Keyspace SCAN cursors are server-side sessions**, not Redis's
  reversed-bit bucket indexes. Guarantees are Redis-compatible (keys
  present throughout the scan are returned — here exactly once; COUNT
  bounds rows examined per batch), with two visible differences: a cursor
  Flint never issued (or one idle > 2 minutes, or one whose shard failed
  over mid-scan) answers `ERR invalid cursor` — restart the scan — where
  Redis would silently accept any integer; and cursors are bound to the
  tenant that opened them. Client iterators only ever echo server cursors,
  so real tools (redis-cli `--scan`, RedisInsight, client `scan_iter`s)
  are unaffected.
- **JSON paths are a single-match subset.** `$`, object members, and array
  indexes in any mix — `$.user.tags[0]`, `$["odd key"].n`, negative indexes
  counting from the end, and the legacy dot form (`user.tags[0]`). Wildcards
  (`$.a[*]`, `$.*`), recursive descent (`$..a`), slices, and filters are
  rejected as UNSUPPORTED — a distinct error from a malformed path, because
  they would turn every command into a multi-match API and we would rather
  say no than guess a semantic we cannot change later.
- **JSON writes create the leaf, never intermediate levels**, so a typo
  cannot silently grow a document a shape you did not ask for; a sub-path
  write preserves the key's TTL, while a root write clears it like a plain
  SET. **JSON.SET will not overwrite a non-JSON key** (WRONGTYPE) — unlike a
  plain SET, a document write is never a silent way to destroy a string or a
  hash. JSON.NUMINCRBY keeps integers integral. Documents are stored as one
  row, so they live beyond RAM like any value; sub-document writes rewrite
  that row.
- **INCRBYFLOAT** formats like Redis (`%.17f`, trailing zeros trimmed).
- **Expiry is lazy + swept**: an expired key reads as missing immediately;
  physical reclamation is background.
- **Cluster is invisible**: clients never see `-MOVED`/`-ASK`; the proxy
  absorbs topology. Hash tags (`{...}`) work as in Redis Cluster.
- **Error vocabulary** beyond Redis's: `-QUOTA` (writes shed over storage
  quota; reads and space-reducing commands still served), `-THROTTLED`
  (rate quota / back-pressure; retry with backoff), `-TRYAGAIN`
  (mid-migration write or fenced stale replica; the proxy retries/falls
  back for you).

## Excluded by design

- **Cross-slot multi-key commands** (SINTERSTORE, RENAME across slots,
  etc.), **MULTI/EXEC/WATCH**, **pub/sub**, **streams**, **blocking
  commands** (BLPOP, BLMOVE …), **KEYS/RANDOMKEY**, and **EVAL/EVALSHA**.
  These conflict with slot-sharded multi-tenancy or reintroduce the
  single-threaded bottlenecks Flint exists to avoid. Common patterns they
  serve are covered by first-class commands instead; if you need one of
  these, open an issue describing the workload — patterns with broad
  demand get first-class implementations.
