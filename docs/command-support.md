# Command support

Every supported command is gated by the conformance oracle: a corpus case
validated against a real Valkey and both Flint engines (mem, rocks). If it
is listed here, `flint-conformance` proves it behaves like Redis.

## Supported

**Connection / server**: PING, ECHO, AUTH (at the proxy), COMMAND, SELECT,
HELLO, QUIT (minimal, compatibility-shaped), DBSIZE, FLUSHALL (both scoped
to the tenant namespace).

**Keyspace**: DEL, UNLINK, EXISTS, TYPE, EXPIRE, PEXPIRE, EXPIREAT,
PEXPIREAT, TTL, PTTL, EXPIRETIME, PEXPIRETIME, PERSIST.

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

- **Scans are single-shot.** HSCAN/SSCAN/ZSCAN return the whole (filtered)
  collection with cursor `0` in one iteration — exactly Redis's own
  behavior for listpack/intset encodings, and a valid SCAN contract
  (every element once, terminating). COUNT is accepted as the hint it is.
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

## Not yet supported

- **Keyspace SCAN** (deferred: cursor semantics across migrating shard
  pairs need a design pass; per-key HSCAN/SSCAN/ZSCAN cover most uses).

## Excluded by design

- **Cross-slot multi-key commands** (SINTERSTORE, RENAME across slots,
  etc.), **MULTI/EXEC/WATCH**, **pub/sub**, **streams**, **blocking
  commands** (BLPOP, BLMOVE …), **KEYS/RANDOMKEY**, and **EVAL/EVALSHA**.
  These conflict with slot-sharded multi-tenancy or reintroduce the
  single-threaded bottlenecks Flint exists to avoid. Common patterns they
  serve are covered by first-class commands instead; if you need one of
  these, open an issue describing the workload — patterns with broad
  demand get first-class implementations.
