# Command support

Every supported command is gated by the conformance oracle: a corpus case
run against both Flint engines (mem, rocks). For every family with a
counterpart in the reference implementation, the same case also runs
against a real Valkey — so a green run proves two independent things: the
case encodes real Redis behavior, and Flint matches it.

**One exception, stated plainly: the JSON family has no reference in that
run.** Stock Redis/Valkey have no JSON type — it is the separate RedisJSON
module — so `flint-conformance --reference` skips those cases rather than
reporting a failure that would say nothing about either side.

They are checked against the real thing separately. `tools/redisjson_compare.sh`
loads a RedisJSON module built from source and runs this same corpus against
it; the gate is that exactly three cases differ, and that those three are the
ones listed under "Where we differ from RedisJSON" below. So the JSON
contract is a verified match, not a reading of the docs — but the check is
on-demand rather than in CI, because it needs a module you have to compile.

## Supported

**Connection / server**: PING, ECHO, AUTH (at the proxy), COMMAND,
SELECT (index 0 only), HELLO, QUIT (minimal, compatibility-shaped), DBSIZE,
FLUSHALL (both scoped to the tenant namespace).

> A namespace is one logical database, so `SELECT 0` succeeds and any other
> index is refused. Tenancy replaces numbered databases here: isolation is
> the namespace, which the proxy pins per connection.

**Keyspace**: DEL, UNLINK, EXISTS, TYPE, EXPIRE, PEXPIRE, EXPIREAT,
PEXPIREAT, TTL, PTTL, EXPIRETIME, PEXPIRETIME, PERSIST, COPY (REPLACE,
DB 0).

> COPY is **same-slot only**, for the same reason as the set operations: the
> destination is written into the node's local rows, so a destination in a
> slot the node does not own would be stored where nothing can read it and
> COPY would report success having created nothing. Colocate with a hash tag
> (`COPY {u1}:a {u1}:b`) or the request is refused with `CROSSSLOT`.
>
> `DB` is accepted only as `DB 0`. A namespace has exactly one logical
> database, so index 0 names the one the client is already in; any other
> index is refused rather than quietly redirected into database 0. This is a
> deliberate divergence from a stock Valkey, which has sixteen.

**JSON documents**: JSON.SET (NX, XX), JSON.GET, JSON.DEL / JSON.FORGET,
JSON.TYPE, JSON.NUMINCRBY, JSON.ARRAPPEND, JSON.ARRLEN.

**Keyspace iteration**: SCAN (MATCH, COUNT, TYPE) — incremental, works
through the proxy across all shard pairs as one cursor stream (redis-cli
`--scan`, RedisInsight, and client iterators work as-is).

**Strings**: SET (NX, XX, EX, PX, EXAT, PXAT, KEEPTTL, GET), SETNX, SETEX,
GET, GETDEL, GETEX (EX, PX, EXAT, PXAT, PERSIST), GETSET, MSET, MGET,
APPEND, STRLEN, GETRANGE, SETRANGE, INCR, DECR, INCRBY, DECRBY,
INCRBYFLOAT.

**Hashes**: HSET, HSETNX, HGET, HMGET, HGETALL, HDEL, HLEN, HEXISTS,
HINCRBY, HSTRLEN, HSCAN (MATCH, COUNT, NOVALUES).

**Sets**: SADD, SREM, SISMEMBER, SMISMEMBER, SMEMBERS, SCARD, SPOP,
SRANDMEMBER, SSCAN (MATCH, COUNT), SINTER, SUNION, SDIFF.

> SINTER / SUNION / SDIFF are **same-slot only**, exactly as in Redis
> Cluster: colocate the keys with a hash tag (`SINTER {u1}:a {u1}:b`) or the
> request is refused with `CROSSSLOT`. Refused rather than answered, because
> a key the node does not own reads as an empty set and an intersection
> against a phantom empty set is silently wrong. The `STORE` variants
> (SINTERSTORE etc.) remain excluded — see below.

**Lists**: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LTRIM,
LREM, LINSERT, LPOS (RANK, COUNT, MAXLEN).

**Sorted sets**: ZADD, ZSCORE, ZMSCORE, ZINCRBY, ZREM, ZCARD, ZRANGE,
ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE (WITHSCORES, LIMIT, exclusive
bounds, ±inf), ZRANGEBYLEX, ZREVRANGEBYLEX (LIMIT, exclusive bounds,
`-`/`+`), ZRANK, ZREVRANK, ZCOUNT, ZPOPMIN, ZPOPMAX, ZREMRANGEBYSCORE,
ZREMRANGEBYRANK, ZSCAN (MATCH, COUNT).

> The lex forms are meaningful only when every member shares one score —
> the same condition Redis states — because the index is ordered by
> (score, member). Flint matches upstream's seek-then-walk behaviour rather
> than filtering, so a mixed-score set returns what Valkey returns even
> though neither defines it.

## Protocols: RESP2 and RESP3

Both, negotiated per connection with `HELLO`. Connections start at RESP2;
`HELLO 3` switches, and `HELLO` reports which is in force. This is not
cosmetic — **redis-py 8 defaults to RESP3 and sends its credentials inside
the handshake** (`HELLO 3 AUTH default <token>`) rather than as a separate
`AUTH`, so a server without it is unreachable from most of the current
Python ecosystem, not merely degraded.

Under RESP3 the replies that have a real type get one, exactly as Redis
sends them: `HGETALL` is a map, `SMEMBERS` and `SPOP key count` are sets,
`ZSCORE`/`ZINCRBY` are doubles, `ZRANGE … WITHSCORES` and `ZPOPMIN key
count` are member/score pairs, and null is `_`. Clients therefore hand you
a `dict`, a `set`, and a `float` without post-processing. RESP2 keeps the
flattened spellings it always had, byte for byte.

Worth knowing because the obvious guess is wrong: `HSCAN`/`SSCAN`/`ZSCAN`,
`SRANDMEMBER`, `SMISMEMBER`, `SCAN`, `LPOS`, and `INCRBYFLOAT` are
identical in both protocols — scan cursors still carry string scores. The
shapes here were captured off the wire from a real Redis 8.2 rather than
read off a spec, and `flint-conformance --proto 3` runs the whole corpus
over RESP3 (against Flint and against a reference Valkey) to keep them
honest.

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
- **JSON paths come in two dialects, and the leading `$` picks which.**
  This is RedisJSON's rule and clients depend on it, so we follow it
  exactly. A `$` path (`$.a`) is JSONPath: the reply is a *container of
  matches*, and a path matching nothing is an empty container. A path
  without it (`.a`, `a`, or no path at all) is the legacy dialect: the
  reply is the bare value, and a path matching nothing is an error.

  | | `JSON.GET d $.a` | `JSON.GET d .a` |
  |---|---|---|
  | match | `[1]` | `1` |
  | no match | `[]` | `ERR Path does not exist` |
  | wrong shape (`ARRLEN` on an object) | one null element | error |

  JSON.GET and JSON.NUMINCRBY carry the container inside the JSON they
  return; JSON.TYPE, JSON.ARRLEN, and JSON.ARRAPPEND use a RESP array.
  JSON.DEL counts what it removed in both dialects, and a rejected NX/XX is
  nil in both — neither is a set of matches. One exception, RedisJSON's and
  ours: JSON.TYPE answers nil, not an error, for a missing legacy path.
- **The path subset is single-match.** `$`, object members, and array
  indexes in any mix — `$.user.tags[0]`, `$["odd key"].n`, negative indexes
  counting from the end. Wildcards (`$.a[*]`, `$.*`), recursive descent
  (`$..a`), slices, and filters are rejected as UNSUPPORTED — a distinct
  error from a malformed path. Adopting the container reply shape now is
  what keeps that door open: when multi-match lands, `$..a` will return two
  elements where it returns one today, and no reply type changes.
- **JSON writes create the leaf, never intermediate levels**, so a typo
  cannot silently grow a document a shape you did not ask for.
  **JSON.SET will not overwrite a non-JSON key** (WRONGTYPE) — unlike a
  plain SET, a document write is never a silent way to destroy a string or a
  hash. JSON.NUMINCRBY keeps integers integral. Documents are stored as one
  row, so they live beyond RAM like any value; sub-document writes rewrite
  that row.
- **A document write preserves the key's TTL — root replacement included.**
  Every JSON.SET is a mutation of an existing key, not a fresh one, so an
  expiring document stays expiring. This differs from plain SET, which
  clears the TTL, and deliberately so: in a cache, silently promoting a
  TTL'd document to an immortal one is the expensive direction to be wrong
  in. A genuinely new key has no expiry to keep.

### Where we differ from RedisJSON

Everything above matches the RedisJSON module reply-for-reply, verified by
running the conformance corpus against it (`tools/redisjson_compare.sh`).
Three cases differ, each on purpose:

1. **`TYPE key` answers `json`**, where RedisJSON answers its module type
   name `ReJSON-RL`. Ours fits the rest of our TYPE vocabulary. Tools that
   dispatch on the literal `ReJSON-RL` will not recognize the type.
2. **Writing at index == length appends.** `JSON.SET d $.a[3] 40` on a
   3-element array grows it; RedisJSON refuses. Past the end is refused
   either way, so no write can punch a hole.
3. **Multi-match paths are refused, not evaluated** — see the subset note
   above. RedisJSON evaluates them.

A fourth, smaller one: a write to a missing intermediate (`$.x.y` where `x`
does not exist) is an error here and a silent nil in RedisJSON. Both refuse
the write; ours says why.
- **Keys are capped at 4 KiB**, where stock Redis treats a key as just
  another string and accepts up to 512 MB. The cap matches what ElastiCache
  Serverless enforces, so a key that works on the managed service people
  migrate from works here — and one that does not is refused at both ends
  instead of found in production. A multi-megabyte key is never a working
  cache key, and every one of them is copied into each subkey envelope.
  Raise it with `--max-key-bytes` (up to a structural 64 KiB ceiling: the
  subkey envelope frames key length in two bytes) or set `0` for the
  ceiling alone. Values stay at Redis's own 512 MB
  (`--max-value-bytes`).
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
