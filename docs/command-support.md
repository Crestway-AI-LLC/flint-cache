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
DB 0), RENAME, RENAMENX.

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
>
> RENAME / RENAMENX are same-slot too, and both are **O(size) for
> collections, where upstream is O(1)** — a difference in cost, not in
> behaviour, and one worth knowing before renaming a large key on a hot
> path. Flint's subkey rows embed the user key, so there is no pointer to
> re-aim: renaming a collection costs what copying it costs. Strings and
> JSON documents are O(1), since their metadata row *is* the value.

**Transactions**: MULTI, EXEC, DISCARD, WATCH, UNWATCH (same-slot).

> **What a Flint transaction guarantees, and what it does not.** Three
> promises, all of them real: every command's writes land in ONE engine
> batch or none do; no other writer interleaves with an executing
> transaction; and a replica applies the transaction whole, because that
> batch is a single WAL group.
>
> It does **not** guarantee that a concurrent reader sees a serial history.
> Redis is single-threaded and so gives transactions isolation against
> everything; Flint's readers take no lock — deliberately, and since long
> before transactions existed — so a reader performing a multi-part read
> may observe a partial view of an executing transaction, exactly as one
> already may racing a single HSET. If you need a reader to see all-or-
> nothing, read the keys inside a transaction of your own.
>
> Same-slot, like every other multi-key command: the slot is taken from the
> first key queued, and a later command naming a key elsewhere is refused
> with `CROSSSLOT` at QUEUE time, which also poisons the transaction.
>
> Queue-time errors — an unknown command, a wrong argument count, a
> cross-slot key — poison the transaction, and EXEC then returns
> `EXECABORT` having applied nothing. Runtime errors (WRONGTYPE, a bad
> float) do not: they appear as one element of EXEC's reply while every
> other command applies. That is upstream's split and it is worth knowing,
> because only the first kind protects you from partial application.
>
> Commands inside a transaction see each other's effects, collections
> included — `SADD` then `SMEMBERS` in one transaction returns the member
> just added.
>
> **An error reply to EXEC means nothing was applied.** EXEC answers either
> with the array of per-command replies, or with an error, and there is no
> third outcome — so a client that sees an error can retry the whole
> transaction without checking what landed. A transaction is admitted as one
> unit and faces the same conditions a single write faces, evaluated over
> every command in it: `READONLY` if the node has become a replica since
> MULTI (a failover fenced it), `THROTTLED` if replication lag or the live
> replica count is outside the bound writes must clear, `TRYAGAIN` if the
> slot is frozen mid-cutover, `MOVED` if it has been handed off, or the disk
> guard's error if the disk is shedding and any queued write would grow the
> keyspace. One write among ten reads makes the whole transaction a write;
> one growing write puts the whole transaction behind the disk guard.
>
> If the node dies between MULTI and EXEC, the queue dies with it, and the
> client is told: a direct connection breaks, and through the proxy EXEC
> returns `EXECABORT`. The proxy will not repair a transaction the way it
> transparently repairs a single command — no retry, no MOVED chase, no
> replica routing, no cached answer — because a repaired transaction is one
> whose queue was silently discarded, and the EXEC that followed would apply
> a subset.
>
> **WATCH** arms optimistic concurrency: if any watched key is modified
> between WATCH and EXEC — by any client including your own connection, and
> counting expiry and deletion as modifications — EXEC does nothing and
> replies with a null array, and you retry. EXEC and DISCARD both clear the
> watches. WATCH is refused inside a transaction, since a watch added after
> MULTI could only describe a window that has already closed.
>
> Detection is conservative by construction: it may occasionally abort a
> transaction whose watched key did not actually change, and it will never
> miss one that did. A needless abort costs a retry; a missed modification
> would cost an update.

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
SRANDMEMBER, SSCAN (MATCH, COUNT), SINTER, SUNION, SDIFF,
SINTERSTORE, SUNIONSTORE, SDIFFSTORE.

> SINTER / SUNION / SDIFF are **same-slot only**, exactly as in Redis
> Cluster: colocate the keys with a hash tag (`SINTER {u1}:a {u1}:b`) or the
> request is refused with `CROSSSLOT`. Refused rather than answered, because
> a key the node does not own reads as an empty set and an intersection
> against a phantom empty set is silently wrong. SINTERSTORE, SUNIONSTORE
> and SDIFFSTORE follow the same rule, extended to the destination they
> write.
>
> A sorted set is **not** a legal input to the set commands, even though
> ZUNIONSTORE accepts a plain set at score 1. That asymmetry is upstream's,
> not ours.

**Lists**: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LTRIM,
LREM, LINSERT, LPOS (RANK, COUNT, MAXLEN).

**Sorted sets**: ZADD, ZSCORE, ZMSCORE, ZINCRBY, ZREM, ZCARD, ZRANGE,
ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE (WITHSCORES, LIMIT, exclusive
bounds, ±inf), ZRANGEBYLEX, ZREVRANGEBYLEX (LIMIT, exclusive bounds,
`-`/`+`), ZLEXCOUNT, ZREMRANGEBYLEX (exclusive bounds, `-`/`+`; no LIMIT,
as upstream), ZRANK, ZREVRANK, ZCOUNT, ZPOPMIN, ZPOPMAX, ZREMRANGEBYSCORE,
ZREMRANGEBYRANK, ZSCAN (MATCH, COUNT), ZUNIONSTORE, ZINTERSTORE (WEIGHTS,
AGGREGATE SUM/MIN/MAX).

> ZUNIONSTORE / ZINTERSTORE are **same-slot only**, and the rule covers the
> destination as well as the inputs — these write, so a destination in an
> unowned slot would be stored where nothing can read it while the reply
> claimed a cardinality. Colocate everything with one hash tag
> (`ZUNIONSTORE {u1}:out 2 {u1}:a {u1}:b`).
>
> A plain SET is a legal input, each member scoring 1. An empty result
> removes the destination rather than leaving an empty sorted set behind.
> Where a computed score would be NaN — a zero weight against an infinite
> score, or SUM over both infinities — the score is 0, matching upstream.

> The lex forms are meaningful only when every member shares one score —
> the same condition Redis states — because the index is ordered by
> (score, member). Flint matches upstream's seek-then-walk behaviour rather
> than filtering, so a mixed-score set returns what Valkey returns even
> though neither defines it.

### Flint-specific: the GC ranking primitives (ADR-0013)

Flint never evicts, so "what should my cleanup daemon delete first" has to
be answerable from the client side. Two commands exist for exactly that,
both O(1) reads of the metadata every write already maintains:

- `FLINTKEYSIZE key` — the stored payload size in bytes: a collection's
  cumulative member bytes, a string's or JSON document's payload length.
  Nil if the key is missing or expired.
- `FLINTKEYSTAMP key` — `[written_ms, created_ms]` (unix milliseconds).
  `written_ms` moves on every data mutation and deliberately NOT on
  `EXPIRE`/`PERSIST`; `created_ms` is the current incarnation's creation
  instant for collections and `0` (unknown) for payload-in-metadata types.
  A `0` in either slot means "not tracked", never a guess — keys written
  by a pre-stamp binary report `written_ms` as 0 until their next write.

Together they support least-recently-written and size-weighted policies
without the server tracking read recency (which would turn every read
into a write — the wrong trade under the disk pressure that makes anyone
reach for these). space-reclaim.md is the end-to-end guide for building
a cleanup daemon on them.

### Bloom filters: `BF.*`, and where we differ from RedisBloom (ADR-0016)

The RedisBloom command surface, so an existing client works unchanged:
`BF.RESERVE`, `BF.ADD`, `BF.MADD`, `BF.EXISTS`, `BF.MEXISTS`, `BF.CARD`,
`BF.INFO`, `BF.INSERT`. Note `BF.RESERVE key error_rate capacity` — the
error rate comes FIRST, which reads backwards to most people and is kept
because the point of this family is that nothing about your client has to
change.

Stored as a **blocked** filter: each item hashes to one 4 KiB block and all
its probes land inside it, so `BF.EXISTS` is one disk read and `BF.ADD` is
a read plus a write — the same cost as `HGET`/`HSET`. Blocks materialize on
first use, so a filter reserved for a million items and holding three
occupies three rows, and `FLINTKEYSIZE`/`BF.INFO SIZE` report what is
actually on disk rather than the reserved capacity.

`BF.INFO key FIELD` answers with a **one-element array**, not a bare value
— `*1\r\n:5000\r\n` — matching RedisBloom, whose own clients index `[0]`.
The nil for a `NONSCALING` filter's expansion is wrapped the same way; an
unknown field name is a bare error.

Four deliberate differences, all confirmed against RedisBloom 2.8.16 by
`tools/redisbloom_compare.sh`:

- **`TYPE` answers `bloom`**, where RedisBloom answers `MBbloom--`. Same
  choice JSON already makes against `ReJSON-RL`.
- **`BF.SCANDUMP` and `BF.LOADCHUNK` are refused**, with an error saying
  why. Their payload is a serialized filter and our layout is not
  RedisBloom's, so implementing them would emit a blob that looks portable,
  is accepted by nothing, and fails at the far end of a migration.
  Importing a real RedisBloom dump is a format-reader feature, not a
  command that pretends.
- **`BF.INFO … SIZE` is what is on disk, not what was reserved.** A filter
  reserved for 5000 items reads 0 here and ~9984 on RedisBloom, which
  allocates up front. Ours is the number you are billed for.
- **An unknown `BF.RESERVE` option is an error, not ignored** — the one
  place we are STRICTER. RedisBloom accepts and drops tokens it does not
  recognise (`BF.RESERVE k 0.01 100 WAT` returns `OK`, as does `EXPANSION
  notanum`). Matching that would let a misspelled `NONSCALNG` hand back a
  scaling filter the caller believes is capped.

Plus **defaults differ.** An auto-created filter (a `BF.ADD` with no prior
  `BF.RESERVE`) is sized for 100,000 items rather than RedisBloom's 100,
  and a scaling chain is capped at 32 links. Both are constants today, not
  yet flags. Every link in a chain is another disk read on every lookup
  here, not a pointer chase, so RedisBloom's default would leave a filter
  that grew to a million items reading ~14 blocks per `BF.EXISTS` — a p99
  set by a default nobody chose. Reaching the cap is an error, not a
  silent degradation past the error rate you asked for.

`BF.CARD` counts items the filter ACCEPTED. An item that false-positives on
insert is reported already-present and never counted, so the card can read
slightly low on a full filter. That is inherent — the filter cannot tell a
collision from a repeat — and RedisBloom under-counts the same way.

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

- **Cross-slot multi-key commands** — the *cross-slot* form, not the
  command. A multi-key command whose keys share a slot is fair game and
  several are supported (SINTER, ZUNIONSTORE, COPY); it is scattering one
  request across slots that is excluded. Colocate with a hash tag.
  Also **pub/sub**, **streams**, **blocking
  commands** (BLPOP, BLMOVE …), **KEYS/RANDOMKEY**, and **EVAL/EVALSHA**.
  These conflict with slot-sharded multi-tenancy or reintroduce the
  single-threaded bottlenecks Flint exists to avoid. Common patterns they
  serve are covered by first-class commands instead; if you need one of
  these, open an issue describing the workload — patterns with broad
  demand get first-class implementations.
