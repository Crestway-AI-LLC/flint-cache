# Command support

Every supported command is gated by the conformance oracle: a corpus case
run against both Flint engines (mem, rocks). For every family with a
counterpart in the reference implementation, the same case also runs
against a real Valkey — so a green run proves two independent things: the
case encodes real Redis behavior, and Flint matches it.

That sentence is enforced, not asserted. `tools/gates.sh` reads the server's
dispatch table and the corpus and refuses a build where a command the
dispatcher answers is named by no case. There is no exemption list: a
command whose reply has no oracle — because Valkey has none, or because
Flint diverges on purpose — still gets a case, and is simply skipped by the
`--reference` run. Until 2026-09-05 the sentence was unchecked and six
commands were not gated; `PEXPIREAT` and `PEXPIRETIME` could both be off by
a factor of 1000 with the whole corpus green.

The one thing a case can be excused from is a target that cannot serve it.
The `FLINT*` admin commands are refused by the proxy on purpose — it is the
tenant boundary — so a run through the edge, or against a foreign server,
skips them and SAYS how many it skipped. A skip that went unmentioned would
be the same failure as an ungated command, one step later.

**One exception, stated plainly: the JSON family has no reference in that
run.** Stock Redis/Valkey have no JSON type — it is the separate RedisJSON
module — so `flint-conformance --reference` skips those cases rather than
reporting a failure that would say nothing about either side.

They are checked against the real thing separately. `tools/redisjson_compare.sh`
loads a RedisJSON module built from source and runs this same corpus against
it; the gate is that exactly the cases listed under "Where we differ from
RedisJSON" below differ, and every one of them still does. So the JSON
contract is a verified match, not a reading of the docs — but the check is
on-demand rather than in CI, because it needs a module you have to compile.

## Supported

**Connection / server**: PING, ECHO, AUTH (at the proxy), COMMAND, TIME,
SELECT (index 0 only), HELLO, QUIT (at the proxy — see below), DBSIZE,
FLUSHALL, FLUSHDB (all three scoped to the tenant namespace; a tenant has one
database, so FLUSHDB and FLUSHALL clear the same keys), INFO (at the proxy —
see below).

> **QUIT is answered by the proxy, and by a seat that is still LOADING. A
> READY seat answers `ERR unknown command 'QUIT'`.** Clients connect through
> the proxy, so the client path is the working one — but the seat's
> behaviour is backwards from any expectation: the command works while the
> node is coming up and stops working once it is serving. Serving it on the
> ready path means replying and then closing a connection that may have a
> write batch in flight, so it is a change to the write path rather than a
> one-line addition. Tracked in docs/bugs/0106.

> **`COMMAND` returns an EMPTY array.** It is answered rather than refused,
> so a client that probes with it gets a well-formed reply — and a client
> that uses it for capability discovery will conclude Flint implements
> nothing. Nothing here is derived from that reply: this matrix is the list,
> and sending a command to a server is the other way to find out. The
> conformance case asserts the shape (`COMMAND` returns an array, which is
> the regression worth catching), not the emptiness, because the shape is
> what a real Redis agrees with and therefore the only part an oracle can
> check.

> A namespace is one logical database, so `SELECT 0` succeeds and any other
> index is refused. Tenancy replaces numbered databases here: isolation is
> the namespace, which the proxy pins per connection.

**Keyspace**: DEL, UNLINK, EXISTS, TYPE, EXPIRE, PEXPIRE, EXPIREAT,
PEXPIREAT (each with NX, XX, GT, LT), TTL, PTTL, EXPIRETIME, PEXPIRETIME,
PERSIST, COPY (REPLACE, DB 0), RENAME, RENAMENX.

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
>
> **Spring Session** renames its session key at every login
> (`changeSessionId`, which is Spring Security's default protection against
> session fixation). Old id and new id are in different slots, so the rename
> is refused and the login fails (ADR-0049). Either configuration below,
> both measured on Flint, makes login work:
>
> - `spring.session.redis.namespace={spring}:session`: one hash tag, so every
>   session key is in one slot. Every session then lives on one pair.
> - `sessionFixation(f -> f.migrateSession())` in Spring Security: a new
>   session with the old one's attributes, and the old one deleted. No rename,
>   sessions stay spread across the fleet, and the old id is dead after login.

**Transactions**: MULTI, EXEC, DISCARD, WATCH, UNWATCH (same-slot).

**Scripting**: EVAL, EVALSHA, SCRIPT (LOAD, EXISTS, FLUSH, KILL): Lua 5.1,
single-slot, all or nothing. See "Lua scripts" below (ADR-0051).

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
> with `CROSSSLOT` at QUEUE time, which also poisons the transaction. Through
> the proxy a key on another pair is refused with `EXECABORT` instead, since
> no node could queue it; the transaction stays open, every later command
> answers `QUEUED`, and EXEC applies nothing (BUG-0190).
>
> **A tenant placed on one pair** (ADR-0053) is the exception: all its keys
> live on that pair, so its transactions may span slots, with the same
> guarantees. A multi-key command inside one still keeps its one-slot rule.
> rq and Sidekiq name keys that cannot share a hash tag, and need this. The
> operator places a tenant when creating it (`self-hosting.md`).
>
> **DBSIZE, SCAN, FLUSHDB and FLUSHALL inside a transaction** are refused
> through the proxy, poisoning it, unless the tenant's keys all live on the
> node the transaction runs on: a placed tenant, or a fleet of one pair.
> Outside a transaction each fans out over every pair; inside one it would
> answer for that one node, so FLUSHDB would flush part of the keyspace and
> answer OK (BUG-0222). redis-py's `pipeline()` is a transaction by
> default; `pipeline(transaction=False)` sends them as ordinary commands.
> KEYS and INFO are answered by the proxy only outside a transaction, and
> inside one are refused as unknown.
>
> **HELLO and AUTH inside a transaction** are answered at once by the proxy
> rather than queued, so EXEC's reply has no element for them. Upstream
> queues both. No client library sends either inside MULTI.
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
> MULTI could only describe a window that has already closed. UNWATCH
> inside one is queued, as upstream queues it, so EXEC still checks the
> watches (BUG-0220).
>
> Detection is conservative by construction: it may occasionally abort a
> transaction whose watched key did not actually change, and it will never
> miss one that did. A needless abort costs a retry; a missed modification
> would cost an update.

**JSON documents**: JSON.SET (NX, XX), JSON.GET (several paths; INDENT,
NEWLINE, SPACE, NOESCAPE), JSON.MGET, JSON.MSET, JSON.MERGE, JSON.DEL /
JSON.FORGET, JSON.CLEAR, JSON.TYPE, JSON.NUMINCRBY, JSON.NUMMULTBY,
JSON.STRAPPEND, JSON.STRLEN, JSON.TOGGLE, JSON.ARRAPPEND, JSON.ARRINSERT,
JSON.ARRINDEX, JSON.ARRLEN, JSON.ARRPOP, JSON.ARRTRIM, JSON.OBJKEYS,
JSON.OBJLEN, JSON.RESP, JSON.DEBUG (MEMORY, HELP): every command RedisJSON
v8.2.8 has (ADR-0055).

**Keyspace iteration**: SCAN (MATCH, COUNT, TYPE) — incremental, works
through the proxy across all shard pairs as one cursor stream (redis-cli
`--scan`, RedisInsight, and client iterators work as-is).

**Strings**: SET (NX, XX, EX, PX, EXAT, PXAT, KEEPTTL, GET), SETNX, SETEX,
GET, GETDEL, GETEX (EX, PX, EXAT, PXAT, PERSIST), GETSET, MSET, MGET,
APPEND, STRLEN, GETRANGE, SETRANGE, INCR, DECR, INCRBY, DECRBY,
INCRBYFLOAT, BITFIELD (GET, SET, INCRBY, OVERFLOW WRAP/SAT/FAIL, `i1`-`i64`
and `u1`-`u63`, `#n` offsets), BITFIELD_RO (BUG-0192), SETBIT, GETBIT,
BITCOUNT (BYTE, BIT), BITPOS (BYTE, BIT), BITOP (AND, OR, XOR, NOT, and Redis
8.2's DIFF, DIFF1, ANDOR and ONE).

> Bitmaps are strings, kept whole in one row like every string. A write to
> one bit (SETBIT, like SETRANGE and BITFIELD) rewrites the string, and a
> read of one bit (GETBIT) reads it, so their cost grows with the string's
> size where Redis's does not. Measured on the RocksDB engine (a laptop,
> 2026-10-08), SETBIT p50 / p99: up to 64 KiB, 0.04 ms / 0.12 ms, as fast as
> Redis 8.2's 0.12 ms; 1 MiB, 0.39 ms / 10 ms; 8 MiB, 0.84 ms / 16 ms;
> 64 MiB, 5.6 ms / 85 ms. A bitmap written often past a megabyte or so (a
> daily-active bitmap over 10 million user ids is 1.25 MiB) is better split
> across several keys. BITOP's keys must share a slot (use a
> hash tag), as every multi-key command's must. Where Valkey 9.1 and Redis
> 8.2 differ, one answer follows each: `BITCOUNT key start`
> without an end counts to the end of the string, as Valkey answers (Redis
> 8.2 refuses it); and BITOP's `DIFF`, `DIFF1`, `ANDOR` and `ONE`, which
> Redis 8.2 added and Valkey 9.1 does not have, are served as Redis serves
> them.

**Hashes**: HSET, HMSET, HSETNX, HGET, HMGET, HGETALL, HKEYS, HVALS, HDEL, HLEN,
HEXISTS, HINCRBY, HINCRBYFLOAT, HSTRLEN, HSCAN (MATCH, COUNT, NOVALUES).

> HKEYS and HVALS were implemented and served from the first release and
> were missing from this list until 2026-09-05, so the matrix under-reported
> for months. Read them as the whole-collection reads they are: both
> materialise every field of the hash, and both are counted by the
> collection-read admission ADR-0022 describes. HSCAN is the cursored
> alternative when the hash is large.

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

**Lists**: LPUSH, RPUSH, LPOP and RPOP (with a count), LLEN, LRANGE, LINDEX, LSET, LTRIM,
LREM, LINSERT, LPOS (RANK, COUNT, MAXLEN), LMOVE, RPOPLPUSH, BLPOP, BRPOP,
BLMOVE, BRPOPLPUSH (see "Blocking commands").

> LMOVE and RPOPLPUSH are **same-slot only**, like the set operations:
> colocate source and destination with a hash tag or the move is refused
> with `CROSSSLOT`. So are their blocking forms, BLMOVE and BRPOPLPUSH.

**Sorted sets**: ZADD (NX, XX, GT, LT, CH, INCR), ZSCORE, ZMSCORE, ZINCRBY,
ZREM, ZCARD, ZRANGE (BYSCORE, BYLEX, REV, LIMIT, WITHSCORES),
ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE (WITHSCORES, LIMIT, exclusive
bounds, ±inf), ZRANGEBYLEX, ZREVRANGEBYLEX (LIMIT, exclusive bounds,
`-`/`+`), ZLEXCOUNT, ZREMRANGEBYLEX (exclusive bounds, `-`/`+`; no LIMIT,
as upstream), ZRANK, ZREVRANK (WITHSCORE), ZCOUNT, ZPOPMIN, ZPOPMAX, ZREMRANGEBYSCORE,
ZREMRANGEBYRANK, ZSCAN (MATCH, COUNT), ZUNIONSTORE, ZINTERSTORE (WEIGHTS,
AGGREGATE SUM/MIN/MAX), BZPOPMIN, BZPOPMAX (see "Blocking commands").

> ZUNIONSTORE / ZINTERSTORE are **same-slot only**, and the rule covers the
> destination as well as the inputs — these write, so a destination in an
> unowned slot would be stored where nothing can read it while the reply
> claimed a cardinality. Colocate everything with one hash tag
> (`ZUNIONSTORE {u1}:out 2 {u1}:a {u1}:b`).
>
> A plain SET is a legal input, each member scoring 1. An empty result
> removes the destination rather than leaving an empty sorted set behind.
> Where a computed score would be NaN, Flint does what upstream does. A
> zero weight against an infinite score is 0 for every union input and for
> the intersection's first input. A later intersection input's NaN is
> aggregated as it is: SUM makes it 0, and MIN or MAX keep the score so far
> (BUG-0231). SUM over both infinities is 0.
> A score of `-0` is stored as `0`, as Redis's listpack stores it, and ties
> with `0` by member (BUG-0228). ZINCRBY or ZADD INCR on a new member with
> `-0` still answers `-0`, as upstream does.
> `ZINCRBY` to a NaN (`-inf` added to `+inf`) is refused with upstream's
> `ERR resulting score is not a number (NaN)`, and the member keeps its
> score (BUG-0212).

> The lex forms are meaningful only when every member shares one score —
> the same condition Redis states — because the index is ordered by
> (score, member). Flint matches upstream's seek-then-walk behaviour rather
> than filtering, so a mixed-score set returns what Valkey returns even
> though neither defines it. That is the walk Redis does on a set of up to
> 128 members (its listpack); past that, Redis's skiplist seek depends on
> random levels, and no other implementation can match it (BUG-0215).

> Sorted-set reads cost what they return: a score range, a rank window, a
> LIMIT, a pop or a count reads only the rows it answers from (BUG-0216).
> **ZRANK and ZREVRANK are the exception: a rank costs its position**,
> counted from the end the rank is measured from. The top of a leaderboard
> (`ZREVRANK` of a high scorer) is as cheap as in Redis; the middle of a
> million-member set is about 79 ms on the RocksDB engine, where Redis's
> skiplist answers in O(log n).

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
unknown field name is a bare error. Under RESP3 the replies take RedisBloom's
RESP3 kinds: `BF.ADD`, `BF.EXISTS`, `BF.MADD`, `BF.MEXISTS` and `BF.INSERT`
answer booleans (`#t`/`#f`), `BF.INFO` a map, and `BF.INFO key FIELD` a
one-pair map naming the field (BUG-0239).

`TYPE` answers RedisBloom's module type name, `MBbloom--`, and `SCAN … TYPE
MBbloom--` finds filters (BUG-0241). Until 2026-10-08 it answered `bloom`, a
deliberate difference Jeff withdrew because matching cost little.

A batch that fills a `NONSCALING` filter part-way answers each item, with the
error in the place of the item it stopped at — `[0, 1, (error) ERR non
scaling filter is full]` — because the items before it are stored
(BUG-0237). `NONSCALING` holds whichever side of `EXPANSION` it is written
on, `EXPANSION 0` means `NONSCALING`, and `BF.RESERVE` refuses both together
as RedisBloom does (BUG-0238). Every argument of `BF.RESERVE` and
`BF.INSERT` is read, and refused in RedisBloom's words, before the key is
(BUG-0240).

Seven deliberate differences, confirmed against RedisBloom 8.2.8 (built from
source on 2026-10-08) by `tools/redisbloom_compare.sh`, and kept by Jeff on
2026-10-08 when the cheap one (`TYPE`) was closed:

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
  recognise (`BF.RESERVE k 0.01 100 WAT` returns `OK`). Matching that would
  let a misspelled `NONSCALNG` hand back a scaling filter the caller
  believes is capped.
- **`BF.EXISTS` and `BF.MEXISTS` on a key of another type answer WRONGTYPE**,
  where RedisBloom answers 0, though its own `BF.ADD`, `BF.CARD` and
  `BF.INFO` answer WRONGTYPE there. "Not present" for a string key would hide
  the caller's bug.
- **An `EXPANSION` above 255 is refused** when it would make a filter
  (`ERR expansion above 255 is not supported`); RedisBloom takes up to
  32768. A filter keeps its growth factor in one byte, so widening it is a
  format change, and a chain is capped at 32 links anyway.
- **`BF.INSERT`'s option words are spelled out.** RedisBloom reads the first
  letter or two, so `BF.INSERT k I x` is `ITEMS x` there and `Unknown
  argument received` here, for the same reason as the one above.
- **`BF.DEBUG` is not served.** It prints RedisBloom's in-memory layout,
  which ours is not, like the dump commands.

Plus **defaults differ.** An auto-created filter (a `BF.ADD` with no prior
`BF.RESERVE`) is sized for 100,000 items rather than RedisBloom's 100, and a
scaling chain is capped at 32 links. Every link in a chain is another disk
read on every lookup here, not a pointer chase, so RedisBloom's default would
leave a filter that grew to a million items reading ~14 blocks per
`BF.EXISTS` — a p99 set by a default nobody chose. Reaching the cap is an
error, not a silent degradation past the error rate you asked for.

Both stay **constants, deliberately, rather than becoming node flags.** The
default capacity is baked into a filter's stored metadata at creation, so a
per-node flag would mean two nodes in one cluster creating differently sized
filters for the same tenant depending on which slot the key landed in — and
a slot migration would then move a filter built to another node's idea of
the default. That is a data-consistency knob wearing a tuning knob's
clothes. If these become configurable they belong on the CP snapshot beside
`replica_reads` and `async_writes`, where a cluster has one answer; a
`--bloom-default-capacity` on `flint-server` would be a mistake that only
shows up on a multi-node fleet.

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
`SRANDMEMBER`, `SMISMEMBER`, `SCAN`, `LPOS`, `INCRBYFLOAT`, and `HINCRBYFLOAT` are
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
  | no match | `[]` | `ERR Path '$.a' does not exist` |
  | wrong shape (`ARRLEN` on an object) | one null element | error |

  JSON.GET and JSON.NUMINCRBY carry the container inside the JSON they
  return; JSON.TYPE, JSON.ARRLEN, and JSON.ARRAPPEND use a RESP array.
  JSON.DEL counts what it removed in both dialects, and a rejected NX/XX is
  nil in both — neither is a set of matches. One exception, RedisJSON's and
  ours: JSON.TYPE answers nil, not an error, for a missing legacy path.
- **`$` paths can match many locations** (ADR-0054). Besides `$`, object
  members and array indexes (`$.user.tags[0]`, `$["odd key"].n`, negative
  indexes counting from the end), a `$` path takes wildcards (`$.*`,
  `$.a[*]`), recursive descent (`$..a`, `$..*`), unions (`$['a','b']`,
  `$[0,-1]`), slices (`$.a[1:3]`, `$.a[::2]`) and filters
  (`$.a[?(@.price < 10 && @.tag == "x")]`, with `==`, `!=`, `<`, `<=`, `>`,
  `>=`, `&&`, `||`, `!`, parentheses, `@` and `$` paths, and string, number,
  `true`, `false` and `null` literals). Matches come back in document order,
  a location a union names twice answering twice.
  - Reads (GET, TYPE, ARRLEN) answer one element per match.
  - NUMINCRBY and ARRAPPEND act on each match, a null for one of the wrong
    type; a location a union names twice is acted on twice, as in
    RedisJSON. A refusal at any match (an overflow, a non-finite result)
    fails the whole command and stores nothing.
  - DEL removes each matched location once, a location inside another
    matched one going with it uncounted, and answers how many it removed.
  - SET replaces every match and adds nothing: only a path naming one
    location can add a value, as in RedisJSON. So an indefinite SET that
    matches nothing is an error, NX with one is always an error, and XX
    with one that matches nothing is nil.

  A filter treats an absent operand as equal to nothing, another absent one
  included (RedisJSON's rule; RFC 9535 calls two absent operands equal), and
  orders only two numbers or two strings. The legacy dialect stays
  single-match, and a regex (`=~`) or multi-match operand in a filter is
  refused: each is UNSUPPORTED, an error distinct from a malformed path.
- **The rest of the family follows RedisJSON command by command**
  (ADR-0055), including where its answers for a missing key, a missing path
  and a value of the wrong type differ between commands and dialects. A few
  rules are worth knowing:
  - STRAPPEND, STRLEN, ARRPOP, OBJKEYS, OBJLEN, CLEAR, RESP and DEBUG MEMORY
    take the legacy root when no path is given. A string's length is in
    bytes.
  - ARRINDEX compares type-strictly (the integer `2` is not the float
    `2.0`), its `stop` is exclusive, and `0` means the end. ARRPOP's index
    and ARRTRIM's range clamp to the array; ARRINSERT's index may equal the
    length, and anything outside refuses the command.
  - TOGGLE answers 1 or 0 under `$`, `true` or `false` under the legacy
    dialect.
  - CLEAR empties non-empty objects and arrays and zeroes non-zero numbers,
    counting only what changed.
  - MERGE applies an RFC 7396 merge patch at each match. A null member of
    an object patch deletes that member, and a null patch at a path sets
    null there. A path that matches nothing adds the value where JSON.SET
    would, and a missing key takes the patch as its document, nulls and
    all.
  - MSET applies JSON.SET's rules to each `key path value` triple, all or
    nothing, each triple checked against the documents as they were before
    the command.
  - MGET answers nil for a key that is missing or not a document.
  - NUMMULTBY multiplies as NUMINCRBY adds: integers exactly, overflow
    refused, a multiplier written as a float making a float.
  - JSON.GET with several path arguments answers one object keyed by path,
    in the order given, under `$` if any of them is a `$` path. Its
    INDENT, NEWLINE and SPACE format the text as RedisJSON's do, and may
    appear anywhere among the paths.
- **JSON writes create the leaf, never intermediate levels**, so a typo
  cannot silently grow a document a shape you did not ask for.
  **JSON.SET will not overwrite a non-JSON key** (`Existing key has wrong
  Redis type`, RedisJSON's words) — unlike a
  plain SET, a document write is never a silent way to destroy a string or a
  hash. JSON.NUMINCRBY adds an integer to an integer exactly, in 64-bit
  integer arithmetic, and refuses an overflow; an increment written as a
  float (`2.0`, `1e3`) makes a float. A JSON.DEL that leaves the document
  an empty object or array deletes the key, as in RedisJSON. Documents are
  stored as one
  row, so they live beyond RAM like any value; sub-document writes rewrite
  that row.
- **A document write preserves the key's TTL — root replacement included.**
  Every JSON.SET is a mutation of an existing key, not a fresh one, so an
  expiring document stays expiring. This differs from plain SET, which
  clears the TTL, and deliberately so: in a cache, silently promoting a
  TTL'd document to an immortal one is the expensive direction to be wrong
  in. A genuinely new key has no expiry to keep.

### Where we differ from RedisJSON

Everything above matches the RedisJSON module (v8.2.8, the one Redis 8.2
loads) reply-for-reply, verified by running the conformance corpus against
it (`tools/redisjson_compare.sh`). These cases differ, each on purpose:

1. **Withdrawn 2026-10-08.** `TYPE key` answered `json` where RedisJSON
   answers its module type name; it answers `ReJSON-RL` now (Jeff), as
   Bloom answers `MBbloom--`, and `SCAN … TYPE ReJSON-RL` finds documents.
   The number is kept so the others keep theirs.
2. **Writing at index == length appends.** `JSON.SET d $.a[3] 40` on a
   3-element array grows it; RedisJSON refuses. Past the end is refused
   either way, so no write can punch a hole.
3. **An integer overflow in JSON.NUMINCRBY is refused.** RedisJSON wraps
   `9223372036854775807` + 1 to `-9223372036854775808` and stores it; we
   answer an error and store nothing, as Redis's INCRBY does.
4. **Multi-match in the legacy dialect is refused.** RedisJSON answers the
   first match of `..a` or `.a[*]`; the `$` spelling answers all of them,
   here and there.
5. **A regex filter (`=~`) is refused.** RedisJSON evaluates it.
6. **A multi-match operand inside a filter (`@..a`, `@.*`) is refused.**
   RedisJSON evaluates it.

7. **An integer overflow in JSON.NUMMULTBY is refused**, as in
   JSON.NUMINCRBY (3). RedisJSON wraps.
8. **A value with something after it is refused** (`2 x`, `[1] 2`), in
   serde's words (`trailing characters at line 1 column 3`). RedisJSON's
   JSON.SET, MSET, MERGE, ARRAPPEND and ARRINSERT read the first value and
   drop the rest, so a client bug that sends `2 x` stores `2`; its
   STRAPPEND refuses, as Flint does.
9. **A negative index past the start of an array names nothing**
   (`$.b[-9]` on three elements), as JSONPath has it. RedisJSON takes the
   first element, for writes too: its `JSON.DEL d $.b[-9]` removes `b[0]`.
10. **JSON.SET takes no `FORMAT` option**: a syntax error. RedisJSON takes
    `FORMAT`.

Jeff chose all three on 2026-10-08, after BUG-0236's inventory found them:
each is a place where RedisJSON stores what the caller did not write, or
cannot be asked to.

Smaller ones, which the corpus does not list:
- A write to a missing intermediate (`$.x.y` where `x` does not exist) is an
  error here, from JSON.SET, JSON.MERGE and JSON.MSET alike. RedisJSON
  answers nil from SET and MERGE, and from MSET answers OK without writing
  that triple; both refuse the write, and ours says why. JSON.MSET is all or
  nothing here: a triple that cannot apply to the document an earlier triple
  of the same command wrote refuses the whole command, where RedisJSON
  answers OK and drops that triple.
- A multi-match ARRINSERT, ARRPOP, ARRTRIM or CLEAR whose matches nest (an
  array inside another matched array) applies to every match here, and
  CLEAR counts a match inside another cleared one once. RedisJSON applies
  the first and then answers `Path does not exist`, keeping the first edit.
- Arguments RedisJSON ignores are refused here: one past the last that
  STRAPPEND, ARRPOP or CLEAR takes, and an ARRPOP index that is not an
  integer (RedisJSON pops the last element).
- JSON.DEBUG MEMORY answers the bytes a value occupies as stored, its JSON
  text. RedisJSON answers the size of its in-memory tree. Both are each
  server's own accounting, not a common unit.
- Numbers are spelled by serde_json: `1e+20` where RedisJSON writes `1e20`.
  JSON.RESP renders a double as Redis does (`1e+20`, BUG-0214), `-0`
  included (BUG-0230).
- Filters here also take `!` and exponent literals (`1e3`), which RedisJSON
  refuses as syntax errors, and a `$` path here may hold a space in a member
  name (`$.a b`), which RedisJSON refuses.
- **Error replies are RedisJSON's words** (BUG-0236), down to naming the
  path as each command names it, which key or argument is read first, and
  the module's quirks (`does not contains a number`, `Err wrong static
  path`, `Existing key has wrong Redis type` with no `ERR`). Three kinds
  are not: a path that does not parse is `ERR malformed JSON path`, where
  RedisJSON's text comes from its parser generator (`Error occurred on
  position 7, …`); the refusals this list describes keep texts of their
  own; and so do the deliberate ones above.
- **`SRANDMEMBER key <negative count>` is refused past the seat's
  `max-value-bytes`** (512 MiB by default), estimated as the count times the
  set's mean member size plus 64 bytes a member. A negative count repeats
  members, so `-2000000000000` asks for two trillion of them from any set.
  Redis builds whatever reply is asked for; a shared seat cannot let one
  tenant's reply take every tenant's memory (BUG-0218).
- **`LSET` with an index past the end is `index out of range`, however
  large.** Redis 8.2 and Valkey 9.1 overwrite the last member for an index
  from 2^62 up, and for i64::MIN, which looks like an overflow in their list index; that is
  not copied (BUG-0219).
- **Where Redis 8.2 and Valkey 9.1 disagree, Flint answers as Valkey
  does**, Valkey being the conformance oracle. `GETEX nokey EX 0` is an
  invalid-expire error here and in Valkey, which judge the time first;
  Redis reads the key first and answers nil (BUG-0213).
- **`SET k v PX 9223372036854775807` is refused**, an instant whose sum
  with now overflows. Upstream tests that sum after a signed addition C
  leaves undefined, so its answer depends on the build: a Linux build of
  Valkey 9.1 refuses it, and macOS builds of Redis 8.2 and Valkey 9.1
  answer OK (BUG-0213).
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
- **INCRBYFLOAT** and **HINCRBYFLOAT** format like Redis (`%.17f`,
  trailing zeros trimmed), and compute in a 64-bit double. Redis and Valkey
  compute in C's `long double`, which is 80-bit on x86-64 Linux: there a
  value past 1.8e308 increments, and here it is refused. They also read a
  hexadecimal float (`0x10`, `0x1p3`) as a number; Flint refuses one as
  "not a valid float". Measured 2026-10-07 against Valkey 9.1 on Linux.
- **Expiry is lazy + swept**: an expired key reads as missing immediately;
  physical reclamation is background.
- **Cluster is invisible**: clients never see `-MOVED`/`-ASK`; the proxy
  absorbs topology. Hash tags (`{...}`) work as in Redis Cluster.
- **Error vocabulary** beyond Redis's: `-QUOTA` (writes shed over storage
  quota; reads and space-reducing commands still served), `-THROTTLED`
  (rate quota / back-pressure; retry with backoff), `-TRYAGAIN`
  (mid-migration write or fenced stale replica; the proxy retries/falls
  back for you).
- **`-LOADING` means the same thing it means in Redis**: the node is up but
  its dataset is not, so retry. A Flint node in this state is a fresh
  replica pulling its initial copy from its master; it binds its port
  immediately rather than staying dark (a dark port is indistinguishable
  from a dead host, and operational tooling acted on that), answers `PING`
  and `FLINTINFO` throughout, and refuses everything else with `-LOADING`
  until it is serving. `FLINTINFO` reports `role:loading`, `loading:1` and
  `loading_ms` — how long it has been at it — and `loading:0` once it
  serves. **Tenants do not see a node's LOADING**: the proxy pins each
  backend connection to a namespace before any command travels on it, and a
  loading node refuses that pin, so it stays out of the routing path
  entirely. The one LOADING a tenant can see comes from a proxy itself, in
  the moment after it starts and before its first control-plane snapshot
  arrives (BUG-0211). It holds no tenant tokens yet, and LOADING says to
  retry. The WRONGPASS it used to answer told a client holding a valid token
  that the token was wrong.

## Lua scripts (ADR-0051)

`EVAL` and `EVALSHA` run a script in PUC-Rio Lua 5.1, the interpreter Redis
and Valkey embed, so a script means what it means there: its replies, its
errors (with the line they were raised on), and how a Lua number is spelled
when a script hands it to `redis.call` are Valkey's, checked against Valkey
by the conformance corpus. What Flint adds is the frame around a script:

- **One slot.** Every key in `KEYS` must hash to one slot (use a hash tag,
  `{user1}:a`, to colocate them), and a `redis.call` may touch any key in
  that slot, including one the script builds rather than declares
  (`ARGV[2] .. id`, as asynq builds its task keys). A call that reaches a key
  in another slot, or the whole keyspace (`DBSIZE`, `SCAN`, `FLUSHALL`), is
  refused, and whatever it did is undone, even when the script catches the
  refusal with `pcall`. A script that declares no keys may touch none. This
  is Redis Cluster's rule, enforced rather than advised: it is what lets a
  script run on the one pair that owns its slot, atomically (ADR-0052). A
  tenant placed on one pair (ADR-0053) may declare and touch keys in any
  slot, since every one of them is on that pair; a script that declares no
  keys still may touch none.
  A script that touches only its `KEYS` locks only them. One that reaches
  another key in the slot is stopped there, with nothing kept, and run again
  from the start holding the lock over every writer on its seat, because
  that key's own writers are not excluded otherwise: it costs one wasted
  attempt and a moment in which the seat's other writes wait.
- **All or nothing.** A script's writes commit as one batch, as a
  transaction's do: all of them or, after a crash, none. **A script that
  fails keeps none of its writes**, where Redis keeps those made before the
  failure: an uncaught error, the time limit and the memory limit all
  discard them. A script that *returns* an error (`redis.error_reply`) has
  succeeded, and its writes are kept.
- **Limits.** A script is stopped at 50 ms of run time or 64 MiB of Lua
  memory (`--script-time-limit-ms`, `--script-memory-limit-mb` on the seat),
  and answers an error saying which; nothing it wrote is kept. The time
  limit stops a loop wherever it hides, in a `pcall`, an `xpcall` or a
  coroutine. `SCRIPT KILL` answers `NOTBUSY`: the limit does its job, and a
  script holds its keys' write locks only while it runs.
- **The sandbox.** The base, `table`, `string` and `math` libraries, and
  `redis`: `call`, `pcall`, `error_reply`, `status_reply`, `sha1hex`, `log`
  (a no-op), `setresp(2)`, `replicate_commands`, `set_repl`. **Not
  available:** `load`, `loadstring`, `dofile`, `loadfile`, `require`, `os`,
  `io`, `debug`, `setfenv`, `getfenv`, `print`, and the `cjson`, `cmsgpack`,
  `bit` and `struct` libraries Redis also loads; a script that uses one fails
  with "nonexistent global variable". Globals and the libraries are
  read-only, and a script's text compiles as text, never as bytecode.
  `redis.setresp(3)` is refused: a script sees replies in RESP2's shapes.
- **What a script returns.** As upstream converts it, Redis 7's typed tables
  included: `{double=n}`, `{map={...}}` and `{set={...}}` answer a double, a
  map and a set (a bulk string, a flat array and an array under RESP2)
  (BUG-0232). `{big_number='...'}` and `{verbatim_string={...}}` answer
  their text as a bulk string, which is upstream's RESP2 reply. Under
  RESP3, upstream frames them as `(` and `=`.
- **`EVALSHA` through the proxy.** The proxy keeps each tenant's script
  texts (from `EVAL` and `SCRIPT LOAD`) and forwards an `EVALSHA` it knows as
  the `EVAL` it stands for, so a script loaded once runs on either pair; one
  it does not know answers `NOSCRIPT`, and every client library then sends
  the text. The cache is bounded (1,000 scripts and 8 MiB per tenant), and
  per proxy: a client that reconnects to another proxy reloads on its first
  `NOSCRIPT`, as it would after a Redis restart.

Measured with each library's defaults (`client_compat_drill`): redis-py's
`Lock`, django-redis, node `redlock`, Go `redsync`, Ruby `redlock`, Python
`limits` (so Flask-Limiter and SlowAPI) in all three strategies, Go
`redis_rate`, node `rate-limiter-flexible` and `rate-limit-redis`, and
`python-redis-lock` when the lock's name carries a hash tag (its scripts
name the lock and a signal list, which must then share a slot), its blocking
acquire included.

## Blocking commands (ADR-0052)

`BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`, `BZPOPMIN` and `BZPOPMAX` block
as in Redis: the reply is the first element any of the keys yields, in the
order they are given, or a null array at the timeout (`0` waits for ever).
The keys of `BLPOP`, `BRPOP` and the sorted-set pops need not share a slot:
the order is a priority across pairs, which is what Sidekiq's
`BRPOP critical default low` relies on. `BLMOVE` and `BRPOPLPUSH` need their
two keys in one slot, as `LMOVE` does.

The wait happens at the proxy, never on a seat, so a blocked client holds no
seat connection or thread. The proxy tries each key in turn and, finding
nothing, waits before trying again, 1 ms at first and doubling to 20 ms.
What that means:

- **An element is taken within about 20 ms of arriving.** Measured through a
  local proxy, from a push to a blocked `BRPOP`'s reply: 13 ms at the median,
  24 ms at most over 60 pushes. Redis wakes a blocked client at once.
- **An idle blocked client costs about 44 attempts a second per key** at its
  seat (measured), so a Sidekiq process with 5 threads on one queue costs
  about 220 a second while idle.
- **Blocked clients are not served in arrival order.** When several wait on
  one key, the one whose next attempt comes first takes the element. Redis
  serves the longest waiter first.
- **A client that disconnects while blocked takes nothing afterwards.** A pop
  already on its way to a seat when the client leaves completes, and that
  element is lost with the connection, as a Redis reply to a closed socket
  is. `BLMOVE` into a processing list is the pattern that loses nothing.
- **Inside `MULTI` or a script a blocking pop does not wait**, as in Redis:
  it answers what is there now, or a null.
- **A seat answers these commands without waiting**, in every context, and
  only a client of the proxy blocks. A seat's `BLMOVE` or `BRPOPLPUSH` that
  finds nothing answers the null bulk that Redis gives inside `MULTI`, where
  the proxy answers a null array at the timeout as Redis does.

Commands a client pipelines behind a blocked pop run after it answers, in
order, as in Redis. A blocking pop is charged once against the tenant's
ops/s quota however long it waits, and its time is not counted in the
tenant's latency histogram, where a two-second `BRPOP` would read as a
two-second write.

## Excluded by design

- **Cross-slot multi-key commands** — the *cross-slot* form, not the
  command. A multi-key command whose keys share a slot is fair game and
  several are supported (SINTER, ZUNIONSTORE, COPY); it is scattering one
  request across slots that is excluded. Colocate with a hash tag.
  **`DEL`, `UNLINK` and `EXISTS` are the exception** (BUG-0179): each key is
  answered on its own, so through the proxy they take keys in any slots, split
  by the pair that owns them, and the counts are summed. The one thing a split
  does not give is a single atomic step across pairs: a reader racing a
  multi-pair `DEL` can see one pair's keys gone before another's. Inside a
  transaction they are same-slot like every other command.
  **`MGET` is the other exception** (ADR-0048): through the proxy it takes keys
  in any slots, the proxy sends one `MGET` per slot, and the values come back
  in the order asked. What it gives up is the single snapshot: a reader racing
  an `MSET` can see some slots before it and some after. A pair that cannot be
  read fails the whole call; it is never answered as nil. `JSON.MGET` splits
  the same way, each per-slot command carrying the path (ADR-0055).
  **`MSET` and `JSON.MSET` are not split**, because their atomicity is their
  contract, and nothing inside a
  transaction is split. So a framework cache store whose multi-write is a
  transaction (Django's `set_many`: `MULTI`, `MSET`, `EXPIRE`s, `EXEC`) is
  still refused across slots. Either write those keys one at a time, or give
  the cache a `KEY_FUNCTION` that puts one hash tag on every key, which puts
  that whole cache in one slot, on one pair.
  Also **pub/sub** (out of v0 scope, ADR-0052: so Celery is not supported,
  and rq's worker commands and asynq's task cancellation are unavailable),
  **streams** (planned, ADR-0052),
  **RANDOMKEY**, and **`EVAL_RO`,
  `EVALSHA_RO`, `FUNCTION` and `FCALL`** (Redis 7's read-only scripts and
  functions; `EVAL` and `EVALSHA` are supported, see "Lua scripts").
  These conflict with slot-sharded multi-tenancy or reintroduce the
  single-threaded bottlenecks Flint exists to avoid. Common patterns they
  serve are covered by first-class commands instead; if you need one of
  these, open an issue describing the workload — patterns with broad
  demand get first-class implementations.

- **`KEYS` through the proxy** (ADR-0050), answered from `SCAN` over every
  master of your pairs: no node runs a `KEYS`, so nothing blocks, and what a
  call costs is one pass of your keyspace. A reply of more than 100,000 keys
  is refused with an error naming `SCAN`. Flask-Caching's `clear()` and
  Spring's `RedisCacheManager` send it. A seat does not answer `KEYS`.

## Introspection and admin commands are absent

`CONFIG` and `SHUTDOWN` are not implemented, by any component, and neither is
`INFO` at a seat. They answer `ERR unknown command`:

    PING             -> PONG
    CONFIG RESETSTAT -> ERR unknown command 'CONFIG RESETSTAT'
    SHUTDOWN NOSAVE  -> ERR unknown command 'SHUTDOWN NOSAVE'

**`INFO` through the proxy answers, minimally** (BUG-0176). Clients send it on
their own: ioredis, by default, sends `INFO` before its first command and treats
an error as fatal, so until this it could not connect at all. The proxy
answers it itself rather than asking a seat, because a seat's figures are
shared by every tenant on its pair:

    INFO             -> # Server / redis_version:7.2.4 / redis_mode:standalone / flint_version:<build>
                        # Persistence / loading:0
                        # Memory / maxmemory_policy:noeviction
    INFO persistence -> just that section; an unknown section is empty, as in Redis

`maxmemory_policy` is your namespace's, asked of your pair's master when
the memory section is wanted (BUG-0192): `noeviction` unless the operator
declared your namespace evictable, and then `allkeys-lru`, the nearest
Redis name for Flint's evictor, which may remove any of your keys. Sidekiq
warns that its data will be evicted unless it reads `noeviction`. When the
seat cannot be asked, the memory section is left out, not guessed.

**`CLIENT` through the proxy answers for your own connection** (BUG-0183):
`SETNAME`, `GETNAME`, `ID`, `SETINFO` and `INFO`. A name given in
`HELLO ... SETNAME` is the same name. redis-py (`client_name=`), go-redis
(`ClientName`) and node-redis (`name`) send `CLIENT SETNAME` while
connecting and fail when it is refused. `CLIENT LIST`, `KILL`, `PAUSE` and
the other subcommands are refused with upstream's "unknown subcommand": a
connection list at the proxy would show other tenants. Ids are per proxy
process.

`redis_version` is 7.2.4, the value Valkey reports (ADR-0052). Clients choose
their code paths by it, and rq and BullMQ refuse to run without one. It is
not a promise of every Redis 7.2 command: Flint implements the commands
listed above. `flint_version` is Flint's own.

Use **`FLINTINFO`** where you would reach for `INFO`. It is a flat
`field:value` list covering what a client or an operator actually needs from a
seat — `role`, `loading`, `role_epoch`, `build`, `sst_bytes`, `latest_seq`,
`last_applied`, `acked_seq`, `seq_lag`, and the WAL-headroom fields ADR-0022's
shedding is driven by. **That field set is the rocks build's**, which is the
one you run: a mem-only build reports `role`, `loading` and `live_replicas`
and nothing else, because the rest describe a durable engine it does not
have. Parse by field name and tolerate absence; do not parse by position.

In particular `loading:1` is how you tell a seat that
has BOUND from a seat that is READY: a node answers `PING` with `PONG` while
still loading, so `PONG` alone is not readiness.

Use **`FLINTCONFIG`** where you would reach for `CONFIG`. With no arguments
it dumps the live tunables; with `<key> <value>` it hot-reloads one with no
restart — `wal-fsync-ms`, `lag-soft-ms`, `lag-hard-ms`, `wal-headroom-seq`,
`min-replicas-to-write`, `max-conns`, `migrate-rate-bytes`,
`fullsync-rate-bytes`, `write-deadline-ms`, `gc-sweep-ms` among them. The
values are read live on the hot paths, so a change lands on the next
write, tick or accept. This page said "there is no substitute for `CONFIG`"
until 2026-09-06, which was wrong in the expensive direction: an operator who
believed it would take a RESTART to change a value that is hot-settable.

There is no substitute for `SHUTDOWN`. Stop a seat with a signal, or through
`flintctl`, which is the supported path.

### Operator and per-tenant commands

These are served, documented in the operating guides, and not part of the
Redis-compatible surface above — so they are listed here rather than under
**Supported**, and `tools/gates.sh` checks that anything the guides tell you
to run appears on this page.

| Command | Where | What |
|---|---|---|
| `FLINTINFO` | seat | the `field:value` health block described above |
| `FLINTCONFIG` | seat | dump or hot-set a runtime tunable |
| `FLINTKEYSIZE` / `FLINTKEYSTAMP` | seat | the GC ranking primitives (above) |
| `FLINTNSBYTES <ns>` | seat | stored bytes for one namespace — per-tenant attribution when deciding *whose* data to trim (space-reclaim.md) |
| `PROXYSTATS` | proxy | connections, command/read/write totals, cert expiry |
| `PROXYLATENCY` | proxy | per-lane read/write latency |
| `PROXYHOTKEYS` | proxy | the tenant's hot keys |
| `PROXYCACHE` | proxy | near-cache TTL — a tenant reads and sets **its own**; the fleet default, byte budget and ceiling are the operator's |

> `PROXYLATENCY`, `PROXYHOTKEYS` and `PROXYCACHE` answer **per-tenant** and
> are the three a tenant can run for themselves (tenant-guide.md).
> `PROXYCACHE` is the only one of them that CHANGES anything, and what it
> changes is that tenant's own accepted staleness: `PROXYCACHE <ttl_ms>` on an
> authed connection sets the namespace's TTL, clamped to the operator's
> `--cache-ttl-max-ms` (60 s by default) and answering with the value actually
> applied. `PROXYCACHE <ttl_ms> <max_bytes>` — the two-argument operator form
> — still sets the fleet default and the shared budget, and an operator's
> `ttl_ms 0` disables the cache for everyone regardless of any per-tenant
> value. The `FLINT*` commands are
> seat-local: the proxy refuses the whole prefix, because it is the tenant
> boundary. Reach a seat directly to use them.
>
> Every other `FLINT*` verb the server matches on — `FLINTPROMOTE`,
> `FLINTFENCE`, `FLINTLEASE`, `FLINTSYNC` and the rest — is control-plane
> machinery that `flintctl` and the controller drive. They are described in
> architecture.md and failover.md as MECHANISM, not as things to run, and
> they are deliberately not listed here.

### The trap: `NO_KEY` is a routing table, not a dispatch table

Both `flint-server` and `flint-proxy` carry a `NO_KEY` list that includes
`INFO`, `COMMAND`, `CLUSTER`, `SELECT`, `HELLO` and others. **It answers one
question only: can a routing slot be derived from argument 1.** It says nothing
about whether any component implements the command.

Reading `CLUSTER` in the proxy's `NO_KEY` list and concluding the proxy handles
it is a natural inference and a wrong one. A keyless command is *forwarded* —
to pair 0's master — which then returns the same `ERR unknown command`. So these
commands fail identically through the proxy as against a bare seat. (`INFO` was
this section's example until BUG-0176; the proxy now answers it before routing
is ever asked, which the list still does not say either.)

If you are deciding whether Flint implements something, the list to consult is
**Supported** above, or simply send it to a server and read the reply. Someone
reached the opposite conclusion from `NO_KEY` while holding an open connection
to a seat that would have answered the question in one round trip.
