# ADR-0051: Rate limiters on Flint, measured, and running Lua

Status: **PROPOSED 2026-09-26, for Jeff's decision.** Nothing is built.

## Context

ADR-0050 chose to recognise the few Lua scripts that locks and django-redis
send, by SHA1, and run each natively, on one condition: a script reads its one
key and writes it at most once, so the ordinary write lock makes it atomic. It
left running Lua itself (its option B) until a need was measured, and costed
it partly on "a way for one script to be a batch in the WAL".

Rate limiting is one of the jobs applications most often give Redis, with
caching, sessions and locks. **Measured 2026-09-26**, through the proxy on a
two-pair fleet on a gate box, each library on its defaults, with a plain
Valkey as the control (every library below worked there):

| library | on Flint | what it sends |
|---|---|---|
| Python `limits` 4.2 (Flask-Limiter, SlowAPI), fixed window | every hit refused | `INCRBY`, then `EXPIRE` on the first hit: two writes |
| `limits`, moving window | every hit refused | `LINDEX`, then `LPUSH` per hit, `LTRIM`, `EXPIRE`; and a read-only `LRANGE` for its stats |
| `limits`, sliding window counter | every hit refused | two keys sharing a hash tag (one slot): `PTTL`, a `RENAME` between them when the window rolls, `SET PX`, `INCRBY` |
| Go `redis_rate` v10 (GCRA), `Allow` and `AllowAtMost` | every call refused | `TIME`, then floating-point arithmetic, then one `SET EX` |
| node `rate-limiter-flexible` 11.2.1, `RateLimiterRedis` on ioredis | refused: its script is not recognised | `SET NX EX`, `INCRBY`, `PTTL`, and `EXPIRE` when there is no TTL: up to three writes |
| node `rate-limit-redis` 6.0.1 (express-rate-limit 8.7.0) | **the process crashes**: the store loads its scripts when constructed, and the refusal is an unhandled rejection | `PTTL`, then `SET PX` on a new window or `INCR`: one write. Fits ADR-0050, and is now recognised there |
| Rack::Attack 6.8.0 over Rails 8.1 `RedisCacheStore` | **works** | no Lua: `INCRBY`, then `TTL` and `EXPIRE`. (On a server whose `INFO` reports Redis 7 it sends `EXPIRE ... NX` instead; Flint's `INFO` reports no version, so Rails takes the older path.) |

All but Rack::Attack are Lua. `rate-limit-redis` 6.x fits ADR-0050's
condition, and because its failure was a crash rather than a refusal, its two
scripts were recognised under ADR-0050 the same day (its second amendment),
without waiting on this record; its 4.x and 5.0 increments write twice and
wait with the rest.

The rest do not fit, and cannot: the `limits` and `rate-limiter-flexible`
scripts write two to four times, and `redis_rate`'s answer is a function of
the server's clock computed in doubles, whose results are returned and stored
through Lua's own number formatting (`tostring` is `%.14g`, and a number
handed to `redis.call` goes through the server's own double-to-string
routine).

Two facts change ADR-0050's costing:

1. **The batch already exists.** A single-slot `MULTI`/`EXEC` (ADR-0012) runs
   its commands against `BatchingKv`, which buffers every mutation with
   read-your-writes (scans included), and commits the buffer as ONE RocksDB
   `WriteBatch`: atomic at the engine, one WAL group, all-or-nothing on a
   replica. A script is the same shape. Its writes buffer; it commits once;
   and an error or a timeout discards the buffer, which is a rollback Redis
   itself cannot offer.
2. **Hand-porting has stopped being cheap.** The lock scripts were
   compare-and-set. These are small programs with arithmetic, and a port that
   formats one float differently from Lua is wrong silently: a limiter that
   admits too much, or too little, with no error anywhere.

## Options

**A. Keep Lua excluded; document per library.** Flask-Limiter and SlowAPI then
need another storage (their in-memory one is per process, so wrong behind
more than one worker), and `redis_rate` users need another limiter. Rate
limiting would be a documented gap.

**C'. Recognise these scripts too, and run the multi-write ones as a batch.**
Relax ADR-0050's condition from "one key, one write" to "one slot": a
recognised script runs on `BatchingKv` under the write locks of all its keys
and commits once, as a transaction does. Port them natively (the seven
measured here and `rate-limiter-flexible`'s), with Lua's number formatting
reproduced exactly. Days of work, no tenant code runs, and each new library
remains a measurement and a port.

**B. Run Lua: single-slot scripts in an embedded Lua 5.1, on the owning
seat.** The same interpreter Redis and Valkey embed (PUC-Rio 5.1, through the
`mlua` crate with its source vendored; MIT), so every script means what it
means on Redis, formatting included, with no port to get wrong. As built:

- **Atomicity**: the script runs against `BatchingKv` and commits once
  (above). Its `redis.call`s go through the seat's ordinary dispatcher, under
  the connection's namespace, so a script can reach nothing a command could
  not.
- **Keys**: every key a script touches must be in its `KEYS` and in one slot,
  which is Redis Cluster's rule; the seat takes their write locks in sorted
  order before running, and a `redis.call` on any other key is refused with
  an error naming the rule. (Every measured library declares its keys.)
- **Sandbox**: chunks load as text only (no bytecode, the historical escape
  route); `load`, `loadstring`, `dofile`, `loadfile`, `require`, `os`, `io`,
  `debug` and `package` are absent; globals are read-only, as in Redis. A
  memory cap per call, and an instruction-count hook that aborts a script past
  a time limit (50 ms proposed) and discards its writes. No state survives a
  call, so nothing leaks between tenants.
- **Scripts by SHA**: the proxy keeps the texts it has seen (`SCRIPT LOAD`,
  `EVAL`) and forwards an `EVALSHA` it knows as `EVAL`, so no seat needs a
  cache and a failover loses nothing; one it does not know answers `NOSCRIPT`,
  which every client handles by sending the text.
- **Not in the first cut**: the `cjson`, `cmsgpack`, `bit` and `struct`
  libraries Redis also loads (none of the measured libraries uses them), and
  `SCRIPT KILL` (the time limit replaces it).
- **Verification is stronger than for any port**: the conformance oracle runs
  the same text on Valkey, so any script can be checked against Redis, not
  only the ones someone ported.

Cost: one to two weeks, and a real security surface, since tenant-supplied
code runs inside the process that holds every tenant's data. The sandbox
above is what makes that acceptable, and it is the part to review hardest.

## Recommendation

**B.** ADR-0050 recommended ports first and Lua "only if a customer's need is
measured". Both of its premises have moved: the batch it counted as new work
is built and proven by transactions, and the ports it counted as cheap now
carry floating-point exactness that fails silently. Rate limiters are what a
customer's first application brings, and what comes after them is Lua too:
the queue libraries ADR-0050 names need it (with blocking commands and
pub/sub besides), and so does every application's own script. The recognised
scripts of ADR-0050 stay until B passes the same conformance and drill checks
against them, and are then removed, so one mechanism remains.

If you would rather not run tenant code in the seat, **C'** for the measured
limiters is the fallback, with rate limiting otherwise documented as a gap.

## Verification (if accepted)

- Conformance cases running each measured script against the Valkey oracle
  (deterministic inputs where the script takes its clock as an argument, as
  `limits`' moving window does), plus the sandbox's refusals: bytecode,
  `os.execute`, an undeclared key, a key in another slot, a runaway loop
  (aborted, and nothing written), a memory bomb.
- `client_compat_drill`: each measured limiter admits exactly its limit and
  refuses the next, on keys on both pairs.
- A crash test: kill a seat between a script's first and last write (the
  batch makes that window empty) and read back all-or-nothing on the replica.
