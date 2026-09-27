# ADR-0051: Rate limiters on Flint, measured, and running Lua

Status: **ACCEPTED 2026-09-26** (Jeff: "go with your recommendation on
ADR-0051"): option B. Built; see "As built" at the end. **Amended
2026-09-27 by ADR-0052 D2**: a script may touch any key in its declared
keys' slot, not only the declared keys.

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
| Rack::Attack 6.8.0 over Rails 8.1 `RedisCacheStore` | **works** | no Lua: `INCRBY`, then `TTL` and `EXPIRE`. (On a server whose `INFO` reports Redis 7 it sends `EXPIRE ... NX` instead; Flint's `INFO` reports no version, so Rails takes the older path. Since
ADR-0052 it reports 7.2.4, and `EXPIRE ... NX` is served: BUG-0185.) |

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
  *Amended by ADR-0052 D2: any key in their slot, as Redis Cluster allows;
  asynq writes keys it builds.*
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

## As built

- **The engine** (`flint-server`, `script.rs`): `mlua` 0.12 with PUC-Rio Lua
  5.1 vendored. `EVAL`/`EVALSHA` run the script against a `BatchingKv` over
  the dispatcher's store, and its writes reach that store only when the
  script ends normally. On the seat, `main` wraps the command in a second
  `BatchingKv` committed through `commit_watched` as ONE engine batch, which
  also moves the WATCH version of every key written (BUG-0186 is the same
  step for transactions). The declared keys are locked by the write-lock
  module's existing rule: one key, its stripe; several, every writer, as
  `MSET` does; none, nothing, since a script without keys can write none.
  The recommendation's "locks in sorted order" was not needed: that rule
  already excludes every other writer without an ordering protocol.
- **Declared keys, checked at the row** (`KeyGuard`). Every `redis.call`
  runs against its own overlay of the script's buffer behind a guard that
  decodes each row key it reads or writes (metadata, subkey and zscore
  envelopes all carry namespace, slot and user key) and refuses any row that
  is not a declared key's. A refused call's overlay is discarded, so a
  `pcall`ed `RENAME` to an undeclared key leaves its source where it was.
  The check needs no table of which argument of which command is a key, and
  a row it cannot attribute is refused. *Since ADR-0052 D2 the guard allows
  any key in the declared keys' slot; one it did not declare stops the
  script, which runs again under the lock over every writer.*
- **The sandbox** is as proposed, with three findings. (1) mlua's
  per-thread hook removes itself from a coroutine the script creates (it
  looks the callback up by thread and finds none), so a coroutine ran with
  no time limit; the state-wide hook, which Lua 5.1 copies into every
  coroutine, closes it. (2) `redis.call` had to be a Rust function, as it is
  a C function in Redis: as a Lua wrapper, `return redis.call(...)` was a
  tail call that erased the script's frame and lost the error's line. (3)
  Globals, the libraries and `redis` are read-only (proxies, a `rawset` that
  refuses them, a hidden string metatable).
- **States are reused, per namespace per thread.** A fresh state per call
  measured 83 us (the libraries 31 us, the prelude 28 us); a prepared one
  4.7 us, the script's own work aside. A state serves one namespace, so
  nothing crosses a tenant boundary; within one, the read-only environment
  means a script cannot change what the next one sees, KEYS/ARGV and the
  call functions are replaced per call, a state that hit a limit is dropped,
  and each is rebuilt after 100,000 uses. Measured on loopback (release
  build, mem engine, `valkey-benchmark`), `EVAL "return
  redis.call('incr', KEYS[1])"` against native `INCR`: at one connection
  33,000/s against 50,000/s (it was 9,000/s with a fresh state per call);
  at eight connections on one key 53,500/s (was 10,400/s), and on random
  keys 81,600/s, against `INCR`'s 119,000/s.
- **The script cache** (`flint_commands::scripts`) is one bounded type for
  both planes: 1,000 scripts and 8 MiB per namespace, 128 MiB in all. The
  proxy keeps each tenant's texts from `EVAL` and `SCRIPT LOAD` (forwarded
  to a seat so the compile check is real) and turns a known `EVALSHA` into
  its `EVAL`; scripts never ride the prefetch pipeline, so the translation
  always runs. A seat keeps its own for direct clients.
- **ADR-0050's recognised-script table is gone.** Its unit tests now run the
  same library texts through Lua, the conformance corpus runs them against
  the Valkey oracle, and `client_compat_drill` runs every library through
  the proxy; all pass with the table removed.
- **Also:** `TIME` (a keyless read, which `redis_rate` calls from its
  script) and `--script-time-limit-ms` / `--script-memory-limit-mb` on the
  seat. A queued `EVAL` inside `MULTI` is checked (arity, slots), never run
  at queue time.
- **Verified:** reply conversion, error texts with lines, and number
  spelling byte-for-byte against Valkey 9.1 (95 scripts diffed on the wire;
  the 8 that differ are this record's decisions: one slot, and the absent
  loaders and libraries); the conformance corpus's `lua`, `scripting` and
  flint-only `sandbox` families on the oracle, mem and rocks, RESP2 and
  RESP3; unit tests of the limits (a loop in `pcall`, `xpcall`, a coroutine
  and `coroutine.wrap` all stopped, nothing kept), the key guard, rollback
  and the read-only environment; and `client_compat_drill` with `limits`
  (all three strategies), `redis_rate`, `rate-limiter-flexible`,
  `python-redis-lock` (hash-tagged) and every lock library.
- **Known limits:** time spent inside one C library call (a pathological
  `string.find` pattern over a long string) is counted only when the call
  returns, as in Redis; replies nest at most 1,000 levels deep. A script
  returning a number outside `i64` (`math.huge`) answers `i64::MAX` or
  `i64::MIN` by sign: Valkey casts it in C, which is undefined, and answers
  `i64::MIN` for `math.huge` on x86-64 and `i64::MAX` on arm64, so the corpus
  does not pin it.
