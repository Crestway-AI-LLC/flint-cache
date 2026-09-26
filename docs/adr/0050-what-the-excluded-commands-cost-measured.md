# ADR-0050: What the excluded commands cost, measured, and the two worth revisiting

Status: **ACCEPTED 2026-09-26** (Jeff): Lua option C, `KEYS` option B,
decision 3 as recommended. Built; see "As built" at the end.

## Context

`command-support.md` excludes some commands by design, as a class: Lua
(`EVAL`, `EVALSHA`, `SCRIPT`), `KEYS`, cross-slot transactions, and cross-slot
`MSET`. Each exclusion was argued on principle. This records what each one
costs, measured against the libraries that send them, so it can be decided on
evidence.

**Measured 2026-09-25**, through the proxy on a two-pair fleet on a gate box,
each library on its default settings, with every fix of that day in place
(BUG-0182 `HMSET`, BUG-0183 `CLIENT`, BUG-0184 one-line errors):

| class | library call | what happens on Flint |
|---|---|---|
| Lua | redis-py 7 `Lock.release()` (and `extend`, `reacquire`) | `EVALSHA` refused: **a lock is never released**, it only expires |
| Lua | django-redis 6 `cache.lock()` | the same (it is redis-py's `Lock`) |
| Lua | django-redis 6 `incr` | `EVAL` refused; its fallback reads, adds and writes: **answers, but not atomically** |
| Lua | ASP.NET Core `IDistributedCache` 8.0 | every write refused (9.0 and later do not use Lua) |
| cross-slot transaction | django-redis `set_many`, `delete_pattern`; Django `set_many` | refused at queue time |
| `KEYS` | Flask-Caching 2.3 `clear()` | refused |
| `KEYS` | Spring Data Redis 3.4 `RedisCacheManager` `clear()`, what `@CacheEvict(allEntries = true)` calls | refused (`KEYS users::*`); works with `BatchStrategies.scan` configured |
| cross-slot `MSET` | aiocache 0.12 `multi_set` | refused |

Not measured, and known from their source to need Lua, blocking commands or
pub/sub: Sidekiq, BullMQ, Celery's broker, Laravel's queues and locks. Those
are queue systems, a different product question, and not this record's.

## Decision 1: Lua

**A. Keep it excluded.** Locks taken with redis-py, the most common Python
lock, are never released; they hold until their TTL. Code that releases a lock
and immediately takes it again waits out the whole TTL.

**B. Run single-slot scripts on the owning seat.** Every key a script names
must share a slot (the ADR-0012 rule for transactions), and the seat runs it
atomically against that slot in an embedded, sandboxed Lua 5.1, as Redis
does. It fixes every script above, and most scripts in the wild, at once. It is
a large piece of work: a VM, a sandbox, a time limit, `SCRIPT LOAD` and a
script cache per seat, and a way for one script to be a batch in the WAL. And
a long script holds its slot's writers for its whole run.

**C. Recognise the handful of scripts that matter, and run them natively.**
redis-py's `Lock` sends three scripts (release, extend, reacquire), fixed text
in its source; django-redis's `incr` sends two (one checks the key exists).
Read from the published wheels: all five are byte-identical across redis-py
4.5.5 to 7.0.1, sync and asyncio, and django-redis 5.2.0 to 6.0.0. The proxy or seat
matches the script by its SHA1 (for `EVALSHA`) or its exact text (for `EVAL`)
and does what it does, atomically, as a native command. Any other script is
refused as today. If a library changes its script, it falls back to today's
refusal, never to something wrong.

**Recommendation: C now, and B only if a customer's need is measured.** C
takes the measured damage, locks that never release, off the table in days,
not months, and costs nothing if it is later superseded. B is the real answer
for scripts in general, and big enough to be its own design.

## Decision 2: `KEYS`

**A. Keep it excluded.** `clear()` fails in Flask-Caching, and in Spring's
cache abstraction on its default settings, which is every
`@CacheEvict(allEntries = true)` in a Spring application. Spring's workaround
is one line (`BatchStrategies.scan`), and the tenant guide would name it.

**B. Answer `KEYS pattern` at the proxy from `SCAN`**, over every master of the
tenant's pairs, as `SCAN` already is. Nothing blocks, because no node runs a
`KEYS`; what it costs is one full pass of the tenant's keyspace per call, and a
reply as large as the match. A cap on the reply (refuse beyond it, with an
error naming `SCAN`) keeps one call from exhausting the proxy.

**Recommendation: B, with the cap.** The exclusion was about a command that
blocks a single-threaded server; answered from `SCAN` it blocks nothing, and
the frameworks that send it use it to clear a namespace, which is what a
tenant's own keyspace is.

## Decision 3: cross-slot transactions and `MSET`

**Keep both excluded, and document per framework.** A transaction's contract
and `MSET`'s is atomicity, as ADR-0048 and ADR-0049 concluded for `MSET` and
`RENAME`. The frameworks above use `MULTI` for batching, but a proxy cannot
know that the caller does not rely on the atomicity it asked for. The tenant
guide's Frameworks section names each measured call and its alternative.

## Verification

- C: `client_compat_drill` takes, releases, extends and re-takes a redis-py
  `Lock` across both pairs, and django-redis's `incr` runs natively under
  concurrent increments with none lost.
- `KEYS`: a proxy conformance case (pattern, empty match, the cap), and
  Flask-Caching's and Spring's `clear()` in the drill.

## As built

- **Scripts** (`flint-server`, `KnownScript`): `EVAL` hashes the text and
  `EVALSHA` takes the SHA; the five SHAs map to native code that does what the
  Lua does. Each reads its one key and writes it at most once, and runs inside
  the ordinary single-command dispatch under that key's exclusive write lock,
  because `command_key` now names `KEYS[1]` (`flint_commands::eval_keys`), as
  do the proxy's routing and near-cache invalidation. `SCRIPT LOAD`, `EXISTS`
  and `FLUSH` answer for the five; anything else is refused, and an unknown
  `EVALSHA` answers `NOSCRIPT`. SHA1 comes from `ring`, already a dependency
  (`flint_tls::sha1_hex`).
- **`KEYS`** (`flint-proxy`, `keys_forward`): `SCAN cursor MATCH pattern COUNT
  1000` to the end on every master, capped at 100,000 keys. A master that
  cannot be reached fails the call.
- **Verified**: unit tests of every script path (including that each text
  hashes to its table entry); a conformance case running the exact texts, so
  the Valkey oracle executes them as Lua and Flint natively, and the two must
  agree; `client_compat_drill` runs a redis-py `Lock` through its whole life
  on both pairs, a wrong token refused, and `KEYS` over keys on both pairs from
  redis-py and node-redis.
- The docs gate's command-coverage check read method-local `match` arms after
  the dispatch table as commands (it scanned to the end of the file), and
  reported `SCRIPT`'s subcommands as ungated commands. It now stops where the
  dispatch `match` closes; the dispatched set otherwise is unchanged (130).
