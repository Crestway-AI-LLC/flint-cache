# BUG-0182: HMSET was an unknown command, and Spring Session and ASP.NET Core write every entry with it (FIXED 2026-09-25)

**Status:** **FIXED 2026-09-25**. Not in v0.1.0-rc.77: it ships with the next
release.
**Severity:** high for the two stacks named below: neither can store anything.

## What was measured

Through the proxy on a two-pair fleet on a gate box, 2026-09-25, each client
with its default options:

| client or framework | result |
|---|---|
| Jedis 5.2.0 (RESP2 and RESP3), Lettuce 6.5.1 (RESP3) | works: strings, hashes, cross-slot MGET, pipelines, same-slot MULTI |
| StackExchange.Redis 2.8.16 | connects with no handshake error; the same calls work |
| Spring Session 3.4.1 `RedisSessionRepository`, on Spring Data Redis 3.4.1 | **every `save` failed**: `ERR unknown command 'HMSET'` |
| ASP.NET Core `IDistributedCache` (Microsoft.Extensions.Caching.StackExchangeRedis) 9.0.0 and 10.0.0 | **every `Set` failed**: `ERR unknown command 'HMSET'` |
| the same package, 8.0.11 | every `Set` fails on `EVAL`: Lua is excluded by design, and this fix does not change that |

`HMSET` has been deprecated upstream since Redis 4.0 in favour of `HSET` with
several fields, and every Redis and Valkey still answers it. Frameworks never
moved off it.

## The fix

`HMSET key field value [field value ...]` is `HSET` answering `+OK`: the
same handler, the same arity check with its own name in the error, the same
write classification (`flint-commands`), so a replica refuses it and it is
metered as a write. Listed in `command-support.md` and with `HSET` in
`retry-safety.md`.

Covered by a conformance case (checked against Valkey: `+OK`, the overwrite,
both arity errors and WRONGTYPE) and a server unit test. Re-measured on the
gate box with the fix: Spring Session saves and finds sessions, and
`IDistributedCache` 10.0.0 sets, gets, refreshes and removes.

## Found beside it, and not fixed here

Spring Session's `changeSessionId`, which Spring Security calls at every login
to prevent session fixation, sends `RENAME` from the old session key to the
new one. They are in different slots, so it is refused with `CROSSSLOT`, and
a login through Spring Session fails. Cross-slot `RENAME` is excluded by
design, and ADR-0049 (accepted the same day) keeps it so: its contract is
atomicity. Two configurations, both measured, make login work, and
`command-support.md` and the tenant guide name them.
