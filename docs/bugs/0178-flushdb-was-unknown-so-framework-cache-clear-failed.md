# BUG-0178: FLUSHDB was an unknown command, so framework cache stores could not clear (FIXED 2026-09-24)

**Status:** **FIXED 2026-09-24**, found the same day in a survey of framework
cache stores against a Flint cluster. Not in v0.1.0-rc.77: it ships with the
next release.
**Severity:** medium, and the worse half is silent. Django's `cache.clear()`
raised. Rails' `RedisCacheStore#clear` reported nothing and cleared nothing,
so an application that clears its cache on deploy kept serving stale entries
with no error anywhere.

## What was measured

On a gate box, through the proxy, 2026-09-24:

| store | `clear()` |
|---|---|
| Django 4.2.30 `django.core.cache.backends.redis.RedisCache` | raises `ResponseError: unknown command 'FLUSHDB'` |
| Rails activesupport 8.1.4 `RedisCacheStore`, no namespace (redis-rb 6.0.0) | returns `nil`; the store's error handler received `ERR unknown command 'FLUSHDB'` |
| the same store with a `namespace:` | works: it deletes by `SCAN` instead |

## The mechanism

Both stores clear an un-namespaced cache with `FLUSHDB`, the command for "this
database". Flint implemented `FLUSHALL` (scoped to the tenant's namespace) and
not `FLUSHDB`. A tenant has exactly one database (`SELECT` accepts 0 only), so
the two mean the same set of keys, and nothing about Flint's model excluded
`FLUSHDB`. It was simply never added.

## The fix

`FLUSHDB [ASYNC|SYNC]` is `FLUSHALL`, at every place the latter is named: the
server's dispatch and keyless lists, the shared write and space-freeing
classifications (so a replica refuses it, and the disk guard still allows it
as a command that frees space), and the proxy's routing, fan-out and near-cache
invalidation. Covered by a conformance case (sync and async, asserting the
effect, not only the `+OK`) and a redis-py check in `client_compat_drill`.

## Found beside it, and not fixed here

The same survey found Rails' `read_multi` and Django's `get_many`/`set_many`
refused with `CROSSSLOT`, even on a single-pair fleet. Cross-slot multi-key
commands are **excluded by design** (`docs/command-support.md`), so changing
that is a design decision, not this fix.

Decided 2026-09-25 as ADR-0048: the proxy now splits `MGET` per slot, so
`read_multi` works, and `client_compat_drill` gates it. Django's `set_many` is
a transaction and stays refused across slots.
