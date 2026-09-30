# BUG-0193: a write inside MULTI/EXEC never invalidated the proxy's near-cache (FIXED 2026-09-27)

**Status:** **FIXED 2026-09-27**. Not in v0.1.0-rc.77; live since
v0.1.0-rc.78 (rolled 2026-09-30).
**Severity:** medium: a tenant that opted in to the near-cache read its own
transaction's writes stale, through the proxy it wrote by, for up to the
cache TTL (300 ms by default). The tenant guide promised the opposite.

## What was measured

Suspected reading the proxy for ADR-0053, then measured locally at `5ba5388`:
a seat, a control plane and a proxy with `--cache-ttl-ms 30000`, and a tenant
with `CPTENANTCACHE acme on`.

    SET k v1                 -> OK
    GET k                    -> v1     (now cached by this proxy)
    MULTI / SET k v2 / EXEC  -> OK, QUEUED, [OK]
    GET k                    -> v1     (stale)
    SET k v3 / GET k         -> OK, v3 (a plain write invalidates: the control)

## Why

A write through a proxy drops that proxy's cache entries for the keys it
names, in `cache_writeback`, which runs after `handle` answers a command.
Inside a transaction a command never reaches it: `transaction_step` answers
QUEUED and returns, and so does EXEC. Nothing a transaction wrote was ever
invalidated.

## The fix

The proxy keeps, for a near-cache tenant, the write commands a transaction
queues. At EXEC it drops what each of them names, by the rule a plain write
uses (`cache_invalidate_written`, taken out of `cache_writeback` so there is
one rule). It does so whatever EXEC answered: a key dropped needlessly costs
one miss, and one kept serves a value the tenant has overwritten. DISCARD
forgets the list.

`near_cache_cross_client_drill` now caches two keys in one slot, runs a
transaction that overwrites one and deletes the other through the same
proxy, and requires the new value and the deletion to read back at once.
Re-measured with the fix: `GET k` after the EXEC answers `v2`.

Unchanged, and still the contract: a write through **another** proxy is seen
here only once the TTL lapses.
