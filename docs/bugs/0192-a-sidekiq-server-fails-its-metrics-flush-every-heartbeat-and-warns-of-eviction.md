# BUG-0192: a Sidekiq server fails its metrics flush on every heartbeat, and warns of eviction (FIXED 2026-09-27)

**Status:** **FIXED 2026-09-27**. Not in v0.1.0-rc.77: it ships with the next
release.
**Severity:** low: jobs run and the heartbeat registers the process. What
fails is Sidekiq's execution metrics, and an operator reading the server's
log sees an exception every beat and a warning that is not true.

## What was measured

A real Sidekiq 8.1.7 server, run against a tenant placed on one pair
(ADR-0053, `client_compat_drill` on a gate box), ran its job, and
`Sidekiq::ProcessSet` counted the process. Its own log said two more things.

**`BITFIELD` is not served.** On each heartbeat, `Sidekiq::Metrics` flushes
the execution-time histograms it keeps per job class, in a pipeline of

    BITFIELD h|DoneJob-27-20:53 OVERFLOW SAT INCRBY u16 #0 1

which answers `ERR unknown command 'BITFIELD'`. Sidekiq catches it and logs
"Exception during Sidekiq lifecycle event. event=beat" with a backtrace,
every beat. The Metrics page of Sidekiq's web UI then has no data.

**INFO has no `maxmemory_policy`.** At start the server warns:

    WARNING: Your Redis instance will evict Sidekiq data under heavy load.
    The 'noeviction' maxmemory policy is recommended (current policy: '').

Flint evicts nothing unless a namespace opts in, so for most tenants this is
false, and it tells an operator to change a setting that does not exist.

## The fix

- **`BITFIELD` and `BITFIELD_RO`, whole**, not only what Sidekiq sends,
  since a command half-served is a trap: `GET`, `SET` and `INCRBY` on
  `i1`-`i64` and `u1`-`u63` fields at bit or `#n` offsets, and
  `OVERFLOW WRAP`, `SAT` and `FAIL`. The overflow arithmetic is Valkey's
  (`checkSignedBitfieldOverflow` and its unsigned twin), ported with
  wrapping operations where C's wrap, and pinned at the `i64` and `u63`
  edges by a unit test. One read and at most one write per command.
  - **Valkey's edges, taken from Valkey.** A new conformance case, 37
    steps, passes against Valkey 9.1 and against Flint, in RESP2 and RESP3.
    It covers Sidekiq's exact command, each overflow mode at both ends of
    both signs, fields that straddle bytes, and reads past the end, which
    read zeros and create nothing. A write refused by `FAIL` still grows the
    string, as Valkey's does. The TTL is kept, and every error has Valkey's
    text in Valkey's order.
- **`maxmemory_policy` in the proxy's INFO**, in a memory section. The proxy
  asks the master of the tenant's own pair (`FLINTINFO`'s `evictable_ns`)
  only when that section is wanted. The answer is `noeviction` unless the
  namespace is declared evictable, and then `allkeys-lru`. A seat that
  cannot be asked leaves the section out rather than a guess in it.
- **`client_compat_drill`**: the Sidekiq server's heartbeat must now flush
  the job's metrics (its histogram and its per-minute totals both appear),
  its log must hold no failed lifecycle event, and no eviction warning.
