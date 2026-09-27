# BUG-0192: a Sidekiq server fails its metrics flush on every heartbeat, and warns of eviction (OPEN)

**Status:** OPEN, found 2026-09-27. To be fixed after ADR-0053 ships.
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

## What a fix needs

- `BITFIELD` with what Sidekiq sends: `INCRBY` on unsigned fields at `#n`
  offsets, `OVERFLOW SAT`; and, since a command half-served is a trap, the
  rest of the command (`GET`, `SET`, `WRAP`, `FAIL`, signed types) checked
  against Valkey, with `BITFIELD_RO`.
- `maxmemory_policy` in the proxy's INFO: `noeviction` for a tenant whose
  namespace is not evictable, and the policy an evictable one gets.
- `client_compat_drill` fails on a lifecycle exception in the server's log,
  which it now passes over.
