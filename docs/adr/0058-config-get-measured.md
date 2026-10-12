# ADR-0058: CONFIG GET, measured: no client measured here sends it

Status: **ACCEPTED 2026-10-12, option A** (Jeff: "go with your
recommendations on ADR-0057 and ADR-0058"). `CONFIG` stays absent, and
command-support.md names the known senders and their way out. Option B
waits for a tenant or a measured client that needs it.

Proposed 2026-10-10. The recommendation reversed the approved plan, so it
went back to Jeff.

**What was approved.** Jeff, 2026-10-10, approved this suggestion: a short
ADR for a read-only `CONFIG GET` that answers a small, truthful list. The ADR
would first check which clients send `CONFIG GET`, so the list would not be
guesswork.

**What the check found, and the recommendation.** The check found no client
that needs `CONFIG GET` alone. So this ADR recommends not serving it yet
(option A). Option B, the approved shape, is written out in full below, in
case Jeff prefers it.

## Context

`CONFIG` is not served by any component. Every subcommand answers
`ERR unknown command` (command-support.md, "Introspection and admin
commands are absent"). The suggestion behind this ADR was that some clients
and tools ask for configuration when they start, and it named Spring
Session's keyspace-notification setup as the known case.

## Measured (2026-10-10, on the laptop)

**Client libraries.** These are the libraries installed for
`client_compat_drill`. Searching their code found no call that sends
`CONFIG`. The only mention is an example in ioredis's README.

| Language | Libraries and versions |
|---|---|
| Python | redis-py 8.1.0 and 6.4.0, rq 2.8.0, Celery 5.6.3, kombu 5.6.2, limits 4.8.0 |
| Node | ioredis 6.0.0, node-redis 6.3.0, BullMQ 6.3.9, express-rate-limit 8.7.1, rate-limit-redis 6.0.1, rate-limiter-flexible 11.2.1, redlock 4.2.0 and 5.0.0-beta.2 |

BullMQ checks the eviction policy, as Sidekiq does, through `INFO`'s
`maxmemory_policy`, which the proxy already answers (BUG-0192).

**Tools.** Each ran against a Valkey 9.1 under `MONITOR`. None sent
`CONFIG`:
- `valkey-benchmark` 9.1;
- `redis-benchmark` 8.2.8;
- `valkey-cli --bigkeys`;
- `valkey-cli --memkeys`.

The whole recording was `PING`, `SCAN`, `DBSIZE` and `READONLY`.

**Not measured, because they are not installed:**
- Sidekiq and asynq. The drill installs them only while it runs.
- Spring Session.
- Prometheus's `redis_exporter`.
- Redis GUIs.

These would be downloads, which need Jeff's approval.

**The named case does not need `GET` alone.** From Spring Session's
documentation, its startup action reads `notify-keyspace-events` and then
runs `CONFIG SET` to add the flags it needs. A truthful `GET` answers `""`,
because Flint sends no keyspace notifications. Spring would then send
`CONFIG SET`, which stays refused, so the application fails to start just as
it does today. Answering with the flags Spring wants would let it start, but
its session-expiry events would never arrive: a silent failure in place of
a loud one. Spring Session documents the way out for managed Redis services
that refuse `CONFIG`: configure `ConfigureRedisAction.NO_OP`. That works on
Flint today.

## Options

**A: keep `CONFIG` absent, and document the two known senders.**
- command-support.md names Spring Session and monitoring exporters, and
  points each at its documented switch for managed services that refuse
  `CONFIG`.
- This costs a paragraph, and changes no reply.

**B: a read-only `CONFIG GET` at the proxy.** This is the approved shape:
- **Answered by the proxy,** as `INFO` is, because a seat's settings are
  shared by every tenant on its pair and belong to the operator
  (`FLINTCONFIG`).
- **Three parameters:**
  - `maxmemory-policy`: from the same source as `INFO`'s
    `maxmemory_policy` (`noeviction`, or `allkeys-lru` for a namespace
    declared evictable), and left out when the seat cannot say;
  - `databases` = `1` (only `SELECT 0` succeeds);
  - `notify-keyspace-events` = `""`.
- **Redis's matching:** glob patterns over the names, so `CONFIG GET *` and
  `CONFIG GET maxmemory*` work, and several parameters in one call, as
  Redis 7 allows. An unmatched pattern answers an empty list.
- **The reply shape follows the protocol:** a flat array in RESP2, a map in
  RESP3.
- **Every other subcommand is refused,** including `SET`, `REWRITE` and
  `RESETSTAT`, with upstream's "unknown subcommand" text, as `CLIENT`'s
  refused subcommands are (BUG-0183).
- **About half a day** with its conformance case.

**What each option does to a client:**

| Client | Under A | Under B |
|---|---|---|
| Spring Session, as installed | fails at `CONFIG GET` | fails at `CONFIG SET` instead |
| Spring Session with `NO_OP` | works | works |
| an exporter's `CONFIG GET *` | an error, which exporters can be told to skip | three values |

## Recommendation

**A, now.** No client measured here sends `CONFIG GET`, and the one named
case still fails under B. A reply nobody reads is surface to keep truthful
for nothing.

**Build B when a tenant or a measured client needs it.** It is fully
specified above and takes half a day. Measuring Spring Session or
`redis_exporter` first would need Jeff's approval for the downloads.

## What this does not decide

`CONFIG SET` in any form. Tenants tune nothing on a shared seat, and
operators already have `FLINTCONFIG`.
