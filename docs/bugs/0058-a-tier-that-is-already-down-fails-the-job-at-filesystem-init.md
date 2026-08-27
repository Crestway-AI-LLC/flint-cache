# BUG-0058: a tier that is already down fails the job at FileSystem init (OPEN)

Found 2026-08-26 by a deliberate control in the Spark TPC-DS end-to-end run
(`flint-cache:packaging/aws/spark-e2e/`): stop `flint-server`, run the cached
arm anyway, and check that it lands near the plain arm's timings. It did not
land anywhere. **Severity: high — this is the exact property ADR-0023 D12.9
calls "the property that decides deployability".**

## Symptom

`flint-server` stopped and confirmed unreachable (`ConnectionError` from a
separate probe on the Spark box), then one cached arm submitted:

```
WARN FileSystem: Failed to initialize filesystem s3a://flint-accel-spark-e2e-20260826/tpcds/call_center:
  ai.crestway.flintaccel.shaded.lettuce.core.RedisConnectionException: Unable to connect to 172.31.70.111/<unresolved>:6379
	at ...lettuce.core.AbstractRedisClient.getConnection(AbstractRedisClient.java:352)
Caused by: ...netty.channel.AbstractChannel$AnnotatedConnectException: Connection refused: /172.31.70.111:6379
Caused by: java.net.ConnectException: Connection refused
```

Not one query ran. The failure repeats per table and the job aborts.

## What D12.9 promises

> **S3 is authoritative and always reachable**, so every tier interaction is an
> optimisation and must be written as one: any failure falls through to the
> origin and the read still succeeds. A cache that can take down the job it
> accelerates is strictly worse than no cache.

A Spark job whose tier is down at submission time is taken down by its cache.

## Root cause

`FlintS3AFileSystem.initialize()` calls `TierSupport.build(...)`, which
constructs the `RedisClient` **and opens its connection eagerly**. Lettuce's
`getConnection` throws `RedisConnectionException` when the endpoint refuses,
that propagates out of `initialize()`, and Hadoop treats a FileSystem that
cannot initialize as unusable — correctly, since it has no instance to hand
back.

D12.9's mitigations are all on the **read** path: `REJECT_COMMANDS` so a
disconnected tier produces an observable failure rather than a queued future
that never settles, and a latency budget around the lookup. Those protect a
tier that dies *after* a working connection exists. Nothing covers a tier that
is *already* down when the FileSystem is built, because the connection is made
before there is any read to fall through from.

**Measured:** the stack above, with the tier confirmed down beforehand and the
same arm passing minutes earlier with the tier up.
**Not measured, and NOT claimed broken:** the case D12.9 was actually written
for — a tier killed *mid-job*, after initialization. That path has its own
handling and this run says nothing about it. The gap is initialization only.

## Fix

Connect lazily, or tolerate a failed initial connect. `initialize()` should
build the client without requiring the endpoint to answer, and the first read
that needs the tier should degrade to origin when it cannot get a connection —
the same fall-through D12.9 already specifies, just also reachable from a
cold start.

The subtlety worth stating so the fix does not swap one failure for another:
a lazily-connecting client must not reintroduce the five-minute hang D12.9
records. `REJECT_COMMANDS` has to be set on the client from the outset, so a
never-connected client fails fast in exactly the way a disconnected one does.

## Verification the fix needs

- Start the tier, populate it, stop it, submit a job: every query must complete
  with origin reads. **Positive control:** the same test against today's code
  must fail at `initialize`, otherwise it is not exercising this path.
- Timings in that state should sit near an unaccelerated run — near, not equal,
  since each read now also pays a failed connection attempt. If they sit far
  above, the fall-through is retrying rather than degrading, which is the hang
  in a slower disguise.
- The tier never comes up at all for the life of the job, and the job still
  finishes. This is the deployment case: a customer whose Flint is down should
  see a slow job, not a failed one.

## Related

- `flint-cache:docs/adr/0023-*.md` D12.9 — the property this violates, and the
  five-minute hang that shaped its fix.
- `docs/bugs/0057-*.md` — same run, also in `initialize()`: it builds a tier
  client per FileSystem instance. Both point at initialization doing work that
  belongs later and being unable to cope when that work fails.
