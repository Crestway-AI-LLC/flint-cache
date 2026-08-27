# BUG-0058: a tier that is already down fails the job at FileSystem init (FIXED)

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

`TierSupport.build` no longer lets the initial connect failure escape. On
failure it installs `LazyTierCommands`, a proxy over `RedisAsyncCommands` whose
every call throws `RedisConnectionException` until the tier answers, and which
retries the connect at most once per `flint.tier.reconnect.ms` (default 5 s).

**The fix deliberately adds no new degradation path.** A `RuntimeException` from
a tier call is already what `FlintObjectClient.guard` catches inline — its
comment reads "already-dead connection throws inline" — and degrades to origin
on. So the outage is routed into the fall-through that was already tested,
rather than into a second one written for this case.

The interface is proxied rather than reimplemented because
`RedisAsyncCommands` has several hundred methods and all of them must fail
identically; an enumerated subset works until the first call to a method nobody
listed, which is how `TierSupport`'s allowlist-by-case-label was a bug once
already. `Object`'s own methods are answered locally — `toString()` on a tier
handle is something a logger does, and a log line is not a reason to open a
socket.

Rate-limiting matters as much as the retry. A connect attempt per read against
a dead endpoint is a TCP handshake per read, which is slower than having no
cache at all. `FlintObjectClient`'s circuit breaker already skips tier calls
once failures accumulate, so this is the second line rather than the first, but
the two cover different windows: the breaker half-opens periodically, and that
is exactly when a dead endpoint would be dialled.

## Verification

`client.TierDownSuite`, 10 checks, now run by the gate as "tier down at build".
It kills the tier, builds against it, and requires all of:

- `TierSupport.build` survives — **the check that fails before the fix**
- the read returns the object, and origin GETs moved, so it came from S3
- `tierFailures > 0`, so the client actually TRIED. Without this, "reads work"
  is equally true of a client that gave up at build time and never intended to
  try again
- reconnects are rate-limited — attempts held at 2 across 6 further reads
- and it RECOVERS: after `startTier()` the lazy handle connects and the bytes
  are still correct. This is what separates a lazy connect from simply
  disabling the cache on first failure

**Positive control, run:** with the fix reverted to rethrow, the suite fails at
the second check with the production symptom —
`RedisConnectionException: Unable to connect to 127.0.0.1:9399`. With the fix,
10/10.

**Regression:** client suite 25, S3A properties 9, adoption paths 10, SSE-C
bypass 5 — all pass unchanged, 0 failures. `TierSupport` is on every adoption
path, so this was the risk the change carried.

**Not covered, still:** the mid-job kill D12.9 was written for. `ResilienceSpike`
exercises that and **the gate does not run it** — which is how this gap survived
to reach a real Spark job. Gating it is separate work.

## Related

- `flint-cache:docs/adr/0023-*.md` D12.9 — the property this violates, and the
  five-minute hang that shaped its fix.
- `docs/bugs/0057-*.md` — same run, also in `initialize()`: it builds a tier
  client per FileSystem instance. Both point at initialization doing work that
  belongs later and being unable to cope when that work fails.
