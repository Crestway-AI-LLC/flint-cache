# BUG-0057: the tier client is per-FileSystem, so disabling the FS cache exhausts Netty (FIXED — connections only, and the reason matters)

Found 2026-08-26 by the Spark TPC-DS end-to-end harness
(`flint-cache:packaging/aws/spark-e2e/`), on a c6i.4xlarge running Spark 4.0.4
against ~11.2 GiB of Parquet. **Severity: medium — it needs a non-default
Hadoop setting to trigger, but that setting is one customers legitimately use,
and the failure arrives mid-job with a message that names Netty rather than
anything of ours.**

## Symptom

After roughly 30 successful table reads through `FlintS3AFileSystem`, every
subsequent read failed:

```
py4j.protocol.Py4JJavaError: An error occurred while calling o368.parquet.
: java.lang.IllegalStateException: failed to create a child event loop
	at ...shaded.netty.util.concurrent.MultithreadEventExecutorGroup.<init>(MultithreadEventExecutorGroup.java:88)
	at ...shaded.netty.channel.nio.NioEventLoopGroup.<init>(NioEventLoopGroup.java:96)
	at ai.crestway.flintaccel.s3a.FlintS3AFileSystem.initialize(FlintS3AFileSystem.java:97)
	at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3615)
	at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:554)
```

`ulimit -n` was 65535 and `ulimit -u` unlimited on that box, so this was not a
tight process limit.

## The wrong conclusion drawn first

That `FlintS3AFileSystem` leaks because it never closes its tier. It does close
it — `FlintS3AFileSystem.java:192`:

```java
public void close() throws IOException {
  try { super.close(); } finally { if (tier != null) tier.close(); }
}
```

The leak is not a missing `close()`. It is that **nothing calls it**.

## Root cause

`initialize()` builds one `TierSupport` per FileSystem instance, and
`TierSupport` holds a `RedisClient` — hence a Netty `NioEventLoopGroup` with a
thread per core — plus an `S3AsyncClient`.

Normally that is once per (scheme, authority, UGI), because Hadoop's
`FileSystem.CACHE` hands back the same instance and closes it at shutdown. With
`fs.s3a.impl.disable.cache=true`, `FileSystem.get` constructs a **fresh**
instance per call and the caller owns it — and Spark does not close the
FileSystems it obtains for `read.parquet`. So one event loop group is created
per read and none are ever released.

Any heavyweight FileSystem leaks under that setting, stock `S3AFileSystem`
included; ours simply exhausts first, because a Netty event loop group per
instance is a much larger allocation than what S3A keeps. **That is the part
worth fixing:** we are disproportionately fragile under a setting we do not
control.

*Measured:* the stack, the limits, and that it reproduces once per read.
*Not measured:* how many instances it survives, and whether stock S3A reaches
its own limit later in the same configuration. Neither changes the fix.

## Fix

Two layers, both reference-counted, in `TierConnections` and `TierSupport`.

**The connection objects are pooled.** `RedisClient` keyed by tier URI,
`S3AsyncClient` keyed by endpoint, region, path-style and credentials — the
credentials are in the key because an `S3AsyncClient` carries its provider, and
handing one tenant's client to another tenant's mount is worse than not caching
at all. The secret is hashed into the key rather than stored in it.

**Sharing only those was not enough, and measuring said so.** It took the
Lettuce threads from 24 to 2 and left the total at +26 for twelve clients,
still linear — AAL starts a scheduled executor behind each
`S3SeekableInputStreamFactory`, and there are two per mount.

**So I pooled the whole `TierSupport` by a configuration signature, got +4 for
twelve, and it was wrong.** The gate caught it: the Iceberg suite read `0
chunks` where it expects 16, and the Hadoop contract suite failed
`testSeekBigFile` with `EOF End of file reached before reading fully`.

Both have one cause. Two mounts sharing a `TierSupport` share its AAL
`S3SeekableInputStreamFactory`, and that factory holds an in-memory object
cache. A second reader was therefore served out of AAL's memory rather than the
tier — `gate.sh` already warns about this in a comment, that reading twice
through one AAL factory "measures AAL's memory rather than our tier" — and,
worse, a cached object LENGTH outlived the mount that fetched it. **That is
D12.29 exactly: a stale length is worse than a stale value, because it presents
as truncation rather than staleness.** An EOF in the middle of a file is the
shape of it.

**The sharing therefore stops at the connection**, which is the resource that
actually ran out. Thread growth for twelve mounts:

| | 1 mount | 12 mounts |
|---|---|---|
| before | +4 | **+48** (exactly linear) |
| connections pooled — **shipped** | +4 | **+26** |
| whole `TierSupport` pooled — **rejected** | +4 | +4, and two suites fail |

The middle row is the smaller win and the one that does not buy a resource
bound with a correctness bug. The event loop groups named in the original stack
trace are shared; AAL's per-factory executors are not, and cannot be.

## Verification

`client.TierSharingSuite`, 8 checks, gated as "connection sharing".

Measured thread growth, twelve clients:

**Positive control:** the suite fails against pre-fix code, measured at +48 for
twelve where the assertion requires growth well under per-instance.

The suite asserts the connection counters directly (`redisCreated=1,
redisReused=11`, `s3Created=1`) and not only thread counts, since a thread
assertion alone would pass for any change that happened to allocate less. It
also asserts the constraint that killed full pooling, so it stays killed: each
mount keeps its own `TierSupport` **and its own AAL factory**. And it requires
the pool to be empty after everything closes — a pool that never drains is the
same leak with extra steps.

**Regression: the full gate.** This is the part I got wrong the first time by
running suites individually instead of the gate — 108 individual checks passed
while the two suites that catch shared-cache staleness were never run. The gate
runs them; run it.

**One thing this does not do:** it does not make an unclosed `TierSupport` free.
It makes the *second* one free. A caller that builds mounts with genuinely
different configurations in a loop and never closes them still grows, and
nothing here bounds that. Under the reported condition —
`fs.s3a.impl.disable.cache=true` with one configuration, which is what Spark
does — growth is now flat.

## Related

- `flint-cache:docs/bugs/0051-*.md` — the harness change that exposed this. It
  disabled the FileSystem cache to stop the cached arm from being handed the
  plain arm's FileSystem; the harness has since moved each arm into its own JVM,
  which removes the need to disable the cache at all.
- `docs/adr/0023-*.md` D5 (single-flight) — the same theme one layer down: the
  tier client is a shared resource, and treating it as per-caller is what makes
  it expensive.
