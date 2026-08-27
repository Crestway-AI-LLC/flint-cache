# BUG-0057: the tier client is per-FileSystem, so disabling the FS cache exhausts Netty (FIXED)

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
`S3SeekableInputStreamFactory`, and there are two per mount. So `TierSupport`
itself is pooled, keyed by a signature over every configuration value that
affects a read. Identically configured mounts share one stack; differently
configured ones do not.

**The signature has a guard, because an allowlist that silently misses a key is
the shape this file already carried once** (`TierSupport` enumerated four config
keys with `default -> null`, so a fifth resolved to null and its opt-in could
not be turned on). `create()` wraps the config accessor in a recorder and
throws if it reads any key not in `CONFIG_KEYS`. A key added to the code and
forgotten in the list now fails loudly instead of quietly merging two mounts
that differ by it.

## Verification

`client.TierSharingSuite`, 8 checks, gated as "connection sharing".

Measured thread growth, twelve clients:

| | 1 client | 12 clients |
|---|---|---|
| before | +4 | **+48** (exactly linear) |
| connections pooled only | +4 | +26 (still linear) |
| pooled `TierSupport` | +4 | **+4** (flat) |

**Positive control:** the suite fails against pre-fix code — recorded above as
+48 for twelve, where the assertion requires under half of per-instance growth.

The suite asserts the pool counters directly (`created=1, reused=11`) and not
only thread counts, since a thread assertion alone would pass for any change
that happened to allocate less. It also requires that a mount setting
`flint.immutable` gets its **own** instance: that flag stops the revalidation
HEADs, so merging it with a mount that did not set it would buy the saving with
a correctness bug. And it requires the pool to be empty after everything closes
— a pool that never drains is the same leak with extra steps.

**Regression: 108 checks, 0 failures** — client 25, S3A 9, adoption 10, SSE-C 5,
integrity 21, tier-down-at-build 10, mid-job-kill 4, sharing 8, plus
`ImmutableSuite` 3 and `MetricsSuite` 13. The last two matter most here: both
build several `TierSupport`s in one JVM, and `MetricsSuite`'s "a second client
gets its OWN bean, not a silent overwrite" is an independent check that
differently configured clients did not get merged.

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
