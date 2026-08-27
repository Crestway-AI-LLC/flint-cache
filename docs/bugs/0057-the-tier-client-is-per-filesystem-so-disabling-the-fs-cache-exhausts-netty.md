# BUG-0057: the tier client is per-FileSystem, so disabling the FS cache exhausts Netty (OPEN)

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

Share the tier client across FileSystem instances instead of building one per
instance — a small reference-counted registry keyed by the resolved tier URI
plus whatever else distinguishes a connection (credentials, TLS), with
`close()` dropping a reference and the last one out shutting the client down.
That makes the accelerator's footprint a function of how many distinct tiers
are configured rather than how many times Hadoop happened to construct a
FileSystem.

Not fixed here. This was found by a harness, not by a customer, and the change
touches the lifecycle of a shared client — it wants its own change with its own
test, not a drive-by in a benchmarking commit.

## Verification the fix needs

- A test that calls `FileSystem.get` for the same `s3a://` URI N times with
  `fs.s3a.impl.disable.cache=true` and asserts the JVM's thread count is flat
  in N. **Positive control:** the same test against today's code must fail —
  otherwise it is asserting something the setting does not exercise.
- A test that two *different* tier URIs still get two clients, so the sharing
  key is the tier and not a global.
- The end of the reference count: after the last `close()`, the client is shut
  down and a subsequent `get` builds a new one rather than handing back a dead
  connection.

## Related

- `flint-cache:docs/bugs/0051-*.md` — the harness change that exposed this. It
  disabled the FileSystem cache to stop the cached arm from being handed the
  plain arm's FileSystem; the harness has since moved each arm into its own JVM,
  which removes the need to disable the cache at all.
- `docs/adr/0023-*.md` D5 (single-flight) — the same theme one layer down: the
  tier client is a shared resource, and treating it as per-caller is what makes
  it expensive.
