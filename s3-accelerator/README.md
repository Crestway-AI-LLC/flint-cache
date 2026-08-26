# flint-accel

A look-aside S3 read cache. The library runs **in your process, under your own
IAM role**, and on a miss fetches from S3 with **your** credentials — so the
cache tier never speaks S3, never holds a credential, and cannot read your
bucket.

**Apache License 2.0.** Deliberately so, and deliberately backend-agnostic: it
speaks the Redis protocol and works against Valkey, Redis, or
[Flint](https://github.com/Crestway-AI-LLC/flint). Putting a closed jar in the
data path of someone's Spark cluster is the hardest possible trust ask, and the
moat was never the interface.

> Flint itself is **Elastic License 2.0** — source-available, not OSI open
> source. **This directory is a separate work under a separate licence**: the
> Apache-2.0 terms in `s3-accelerator/LICENSE` govern everything under
> `s3-accelerator/`, and the Elastic-2.0 licence at the repository root does
> not. Nothing here is derived from Flint's source, and this library speaks the
> Redis protocol — it works against Valkey or Redis with no Flint anywhere.

---

## Start here

```bash
tools/preflight.sh                       # uses `hadoop classpath`
tools/preflight.sh /opt/spark/jars       # or a directory of jars
```

It needs no JVM and installs nothing. It reads the jars you already have and
prints the exact `--conf` lines for **your** cluster, the encryption trade
before you configure anything, and any version clash it can see. Refusing to
start on a bad classpath is a backstop; this is the plan.

---

## Three ways in. Pick one.

Nothing changes your paths — `s3a://…` stays `s3a://…`. A scheme you have to
rewrite everywhere is a migration, not a cache.

### 1. S3A custom stream type — *recommended, needs Hadoop 3.4.2+*

Covers **every** `s3a://` read, not just table reads. Hadoop's own designed
extension point.

```
--conf spark.hadoop.fs.s3a.input.stream.type=custom
--conf spark.hadoop.fs.s3a.input.stream.custom.factory=ai.crestway.flintaccel.s3a.FlintStreamFactory
--conf spark.hadoop.fs.s3a.flint.tier.uri=redis://<your-endpoint>:6379
```

### 2. `fs.s3a.impl` — *any Hadoop version*

Same coverage, for clusters older than 3.4.2. A subclass of `S3AFileSystem`,
so credentials, writes, listings and delegation tokens are inherited untouched.
It is **not** a designed extension point, so prefer path 1 where it exists.

```
--conf spark.hadoop.fs.s3a.impl=ai.crestway.flintaccel.s3a.FlintS3AFileSystem
--conf spark.hadoop.fs.s3a.flint.tier.uri=redis://<your-endpoint>:6379
```

### 3. Iceberg `io-impl` — *any Hadoop, table reads only*

`FileIO` is a **supported** plug point selected by configuration — no fork, no
PR, no waiting on a release. Accelerates Iceberg table reads and nothing else,
which is also its safety: nothing outside table reads can be affected.

```
--conf spark.sql.catalog.<cat>.io-impl=ai.crestway.flintaccel.iceberg.FlintFileIO
--conf spark.sql.catalog.<cat>.flint.tier.uri=redis://<your-endpoint>:6379
```

Iceberg builds its own S3 client and reads **`client.region`** — not
`s3.region`. If your cluster has no ambient AWS region (no `AWS_REGION`, no
`~/.aws/config`, not on EC2), add
`--conf spark.sql.catalog.<cat>.client.region=<region>` or the SDK throws
*"Unable to load region from any of the providers in the chain"* before any of
this runs. Easy to miss, because a developer laptop almost always has one and a
clean CI runner never does.

### Python / PyTorch / Ray

`install()` re-registers the `s3` protocol, so existing `s3://` paths and
anything built on fsspec route through the tier unchanged.

```python
import flint_accel
flint_accel.install(tier_uri="redis://<your-endpoint>:6379")
```

**The JVM and Python clients share one tier by design** — a Spark job and a
PyTorch job over the same dataset pay S3 once between them, not once each.
`tools/cross_language_drill.sh` asserts that in both directions.

---

## Encryption — read this before configuring

| Mode | Cached? | Why |
|---|---|---|
| **SSE-C** | **Never.** Not tunable. | The tier would hold plaintext readable without the key, defeating the control outright. No acceleration on SSE-C data. |
| **SSE-KMS** | **Not by default.** Opt-in. | S3 decrypts server-side, so caching is lawful — but anyone who can read the tier reads the plaintext **without holding `kms:Decrypt`**, and cache hits produce **no CloudTrail decrypt record**. If that audit trail is your compliance requirement, leave it off. |
| **SSE-S3** | Yes. | No grant and no audit trail is bypassed. |

To accelerate KMS-protected buckets after your own review:

```
--conf spark.hadoop.fs.s3a.flint.cache.sse-kms=true     # paths 1 and 2
--conf spark.sql.catalog.<cat>.flint.cache.sse-kms=true # path 3
```
```python
flint_accel.install(tier_uri="…", cache_sse_kms=True)   # python
```

Detection is not free of caveats: if the probe fails, the object is cached and
a counter records it. `kmsUndetectable` (JVM) / `kms_undetectable` (Python) is
the exact size of the hole in the guarantee — it is reported rather than
assumed to be zero.

---

## Configuration

| Setting | S3A (paths 1, 2) | Iceberg (path 3) | Python | Default |
|---|---|---|---|---|
| tier endpoint | `fs.s3a.flint.tier.uri` | `flint.tier.uri` | `tier_uri` | `redis://127.0.0.1:6379` |
| chunk size | `fs.s3a.flint.chunk.bytes` | `flint.chunk.bytes` | `chunk` | 64 KiB |
| tier timeout | `fs.s3a.flint.tier.budget.ms` | `flint.tier.budget.ms` | `tier_budget_s` | 50 ms |
| metadata TTL | `fs.s3a.flint.meta.ttl.seconds` | `flint.meta.ttl.seconds` | `meta_ttl_s` | 60 s |
| cache SSE-KMS | `fs.s3a.flint.cache.sse-kms` | `flint.cache.sse-kms` | `cache_sse_kms` | `false` |
| declare immutable | `fs.s3a.flint.immutable` | `flint.immutable` | — | `false` (**`true`** on the Iceberg path) |
| max cached object | `fs.s3a.flint.max.object.bytes` | `flint.max.object.bytes` | `max_object_bytes` | 512 MiB |
| read block size | — (AAL fetches the exact range) | — | `default_block_size` | **256 KiB** = 4 chunks (not fsspec's 5 MiB, not our chunk) |
| immutable TTL | `fs.s3a.flint.meta.ttl.immutable.seconds` | `flint.meta.ttl.immutable.seconds` | — | 86400 s |

**Immutability is a declaration the engine can make and the cache cannot
infer.** Metadata normally revalidates on a 60 s TTL, because an object at a
path can be replaced and a stale *length* makes reads hit EOF early — which
looks like truncation, not staleness. If your data is write-once, that
revalidation is a HEAD per object per minute guarding against something that
cannot happen.

The Iceberg path declares it **by default**, because the format guarantees it:
Iceberg never rewrites a file — data files, manifests, manifest lists and
metadata JSON are all write-once, and a change is a new file plus a commit. An
arbitrary `s3a://` path carries no such promise, so that path keeps the short
TTL unless you opt in.

It is a long TTL, not an infinite one. If you declare immutability and are
wrong — delete a path and write different bytes there out-of-band — a day
bounds the damage where "never revalidate" would not. Writes made *through*
this library invalidate regardless.

**The chunk grid costs 1.25x tier memory to be a power of two, and pays it on
purpose.** A chunk is stored as `CHUNK + 4` bytes of identity seal, which lands
one byte past the tier allocator's 64 KiB size class, so the tier takes the next
class and charges 1.2522x the bytes you cached.

A grid 128 bytes smaller escapes that — measured, 19.5% less tier memory on a
full-object read — and was implemented and then withdrawn. Application read
offsets are themselves powers of two, so a grid that is not one stops dividing
them, and every selective read straddles an extra chunk. On the same 16 x 64 KiB
pattern that costs **+19.8% origin bytes** — measured — while the memory saving
on that pattern nets out to a few percent rather than the 19.5%.

The tension is structural rather than a tuning miss: the grid must divide the
application's alignment to avoid the extra chunk, every value that divides a
power of two is a power of two, and every power of two plus the seal crosses a
size class. Which cost dominates is a function of read size — the extra chunk is
20% of a five-chunk fetch and 0.3% of a thousand-chunk one — so it is a workload
question, and it is open.

**The block size must be a whole number of chunks.** fsspec anchors blocks at
multiples of the block size, so a block nests inside the grid only if the chunk
divides it; otherwise every block straddles one more chunk than it needs, on the
origin side as well as the tier, because a miss is fetched on chunk boundaries.
The Python default is written as `4 * CHUNK` and asserted at import, rather than
as a round number that happens to be one today.

Both clients must agree on the grid, and it is spelled once per language for
that reason. Chunks live under a versioned prefix (`c2/`) so that a future
change to it retires the old entries instead of mixing two grids in one
keyspace — an index is an offset divided by the grid, so a disagreement would
be a correctness bug rather than a miss.

Any tier failure — down, slow, or lying — degrades to a plain S3 read inside
the budget. The tier is an optimisation and is written as one.

The budget bounds a whole tier **command**, not one network read of it, so a
tier that answers promptly and then delivers slowly is caught as well as one
that is slow to answer. The consequence worth sizing before you tune it down:
a warm read whose reply is R bytes needs `R / budget` of tier bandwidth or it
degrades to the origin. A large sequential read fetches up to ~8 MiB per tier
round trip, so it wants roughly 1.3 Gbit/s of effective tier throughput to stay
cached at the 50 ms default. Below that the tier genuinely is slower than S3
for those reads, and degrading is the correct answer rather than a regression.

---

## Is it working?

Every counter is exposed over **JMX** under `ai.crestway.flintaccel:type=Cache,*`.
No dependency is added to do it — `javax.management` is in the JDK, and the JMX
exporters Spark and Prometheus deployments already run will scrape it with no
code from you.

The one attribute to look at first is `Summary`:

```
flint-accel: 94.2% hit rate, 18104 hits / 1113 misses, 214 origin GETs, 806 MiB from S3
```

It only mentions a failure mode when one is happening, so a clean line means a
clean cache. When something *is* wrong, it says which and what to do:

```
flint-accel: no chunk reads yet, 12 origin GETs, 0 MiB from S3; 12 reads BYPASSED
(SSE-KMS, off by default -- set flint.cache.sse-kms=true to accelerate them)
```

```
flint-accel: 3.1% hit rate, 44 hits / 1372 misses, 1372 origin GETs, 5.3 GiB from S3;
breaker OPEN after 9 opens (the tier is sick)
```

Those two lines exist because both are **silent** otherwise: an SSE-KMS bucket
and a sick tier both produce correct, unaccelerated reads with nothing to look
at. "The cache does nothing" is the symptom for both, and they need opposite
responses.

Other attributes worth alerting on:

| Attribute | Meaning |
|---|---|
| `ChunkHitRatePercent` | the headline. `NaN` means no reads yet — not a 0% cache |
| `SseKmsBypassed` | reads skipped because the object is KMS-encrypted |
| `SseKmsUndetectable` | objects whose encryption could not be determined and were cached anyway — the exact size of the hole in that guarantee |
| `BreakerOpens` | non-zero means the tier has been sick |
| `IntegrityFailures` | non-zero means the tier is corrupting or misplacing data |
| `DegradedReads` | reads that fell through to S3 because the tier was unusable |

Python exposes the same counters as a dict on the filesystem object:

```python
fs = fsspec.filesystem("s3")
print(fs.counters)   # chunk_hits, chunk_misses, kms_bypassed, ...
```

## What is verified

```bash
tools/gate.sh            # 21 stages
tools/gate.sh --quick    # skip the shaded-jar re-run
```

Two of those stages are **suites we did not write**, which is the point:

- **Hadoop's own `AbstractContractSeekTest` / `AbstractContractOpenTest`** —
  45 of 45. On its first run it found six real defects that every suite we
  had written passed, including a stale-length bug that made reads hit EOF
  early.
- **fsspec's `tests.abstract`** — 90 of 91. The one failure fails identically
  against unmodified `s3fs`, so it is inherited, not ours.

And several that only exist because correctness suites cannot see them:

- **Economics.** `tools/counting_s3.py` counts requests, because no inherited
  suite notices that 24 workers missing the same chunk should cost one GET
  rather than 24 — a cache that fetched 24 times still returns the right bytes.
- **Tier integrity.** The tier is corrupted on purpose: absent, truncated,
  wrong-bytes, and right-bytes-wrong-offset. The last two returned wrong data
  as truth until chunks were sealed with their own identity.
- **Cross-language sharing**, both directions, with the negative controls.
- **SSE-KMS on all three adoption paths**, because a rule enforced on one path
  is not a rule.
- **Iceberg end to end** — a real table, Avro and Parquet, read through
  Iceberg's own planner, plus the `FileIO` serialisation round trip that Spark
  performs when it ships the catalog to executors.

---

## Known limits

- **Spark and Trino are not in the test loop.** The Iceberg suite drives
  Iceberg's own planner and readers; Spark's split planning and vectorised
  reader are not exercised, though both funnel through the same
  `InputFile.newStream()` seam.
- **No AWS-scale measurement yet.** Every number here is from a local counting
  fixture. Throughput and GPU-utilisation claims are unmeasured until a real
  workload runs on real hardware.
- **`hadoop-aws` 3.4.3 / 3.5.0 need the shim jar** — their audit path
  references an AAL class no published AAL ships. `preflight.sh` detects this
  and says so.
- **Iceberg ≥ 1.9 with Avro < 1.12** throws `NoSuchMethodError` when *writing*
  Avro-format tables. Pre-existing, unrelated to this library, and detected by
  `preflight.sh`.

---

## Repository layout

| Path | What it is |
|---|---|
| `jvm-spike/…/client/FlintObjectClient.java` | **the reference implementation** — two-level keys, absolute chunk grid, per-chunk single-flight, identity-sealed values, bounded degradation |
| `jvm-spike/…/s3a/` | paths 1 and 2, plus the classpath shim guard |
| `jvm-spike/…/iceberg/` | path 3 |
| `python/flint_accel/` | the Python path |
| `tools/counting_s3.py` | the counting S3 fixture — self-describing bodies, armed counters |
| `tools/preflight.sh` | can this cluster adopt it, and with what configuration |
| `tools/gate.sh` | everything |

The `*Spike.java` mains are the record of how each finding was made, not code
to build on. They have three different fill paths between them and one of those
cached nothing at all while passing every correctness check. They are kept
because each documents a measurement; `FlintObjectClient` is the reference.
