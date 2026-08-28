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
| max cached **part** | `fs.s3a.flint.max.part.bytes` | `flint.max.part.bytes` | `max_part_bytes` | **65 MiB** |
| max cached object | `fs.s3a.flint.max.object.bytes` | `flint.max.object.bytes` | `max_object_bytes` | **off** (deprecated) |
| read block size | — (AAL fetches the exact range) | — | `default_block_size` | **256 KiB** = 4 chunks (not fsspec's 5 MiB, not our chunk) |
| immutable TTL | `fs.s3a.flint.meta.ttl.immutable.seconds` | `flint.meta.ttl.immutable.seconds` | — | 86400 s |
| tier reconnect | `fs.s3a.flint.tier.reconnect.ms` | `flint.tier.reconnect.ms` | — | 5000 ms |

**The cap is on the PART, not the object.** A single request larger than
65 MiB is read straight from S3 and never cached; the object it belongs to is
irrelevant. What a cache saves on a read is the 25-45 ms of time-to-first-byte,
so what decides the payoff is how big each *request* is — 30 ms against an
8 MiB part is most of it, and against a 900 MiB one it is a rounding error.

`max.object.bytes` used to be 512 MiB and is now **off by default**. It was
wrong in a way worth stating: readers chunk large objects into small parts, so
a 1 GiB shard read in 256 KiB pieces is the case this cache helps *most* — and
an object-size cap refused it outright, before the part gate saw a single
request. The setting is kept and still works when set explicitly, because it
may already be configured in the field.

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

**Run the client in the same region as its tier.** The 50 ms default assumes
they are close, and it is not a soft assumption: measured 2026-08-26 from a
laptop to a tier in `us-east-1`, PING round trip was **80 ms** and an `MGET` of
five 64 KiB chunks was **102 ms** — so *every* read exceeded the budget, every
read degraded to S3, and the cache did nothing at all. It failed the way it is
designed to, quietly and correctly, and the counters said so (`degraded` rising,
`chunk_hits` flat) — but nothing in the configuration would have hinted at it.

If your reads are degrading and you cannot see why, measure the round trip to
the tier first. Raising `tier.budget.ms` past your RTT will make the cache work
again, at the cost of a slow tier no longer degrading promptly — which is a
real trade, not a fix.

---

## What the tier must implement

The client speaks **six commands**, and nothing else:

    GET   SET   SETEX   MGET   MSET   DEL

`SET` is used with `NX` and `PX`. That is the whole surface — any
Redis-protocol server providing those six can be the tier, which is what makes
"point it at the Valkay or Redis you already run" true rather than aspirational.

**Flint provides all six**, and the gate runs against Flint and against a
Redis-protocol server on every push, in both engine modes.

**What Flint does NOT provide, and why it does not matter here:** `INFO`,
`KEYS`, `CONFIG` and `SHUTDOWN` are unimplemented. The client never issues
them; only test harnesses did, and they now use `SCAN` and client-side counters
instead. If you are writing your own tooling against a Flint tier, do not
assume those exist.

**One inference to avoid**, because this README's author drew it and was wrong.
`flint-proxy`'s `route_key` carries a `NO_KEY` list containing `INFO`, and it
is tempting to read that as "the proxy handles INFO itself". It does not.
`NO_KEY` answers only *can a routing slot be derived from argument 1*; a
keyless command is FORWARDED to pair 0's master, which returns the same
`unknown command` error. `INFO` therefore fails identically through the proxy
and against a bare seat. (Established by the Flint core session by probing,
after this author inferred otherwise from the list.)

### What the client stores, and what an operator may drop

Two key namespaces, and **every key in both is safe to delete at any time** —
one key, a prefix, or the entire tier, including while reads are in flight.

| Key | Written with | TTL | Holds |
|---|---|---|---|
| `c2/{<etag>}/<n>` | `SET` / `MSET` | none | chunk *n* of the object with that ETag |
| `m1/s3://<bucket>/<key>` | `SETEX` | `fs.s3a.flint.meta.ttl.seconds` | `length\|etag\|kms` — the cached HEAD |

**Why dropping is always safe.** Chunk keys are content-addressed: the ETag is
*in the key*, so a chunk can only ever be read back for the object version it
came from, and a dropped chunk costs one range GET. Metadata is a cached HEAD,
re-derived the same way. Nothing here exists only in the tier — the origin is
authoritative for all of it, which is what "look-aside" means in practice.

**The braces are load-bearing.** `{<etag>}` is a Redis Cluster hash tag: it puts
every chunk of one object in a single slot, so an `MGET` over a run never spans
slots. Do not rewrite these keys to something tidier.

**So eviction is the operator's policy, not the client's business.** The client
is tested against a tier that loses everything mid-read and against one that
loses half of an object's chunks; both refill byte-identically with zero
integrity failures. Set whatever `maxmemory-policy` the workload wants.

**Telling "full" apart from "broken".** A tier that refuses a write because the
namespace is full answers `-QUOTA`, and Flint keeps serving *reads* while it
does — that is a healthy tier in a configuration someone chose, not a fault. So
the client counts it as **`TierFull`** and deliberately not as `TierFailures`,
and it does **not** open the circuit breaker: opening it would throw away a read
cache that is still working perfectly. A steady `TierFull` with `TierFailures`
at zero is the signature of a tier that is out of room rather than unwell.

**Two causes, one counter, different remedies.** `-QUOTA` is the code for both
"this tenant is over its configured cap" and "this host is low on disk", and the
client cannot tell them apart — nor should it, since its behaviour is the same
either way. But yours is not: the first wants capacity or eviction, the second
wants space reclaimed on the box, and only the tier's own metrics and the error
text say which you have. Read `TierFull` as *go and look at the tier*, not as
*the tenant is over quota*. (`tier_full` in the Python client.)

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

## Which engines, and how we know

Every row here is a claim about **someone else's extension point**, so every row
carries a date. A seam does not break when it is withdrawn — it stops existing
in a release we do not run, and nothing in our gate can see that. Re-verify
before quoting.

| engine | how it gets in | verified against | when |
|---|---|---|---|
| Spark | S3A, path 1 or 2 | Spark 4.0.4 / Hadoop 3.4.1, TPC-DS sf=50 on EC2 | 2026-08-28 |
| Iceberg | `io-impl`, path 3 | `iceberg-core` 1.9.0, gated end to end on real tables | 2026-08-28 |
| Hadoop, anything on `s3a://` | S3A, path 1 or 2 | `hadoop-aws` 3.4.3, 45 contract tests we did not write | 2026-08-28 |
| PyTorch, pandas, Ray | fsspec registry | gated python + 90-test fsspec abstract suite | 2026-08-28 |
| **Trino** | **nothing — no seam it will accept** | Trino 481 removed S3A; its Iceberg connector never used `FileIO` | 2026-08-28 |

**Trino is not supported and this file used to imply it was.** Hadoop S3A was
deprecated in Trino 470 and removed in **Trino 481 (11 May 2026)**; its Iceberg
connector uses Trino's own file system layer rather than Iceberg's `FileIO`, so
that route never applied either. File systems are bound inside the Trino server
and its plugin SPI has no extension point for one. See ADR-0023 D11.4 for the
retraction and ADR-0032 for what supporting it would take.

## Known limits

- **Spark is measured but not gated.** TPC-DS at sf=50 runs on EC2 by hand
  (`packaging/aws/spark-e2e/`), not in the gate — it needs AWS and a budget. The
  gate exercises Spark's seam, not Spark. Trino is not supported at all; see
  the table above.
- **Real-hardware numbers are a range, and a small sample.** Spark TPC-DS on
  EC2 gives **1.9-2.3x warm** across four runs and **1.26-1.47x** on a cold JVM;
  an LLM-shaped sweep of one 1 GiB shard gives **5-7x** from the second epoch.
  Absolute times moved 24% between runs while the ratios held. One dataset, one
  hardware shape. **Quote the range, not a run** — every single-run figure we
  published was later found to be the top of one.
- **The first real cluster run agrees with the single-box range.** A two-worker
  standalone cluster — driver on its own box, executors on two more — gives
  **1.92x** over five queries, per-query **1.16–2.72x**, with the cached arm
  growing the tier 259 → 20,440 keys and no plain arm adding one. That is a
  single run, so it is a data point and not a range; what makes it worth stating
  is *where* it lands. Every earlier Spark number was taken under `local[*]`,
  where the driver and the executor are one JVM and the config-shipping path
  never runs. Crossing that boundary for the first time moved the figure to the
  bottom of the existing range rather than off it.
- **One tier serves 64 concurrent readers** at 8.0 ms p99 per read, against
  0.29 ms at a single reader, with throughput flat near 9,000 reads/s from four
  readers on (ADR-0023 D17.6). Still an order of magnitude inside the 25-45 ms
  of S3 TTFB the cache removes, so the tier is not what gives out at that
  fan-out. **Sixty-four readers from ONE machine — not 64 machines, and not a
  cluster benchmark.** Reproduce it: `mvn -Pbench package` then `FanoutBench`,
  or `packaging/aws/spark-e2e/measure_fanout.sh` on a pair of boxes.
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
