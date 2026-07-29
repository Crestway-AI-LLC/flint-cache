# Large values

Every other published Flint number is measured at 1 KB, while the value cap
is 512 MB. This records what happens across the range people actually cache
— JSON documents, serialized features, rendered fragments — and, separately,
what large values cost the *small* requests sharing the node.

Reproduce with `tools/large_value_bench.sh`.

## Setup

One AWS **i4i.2xlarge** (8 vCPU, 61 GB RAM, 1.7 TB NVMe instance store),
Amazon Linux 2023, rocks engine on the NVMe, memtier_benchmark built from
source. Same instance type as the July latency figures, so these are
comparable to them.

## Value-size sweep

~2 GB dataset at every size, so the value size is the only variable.
1:9 write:read, 20 s per row after a full load.

| value | keys | throughput | p50 | p99 | p99/p50 |
|---|---|---|---|---|---|
| 1 KB | 2,097,152 | 130,677/s | 0.055 ms | 0.255 ms | 4.6× |
| 64 KB | 32,768 | 37,039/s | 0.191 ms | 0.871 ms | 4.6× |
| 1 MB | 2,048 | 2,012/s | 3.63 ms | 13.1 ms | 3.6× |
| 16 MB | 200 | 94/s | 74.8 ms | 367 ms | 4.9× |

**Large values cost what moving the bytes costs, and nothing structural on
top.** p50 tracks size almost exactly — 1 MB in 3.63 ms is ~275 MB/s per
operation, 16 MB in 74.8 ms is ~214 MB/s, and the 16 MB row sustains
~1.5 GB/s in aggregate. The tail stays in a 3.6–4.9× band of the median at
*every* size, which is the signature of ordinary queueing rather than a
cliff.

Practical read: values up to ~64 KB are indistinguishable from small ones
in tail terms. At 1 MB you are paying milliseconds because a megabyte takes
milliseconds. There is no size in this range where the engine stops
behaving.

### A conclusion this run reversed

An earlier pass on a development laptop showed 1 MB at **602 ms** p99 and
16 MB at **9,503 ms** — 46× and 26× worse than the numbers above. That
looked like write amplification: values live inline in the SSTs, so every
compaction level rewrites the whole payload, and the obvious remedy is
key-value separation (BlobDB) above ~1 MB. This document previously said
so, and named adopting BlobDB as the open question.

**It was an artifact of the hardware, not the engine.** On a laptop
RocksDB competes with the OS page cache, Spotlight, and everything else for
a shared consumer SSD; on a dedicated NVMe instance store the same code at
the same sizes shows a flat p99/p50 ratio. The BlobDB question is
withdrawn — there is no evidence for it here. It would be reopened only by
a measurement on real hardware showing the tail diverging from the median
as size grows, which is precisely what this table does not show.

Worth keeping as a caution: a benchmark on the wrong hardware does not
produce noisy numbers, it produces *confidently wrong* ones, and the
conclusion drawn from them looked mechanistic and plausible.

## Isolation: what large values cost everyone else

The measurement that matters more. Small-key traffic on the same node,
measured quiet, then again with 1 MB reads running alongside.

| | throughput | p99 |
|---|---|---|
| 1 KB quiet (before) | 21,423/s | 1.295 ms |
| 1 KB + 1 MB reads alongside | 16,607/s | 1.911 ms |
| 1 KB quiet (after) | 20,629/s | 1.327 ms |

Baseline drift between the two quiet runs: **2%** — the run is valid (see
methodology). While serving ~1,018 large reads/s (roughly 1 GB/s of
payload), small-key p99 rose **1.46×**, 1.311 ms → 1.911 ms, and throughput
fell 22%.

That is the interesting claim. On an engine that is single-threaded per
shard, a large value is a head-of-line blocking problem: it occupies the
shard for its whole duration and every small request queued behind it waits
the full transfer. Flint is not single-threaded per shard, so a large value
costs contention for shared CPU and I/O — real, and visible above — rather
than a serialisation stall.

The 1 KB rows here (~21 K ops/s) are **not** comparable to the sweep's 1 KB
row (~131 K ops/s): this section keeps 2 GB of 1 MB values resident
alongside the small keys, so the working set and cache behaviour differ. The
comparison that means something is quiet-vs-contended within this table.

## Methodology, and three ways to get this wrong

All three produced confident, wrong output before being caught, so they are
written down rather than merely fixed.

**1. Measuring straight after a load.** Writing 2 GB of 1 MB values leaves
RocksDB flushing and compacting long afterwards. A baseline taken then
measures the backlog, and since the contended run happens later — once the
backlog has drained — the contended number comes out *better*. The first
version of this bench duly reported that large values made small keys 3.8×
**faster**. The fix is a settle period (`SETTLE`, 90 s here) before any
measurement.

**2. Writing during the contention run.** Large *writes* in the contending
load leave their own compaction debt, which then degrades the second
baseline: the same confound, moved into the middle of the test. It also
conflates two separate questions — "does a large request block small ones
while it is served?" (head-of-line, what this section measures) versus
"does a burst of large writes stall the write path?" (what the sweep
measures). The contending load is therefore read-only.

**3. Running it on a laptop.** See the reversed conclusion above.

The script guards against the first two by measuring the quiet baseline
**twice**, once either side of the contended run, and refusing to print a
ratio when they disagree by more than 30%. A bench that cannot detect its
own bad run will eventually publish one — and this one did, twice, before
the guard existed.

There is no compaction-progress signal to poll — the node exposes no
pending-compaction or level-backlog field — so settling is by time.
Exposing that in `FLINTINFO` would let this wait on the condition instead
of the clock, and is worth doing.
