# Large values

Every other published Flint number is measured at 1 KB, while the value cap
is 512 MB. This records what happens across the range people actually cache
— JSON documents, serialized features, rendered fragments — and, separately,
what large values cost the *small* requests sharing the node.

Reproduce with `tools/large_value_bench.sh`.

## Provisional: laptop, not a decision run

**These numbers are directional and should not be quoted.** They were taken
on a development machine (Apple silicon, shared page cache, other processes
live), where RocksDB is competing with the OS for exactly the resources
under test. The authoritative run belongs on the same i4i.2xlarge the July
figures used, alongside the scale/chaos work.

They are recorded anyway because two of the findings are large enough that
a quieter machine will move the magnitude, not the conclusion.

### Value-size sweep

~2 GB dataset at every size, so the value size is the only variable.
1:9 write:read, 20 s per row after a full load.

| value | keys | throughput | p50 | p99 |
|---|---|---|---|---|
| 1 KB | 2,097,152 | 69,052/s | 0.087 ms | 1.42 ms |
| 64 KB | 32,768 | 13,401/s | 0.311 ms | 9.15 ms |
| 1 MB | 2,048 | 910/s | 5.73 ms | **602 ms** |
| 16 MB | 200 | 35/s | 53.5 ms | **9,503 ms** |

p50 tracks value size about as you would expect — 16 MB of bytes takes time
to move. **The tail does not.** A 16 MB value is roughly 16 ms of bytes over
loopback, so a 9.5-second p99 is three orders of magnitude past transfer
cost, and the gap opens between 64 KB and 1 MB.

That signature is compaction, not I/O width: values live inline in the SSTs,
so every level rewrites the whole payload, and a 1 MB value is rewritten a
megabyte at a time all the way down. The standard remedy is key-value
separation (BlobDB), where values above a threshold are written once to a
blob file and the LSM carries only a pointer. **Adopting it above ~1 MB is
the open question this bench raises**; it is not yet decided, and it wants
the EC2 numbers first.

### Isolation: what large values cost everyone else

The measurement that matters more. Small-key traffic on the same node,
measured quiet, then again with 1 MB reads running alongside.

| | throughput | p99 |
|---|---|---|
| 1 KB quiet (before) | 37,998/s | 1.079 ms |
| 1 KB + 1 MB reads alongside | 29,109/s | 1.423 ms |
| 1 KB quiet (after) | 37,801/s | 1.151 ms |

Baseline drift between the two quiet runs: **6%** — the run is valid (see
methodology). While serving ~950 MB/s of 1 MB values, small-key p99 rose
**1.28×**, 1.115 ms → 1.423 ms, and throughput fell 23%.

That is the interesting claim. On an engine that is single-threaded per
shard, a large value is a head-of-line blocking problem: it occupies the
shard for its whole duration and every small request queued behind it waits
the full transfer. Flint is not single-threaded per shard, so a large value
costs contention for shared CPU and I/O — real, and visible above — rather
than a serialisation stall.

## Methodology, and two ways to get this wrong

Both of these produced confident, wrong numbers before being caught, so they
are written down rather than merely fixed.

**1. Measuring straight after a load.** Writing 2 GB of 1 MB values leaves
RocksDB flushing and compacting long afterwards. A baseline taken then
measures the backlog, and since the contended run happens later — once the
backlog has drained — the contended number comes out *better*. The first
version of this bench duly reported that large values made small keys 3.8×
**faster**. They do not. The fix is a settle period before any measurement.

**2. Writing during the contention run.** Large *writes* in the contending
load leave their own compaction debt, which then degrades the second
baseline: the same confound, moved into the middle of the test. It also
conflates two separate questions — "does a large request block small ones
while it is served?" (head-of-line, what this section measures) versus "does
a burst of large writes stall the write path?" (what the sweep measures).
The contending load is therefore read-only.

The script guards against both by measuring the quiet baseline **twice**,
once either side of the contended run, and refusing to print a ratio when
they disagree by more than 30%. A bench that cannot detect its own bad run
will eventually publish one.

There is no compaction-progress signal to poll — the node exposes no pending
-compaction or level-backlog field — so settling is by time (`SETTLE`,
default 60 s). Exposing that in `FLINTINFO` would let this wait on the
condition instead of the clock, and is worth doing.
