# BUG-0013: bulk writes stall because compaction is left at RocksDB defaults (OPEN)

Status: OPEN, found 2026-08-18 · Severity: medium-high — it does not corrupt
anything, it makes large ingests take multiples of the time they should, and
it silently shaped a published benchmark number

## Symptom

Filling 100 M x 1 KB keys into a **fresh** engine took ~38 minutes. Re-filling
the **same** keyspace over the resulting ~120 GB LSM, on the same hardware and
build, was still running **85 minutes later** when the test fleet's TTL
terminated it.

Steady-state writes are fine: beyond RAM, SET p50 moved only 0.351 -> 0.375 ms
(+7%) against a RAM-resident dataset. So this is not "writes are slow" — it is
bulk writes specifically.

## The wrong conclusion to draw

That the disk is the limit, or that beyond-RAM writes are inherently
expensive. The +7% steady-state number rules both out: the same engine, the
same disk and the same dataset size handle a sustained write rate without
trouble. What changes under a bulk load is that RocksDB stops accepting
writes as fast as they arrive.

## Root cause (hypothesised, NOT yet confirmed — see below)

`crates/flint-storage/src/rocks.rs` configures **nothing** about compaction.
The whole of it:

    opts.create_if_missing(true);
    opts.set_wal_ttl_seconds(...);  opts.set_wal_size_limit_mb(...);
    opts.set_compaction_filter("flint-meta-expiry", ...);
    opts.set_block_based_table_factory(&table_options());

No `max_background_jobs`, no `write_buffer_size` / `max_write_buffer_number`,
no `level0_slowdown_writes_trigger` / `level0_stop_writes_trigger`, no rate
limiter, no `soft`/`hard_pending_compaction_bytes_limit`.

So RocksDB's defaults apply, and the binding one is **`max_background_jobs =
2`** — two threads for every flush and compaction, on an 8 vCPU box with
NVMe. Under a bulk load L0 accumulates faster than two threads can drain it,
RocksDB applies its write stall, and the ingest rate collapses to whatever
compaction can sustain.

Durability is not what is waiting. A write is durable at the WAL, so the ack
never depends on compaction; it depends on RocksDB's **back-pressure**, which
exists to stop L0 growing without bound and destroying read latency.
Compaction speed is therefore a dial between write throughput and read
amplification — and it is currently set to a conservative default rather than
to the hardware.

## Confirm before tuning

`INFO` already exports **`write_stopped`** and **`delayed_write_rate`** and
nobody has ever read them. Step one is a bulk fill with both sampled, plus
`rocksdb.num-files-at-level0` and the pending-compaction property.

**If `write_stopped` is zero the hypothesis above is wrong** and the cause is
elsewhere — disk throughput, the WAL fsync cadence, or the proxy. Tuning an
LSM from reasoning instead of from its own stall counters is how a write
problem becomes a read problem.

## Then

Raise `max_background_jobs` toward the core count, size the write buffers for
the box, and consider a rate limiter so compaction IO is smoothed rather than
starving foreground reads. Every value justified against a measured stall.

**Re-measure read latency afterwards, and treat that as part of the fix, not
a follow-up.** Turning back-pressure down spends read latency to buy write
throughput; the beyond-RAM GET numbers in
`docs/bench/2026-08-18-beyond-ram-current-build.md` (private repo) are what
must not regress.

Make the knobs configurable as `open_with_retention` already does for WAL
retention, so a co-located marketplace VM and a dedicated node can differ.

## Why it matters beyond ingest time

The last published write number, `51,191 ops/s` for a 32-minute pipelined
ingest (rc.6, July), was measured under exactly this stall. It has since been
removed from the public site, but it was a headline figure for a month. A
number produced by an untuned default is not a property of the product.
