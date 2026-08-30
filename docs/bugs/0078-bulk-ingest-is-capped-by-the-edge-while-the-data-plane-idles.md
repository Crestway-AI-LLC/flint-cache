# BUG-0078 — bulk ingest is capped by the edge while the data plane idles (OPEN)

**Found** 2026-08-30, measured on a loaded 5-host fleet during a 2 TB ingest
run (`i4i.4xlarge`, 1 pair, 1 KB values, load driven through the proxy edge).
The run was asking "how long does 2 TB take"; the answer turned out to be a
statement about the loader and the edge, not about Flint.

## The symptom

The fill sustained **~62 MB/s** against a 150 MB/s cap with all 16 drivers
alive, and nothing anywhere looked busy.

## What is NOT the cause

Each was measured, not reasoned about:

- **Not the load generator.** The orchestrator ran 16 feeders and 16
  `valkey-cli --pipe` at **0.4% CPU each**, box 97.7% idle.
- **Not the seat's CPU, disk, or compaction.** The master reported
  `write_service_us:17`, `write_inflight:0`, `async_write_queue:0`, every
  `writes_shed_*` counter at 0, `l0_files:2`,
  `pending_compaction_bytes:0`, `delayed_write_rate:0`. It used **1.07 of 16
  cores** across 36 threads, busiest single thread 0.22 cores.
- **Not the fsync path or replication.** `seq_lag:925` on a stream running at
  ~14.5K seq/s — under a tenth of a second behind.
- **Not TLS.** The faster path below is the *encrypted* one.

## What it is

The same loader — 8 connections, pipeline depth 256, waiting for replies —
pointed at two destinations while the fill continued underneath:

| destination | per connection | server seq/s added | proxy CPU |
|---|---|---|---|
| master directly (internal mTLS) | **5,030 writes/s** | +40,000 | n/a |
| proxy edge (plaintext + AUTH) | **690 writes/s** | +4,000 | 0.08 cores |

**7.3× fewer writes per connection through the edge, with the proxy using
eight hundredths of a core.** It is not resource-starved; 19 threads, none of
them busy. A 256-command batch takes ~370 ms through the proxy against ~51 ms
direct, so what the edge costs is LATENCY PER BATCH, which a client that
waits for replies pays in full.

The server, meanwhile, went from 14,488 to 54,852 seq/s the moment it was
offered more concurrency — **3.8× more throughput, still at 16% of one box's
CPU.** The data plane was never the limit.

`valkey-cli --pipe` does better than the test client (~3,600 writes/s per
connection through the same proxy) precisely because it never blocks on
replies. That is the shape of the problem: the edge punishes round-trips, so
throughput through it depends on the client never waiting.

## Why it matters beyond a benchmark

- **The published ingest figure would have been wrong.** "2 TB takes ~9.5
  hours" describes this loader through this edge. The pair absorbs
  substantially more.
- **Bulk import is a real customer path.** Anyone loading a dataset through
  the edge inherits this ceiling and will conclude the store is slow.
- **It is invisible from every dashboard we have.** CPU idle, no shed writes,
  no lag, no stalls, no errors. Only a comparison against the direct path
  shows it.

## Not covered

- **Where the latency actually goes inside the proxy.** Candidates: a
  per-connection request/response loop that does not pipeline to the backend,
  a flush per command, or Nagle interacting with small replies. Not yet
  measured, and the fix depends entirely on which.
- **Whether reads share it.** Everything here is writes.
- **The right remedy.** Options range from pipelining backend-side, to
  documenting direct-to-master bulk load, to a dedicated import path. Picking
  one needs the paragraph above answered first.
