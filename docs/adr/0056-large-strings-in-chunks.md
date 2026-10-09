# ADR-0056: Large strings in chunks, so a bit write costs a bit

Status: **PROPOSED 2026-10-08**, for Jeff. Nothing is built.

## Context

Bitmaps landed on 2026-10-08 (SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP) on
the string's existing storage: one metadata row holding the whole value.
SETRANGE, APPEND and BITFIELD already work that way. A write to one bit
therefore rewrites the string, and a read of one bit reads all of it.

Measured on the RocksDB engine, on a laptop, against Redis 8.2 on the same
machine (one client, random offsets):

| string | Redis SETBIT p50 / p99 | Flint SETBIT p50 / p99 | Flint GETBIT p50 |
|---|---|---|---|
| ≤ 64 KiB | 0.12 / 0.2 ms | 0.04 / 0.12 ms | 0.03 ms |
| 1 MiB | 0.12 / 0.17 ms | 0.39 / 10 ms | 0.10 ms |
| 8 MiB | 0.12 / 0.16 ms | 0.84 / 16 ms | 0.82 ms |
| 64 MiB | 0.12 / 0.13 ms | 5.6 / 85 ms | 5.5 ms |

The latency is the visible half. The hidden half is write volume: each SETBIT
on an 8 MiB bitmap writes 8 MiB to the WAL and memtable. At 1,000 bit writes
a second that is 8 GB/s, so a bitmap tenant would starve every other
tenant's flushes and compactions long before it saw its own p99.

Bitmaps that size are ordinary. A daily-active-users bitmap over 10 million
user ids is 1.25 MiB, and over 100 million it is 12.5 MiB, written once per
active user per day.

## Options

**A: keep one row, and document it (what shipped).** command-support states
the cost and advises splitting a bitmap past about a megabyte across keys.
This costs nothing. It also leaves a trap: the command works, and it is
slow and expensive enough to hurt neighbours only once the bitmap grows.

**B: chunked strings past a threshold.** A string longer than T (for example
64 KiB) is stored as its metadata row (length, TTL, version) plus fixed-size
chunk rows (for example 32 KiB each), keyed like a Bloom filter's blocks
(ADR-0016 D2). SETBIT, GETBIT, SETRANGE, GETRANGE, BITFIELD, and BITCOUNT or
BITPOS over a range touch only their chunks. A whole-value GET reads the
chunks in order. A SET of a large value writes chunks. A shrink below T
rewrites the value inline.

- **Cost of B.** The rows a string can have are new, so it touches:
  - every string path in `strings.rs`;
  - GC, since an overwritten or deleted chunked string leaves chunk rows,
    versioned like a collection's;
  - slot migration and backup, which move every row of a key and must be
    shown to;
  - FLINTKEYSIZE;
  - eviction;
  - the watch-table invariant. Every write must still write the metadata
    row, which a chunk write does by bumping its length or stamp.

  Two to three weeks with its drills.
- **The rollout is the constraint.** A release that writes chunked strings
  makes a rollback to the release before it unable to read them. Every
  release must roll back (Jeff, 9/23). So B ships in two steps, as ADR-0032
  did:
  1. release R+1 reads chunked strings and never writes them;
  2. R+2 writes them, once R+1 is the oldest release anywhere.

**C: a bitmap-only type.** Rejected. Redis bitmaps are strings: TYPE answers
`string`, and GET, SETRANGE and APPEND work on them. A separate type would
change what clients see.

## Recommendation

**B, sequenced after pub/sub and streams** (Jeff's order, 10/8), with
T = 64 KiB and 32 KiB chunks, chosen because the measurement is flat up to
64 KiB. In the meantime, A stands as documented. If a tenant's bitmap
passes about 1 MiB with frequent writes before then, that brings B forward.

## What this does not decide

The chunk size's tuning, and any move of large JSON documents or other
payload-in-row types to chunks. Those share the problem but not the urgency.
