# ADR-0056: Large strings in chunks, so a bit write costs a bit

Status: **ACCEPTED 2026-10-09** (Jeff: "go with your recommendation on ADR-0056"): option B, as decided below.

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

## Decision (2026-10-09)

Option B, with T = 64 KiB and 32 KiB chunks.

- **D1. A chunked string is its own value type inside Flint, and a string
  to every client.** It is type 8, whose `TYPE` and `SCAN … TYPE` name is
  `string`. Its metadata row has the collections' layout: header, write
  stamp, version, `size` (the chunks its length spans) and `bytes` (its
  length). Chunk `i` holds bytes `[i·32 KiB, (i+1)·32 KiB)` of the string
  in a subkey row under that version, keyed by `i` as 4 bytes big-endian.
  A missing chunk, or the part of one past the end of its row, reads as
  zeros, as Redis pads a string that SETBIT or SETRANGE grows.

  **Why a type of its own, not an encoding variant of type 0.** A release
  from before this one reads an unknown encoding of type 0 by its header
  length, and would answer a chunked string's metadata as its value: wrong
  bytes, silently. An unknown type it answers as no type at all: `GET`
  says WRONGTYPE and `TYPE` says `none`. Its sweeper still reads the
  version where a collection's is, so it keeps the chunks. A rollback past
  this release then breaks loudly and loses nothing.

- **D2. When a string is chunked.** When writing them is on (D4), any
  write that leaves a string longer than 64 KiB stores it chunked. That is
  `SET` and its relatives with a long value, and the first `APPEND`,
  `SETRANGE`, `SETBIT` or `BITFIELD` that grows a string past 64 KiB, which
  rewrites it once. A chunked string stays chunked until it is replaced by
  a value of 64 KiB or less, or deleted. A replacement mints a new version,
  so the old chunks become orphans for the sweeper, as a deleted
  collection's rows do.

- **D3. What each command reads and writes.**
  - **Bounded by the range:** `SETBIT`, `GETBIT`, `SETRANGE`, `GETRANGE`,
    `BITFIELD`, and `BITCOUNT` or `BITPOS` over a range read and write only
    the chunks their range covers, plus the metadata row.
  - **Cheaper than before:** `APPEND` writes only the tail, and `STRLEN`
    reads only the metadata row.
  - **Whole value:** `GET`, `GETDEL`, `GETEX`, `BITOP`'s sources, and
    `BITCOUNT` or `BITPOS` over the whole string read every chunk, in order.
  - **Errors without reading:** `INCR` and `INCRBYFLOAT` refuse a chunked
    string without reading it. No string that long is a number to Redis
    either.

  Every write rewrites the metadata row (its length, size and stamp), so
  WATCH sees every change (ADR-0012 D5).
- **D4. The rollout: this release reads, the next writes.** This release
  reads chunked strings everywhere. It writes them only on a seat started
  with `--chunked-strings` (inventory `chunked-strings on`), which is off
  by default. The release after it turns writing on, once this one is the
  oldest anywhere. An operator who turns it on sooner gives up rolling
  back below this release while a chunked string exists.
- **D5. The generic machinery needs one change.** `COPY` and `RENAME`
  re-key the chunks as they re-key any collection's rows, and so must
  count type 8 among the collections. Without that, they would copy the
  metadata row alone. Everything else already handles type 8 unchanged:
  - DEL and expiry drop the metadata row and leave orphans;
  - the sweeper keeps rows whose version is live;
  - slot migration and backup move every row of a slot;
  - FLINTKEYSIZE reads `bytes`;
  - eviction deletes through DEL.
- **D6. Not decided here.** Tuning the chunk size, and moving JSON
  documents to chunks.

## Built and measured (2026-10-09)

Built as decided, step one of D4: every seat reads chunked strings, and a
seat writes them only with `--chunked-strings` (inventory
`chunked-strings on`). With writing off, a string already in chunks is
still changed in place, and a `SET` replaces it with one stored whole.

**Latency.** Measured on the RocksDB engine on a laptop (release build, one
client, random offsets, a dense string of random bytes; Redis 8.2.8 on the
same machine). Without chunks is this release with the flag off, so the
cost this ADR removes is measured the same day.

| string | Redis SETBIT p50 / p99 | chunked SETBIT p50 / p99 | chunked GETBIT p50 | without chunks SETBIT p50 / p99 | without chunks GETBIT p50 |
|---|---|---|---|---|---|
| 64 KiB | 0.02 / 0.08 ms | 0.05 / 0.15 ms | 0.02 ms | 0.06 / 0.15 ms | 0.03 ms |
| 1 MiB | 0.02 / 0.04 ms | 0.04 / 0.12 ms | 0.03 ms | 0.21 / 4.0 ms | 0.10 ms |
| 8 MiB | 0.02 / 0.15 ms | 0.04 / 0.12 ms | 0.03 ms | 1.1 / 28 ms | 0.76 ms |
| 64 MiB | 0.03 / 0.20 ms | 0.05 / 0.18 ms | 0.04 ms | 162 / 967 ms | 5.6 ms |

A bit write now costs the same at every size, within 0.03 ms of Redis at
the median.

**Write volume.** Each SETBIT on a chunked string puts one 32 KiB chunk and
the metadata row, whatever the string's length. On the same run, the
seat with chunks took 8,200 SETBITs, which is 256 MiB of chunk writes
beside 73 MiB of values, and its directory held 315 MiB. The seat without
chunks took 5,424, which rewrote 26 GiB of strings through the WAL, and
its directory held 14 GiB.

**Correctness.**
- A randomised differential runs every string, bit and keyspace command
  against a store that chunks past 16 bytes in 8-byte chunks and one that
  never chunks, and compares every reply, then sweeps and compares again
  (`chunked_strings_answer_as_inline_ones_do`).
- Tests pin the rows each command touches: one chunk and the metadata row
  per write, one row read by STRLEN.
- A new conformance case puts strings of 70 KB to 1 MB across chunk edges,
  computing every expected reply from a model of the bytes. It passes on
  Valkey 9.1, and on mem and RocksDB seats with chunks on and off, over
  RESP2 and RESP3. The conformance seats and the proxy and client drills
  now run with chunks on.
- The watch invariant covers SET, SETBIT, SETRANGE and APPEND on a
  chunked string.

**Rollback, checked.** Strings written in chunks by this build, then
opened by the previous release (20b2a7a) on the same data directory:
- `TYPE` answers `none`, and `GET`, `STRLEN` and `GETBIT` answer WRONGTYPE;
- a short string beside them reads as before;
- its sweeper ran (it reclaimed a deleted hash's two rows) and kept every
  chunk;
- this build then read every chunk back intact.

**One further change D5 did not list.** `SCAN … TYPE string` compares type
names rather than type numbers, so it finds chunked strings as well.
