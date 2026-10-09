# BUG-0248: a seeking read inside `MULTI` or a script walked the whole collection (FIXED 2026-10-09)

**Status:** **FIXED 2026-10-09**. The write-batching overlay that
transactions and scripts read through seeks the underlying store in both
directions, merging its buffered writes as it goes, as its forward prefix
scan already did. Held by the storage tests
`seeking_scans_merge_the_buffer_as_a_model_does` (300 random stores and
buffers, every scan from a random bound, stopped early at random) and
`a_seek_through_the_overlay_reads_what_it_returns`.
**Severity:** medium for a tenant whose scripts read the top of a large
sorted set or stream, which is what job queues do. Plain commands were not
affected.

## What happened

BUG-0216 made a sorted set's one-element reads cost what they read. It gave
the `Kv` trait a descending scan (`for_each_before`), and gave RocksDB and the
in-memory store a real reverse seek. It left the overlay, `BatchingKv`, on
the trait's default body, which materialises the whole prefix and walks it
backwards. The overlay's forward seek (`for_each_from`) also used the
default, which walks the prefix from its start to the seek point.

`EXEC` and `EVAL` run their commands through that overlay, so each one could
read back its own writes. So inside either, a read that seeks paid for the
whole collection. Measured on a debug build, in-memory store, 200,000
members or entries, median of 20:

| | plain | in `MULTI` | in a script |
| --- | --- | --- | --- |
| `ZREVRANGE k 0 0` | 0.23 ms | 56 ms | 55 ms |
| `ZRANGEBYSCORE k <near the top> +inf LIMIT 0 1` | 0.05 ms | 40 ms | 40 ms |
| `XREVRANGE s + - COUNT 1` | 0.27 ms | 68 ms | 68 ms |
| `XRANGE s <near the top> + COUNT 1` | 0.05 ms | 45 ms | 45 ms |

Found while measuring streams (ADR-0052 stage 4) for queues, whose scripts
read the newest entries of a stream.

## The fix

`BatchingKv` overrides `for_each_from` and `for_each_before`. Each takes the
buffered rows past its bound, sorts them in the scan's direction, and merges
them into the underlying store's own seek. This is the same one-pass merge
`for_each_prefix` does, now shared by all three. The two properties that
scan's comment insists on still hold: the underlying range is never
materialised, and the buffer's lock is released before the first visit.

After, on the same build and data:

| | plain | in `MULTI` | in a script | Valkey 9.1: plain, `MULTI`, script |
| --- | --- | --- | --- | --- |
| `ZREVRANGE k 0 0` | 0.21 ms | 0.25 ms | 0.26 ms | 0.11, 0.34, 0.12 ms |
| `ZRANGEBYSCORE k <near the top> +inf LIMIT 0 1` | 0.04 ms | 0.09 ms | 0.10 ms | 0.12, 0.34, 0.13 ms |
| `XREVRANGE s + - COUNT 1` | 0.25 ms | 0.29 ms | 0.33 ms | 0.12, 0.35, 0.13 ms |
| `XRANGE s <near the top> + COUNT 1` | 0.04 ms | 0.09 ms | 0.12 ms | 0.12, 0.35, 0.13 ms |

`MULTI` is three round trips on both. A second test,
`a_seek_through_the_overlay_reads_what_it_returns`, counts the rows the store
hands out: two one-row seeks through the overlay read at most four of 5,000.
With either override removed, the default bodies read all of them, and the
test fails. That was checked by removing each in turn.
