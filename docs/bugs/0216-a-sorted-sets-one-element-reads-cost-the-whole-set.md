# BUG-0216: a sorted set's one-element reads cost the whole set (FIXED 2026-10-07)

**Status:** **FIXED 2026-10-07**. Held by the storage tests
`one_element_reads_do_not_read_the_whole_set` (a counting store: each read
may touch 12 rows of a 5,000-member set, and touched all 5,000 before),
`seeking_reads_agree_with_a_whole_set_model` (300 random sets against the
whole-set reads they replaced) and `rocks_walks_backwards_as_memory_does`.
**Severity:** high for a tenant with a large sorted set. A leaderboard's
ZRANK, a scheduler's `ZRANGEBYSCORE … LIMIT 0 1` and a queue's ZPOPMIN each
cost time in proportion to the whole set, about 1,400 times Redis's at a
million members on RocksDB, and allocated the whole set outside BUG-0060's admission.

## Why

Measured 2026-10-07 on release builds on the laptop, at 1M members of 14
bytes, against Redis 8.2.8 on the same machine. RocksDB is the engine
production runs:

| | RocksDB before | RocksDB after | in-memory before | in-memory after | Redis |
|---|---|---|---|---|---|
| `ZRANGE k 0 0` | 172 ms | 0.04 ms | 79 ms | 0.07 ms | 0.12 ms |
| `ZRANGEBYSCORE k -inf +inf LIMIT 0 1` | 174 ms | 0.04 ms | 80 ms | 0.08 ms | 0.12 ms |
| `ZCOUNT k 0 10` | 177 ms | 0.18 ms | 78 ms | 0.08 ms | 0.12 ms |
| ZPOPMIN | 172 ms | 0.23 ms | 80 ms | 0.08 ms | 0.12 ms |
| ZPOPMAX | 172 ms | 0.07 ms | | 0.07 ms | 0.12 ms |
| `ZREVRANGE k 0 9` | 173 ms | 0.05 ms | | 0.09 ms | 0.13 ms |
| ZREVRANK, second from the top | 174 ms | 0.04 ms | | 0.07 ms | 0.11 ms |
| ZRANK of the middle member | 173 ms | 79 ms | 82 ms | 28 ms | 0.11 ms |
| ZSCORE | 0.04 ms | 0.05 ms | 0.03 ms | 0.03 ms | 0.13 ms |

Before, each grew linearly: on RocksDB 1.5 ms at 10k members and 15 ms at
100k. Every one
called `ZSetStore::all_ordered`, which built the whole set as a `Vec`, and
then took what it needed. The rows were already in the order every one of
these reads wants: `prefix || score (8 bytes, order-preserving) || member`.
The `Kv` trait could seek forward (`for_each_from`) but not backward, so
the reverse reads had no way to start at the top.

BUG-0060's admission sized ZRANGE and its siblings by the whole set, which
was right while they built it, and did not size ZRANK, ZCOUNT or ZPOPMIN at
all, so those allocated the set unadmitted.

## The fix

- `Kv::for_each_before`, the descending scan: RocksDB's reverse iterator,
  the in-memory store's reversed range, and a default body that walks the
  materialised prefix backwards, so every other store stays correct (the
  write-batching one, which merges its buffer, takes the default).
- `ZSetStore::walk` reads rows in score order, either way, from a seek, and
  stops when told. Every sorted-set read goes through it: a score range
  seeks to its near bound and stops at its far one or at its LIMIT; a rank
  window is read from whichever end of the set is nearer; a pop reads what
  it pops; ZCOUNT counts without building anything.
- The lex walk keeps Redis's semantics (BUG-0215). When every member shares
  one score, the case Redis defines, it seeks straight to the bound; with
  mixed scores it reads from the end it starts at.
- BUG-0060's admission charges a sorted-set read what it can return: a rank
  window its length, a LIMIT its count, a pop its count, and a score or lex
  range without a LIMIT the whole set.

## What remains

A rank costs its position: ZRANK of the middle of a million members is
28 ms in memory, counted from the end the rank is measured from. Redis's skiplist
answers in O(log n) from span counts kept on every node, and this index
keeps no counts. On RocksDB that is 79 ms. ZRANK of the top or bottom of a set, the leaderboard case,
is as cheap as Redis's. Counts per block of rows would close it; that is a
change to the storage format, and would need its own decision.

The first build of this fix returned an empty array to
`ZRANGE k … BYSCORE LIMIT 0 0` on a key of another type, where Redis
answers WRONGTYPE: the early return for a zero count skipped the type check.
A randomised differential of 64,000 commands against Redis 8.2.8 and Valkey
9.1.0 caught it before it was gated, and a corpus step now holds it.
