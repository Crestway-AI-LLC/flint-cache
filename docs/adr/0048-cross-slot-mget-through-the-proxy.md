# ADR-0048: Cross-slot MGET through the proxy

Status: **ACCEPTED 2026-09-25: option B**, as recommended (Jeff). Built in
`flint-proxy` (`split_mget`); see "As built" at the end.

> Numbering: shared across the public and ops repositories, as ADR-0012's note
> explains. 0047 is the ops agent's rate-headroom record.

## Context

`command-support.md` lists cross-slot multi-key commands under "Excluded by
design": colocate with a hash tag or be refused with `CROSSSLOT`. That rule came
from correctness, and it was right. A node asked about a key it does not hold
answers as though the key were absent, so a forwarded cross-slot command gives
a plausible wrong answer (BUG-0053, and BUG-0179 for the three commands it
missed).

But Flint presents itself as ONE Redis server. `INFO` says
`redis_mode:standalone`, `CLUSTER` is unknown, and a client has no way to learn
that it should colocate keys. Frameworks do not add hash tags on their own.

**Measured 2026-09-24, through the proxy on a gate box, with each framework's
default settings:**

| framework call | what it sends | what happened |
|---|---|---|
| Rails 8.1 `RedisCacheStore#read_multi` / `fetch_multi` | `MGET` | refused; the store's error handler swallowed `CROSSSLOT` and returned `{}`, so every multi-read is a miss and recomputes |
| Rails `write_multi` | pipelined `SET`s | works |
| Django 4.2 `RedisCache.set_many` | `MULTI`, `MSET`, `EXPIRE`s, `EXEC` | raised `CROSSSLOT` (from the transaction) |
| Django `delete_many` (measured working on one pair) | multi-key `DEL` | across pairs, answered for one pair only until BUG-0179; now split and correct |

A single-pair fleet is no exception: the rule is per slot, not per pair.

## Options

**A. Keep the rule.** Document per framework that multi-key reads and writes
need colocated keys. Honest, and it leaves Rails' `read_multi` silently
ineffective and Django's `set_many` raising. (Django's `get_many` was not
measured separately; it would need the same drill.)

**B. Split `MGET` in the proxy.** Group the keys by slot, send one `MGET` per
slot (pipelined per pair), and reassemble the reply in the caller's order. The
server keeps its per-command slot check, so nothing about node-side correctness
changes. What the caller loses is the single snapshot: a reader racing an
`MSET` could see some slots before it and some after, as with a Redis Cluster
client that splits the same call. For a cache read that is the right trade.

**C. B, plus split `MSET`.** `MSET` is atomic in Redis, and its callers may rely
on that. A split `MSET` can be half-applied if a pair fails mid-way. Redis
Cluster clients offer this only as an explicit `mset_nonatomic`.

**D. Cross-slot transactions.** Django's `set_many` is a transaction, so B and C
do not fix it. A transaction across pairs needs a distributed commit, which
ADR-0012 ruled out. Not proposed.

## Recommendation

**B.** It fixes the measured failure that is silent (Rails' `read_multi`). It
keeps every node-side guard. And it gives up only the snapshot property of a
read. **Not C**: `MSET`'s atomicity is its contract, and no measured framework
call needs a split `MSET`. **Not D.**

Django's `set_many` stays refused. Its fix is on the application side (a
`KEY_FUNCTION` that colocates, at the price of one hot slot), and
`command-support.md` should say so beside the rule.

## Verification

- A conformance case can't cover this: the oracle is a standalone Valkey with
  no slots. So it is a drill on a two-pair fleet: `MGET` over keys in several
  slots on both pairs answers every value in order; a missing key is nil in its
  own position; a pair down fails the call rather than answering nil for it.
- Rails' `read_multi` in a drill, and Django's `get_many` measured before
  anything is claimed about it.
- The staging exclusion, as for BUG-0179: a split command must never be staged
  whole.

## As built

- `handle` sends an `MGET` whose keys span slots to `split_mget`; keys in one
  slot take the unchanged path. The prefetch pass never stages one
  (`prefetchable`), because staged whole it would be refused.
- One `MGET` per slot, staged on each pair's read-lane connection and flushed
  once per connection, so a read over many slots costs about one round trip
  per pair. Each reply is collected through `forward_collect`, which hands
  MOVED, a dead connection or a failover to `forward`, so the split adds no
  retry logic of its own. A replica-reading tenant's groups go through
  `forward` one at a time, keeping the fall-back to the master.
- `assemble_mget` puts values back by position. A group that answers with an
  error, or with anything but one value per key, is the answer to the whole
  call: nothing is ever filled in as nil (unit tests in `split_tests`).
- `client_compat_drill` gates it on a two-pair fleet: redis-py (order, a
  missing key, a repeated key, 200 keys, inside a pipeline) and node-redis,
  plus **Rails' `RedisCacheStore`** (`read_multi` and `fetch_multi`, with and
  without a namespace, failing if its error handler swallows anything). The
  same drill asserts what did not change: `MSET` and a transaction are still
  refused across slots.
- A pair going down mid-call is covered at the unit level (an error from one
  group fails the call); no drill kills a pair under a split `MGET`.
- Django's `get_many` is not claimed here; it was never measured on its own.
