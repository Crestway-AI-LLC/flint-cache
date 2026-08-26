# ADR-0023: slot-aligned bulk eviction (PROPOSED, not implemented)

Status: PROPOSED, 2026-08-26. Nothing here is built. The per-key evictor it
extends is on main and measured; this is the path it does not cover.

## The problem the per-key evictor cannot solve

Capacity eviction ships as: the policy marks a key, the compaction filter drops
it as compaction next rewrites the row, and under pressure `compact_ns` forces
that pass. No tombstones, no extra writes.

The cost is proportional to **namespace size, not eviction count**.
`compact_ns` reads and rewrites the SURVIVING rows, so reclaiming a thousand
keys from a 400 GB namespace costs the same I/O as reclaiming four hundred
million. That is why the forced pass is rate-limited and batched.

Batching bounds how often the cost is paid. It does not reduce the cost, and
there is a scale at which paying it at all is wrong: a fleet holding trillions
of chunks cannot rewrite a namespace to reclaim a fraction of it. At that size
the only affordable delete is one that **rewrites nothing**.

## What makes that possible

`delete_file_in_range_cf` (rocksdb 0.24, already a dependency) drops whole SSTs
that lie entirely inside a key range, without rewriting anything. Its cost is
per FILE, not per row or per surviving byte. `delete_range_cf` handles the
remainder with a single range tombstone rather than one per key.

The catch is alignment. Dropping files needs the cold data to occupy a
contiguous KEY RANGE, and Flint's keys are ordered `cf | ns_len | ns |
slot(2B BE) | user_key` — by slot, then user key. Nothing about that ordering
correlates with age or coldness, so a scattered set of cold keys touches most
files and drops none of them.

## Decision: the SLOT is the eviction range

A slot is already the unit of contiguity in this keyspace, and `slot_prefix(cf,
ns, slot)` already builds exactly the bound `delete_file_in_range_cf` needs. It
is also already the unit of migration copy and cleanup, so treating it as the
unit of bulk reclaim adds no new concept.

Structurally it inherits the guard that matters: the namespace is IN the key
prefix, so a range built from `slot_prefix(cf, ns, slot)` cannot reach another
namespace. The safety property the per-key evictor gets from re-deriving the
namespace from each key, this gets from arithmetic.

## The signal, and why the obvious one is wrong

Choosing cold slots needs per-(namespace, slot) coldness.

**`FLINTSLOTHEAT` cannot supply it.** It is `[AtomicU64; 16384]` indexed by
slot and **summed across namespaces** — deliberately, because per-(ns, slot)
counting "would need a concurrent map on the hottest path" (`heat.rs`). Using
it would rank slots by traffic that mostly belongs to namespaces that are not
being evicted, and its errors would be silent: a slot cold in aggregate can be
the hottest slot the cache has.

**The S3-FIFO policy already holds the right data, already scoped correctly.**
It tracks per-key queue position and frequency, exists only for declared-
evictable namespaces, and is fed from the request path. Slot is derivable from
any key it holds. So "the coldest slots of this namespace" is an aggregation
over state that is already there, on the eviction path rather than the hot
path, and costs nothing when nothing is declared evictable.

That is the whole reason this ADR is cheap: the signal does not have to be
built, only summed differently.

## Shape

Per-key and bulk are not alternatives; they answer different pressures.

- **Steady state** — S3-FIFO marks individual cold keys. Best hit rate, since
  it evicts exactly what is cold. Compaction drops them for free.
- **Under real pressure** — aggregate the policy's candidates by slot, take
  slots whose keys are overwhelmingly cold, and drop them with
  `delete_file_in_range_cf` first (free) and `delete_range_cf` for the
  remainder. Coarse, and that is the trade: a slot holds hot keys too, so this
  costs hit rate to buy a reclaim that does not rewrite the namespace.

The threshold between them is a measurement nobody has taken, and it should not
be guessed. `marks_at_last_pass` against `forced_passes` already says what a
per-key pass is costing per row reclaimed; the bulk path becomes worth its hit
rate somewhere below a figure that data will show.

## What is NOT settled

- **The coldness threshold for a slot.** "Overwhelmingly cold" needs a number,
  and a slot that is 90% cold still evicts its 10% hot keys.
- **Interaction with migration.** Slots move between pairs. Dropping a slot
  mid-migration, or dropping one the rebalancer is copying, is unexamined.
- **Replication.** A range delete replicates; a file drop is a local physical
  operation. Whether a replica reaches the same state by the same path, or has
  to be told, is the D10 question in another form and is the likeliest place
  for this to be harder than it looks.
- **Whether the per-key path is enough.** At 96 GB the measured cost of a
  forced pass may be tolerable, in which case this stays proposed. That
  measurement is running; this ADR should not be built before it is read.
