# BUG-0197: an HNSW vector set with deletions can stop returning anything, and never frees what it deleted (FIXED 2026-10-01)

**Status:** **FIXED 2026-10-01.** Found starting ADR-0049 step 2 (the local
vector file, whose records are addressed by node), by reading
`Hnsw::set` and then measuring. Held by four tests in `flint-vec`'s `hnsw`
module, each failing on the mutant that undoes its part (below). Released in
the next cut after v0.1.0-rc.78.
**Severity:** high in effect, latent in exposure. `flint-vec` runs only in test
harnesses today (the chaos and scale clusters' vector harness), so no tenant
has reached it. The first tenant with an `INDEX hnsw` set whose ids are
deleted, expire, or are overwritten would have: searches that return nothing
for vectors the set holds, and co-processor RAM that grows with every write
while the D4 cap reads as if nothing had grown. Per-vector TTL (ADR-0017 D7,
the code's "ephemeral vector memory") is exactly that workload. Every
release since v0.1.0-rc.52, the first with HNSW (`599f172`), has it. Flat
sets do not.

## What was measured

A unit probe through the `Store` (the `VEC.*` planning layer), on the unfixed
code:

- **Searches went empty.** 100 live vectors, then ten rounds of 100 vectors
  set with `PX 10` and swept. A search for each live vector by itself, k=10,
  returned **nothing, 100 of 100 times**, at the default `ef` and at 256.
- **Nothing was freed.** 1,000 upserts of one id held **1,000 nodes** with
  the namespace's meter at 771 bytes. Under the TTL churn above, the set held
  1,000 nodes with the meter at **0**: every sweep credited its vectors as
  freed.

## Mechanism

**1. An insert ignored deleted nodes.** HNSW keeps a deleted node in the graph
as a tombstone, because other nodes reach their neighbours through it. A
search follows its links but never returns it, which is right for a query.
The insert used the same search. So on a layer whose nodes were mostly
deleted it found no candidates: the new node got no links on that layer, and
the layer below got no place to start, so none there either. A node built
that way at a new top level became the entry point, and every search starts
there. Upper layers hold few nodes, so they turn mostly deleted first.

**2. A deleted node's slot was never reused or dropped.** `del` marked the
node; `set` on an existing id marked the old node and pushed a new one. Only a
restart's rebuild dropped them. The D4 meter charges a write and credits a
delete or a sweep by the vector's size, so it fell back as nodes accumulated.

## Fix

1. **An insert takes deleted nodes as candidates** (`Walk::Insert`). They
   route, and a new node may link to them. A query still never returns one.
2. **A delete frees the slot, and the next insert takes it** (`Hnsw::free`).
   A set's slots are now bounded by the most it ever held at once, plus the
   entry point, which is never reused because every search starts there (it
   is freed when the entry moves). Upserting one id 1,000 times holds two
   slots.
3. **Before a slot is reused, its neighbourhood is relinked** (`unlink`). A
   node that linked to the old occupant would otherwise point at whatever
   moves in, and a node reachable only through it would be lost. Each old
   neighbour that links back re-selects its links from its own and the
   nearest 128 (`REPAIR_POOL`) live nodes within two hops: hnswlib's repair on
   a replace, narrowed.
4. **Two guards for the links the relink does not reach**, from nodes outside
   the old neighbourhood: a walk does not step onto a node that is no longer
   on that layer (the reused slot's new node may be lower), and an insert
   never walks onto its own slot.

### How the relink was chosen

On 5,000 vectors, ten rounds each replacing half of them, recall@10 at `ef` 40
against brute force, beside a fresh build of the same live vectors:

| relink | clustered (fresh 0.999) | uniform (fresh 0.996) | live nodes unreachable |
|---|---:|---:|---:|
| none | 0.991 | 0.992 | 12 / 1 |
| back-linkers, one hop | 0.983 | 0.996 | 0 / 2 |
| every old neighbour, two hops (hnswlib) | 1.000 | 0.998 | 0 / 0 |
| back-linkers, two hops of live nodes, nearest 128 (**chosen**) | 1.000 | 0.998 | 0 / 0 |
| same, nearest 64 | 0.985 | 0.998 | 0 / 0 |
| no reuse at all (fix 1 alone) | 0.971 | 0.996 | 62 / 0, and 29,999 slots |

hnswlib's form re-scores every candidate for every neighbour, about 1,000 of
them each, and was the slowest arm. The chosen form matched it here. Across
six seeds at 3,000 vectors, it was never more than 0.01 below a fresh build in
11 of 12 runs (the twelfth 0.025 below), and without a relink every run fell
0.014-0.064 below. Times on this laptop were too noisy to quote; the bound on the
candidates is the reason for the cap, not a measured speedup.

## Verification

Each test fails on the mutant named:

- `a_search_finds_live_vectors_among_deleted_ones`: an insert that skips
  deleted nodes (fix 1 undone).
- `deleted_slots_are_reused_so_churn_does_not_grow_the_set`: no reuse, and the
  entry point admitted to the free list.
- `recall_after_churn_matches_a_fresh_build`: no relink (0.960 against 1.000).
  It also catches fix 1 undone and the entry point freed.
- `a_reused_slot_never_links_to_itself`: an insert not hidden from its own
  slot. It plants the stale link, which the larger tests never produced.

Dropping the layer guard panics three of them (`index out of bounds` on a
reused slot's missing upper layer).

## What this does not do

- **The meter still counts live vectors, not slots.** RAM held can exceed it
  by the slots deleted and not yet reused, at most the set's peak minus what
  it holds now. Unbounded before; bounded now.
- **The vectors are not shrunk.** A set that grew and then shrank keeps its
  slots (and their memory) until a restart, ready for its next inserts.
- **A link from outside a reused slot's neighbourhood still points at it.** It
  routes like a random long link and is pruned when its owner's list next
  overflows. The recall measurements above include it.
