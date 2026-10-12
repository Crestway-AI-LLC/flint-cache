# BUG-0257: a deleted vector's memory stays held, and its charge is given back (FIXED 2026-10-12)

**Status:** **FIXED 2026-10-12, option A** (Jeff: "go with your
recommendations on all three"). A deleted HNSW slot stays charged until its
set's next insert takes it or the set is emptied. Option B, compaction, is
for later, under its own ADR, if tenants need deletes to make room across
sets.
**Severity:** medium. It needs a tenant who:
- fills a set;
- deletes all but a few of its vectors;
- fills another set, and repeats.

Each round leaves the deleted slots in the co-processor's memory while the
meter reads them as freed. Neither the per-namespace cap (ADR-0017 D4) nor
the overall cap (ADR-0050 D2) sees that memory. The co-processor shares its
host with the seats, which is what the overall cap exists to protect.

## What happens

`VEC.DEL` marks the vector's slot deleted. The slot keeps its graph links,
its code, its ID and (with no `--vec-dir`) its full vector until the same
set's next insert takes it (BUG-0197). The meter credits the delete at once.
So deleting frees nothing for any other set, but the meter says it did.

BUG-0255 frees a set's slots when the set is emptied. A set with even one
vector left keeps them all.

## Measured (2026-10-12, laptop)

The binaries were at BUG-0255's fix, with no `--vec-dir`, through the proxy.
Each round made a new `hnsw` `sq8` set, filled it with 20,000 vectors of
256 dimensions, and deleted all but one:

| | co-processor RSS | its meter (`ns_mem_bytes`) |
|---|---|---|
| idle | 1.8 MB | 0 |
| one set filled (20,000 vectors) | 27.1 MB | 33.6 MB |
| after each round of 6 | 23.6, 29.4, 28.0, 61.3, 90.5, 65.3 MB | 1.7 KB rising to 10 KB |

macOS's allocator makes RSS noisy round to round. The meter is not noisy: it
ends at six vectors' worth, while the co-processor holds about 120,000 slots.
Nothing bounds the rounds, because the meter never comes near either cap.

## The choice

**A. Charge slots, not live vectors.** A delete gives its charge back only
when the slot is taken again or the set is emptied.
- The meter is honest, so both caps bound what the co-processor holds.
- Inside one set nothing changes. A delete followed by an insert reuses the
  slot, so churn costs what it did.
- What changes: deleting from one set no longer makes room for another.
  Emptying a set does. The overall cap's refusal, which says "delete
  vectors, or retry later", would say so.
- About a day, with the drill.

**B. Compact a set when deleted slots outnumber live ones.** Rebuild its
index from the live vectors and free the rest.
- Keeps today's promise that deleting makes room anywhere.
- Done inline, it holds the co-processor's one store lock for the whole
  rebuild: about 1.9 million inserts for a set at the 1 GiB cap. Every
  tenant's search waits. So it must build off the lock and replay what
  changed meanwhile, which needs an ADR and several days.

**C. Both:** A now, and B later if tenants need deletes to make room
across sets.

**Chosen: C** (Jeff, 2026-10-12): A now, and B if it is needed. A closes the
hole with a small, testable change, and
B is an improvement that can wait for a tenant who needs it.

## Fixed

- **`Hnsw` keeps what its deleted slots hold.** That is their count (the
  slots less the live vectors, a deleted entry point included) and the id
  and meta bytes they keep. The meter charges a deleted slot what the live
  entry cost: the entry's cost is the vector, the id, the meta and the
  structure, and a deleted slot keeps all four.
- **Every write charges the change in what its set holds:**
  - `VEC.SET` of a new id that takes a deleted slot is charged the
    difference, nothing for an id of the same length;
  - `VEC.DEL` and the TTL sweep free nothing in an HNSW set until it is
    emptied, when BUG-0255 starts it again and it all comes back;
  - a flat set frees an entry when it is deleted, as before.
- **The cap check before a write** credits the deleted slot a new id will
  take. So deleting and writing within one set still works at the cap, and
  a write to another set is refused.
- **The overall cap's refusal** now says "delete vectors from the set being
  written, or retry later".

## Verification

- `a_deleted_slot_stays_charged_until_taken_again`: deleting 49 of 50 leaves
  the meter where it was. 48 new ids of the same length take deleted slots
  and leave it there too. Emptying the set takes it to 0, and the TTL sweep
  charges as `VEC.DEL` does.
  - Why 48 and not 49: a deleted entry point is not taken again until the
    entry moves, so the 49th new id took a new slot. The meter charged that
    slot, which the co-processor really does hold.
- `at_the_cap_a_delete_makes_room_only_in_its_own_set`: at the cap, after
  six deletes in set a, set b is still refused. Five writes to set a fit,
  and at most one more.
- **Five mutants failed:**
  - a delete that credits the slot;
  - no credit for a reused slot at the cap check;
  - an insert that ignores the deleted slots on both sides of its charge;
  - `Hnsw::del` not counting the bytes;
  - a sweep that credits the slots.

  A sixth, which read the deleted slots after the insert on one side only,
  was equivalent to the code and passed.

