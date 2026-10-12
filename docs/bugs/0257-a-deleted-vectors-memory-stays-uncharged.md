# BUG-0257: a deleted vector's memory stays held, and its charge is given back

**Status:** OPEN. Measured 2026-10-12. The fix changes what `VEC.DEL` gives
back to a tenant, so it waits for Jeff's choice (below).
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

**Recommendation: C.** A closes the hole with a small, testable change, and
B is an improvement that can wait for a tenant who needs it.
