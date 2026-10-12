# BUG-0255: a new tenant could read a removed tenant's vectors (FIXED 2026-10-12)

**Status:** **FIXED 2026-10-12**. The co-processor forgets a namespace when
its tenant is removed, and again before the namespace is given to a tenant.
An emptied vector set gives back its memory and its file.
**Severity:** high where it applies. The conditions:
- a fleet declares a co-processor (`coproc VEC.`);
- a tenant stores vectors, and is removed;
- another tenant is given the same namespace before the co-processor
  restarts.

That tenant read the removed tenant's vectors and their `META` through
`VEC.GET`, `VEC.SEARCH` and `VEC.INFO`. The playground names a tenant's
namespace after the tenant (`playground-approve`), so approving a removed
name again was the path there. Its co-processor ran for about half an hour on
2026-10-08 and again from 16:20Z on 2026-10-12. At 17:22Z it held one set,
the step 4 measurement tenant's, which was empty, and no vectors.

## What happened

A co-processor keeps each namespace's index in its own memory, loaded from
the namespace's durable rows on first touch and served from memory after
that (ADR-0017 D3). `flintctl tenant remove` deletes the tenant's record and
wipes the namespace on every pair (`FLINTNS` and `FLUSHALL`). Nothing told
the co-processor, so it kept the index:

    tenant A (ns shared):  VEC.SET docs secret 1,2,3 META private-note
    tenant remove A        -> the seat holds 0 keys in ns shared
    tenant add B ... shared
    tenant B:              VEC.GET docs secret -> 1,2,3 private-note

Found while measuring ADR-0050 step 4 on the playground. Deleting all
152,848 vectors and removing the tenant left the co-processor holding a
939 MB vector file and about 80 MB of memory until it restarted. Following
that memory to who could read it turned a leak into this bug.

**The same code kept an emptied set's memory.** Deleting a vector leaves its
slot for the set's next insert (BUG-0197). So a set emptied by `VEC.DEL`, or
by its TTLs, kept every slot and its vector file at their largest. Meanwhile
the meter had credited each delete as freed.

## Fixed

- **`FLINTNSDROP <ns>`** on the co-processor's mesh listener drops the
  namespace's sets, their vector files, its index bytes and its load state,
  and answers how many sets it dropped. The namespace's next touch rebuilds
  from whatever durable rows it has. It is not a family command, so no
  tenant can send it, as with `FLINTINFO`.
- **A drop wins over a rebuild in flight.** A rebuild that began before the
  drop may have read the old tenant's rows. Each namespace has a generation
  that the drop increments, and a rebuild installs rows only if its
  generation is unchanged.
- **`tenant remove`** sends `FLINTNSDROP` to every co-processor the
  inventory declares, after the wipe, and prints what each said.
- **`tenant add` and `tenant add-on-pair`** send it before the tenant is
  made. This covers a removal that never told the co-processor, such as an
  older flintctl's. If a co-processor cannot answer, the add is refused
  ("refusing to add a tenant on namespace …"). A co-processor that is not
  running holds nothing, so a refused connection does not block the add.
- **An emptied set starts again.** It keeps its configuration, and its index
  and vector file are replaced with empty ones, whether `VEC.DEL` or the TTL
  sweep emptied it.

**Compatibility.** A co-processor from before this fix answers
`FLINTNSDROP` with an error. So, against one:
- `tenant remove` reports the failed forget and still removes the tenant;
- `tenant add` refuses until that co-processor is upgraded or restarted.

A restart was always enough, because the index is rebuilt from durable rows.

## What this does not fix

A set with even one vector left keeps its deleted slots, which the meter has
already credited. BUG-0257 covers that.

## Verification

- `dropping_a_namespace_leaves_nothing_of_it`: a namespace's sets, vector
  file and bytes go, the other tenant's set, `META` and bytes stay, and a
  new tenant on the namespace starts empty.
- `an_emptied_set_starts_again`: after 49 of 50 deletes the file keeps its
  slots; after the 50th it is replaced and the set serves again. The TTL
  sweep does the same.
- `flintnsdrop_forgets_a_namespace_and_only_it` and
  `a_rebuild_begun_before_a_drop_installs_nothing`, in the binary.
- **`coproc_family_drill` step 5,** on a flintctl fleet with mesh TLS:
  - `tenant remove` makes the co-processor forget 2 sets and the vector
    file, and a new tenant on the namespace reads nothing of the old one's;
  - after a removal that never told the co-processor, `tenant add` makes
    it forget the set it still held;
  - with a co-processor address that cannot answer, `tenant add` refuses,
    and no tenant is made.
- **Six mutants failed:** five in the store, and the generation check on
  install. A seventh, a `FLINTNSDROP` that answered but dropped nothing,
  failed the drill at its first read: the new tenant got
  `1,2,3,4 private-note`.
