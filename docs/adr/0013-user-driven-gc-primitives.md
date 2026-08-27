# 13. User-driven GC primitives: no eviction, but a closable loop

Date: 2026-08-05

## Status

Accepted

## Context

Flint never evicts (README, "What happens when the disk fills"). The disk
guard sheds writes early, reads and the delete path keep serving, and TTL
expiry plus the GC sweeper (#133) actually return the space. That doctrine
holds only if a user CAN close the loop themselves: an external policy
daemon that watches pressure, ranks keys, and deletes. Today the watch
half exists (FLINTINFO disk_* fields, the exporter, FLINTNSBYTES per
namespace) and the act half exists (DEL/UNLINK/EXPIRE served while writes
shed — drill-proven). What is missing is the RANK half: nothing answers
"which keys should go first" at any granularity finer than a namespace.

What the encoding layer already stores, checked against the code rather
than memory:

- **Per-key size is already tracked, O(1).** `ComplexMeta.bytes` is the
  cumulative payload size of a collection, maintained on every mutation so
  the max-value-bytes policy can reject in O(1). Strings and JSON carry
  their payload inline in the metadata row, so size is the row length.
  Exposing size is a read, not new bookkeeping.
- **Creation time is already recoverable for collections.**
  `VersionGen::next(now_ms)` mints versions as `(now_ms << 20) + counter`,
  so `version >> 20` is the unix-ms instant the key's current incarnation
  was created. Hashes, sets, zsets and lists all carry a version; strings
  and JSON do not.
- **Last-write time is not stored for any type.** Versions mint at key
  creation and never bump on updates. But the metadata row is REWRITTEN on
  every mutation anyway — collections update `size`/`bytes`, strings carry
  the payload — so stamping a write time into the header costs zero extra
  I/O, only 8 bytes per key of space.

## Decision

Three primitives, none of which is eviction:

### D1 — `FLINTKEYSIZE key`: per-key physical size

Returns the stored byte size of a key (collection `bytes` + subkey
envelope overhead estimate; payload-in-metadata types report row length).
Pure read of state we already maintain. This is the ranking input for
size-weighted policies ("delete the biggest cold thing").

### D2 — Write time in the header, exposed as `FLINTKEYSTAMP key`

Encoding variant v2 (the flags byte reserves bits 4–5 for exactly this)
appends `written_ms (8B BE)` to the metadata header. Every write already
rewrites the metadata row, so the stamp rides for free. Old v1 rows decode
as before and report the stamp as unknown; a key upgrades lazily on its
next write — no migration, no rewrite pass. The stamp is written by the
master and replicated as row bytes, so master and replica agree by
construction.

`FLINTKEYSTAMP` returns `written_ms` (v2 rows) and, for collections, the
incarnation-created time from the version. Together with D1 this gives
users least-recently-WRITTEN eviction — which is what we honestly track,
and covers the cache-shaped workloads that would otherwise ask for LRU.

### D3 — A pressure edge users can subscribe to

The disk guard's verdict flip (ok→shed and back) becomes a fleet journal
event and an exporter alert rule, not just a pollable FLINTINFO field. A
policy daemon triggers on the edge instead of tight-polling; operators
get the page before the first shed error reaches a client.

## What we are explicitly NOT building

- **Server-side eviction, even opt-in, for now.** Acked writes stay
  durable; anything else breaks the chaos ledger oracle, the published
  RPO, and replication symmetry between pairs with different disks.

  **AMENDED 2026-08-26 — this one was revisited and built.** The condition
  this section set for revisiting was "a concrete design-partner workload",
  and the S3 accelerator is it: a cache of re-derivable chunks whose working
  set exceeds its disk, for which refusing admissions at the watermark freezes
  the resident set at whatever loaded first. Opt-in server-side eviction ships
  as a declared-per-namespace class; **never-evict remains the DEFAULT and the
  product position**, and a namespace nobody declared behaves exactly as this
  ADR describes.

  Of the three objections, one is answered and two are not, and saying which
  is the point of amending rather than deleting:

  - *Replication symmetry between pairs with different disks* — OPEN. Two
    members of a pair evicting independently no longer hold the same data.
    Guarded at the edges today (members of a pair must agree on the declared
    set, checked at start and at every reload, and visible as
    `evictable_ns_agree`), which prevents divergent POLICY but not divergent
    DECISIONS.
  - *The chaos ledger oracle* — NOT ANSWERED, and the objection was correct.
    The oracle asserts no acked write is lost; eviction deletes acked writes,
    so it cannot be run against an evictable namespace and mean anything. No
    chaos drill declares one today, which makes this UNTESTED rather than
    solved. A chaos run over an evictable namespace needs an oracle that knows
    which losses were licensed, and that does not exist.
  - *The published RPO* — unchanged for durable namespaces, and undefined for
    evictable ones. The published figure is about durability of acked writes,
    which an evictable namespace is explicitly opting out of.

  **The oracle question has an answer, and it is not "track which keys were
  evicted".** Recording every evicted key would defeat the memory bound the
  evictor is built around. It is also unnecessary, because the oracle makes
  FOUR assertions and eviction can only reach one of them:

  | assertion | reachable by eviction? |
  |---|---|
  | corruption — the value's CRC fails to parse | **no**: a row is present and correct, or absent |
  | time travel — a seq above the last one written | **no** |
  | cross-key bleed — the value names another key | **no** |
  | loss — an acked seq is below the durability floor | **yes**, and only this one |

  An evicted key reads as ABSENT. A key re-written after eviction carries a
  HIGHER seq, never a lower one. So eviction manifests as absence and nothing
  else, and the fix is not to learn which keys were licensed but which
  PROPERTY is — which is static, per namespace, and known before the run.

  So: for a namespace declared evictable, relax the PRESENCE check and keep
  corruption, time-travel and bleed fully enforced. The oracle keeps three of
  its four teeth against exactly the workload that most needs them.

  And absence is still bounded rather than free. The seat already counts rows
  the compaction filter dropped (`evict:` line, `dropped`), so the harness can
  assert **missing acked keys <= dropped**. That ties every absence to a
  licensed deletion without recording a single key: more absences than
  deletions is a loss the evictor cannot account for, which is precisely the
  bug the oracle exists to catch.

  Not implemented. `Target::refuse_if_evictable` currently refuses outright,
  which is the honest state until the above exists — a relaxed oracle that has
  not been built is not a weaker guarantee, it is no guarantee.

  See ADR-0023 for the bulk path this per-key evictor does not cover.
- **Read-recency (true LRU) tracking.** Every read becomes a write on the
  LSM — the worst possible trade under disk pressure, which is the only
  time the data would matter.
- **A TTL-ordered index.** "Expire the soonest-dying first" can be had by
  scanning with `TTL`; an index is a second write per write to serve a
  policy nobody has asked to run at scale yet.

Revisit all three only on a concrete design-partner workload.

## Consequences

- The no-eviction doctrine gains its missing half: users can build LRW
  (least-recently-written), size-weighted, or namespace-quota policies
  entirely client-side, against primitives that are O(1) reads.
- Encoding v2 is the first use of the variant bits; the decode path gains
  its first real two-variant dispatch, which ADR-0002 planned for.
- 8 bytes per key of metadata growth once keys are touched under v2.
- The replica WAL-apply disk-guard gap (documented in self-hosting.md)
  becomes more visible once D3 alerts exist; it should be closed in the
  same milestone.
