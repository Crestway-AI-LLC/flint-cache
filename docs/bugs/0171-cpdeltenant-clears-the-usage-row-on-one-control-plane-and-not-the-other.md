# BUG-0171 — CPDELTENANT clears the usage row on one control plane and not the other (FIXED 2026-09-20)

**Status:** FIXED 2026-09-20 · Severity: **low and precisely bounded — the map
is node-local and drives no enforcement, but it feeds an actor that acts.**

Found by [BUG-0146](0146-the-control-plane-implements-every-mutating-verb-twice.md)'s
**second pass**, the fine-grained one its write-up said a close would require.

## The defect

`main.rs`'s `CPDELTENANT` removes the deleted tenant's row from the in-memory
`usage` map after the mutation commits. `ha.rs`'s arm proposes `DelTenant` and
stops.

Measured by **writer**, not by reading around the call site:

| file | writers of the usage map |
|---|---|
| `main.rs` | `usage.insert(` at :752, `usage.remove(` at :338 |
| `ha.rs` | `usage.insert(` at :1105 — **and no `remove` anywhere in the file** |

So on a Raft control plane that map only ever grows. Its three readers —
`CPMYSTATUS` (:1123), `CPMYUSAGE` (:1165) and `CPTENANTS` (:1261) — key it by
tenant **name**, so a tenant re-created under a deleted name reads the deleted
one's byte count until the next `CPTENANTUSAGE` report overwrites it.

**Direction: single-node correct, Raft wrong.** That is BUG-0152's direction,
which BUG-0146 says explicitly cannot be learned from BUG-0148 and BUG-0150.

## Why the shared mutation did not cover it

ADR-0032 made both dispatchers apply the same `Mutation`, and
`assert_cp_verbs_agree_across_paths` guards that both serve the same verbs.
Neither reaches this, because **the usage map is not registry state**: it is
per-node, in memory, and no mutation touches it. It is a side effect sitting
beside the mutation, in an arm each dispatcher owns separately.

That is the residual class BUG-0146's first pass explicitly did not test.

## Severity, stated rather than inflated

- The map is **per-node and not replicated**, so this is already node-local
  state; on a three-seat Raft CP each seat's copy differs anyway, depending on
  which seat the proxy reported to.
- `over_quota` is a tenant **flag** rendered in replies, not computed from
  usage, so **nothing in the control plane enforces on this number**.
- But `CPTENANTS` is described at `main.rs:921` as *"the agent's sweep
  input"*, so a leaked or stale row does reach something that acts on it.

## The fix, and the guard

`ha.rs` performs the same removal after a successful propose, matching the
single-node arm's ordering (clear after the change is durable, not before).

The guard asserts **the invariant, not the instance**. Checking that `ha.rs`
calls `usage.remove` would pin the one call this bug fixed and say nothing
about the next map, so
`both_dispatchers_mutate_the_usage_map_the_same_way` compares the **set** of
operations each dispatcher performs on it. Comments are stripped before
matching, because a `usage.remove(` in prose would make the sets agree on the
strength of an explanation — which is exactly how BUG-0169's count guard first
passed a mutant.

Two mutations, both killed by the assertion that claims them: the original
defect (`main=["insert","remove"] ha=["insert"]`), and patterns that match
nothing, which fires the control rather than certifying both files by reading
neither.

## What is still not covered

The guard is one map. Any other non-registry state an arm touches — the lease
mirror, the controller registry, the journal — has the same exposure and no
equivalent check. Naming them here rather than guarding them: a check per map
written speculatively is a maintenance cost against a fault nobody has
measured, and the method that found this one is repeatable.
