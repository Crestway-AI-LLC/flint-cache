# BUG-0152 — two tenant flags were written nowhere and loaded as false

**Status:** **FIXED 2026-09-15** — found while starting ADR-0032, reproduced
before it was fixed.

**Not a production fault.** Production control planes are three-node Raft
(Jeff, 2026-09-15), and that path persists through serde, which carries all
twelve `Tenant` fields. This is the single-node control plane: drills and
development. It is recorded at that severity deliberately — the first draft of
this file conditioned on "if a production fleet runs one CP process", which was
a premise nobody had confirmed and which turned out to be wrong.

## What it was

`state.rs` persists through a hand-written line format whose tenant record
carries **ten** fields. `Tenant` has **twelve**. The loader filled the gap:

```rust
// Legacy line-format state predates the flag; JSON state carries it (serde).
federated: false,
async_writes: false,
```

The comment is true about the Raft path's serde format and about nothing this
path ever writes — `commit()` calls `serialize()` and `load_or_new` parses only
the line format. So `CPTENANTASYNC <name> on` and `CPTENANTFEDERATE <name> on`
held until the control plane restarted and then silently reverted, with the CP
pushing the reverted configuration out to the proxies.

Measured, before the fix:

```
before restart:  async_writes=1 federated=1 replica_reads=1 local_cache=0
persisted line:  tenant acme <digest> acme - - 1 0 0 0 0
after restart:   async_writes=0 federated=0
```

`replica_reads` survived, which is what says this is two missing fields rather
than a restart losing configuration wholesale.

## Why nothing caught it

**The round-trip test's fixture was made entirely of defaults.**
`state_roundtrips_through_disk` sets every flag to `false`, so it writes a
record whose every field is what the loader would invent anyway — and a field
the writer never emits loads as its default and compares equal. The test was
correct, ran on every gate, and could not fail for this.

That is the third sibling of the checks-that-cannot-fail family, arriving
through the FIXTURE rather than the assertion: suspect the fixture when a
mutation survives.

Nothing else covered it either, because **persistence is not a property of a
verb**. It is a property of a verb AND a restart, and only the pair can be
tested. Every drill that exercises these flags reads them back inside the same
process.

## The direction is reversed, and that is the point

BUG-0148 and BUG-0150 were the single-node path being right and the Raft path
being wrong. This one is the Raft path being right — serde carries every field
it is given — and the single-node path being wrong. Two implementations do not
drift in a direction; they drift.

That is ADR-0032's case, and this bug was found in its first hour of work.

## The fix

Two fields appended to the tenant line, and appended is the whole
compatibility story: a file written before this ends where it always did and
both default to `false`, which is exactly what it meant; a file written after is
read by an older binary that simply stops asking. No migration in either
direction.

`tools/cp_restart_tenant_flags_drill.sh` sets three flags on and leaves a
fourth deliberately off — because every assertion is "this flag is still 1",
and an instrument reporting 1 for everything would satisfy all of them. Red
before the fix, green after.

A second unit test round-trips a tenant whose **every field differs from its
default**, written as a whole-struct comparison rather than a list of asserts,
so a field added to `Tenant` later is covered without anyone remembering to
extend it. Reverting the writer to ten fields fails it and prints both structs.
