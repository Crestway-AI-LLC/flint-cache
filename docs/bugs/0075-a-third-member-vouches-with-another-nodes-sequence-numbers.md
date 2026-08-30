# BUG-0075 — a third member vouches for a lineage it never witnessed, in another node's sequence space (FIXED 2026-08-29)

**Found** 2026-08-29, deliberately, while answering a design question rather
than chasing a failure: Jeff proposed recommending three members per pair so
that `min-replicas-to-write=1` still has a live replica after a failover
(BUG-0074 — survivors are `members - 2`). `manifest.rs`'s
`PROMO_FENCE_KEY_PREFIX` doc already carried the warning: *"A pair with more
than two members breaks the induction — revisit before that exists."* This is
the revisit. It does break, and the failure is silent acked-data divergence.

## Not a hypothetical: three members are constructible today

`flintctl add-replica` is a shipped command with no member cap, and its own
completion message advertises the use — *"pair N now has an extra replica for
D7 read fan-out"*. So the exposed population is not "someone who hand-edits an
inventory" but anyone who took that documented option. Two-member pairs — the
bootstrap default and every configuration validated to date — are unaffected,
and the two-member arm of the repro below confirms that directly.

## What happens

Three members: A master at epoch (0,1), B and C tailing it.

1. B and C die. A stays up and accepts 60 more writes while widowed — legal at
   the default `min-replicas-to-write=0`, and exactly the RPO envelope
   `widowed-grace-ms` publishes. A snapshots at seq 262 and dies.
2. B returns and is promoted to (0,2). It records a fence row for (0,2)
   holding **202** — its applied cursor, which is a position in *A's* sequence
   space. This is correct and is what makes the two-member case sound.
3. C returns, is re-pointed at B, and tails **incrementally**. Fence rows are
   system rows: they never ride the replication stream. Only a checkpoint
   copies them, and an incremental tail takes no checkpoint. **C therefore
   never receives the (0,2) row.**
4. B dies. C is promoted to (0,3) and records its own fence row holding
   **5406** — its applied cursor in *B's* sequence space.
5. A rejoins and asks C, via `FLINTFENCE`, for the bound on epoch (0,1).

`promo_fence_bound` returns the smallest recorded epoch strictly greater than
`since`. On C the (0,2) row is missing, so the smallest is C's own (0,3) row,
and C answers **5406**. A compares its own snapshot seq, 262, in A's space,
against 5406, in B's space, and the comparison is meaningless. `262 <= 5406`,
so A accepts, rewinds to the snapshot instead of re-seeding, and resumes —
carrying 60 writes the surviving branch never had.

Measured, both arms of `fence3.sh`:

```
ARM 2 (control)  B fence for (0,1): 202
                 rewind: snapshot ...-seq262-e0.1 is past the fence (262 > 202); trying older
                 rewind: no snapshot at or before the fence; full re-seed
                 ORACLE: widow:60 on A = <nil> | on master = <nil>          SAFE

ARM 3            C fence for (0,1): 5406        <- B's sequence space
                 rewind: candidate ...-seq262-e0.1 clears the fence for epoch (0,1) (262 <= 5406)
                 rewound to ...-seq262-e0.1: tailing incrementally instead of a full re-seed
                 ORACLE: widow:60 on A = 'v60' | on master = <nil>          DIVERGED
```

The two-member arm is the control, and it earns its place: it fails the same
fixture in the correct direction, so "SAFE" in arm 3 could not have been a
fixture that simply never reaches the assertion.

## Root cause

The doc comment states the invariant precisely — *"only the row whose epoch
immediately supersedes `since` is in the asker's space"* — and the code never
checks it. `promo_fence_bound` takes a minimum over epochs and returns that
row's seq, trusting that the row it found is the immediate successor. With two
members it always is, because the surviving peer executes every promotion. With
three, a member can be promoted having never witnessed the promotion before it,
and the minimum silently skips to a row denominated in a different node's
sequence space.

Two properties make this worse than a missing check:

- **It fires on the fast path only.** Had C full-synced from B, the checkpoint
  would have carried the (0,2) row and C would have answered 202 — a correct
  refusal. The bug needs the *incremental* rejoin, which is precisely the path
  #187 and BUG-0070 exist to make the common one. Making rejoin cheaper makes
  this more likely, not less.
- **The wrong answer is an ordinary integer.** There is no error, no gap in the
  numbering to notice, and the sequence spaces are similar enough in magnitude
  that a bound from the wrong one looks entirely plausible.

## A red herring, recorded so it is not re-found

While confirming this, C answered `10408` to `FLINTFENCE 0 2` — seemingly a
fence row for an epoch nobody had promoted yet. It is not. `flintfence`
(`flint-server/src/main.rs`, the `since >= mine` branch) answers with the
node's own `latest_seq` when the asker's epoch is at or above the node's own
claim, on the grounds that such an asker claims to already be on this
timeline. C was still a replica claiming (0,1) at that moment, so (0,2) took
that branch. Nothing acts on it here, and it is unrelated to the bug.

## Fix options

1. **Record what the bound is denominated in.** Store the superseded epoch
   alongside the seq, and have `promo_fence_bound` return `Bound` only when
   that field equals `since`; otherwise `Unfenced`. This makes the code check
   the invariant the comment already claims, is local to two functions, and
   degrades to a re-seed — safe, and no worse than the pre-#187 behaviour.
   Rows written by older binaries lack the field; treating an absent field as
   `Unfenced` costs those copies one full re-seed each, once.
2. **Propagate fence rows on promotion.** The new master pushes its fence row
   to surviving replicas, so the chain stays complete. Removes the pessimism of
   option 1 but adds a distributed step to the promotion path, which is the
   most timing-critical path there is, and it must be durable to be worth
   anything — a push that can be lost restores the same hole.
3. **Refuse to build the topology.** Cap members at two until the above lands.
   Honest, and cheap, but it withdraws a shipped capability and does not help
   a fleet that already ran `add-replica`.

Option 1 is the one to take first: it closes the divergence with a local
change, and option 2 becomes a later optimisation that removes its extra
re-seeds rather than a prerequisite.

## Two guards, one premise

The rewind decision is not the only check on this path: the master's
`FLINTSYNC` attach independently refuses a cursor past the fence, and it is
fail-closed — `Unfenced` there means `-WALGAP`, which the replica escalates to
the re-seed. It did not save us, because it calls the same
`promo_fence_bound` and was handed the same wrong number: `262 <= 5406`
satisfies both. Two checks in different layers, reading as defence in depth,
sharing a single premise and therefore failing together.

That is also why one change closes both, and why the fix belongs in
`promo_fence_bound` rather than at either call site.

## Fixed

Option 1. Each fence row now carries the epoch it supersedes alongside the
seq, and `promo_fence_bound` returns a bound only to an asker from that exact
lineage — the code checks the invariant its own comment already asserted.
Rows naming the same lineage are comparable, so the earliest is returned.

Two-member pairs keep the fast path: the surviving peer tailed the dying
master, so the row it writes names that master's epoch and a legitimate rejoin
still matches. That is asserted by a POSITIVE control, not inferred — a
two-member rejoin whose snapshot sits below the fence must still print
`rewound to ... tailing incrementally`, because a fix that merely refused
everything would pass every other check here:

```
ARM 2, snapshot late   262 > 202  -> full re-seed                     (unchanged)
ARM 2, snapshot early  202 <= 202 -> rewound, tailing incrementally   (positive control)
ARM 3                  cannot vouch for epoch (0,1) -> full re-seed   (was: DIVERGED)
```

Rows written by earlier binaries name no lineage and are skipped: a promotion
that straddles the upgrade costs one re-seed, once. Held by
`bug_0075_a_row_that_skipped_a_promotion_vouches_for_nobody` and
`bug_0075_a_pre_upgrade_row_cannot_be_checked_so_it_does_not_vouch`, plus the
rewritten `promo_fence_bound_answers_only_the_lineage_the_row_names`, whose
`(0,3)` case is the unit-level shape of the same gap.

## Not covered

- Whether an epoch gap can also be produced on a **two**-member pair by a
  same-node demote/re-promote cycle. The doc argues no divergence is possible
  there because no other master accepted writes in between; that argument is
  about *divergence*, not about which sequence space the answer is in, and it
  has not been tested.
- The controller's role. This repro promotes by hand; whether the controller's
  ordering makes the gap more or less likely on a real fleet is unmeasured.
- Three-member behaviour under `min-replicas-to-write=1`, which is the
  configuration that motivated the question. This removes the fence
  objection to it; it does not survey what else assumes two members. The
  controller's choice among two eligible replicas and the `members - 2`
  arithmetic of BUG-0074 are both untested at three, so the recommendation
  needs its own verification before it ships.

## Repro

`fence3.sh` (scratch, not committed): both arms, ~30 s, rocks engine, ports
6414-6416. Arm 2 must print SAFE and arm 3 DIVERGED on unfixed code.
