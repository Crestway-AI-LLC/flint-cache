# BUG-0041: `controller_ha` only tests its invariant on a fast box — and the once it did, a second promotion landed

Status: drill half FIXED 2026-08-22; the product question OPEN · found 2026-08-22 · Severity: medium — the second promotion is of
the SAME survivor at a higher epoch, so it is not split-brain and not acked-write
loss; but it contradicts the invariant ADR-0004's whole argument rests on, and
the check meant to hold that invariant has been passing without exercising it.

## Two findings, and the second explains why nobody had the first

**1. A second real promotion occurred.** On a 16-vCPU Linux gate box:

    real promotions at (0,2): 1 | promotions at higher epoch: 1 | fenced attempts: 1
    FAIL: a second real promotion at a higher epoch occurred

`controller_ha_drill.sh` runs THREE controllers on one pair, kills the master,
and asserts exactly one effective promotion — the survivor at `(0,2)`, every
other attempt `-FENCED`. One attempt WAS fenced, and one still landed at a
higher epoch.

ADR-0004 is explicit that this is the property the no-per-group-Raft decision
depends on: *"any number of controllers may run concurrently — they observe the
same reality, reach the same conclusions, and duplicates are fenced. Proven by
drill: 3 concurrent controllers, exactly one effective promotion."* The ADR does
allow a bounded transient, but only for *"two controllers promoting different
survivors (multi-replica case)"*. This is one pair with one survivor, promoted
twice.

**2. The drill does not exercise the race on an ordinary box.** Four runs on an
8-core laptop, every one green:

    real promotions at (0,2): 1 | promotions at higher epoch: 0 | fenced attempts: 0
    real promotions at (0,2): 1 | promotions at higher epoch: 0 | fenced attempts: 0
    real promotions at (0,2): 1 | promotions at higher epoch: 0 | fenced attempts: 0
    real promotions at (0,2): 1 | promotions at higher epoch: 0 | fenced attempts: 0

**`fenced attempts: 0` means no second controller ever tried.** One controller
reached the promotion first and the other two observed the finished state, so
there was no race to fence and the invariant was never put under load. The drill
reported PASS four times for a property it had not tested even once.

So this is not a flaky drill. It is a drill whose condition is created only when
the machine is fast enough for three controllers to collide — and the first time
that happened, the property failed. The green history is not evidence the fence
works; it is evidence the race rarely starts.

## Why this reads as flakiness and should not

CI has `controller_ha` green on every recent commit, and the local runs above
are green. The natural reading is "one bad run on EC2". The counts refute it:
the failing run is the only one in which the assertion had anything to assert.
A pass with `fenced attempts: 0` and a fail with `fenced attempts: 1` are not
two samples of one distribution.

This is the same shape as BUG-0030 (`write_deadline`'s positive control never
armed on a slow runner) and the `m3_exit` setup that could not fail — a check
that reports on a condition it did not establish. Three in one week.

## Update: the mechanism IS established, and it is by construction

Candidate 3 (the drill's epoch window too narrow) is REFUTED: phase 2 promotes
P3, not P2, and runs after this check, so a `PROMOTED :P2 at (0,3)` in phase 1
is genuinely a second promotion of the same node.

Candidate 2 is the answer, and it is not a bug in the fence — it is the fence's
definition. `flint-controller/src/main.rs:928`:

    // -FENCED here means another controller already promoted at this
    // or a higher epoch: the desired outcome exists, so it's fine.

`FLINTPROMOTE 0 next` refuses only when `next <= current`. So a controller whose
ROLE view is stale (it has not yet observed the winner's master claim) while its
EPOCH read is fresh computes `next = current + 1` and **passes the fence by
construction**. "Duplicates are fenced" was only ever true of equal-or-lower
proposals.

**So the drill asserted something the design does not promise.** ADR-0004 says
the opposite in its own liveness analysis: *"epochs are monotonic, so colliding
controllers cannot cycle — every effective action strictly increases the max
epoch, and all controllers reconverge on the same observed state within a
tick."* And the #168/#171 recovery paths RE-PROMOTE a self-fenced master at a
higher epoch deliberately, so a blanket "never twice" would forbid the recovery
the product depends on.

### What changed in the drill

`HIGHER == 0` could not tell a bounded transient from a livelock. Now:

    fenced attempts: 0    -> NOTE: the race did not start; property NOT exercised
    1-2 re-promotions     -> NOTE: bounded transient, this bug
    > 2                   -> FAIL: controllers are CYCLING, not converging
    still promoting 1.5s later -> FAIL: the reconvergence ADR-0004 promises

A first draft also asserted "exactly one node claims master" — VACUOUS at that
point, since P1 is killed and P3 is not started, so the count can only be 0 or 1.
Replaced with the settle check, which can actually fail. Recording it because
it was an unfalsifiable assertion written inside the fix for a mis-aimed one.

### What is still open, and it is a design question not a defect

Should a controller with a stale role view promote at all? The re-promotion is
data-safe (same node, higher epoch) and self-limiting, but it costs an epoch
bump and a redundant manifest write, and every proxy parked on the old epoch is
fenced until it re-reads. A cheap guard — do not promote a survivor that already
claims master at the top epoch — would remove it, but it must not break #168 and
#171, which depend on re-promoting a master that is alive and self-fenced. That
distinction is the actual work and it needs a reproduction, not a patch.

## What is NOT established

The mechanism. Candidates, in order of cheapness to test:

1. A controller observes the pair as master-less *after* the first promotion —
   the winner's manifest write not yet visible on its poll — and promotes the
   same node again at `epoch+1`. Idempotent for data, wrong for the invariant.
2. The fence is checked against a stale epoch read, so the second attempt passes
   a comparison it should fail.
3. The drill's epoch window `\(0,[3-9]` is too narrow and a legitimate later
   promotion is being counted. Cheapest to rule out first, and it would make
   this a drill bug rather than a product one.

## Where to start

1. Reproduce on a box with >= 16 vCPU; on 8 cores the race did not start in
   four attempts. `packaging/aws/gate-box/run.sh` (ops repo) is the cheapest way.
2. Keep the per-controller logs — the drill greps `$HA_LOGS` for `PROMOTED … at
   (0,N)` and the gate bundle does NOT include them, which is why this report has
   counts and not a timeline. Copy them out before teardown.
3. Make the race START deterministically rather than hoping for it: hold all
   three controllers at a barrier and release them together, or slow the winner's
   manifest write. A drill that only tests concurrency on fast hardware is the
   defect this bug is half about.

## The check that should hold it

`controller_ha` must FAIL, not pass, when `fenced attempts: 0` — because that
number is the drill saying it never created its own precondition. Today that
outcome is indistinguishable from success, and it has been for as long as the
drill has run on ordinary hardware.
