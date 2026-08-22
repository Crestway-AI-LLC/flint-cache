# BUG-0041: `controller_ha` only tests its invariant on a fast box — and the once it did, a second promotion landed

Status: OPEN, found 2026-08-22 · Severity: medium — the second promotion is of
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
