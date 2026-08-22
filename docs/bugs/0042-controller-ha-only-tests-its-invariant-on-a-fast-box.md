# BUG-0042: `controller_ha` only tests its invariant on a fast box — and the once it did, a second promotion landed

Renumbered from 0041 on 2026-08-22: a peer filed a different BUG-0041
(client error escaping the retry budget) and pushed first. Commit messages
c134191 and 8767399 say 0041 and are left as written — history is not
rewritten to tidy a number.

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

## Second 16-vCPU observation, 2026-08-22: the race engaged and the invariant held

Gate run `20260822T163154Z` on the c7i.4xlarge gate box, the same class of
machine as the original failure:

    real promotions at (0,2): 1 | promotions at higher epoch: 0 | fenced attempts: 2
    PASS: concurrent controllers safe (exactly-once promotion), HA survives losing 2 of 3

**`fenced attempts: 2` is the load-bearing number.** Finding 2 above is that
the drill passes on an ordinary box without testing anything, and `fenced
attempts: 0` is how that announces itself. Here two controllers genuinely
raced, both were fenced, and no higher-epoch promotion landed — so on this run
the property ADR-0004 depends on was exercised and held.

**This does not narrow the product question, and must not be read as closing
it.** The original failure was `fenced: 1 | higher epoch: 1`; this is
`fenced: 2 | higher epoch: 0`. Two 16-vCPU runs disagreeing is what an
intermittent race looks like, and one contrary observation is not evidence of
absence — it establishes only that the second promotion is not deterministic
on fast hardware, which nobody claimed.

What it does establish is that the reworked assertion behaves as designed: it
distinguished a run that exercised the race from one that did not, which is
precisely the distinction the four green laptop runs could not draw.

The open question is unchanged and is a design question, not a flake: **should
a controller that has lost its role propose a promotion at all?** `FLINTPROMOTE`
returns `-FENCED` only when `next <= current`, so a stale-role controller
proposing `current + 1` passes by construction. Answering it needs the failing
shape reproduced, not another passing run.

## Candidate 3 examined, 2026-08-22: the counting was wrong, in the other direction

Candidate 3 above guessed the epoch window `\(0,[3-9]` was "too narrow and a
legitimate later promotion is being counted" — i.e. that it over-counted. It
does not. It under-counts, and it hides something worse.

**The window is a string PREFIX test wearing the shape of a comparison.** It
matches `(0,3)` through `(0,9)`, and also `(0,30)` and `(0,300)`, but NOT
`(0,10)` through `(0,29)` — those begin with 1 or 2. A promotion at epoch 10
was counted by neither `REAL` nor `HIGHER` and simply disappeared. Checked
against synthetic log lines: where the truth is three higher-epoch promotions
(epochs 10, 29, 5), the old counter reported **one**.

**Both counters also pinned `:$P2`, the expected survivor.** A promotion of any
OTHER seat was invisible to both — and that is exactly the case ADR-0004's
bounded-transient allowance excludes: *"two controllers promoting different
survivors"*. The drill could not see the one shape the ADR itself calls
dangerous. That is the worse of the two blind spots, and it is why the fix
adds a third counter rather than widening a regex.

Both are now numeric and seat-aware, and a promotion of another seat FAILS
with no allowance, because every allowance in this drill is about the SAME
survivor being re-promoted at a higher epoch, which is idempotent for data.
Split-brain is not that.

**Candidate 3 is NOT ruled out by this.** The narrowness causes misses, not
false positives, so it cannot explain the original `higher epoch: 1` — a
falsely-counted promotion would have required the opposite defect. What
changed is that the counting is now correct enough for the question to be
asked. The original observation stands unexplained, and candidates 1 and 2
(a controller observing the pair as master-less after the first promotion, and
a fence checked against a stale epoch read) are untouched.

## 2026-08-22 — where the fence actually lives, and why current+1 passes

The design question was "should a stale-role controller promote at all?" Read
the two sides and the asymmetry is sharper than that framing.

**The controller already refuses to promote unfenced.** ADR-0018's ordering is
enforced in `flint-controller/src/main.rs:862-892`: commit `CPFENCE` to the
control plane first, and if it cannot be committed, *"REFUSING to promote
without it. PAGE."* That is a hard stop, on the correct side of the
dependency, and it goes through the CP's 3-seat Raft.

**The seat enforces none of it.** `CPFENCE` appears in `flint-server` exactly
once, in a comment at `main.rs:1504`. Nothing in the promotion path verifies
that a fencing record exists, matches, or is current. `FLINTPROMOTE` is
honoured on one test — is the proposed epoch above mine — so **the ordering is
controller DISCIPLINE, not a seat-enforced invariant.** Every promoter is
trusted to have done the right thing before dialling.

That is why `current + 1` passes by construction. It is not a gap in the
epoch check; the epoch check is doing its job, which is fencing the OLD
master. Nothing anywhere asks the other question: *is the promotion I am about
to perform redundant?*

- `CPFENCE` answers "has someone superseded the old master?"
- Nothing answers "is this survivor already master, making my promotion a
  no-op?"

The second promotion at a higher epoch is exactly the unasked question
arriving as a real event.

### A candidate remedy, and the reason to distrust it

The seat can answer it locally, with state it already holds. `read_only`
(`main.rs:799`) distinguishes a healthy serving master from a self-fenced one.
So:

    FLINTPROMOTE targeting a seat that is ALREADY master and NOT read_only
    is a no-op — acknowledged as already-master, epoch unchanged.

This preserves the recovery path #168/#171 depends on, which re-promotes a
SELF-FENCED master at a higher epoch: that seat is `read_only`, so it takes
the real path and gets its lease deadline reset. It also leaves split-brain
untouched — promoting a DIFFERENT seat is a different case and is now counted
separately by the drill.

**Do not land this yet, and the reason is the point of this whole file.** It
makes ADR-0004's invariant true BY CONSTRUCTION at the seat. It does not
explain why a controller with a stale role view proposed at all, and
candidates 1 and 2 above are still unexamined. A stale-role proposal may be
the visible edge of a coordination defect that matters for reasons unrelated
to whether its effect lands — and silencing the effect would remove the only
evidence anyone has of it.

Making a symptom impossible is a legitimate fix ONLY when the symptom is the
whole harm. Here it is not established that it is. So: the mechanism first,
this second, and if the mechanism turns out to be benign then this is the
right shape of answer.

## 2026-08-22 — candidate 1 has a named code path, and the next firing will say so

Candidate 1 was "a controller observes the pair as master-less after the first
promotion and promotes the same node again". Reading the controller, that is
not a hypothetical race — **there is a deliberate path that does exactly
this**, and it cannot tell the two situations apart.

`flint-controller/src/main.rs:800-841` carries two recovery paths for a pair
with no master-claimer:

- **#168**: *"no master-claimer, but X holds the top epoch with a caught-up
  replica — self-fenced, recovering it"*
- **#171**: the same, falling back to the member last observed holding the
  lineage when no in-sync node can be proven right now

Both were added for real outages, and both are correct for what they were
built for: a master that self-fenced on lease expiry is exactly "nobody claims
master, one node holds the top epoch". They then promote that node at
`max_epoch + 1` (:861).

**A survivor promoted moments ago presents identically.** No master-claimer
yet — the role flip has not landed in this controller's view — and it holds
the top epoch, because the promotion is what gave it that epoch. The predicate
cannot separate "self-fenced, needs recovery" from "just promoted, needs
nothing", and the second reading produces a second effective promotion of the
same survivor at a higher epoch. Which is the observation.

Note the guard that makes this rare rather than constant: the recovery block
sits behind `!converged_ever || last_converged.elapsed() > max_stale`, so it
needs the pair to look unconverged too — which it briefly is after a
promotion, while the replica catches up. That is consistent with a defect that
fires on a fast box and never on a slow one.

### This is a hypothesis with a name, not a confirmation

The failing shape has not been reproduced, and nothing above is measured. What
HAS changed is that the next firing will answer it without needing to be
caught live: `controller_ha_drill.sh` now greps the controller logs for those
two lines whenever `HIGHER > 0` and prints which path was taken.

    recovery paths taken: #168 self-fenced=N | #171 remembered-lineage=N

A hit supports candidate 1 and names the predicate to fix. No hit sends the
next reader to candidate 2 — the fence checked against a stale epoch read —
instead of re-deriving the whole space.

**The discriminator was verified in both directions before landing**, against
synthetic logs with and without the recovery line, because it lives in a
branch that only runs when the bug fires and would otherwise have been code
nobody had ever executed. The first attempt reported NOT SUPPORTED on a log
that contained the line — the test harness was zsh, which does not word-split
`$HA_LOGS`, while the drill runs under bash, which does. Under bash it is
correct both ways.

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
