# BUG-0030: write_deadline's positive control was calibrated to one machine (FIXED)

Status: FIXED 2026-08-19 · Severity: medium — it held `main` red in CI with a
message that reads as a write-path defect

## Symptom

`gate` on `main` at `53d2380`, red, one step:

    GATES FAILED: write_deadline
      | == arm A (negative control): ordinary load at the default deadline
      |   ok=3200 throttled=0 other=0 refused-but-present=0 accepted-but-absent=0
      |   writes_shed_deadline: 0 (must be 0)
      | == arm B (positive control): the same load against a 1ms deadline
      |   ok=3200 throttled=0 other=0 refused-but-present=0 accepted-but-absent=0
      | FAIL: nothing was refused even at a 1ms deadline

The same drill passes locally in 2 s, on every run, on both feature configs.

Read as written, "nothing was refused even at a 1ms deadline" says the write
deadline is not wired into the admit path — a shipped knob doing nothing. **It
is wired.** The drill had not created the condition it was asserting about.

## The measurement that settles it

The drill's own failure branch prints the discriminator, and the CI log kept it
(the inline dump from BUG-0021):

    write_inflight:0
    write_service_us:33
    write_wait_est_ms:0

A write services in **33 µs**. The shed fires when in-flight work × service time
exceeds the deadline, so arming a 1 ms deadline needs **more than ~30 writes
genuinely in flight at once**. The drill drove a fixed **32** threads, each
holding at most one outstanding write.

**32 is one or two above the threshold.** A local sweep at the same 1 ms
deadline, varying only client concurrency:

| threads | ok | throttled | service_us |
|---|---|---|---|
| 1 | 100 | 0 | 20 |
| 2 | 200 | 0 | 15 |
| 4 | 400 | 0 | 36 |
| 8 | 800 | 0 | 15 |
| 16 | 1591 | 9 | 34 |
| **32** | 3001 | **199** | 62 |
| 64 | 75 | 6325 | 2525 |

Shedding begins at 16 and is marginal at 32, so the drill sits astride the
arming threshold rather than inside the armed region.

### It is marginal, not deterministic — an earlier revision of this file overstated it

That revision said a small CI runner "never arms". **That is wrong, and the
correction is the more useful finding.** Measured across the last 30 `gate`
runs: 6 red, and `write_deadline` was the failing step in exactly **one** of
them.

| | |
|---|---|
| gate runs sampled | 30 |
| runs where `write_deadline` FAILED | **1** (`53d2380`) |
| measured failure rate | **~3%** |

The very next CI run, `2621aff`, passed the step. Locally the same fixed rung
produced **12**, **199**, and **3059** refusals on three runs of the same
machine — three orders of magnitude apart, one of them 0.4% of the load.

So arming at 32 threads is close to a coin flip whose bias depends on the
machine and the moment. **That is worse than a deterministic break, not
better:** a positive control that fails outright gets fixed on day one, while
one that arms 97% of the time looks healthy, and the 3% arrives looking like an
intermittent product defect in the write path. This is the same reasoning
BUG-0014 records about base rates — a green streak is not evidence a marginal
condition has been fixed.

A fixed rung is fragile whichever value is chosen, which is the argument
against choosing one.

## Why this is the day's pattern again

The drill's own header says it:

> A gate nobody has watched fire is a gate nobody has tested. So this drill's
> first job is to MAKE it fire, by shrinking the deadline until the machine
> cannot meet it.

It shrinks the *deadline* to a fixed 1 ms and holds the *load* fixed at 32
threads — and the load is the half that decides whether the condition exists.
The drill was written to avoid exactly this failure and then encoded a guess
about the machine in the other variable.

**"Could not create the condition" and "the product did not do the thing" are
different results, and only the second is about the product.** The drill scored
them the same.

## Fix

**1. Ramp the load until the control arms.** `for T in 32 64 128 256`, stopping
at the first rung that sheds, and reporting which rung it was. Failure to arm
at any rung now says so explicitly — that it is a failure to create the
condition, that nothing about it indicts the write path, and that the ladder
may simply be too short for the box.

**2. A new isolating control (arm C), because the fix introduced a second
defect.** Ramping means arm B varies *two* things against arm A — the deadline
AND the concurrency — so a refusal at 1 ms/N threads no longer isolates the
deadline as its cause. A drill that escalates load until *something* sheds will
eventually shed on load alone and call that a pass. Arm C re-runs the **winning
load** at the shipped 2000 ms default and requires zero shed, restoring the
single-variable comparison: same load, two deadlines, opposite outcomes.

Verified: arm C measured 3059 shed at 1 ms and 0 shed at 2000 ms under an
identical 32-thread load.

**3. The escalation branch was tested by forcing it.** After the fix the drill
armed on its first rung, so the loop that escalates never executed — an
untested path added to fix an untested condition. Temporarily starting the
ladder at `1 2 4` made it walk: `threads=1 -> throttled=0`, `threads=2 ->
throttled=57`, armed at 2, and arm C correctly re-ran rung 2.

## Related

- BUG-0021 — the inline failure dump that preserved `write_service_us` in CI;
  without it this would have been "fails in CI, passes locally" with nothing to
  reason from
- BUG-0013 — the other measurement this week whose criterion could not
  distinguish "did not happen" from "was never reached"
- BUG-0022 — `write_stall_readable`, the same distinction one layer down
