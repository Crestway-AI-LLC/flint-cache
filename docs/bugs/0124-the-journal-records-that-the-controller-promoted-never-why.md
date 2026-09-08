# BUG-0124 — the journal records that the controller promoted, never why

Status: OPEN · Severity: low — nothing is wrong with the promotion; what is
missing is the ability to say afterwards WHICH rule fired, which is what turned
a one-line question into a four-hour investigation
Found: 2026-09-08, characterising the residual RTO tail after BUG-0122
Component: `flint-controller` promotion decision, and the `Detected` event it
emits

## What happened

After BUG-0122 removed the harness's dial from the measured RTO, one kill in
391 still breached the (then) 3 s exit budget, and it looked nothing like the
one before it: `max_connect_ms=0`, and a kill dispatch of 3533 ms against a p50
of 723 ms.

Joining all 391 master kills to the retained fleet journal:

- detection latency from `kill_ms`: **p50 567 ms, p95 641 ms, worst 3452 ms**
- **Pearson r(detection latency, kill dispatch) = 0.984**, n = 391

The five slowest detections are exactly the five slowest dispatches, in the
same order. On the breaching kill the master stopped acking at ~+307 ms and was
not detected until **+3452 ms** — it stopped serving roughly 2.7 s before the
controller acted on it.

## The rule that explains it

`should_promote` has two paths, and they differ by more than an order of
magnitude:

```rust
if slow {
    slow_elapsed.is_some_and(|e| e >= slow_promote)   // wall clock
} else {
    no_master_streak >= confirm                        // ticks
}
```

- A master that **refuses** the connection is a dead process: promote after
  `confirm` ticks. At the fleet's `--poll-ms 100 --confirm 3` that is ~300 ms,
  and it is the path every ordinary kill takes.
- A master whose socket **still accepts** but whose app cannot answer is ALIVE
  but slow — CPU starvation, a compaction burst — and promoting away from it
  just flaps. It is held for `--slow-promote-ms`, **default 4000 ms**.

`socket_open` says so in place: *"a CPU-starved master whose app cannot answer
FLINTINFO still reads as alive here, because the kernel completes the handshake
without the process being scheduled."*

So when a host stalls, all four observations move together — the master stops
serving, the SSH hop carrying the kill hangs, the controller keeps seeing an
open socket, and everything completes at once when the host recovers. That is
exactly the r = 0.984 coupling, and **the product is behaving as designed
throughout**. The patience is the feature.

## The defect: the decision is not recoverable

None of the above can be READ from the evidence a run leaves behind. The
journal emits `Detected` — that the controller decided — and nothing about
what drove it:

- not which path fired (`slow` or refused),
- not `no_master_streak` at the moment it crossed `confirm`,
- not when `slow_elapsed` started counting.

So "the slow path was taken here" remains an **inference**, and a fairly strong
one, from a correlation plus a timeline plus a comment in the source. It is not
a measurement. The controller's own log does not close the gap either: it
records `PROMOTED <addr> at (e,s)` and no inputs.

This is the same shape as BUG-0122 one layer down. There the client could not
see its own connect phase, and two runs were spent arguing about a phase
nothing measured; adding one measure answered it on the first outing. Here the
controller cannot see its own decision rule, and the same thing is happening.

## Fix

Carry the decision's inputs on the `Detected` event: the path taken, the streak
count against `confirm`, and the age of the slow state. Three fields, emitted
where the decision is already being made, and the next soak says outright which
rule fired on every kill instead of leaving it to a correlation.

## What this changed elsewhere

M2's exit asked for RTO ≤ 3 s in every run while `--slow-promote-ms` defaults to
4000 ms — so the exit was unreachable whenever a host stalled, and was failing
on the patience setting rather than on failover. The published SLO (`slo.md`,
10 s) was never in conflict; only the milestone's exit was. **Resolved
2026-09-08 by moving M2's exit to 10 s** (Jeff's call), which is what `slo.md`
publishes and what the chaos drill has always judged against.

That resolution belongs to the roadmap, not to this write-up. What stays open
here is the instrumentation: whatever the budget is, a run should be able to say
why the controller promoted when it did.
