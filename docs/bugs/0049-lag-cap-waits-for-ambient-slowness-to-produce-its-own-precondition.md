# BUG-0049: `lag_cap` waits for ambient slowness to produce its own precondition

Status: FIXED 2026-08-26 in `fcc0028` — `--stall-replica-ms 200` added to the
drill's flint-chaos invocation, so the lag is forced rather than awaited.
Verified on the gate box (16 vCPU Linux): `PASS lag_cap (15.6s)`, shedding 70
writes at the 5 ms cap where the old form shed 0 and correctly refused to pass.
· Severity was: LOW as a product matter, MEDIUM as a
gate matter — nothing is wrong with the product, and the drill is behaving
honestly. But it reds main at random, and a gate that fails for reasons
unrelated to the change under test is the thing this repo has spent the week
removing.

## What happened

main red at `21e6747`, `lag_cap`, having passed on the five main runs before
it. The change in that commit was a predicate in a different drill; both merge
parents passed `lag_cap` on their own runs.

    NOTE: nothing was shed — the lag cap never bit, so the mechanism the
    bound RESTS on is unproven by this run (try a smaller --lag-hard-ms)
    FAIL: nothing was shed even at a 5ms cap — the mechanism the published
          RPO bound rests on did not fire, so the bound is unproven.

**The drill is right to fail.** It refuses to report a pass for a mechanism it
did not observe, which is exactly the discipline that makes the rest of this
suite worth reading. The defect is not the assertion, it is that the drill has
no way to guarantee the condition the assertion needs.

## Not misconfiguration — that was checked first

The drill's own failure text points at the clamp: `ReplHub::new` stores
`lag_hard_ms.max(lag_soft_ms)`, so passing only `--lag-hard-ms 5` yields a
500 ms cap and a run that tests nothing. That plumbing is intact.
`flint-chaos` exports `FLINT_CHAOS_LAG_HARD_MS` and `cluster.rs` passes BOTH
caps to every spawn site, deriving soft as `hard - 1`. The run really did use
5 ms hard / 4 ms soft.

So the cap was right and the lag never reached it. From the same log:

    deepest acked-write loss: 0ms before the kill
    NOTE: loss depth 0 means replication kept up throughout

## The actual defect

The drill creates write pressure and HOPES it outruns replication. That is an
ambient property of the machine, not something the run controls. On a
contended runner the burst wins and the cap bites; on a run where replication
keeps up, nothing sheds and the drill correctly reports itself unproven.

Same shape as the four #176 waits fixed this week, one level up: **a proxy
standing in for a property.** There the proxy was PONG for readiness; here it
is ambient slowness for replica lag. Both work until the day they do not, and
neither is under the test's control.

## Fix options, and why none was taken here

1. **Force the lag.** `--stall-replica-ms` already exists and SIGSTOPs the
   replica, which would guarantee lag past any cap. **Rejected for now, and
   this is the interesting one:** the stall runs in the kill window and
   produces acked-write loss depth, which is the BOUNDED-LOSS regime. This
   drill's own header says loss depth "is a separate experiment; see #121".
   Using the stall here would silently convert an RPO-mechanism drill into a
   loss-depth drill, and both would then be measuring the same thing.

2. **Lower the cap** to 1 ms (soft derives to 1 ms). Smallest change, same
   regime, and at 1 ms nearly any burst outruns replication. Still
   probabilistic, just with much more margin.

3. **Raise the write pressure** — more keys, larger values — until the burst
   reliably wins. Keeps the regime, still ambient, and slows the drill.

(2) is the likely answer, but it is a change to a drill that underwrites a
PUBLISHED RPO bound, and picking it should be deliberate rather than a
side-effect of un-redding main. Left open on purpose.

## For whoever re-runs the gate on this

Re-running is legitimate **because the cause is diagnosed and written down
here** — that is the distinction from the re-run-to-green habit. Record the
occurrence so it is not lost under a green label.

Occurrences: run 32917921084 (main @ 21e6747, 2026-08-26).

**The re-run passed, and that is data rather than relief.** Run 32918818215
(main @ b3a5ef1) went green with no change to the drill, the harness, or the
server — the only commit between them adds this file. Two runs, same code,
opposite results, is the definition of the precondition being outside the
run's control, and it is the clearest confirmation of the diagnosis above that
we are going to get without forcing the lag.

It also bounds the severity: the mechanism is not broken, because when the
condition IS met the drill observes the shed and passes. What is unproven on
any given run is whether the condition was met at all — which the drill,
correctly, refuses to paper over.
