# BUG-0122 — two promotions in 407 took ~3.1 s, and nothing kept the timeline

Status: OPEN · Severity: medium — it is the sole remaining blocker on M2's
exit, and it is not yet attributable
Found: 2026-09-07, in the soak re-run on the corrected instruments
Component: promotion path (controller / seat), plus a retention gap in
`packaging/aws/chaos-cluster/run.sh` (ops)

## What happened

`soak-20260907T221110Z`: 800 kills, 407 promotions, 12.7 M writes, on
`v0.1.0-rc.69` with `flintctl` and `flint-chaos` from source. Durability was
clean throughout (see below). Two kills breached the M2 exit's 3 s budget on
the **client-observed outage** — the measure that needs no kill stamp
(BUG-0121):

| | iter 453 | iter 651 | run p50 |
|---|---|---|---|
| client stall | **3123 ms** | **3190 ms** | 269 ms |
| kill dispatch | 701 ms | 694 ms | 714 ms |
| `max_hold_ms` | 7 ms | 10 ms | — |
| acked keys regressed | 98 | 1 | — |
| deepest loss | 459 ms | 414 ms | (cap 1000 ms) |
| volume lost / budget | 136 / 5054 | 1 / 5026 | 0 over budget |

## What the numbers already rule out

**It is not the kill dispatch.** 701 ms and 694 ms are both at the run's median
(714 ms). Whatever happened, it happened after the master was dead.

**It is not one slow request.** `max_hold_ms` is 7 ms and 10 ms — no single
write waited more than 10 ms for an answer. So the client was being REFUSED
promptly, over and over, for 3.1 s. That is a fleet with no writable master,
not a hung write path, and the two want opposite fixes (#186 makes the same
distinction).

**It is not a connect timeout.** `flint_tls::connect` uses
`TcpStream::connect_timeout(.., 3 s)` and the arithmetic is tempting — but a
3 s connect would appear as a 3 s HOLD, and the holds are single-digit
milliseconds. Ruled out by the same number that rules out the previous one.

**It is not a CP leader election.** The harness's own comment records that one
"leaves the lease path dark ~2.4 s", which with ~300-430 ms of detection sums
almost exactly to what was observed. The inventory this run used says
`cp 172.31.69.141:7500` — **one seat**, `CP_SEATS=1`. With a single seat there
is no election. The arithmetic matching was a coincidence and would have made
a convincing wrong answer.

## What is left, and why it cannot be settled from this run

The remaining span is detect → fence → promote → the writer finding the new
master. Attributing 3.1 s to one of those legs needs the fleet journal's
`at_ms` events, and **this run did not keep them**.

That is not an oversight in the moment; it is a seam between two layers.
`chaos-cluster/run.sh` dumps controller and seat logs when the chaos harness
exits non-zero — and chaos judges against ITS budget, `--rto-budget-ms`, which
is 10 s. The 3 s figure is the M2 EXIT budget, applied one layer up by the
failover soak. So the run passed where the evidence lives and failed where it
does not, the box was torn down on the PASS path, and the artifact went with
it. `flint-chaos` prints `kill_ms`, `dead_us` and `recovered_at_ms` on every
kill FOR this join, and says so in place: "a breach is only actionable if it
can be JOINED to the fleet journal's at_ms". The intent was there; the
retention was not.

**Fixed in ops**: the fleet journal is now pulled on every chaos run, pass or
fail, and the soak names the breaching kills with their windows so the join
needs no re-derivation. The next occurrence is attributable.

## Why the run is still worth its cost

Durability was measured properly for the first time and it held on all 407
kills: deepest acked-write loss 459 ms against a 1000 ms cap, `boundary ties`
0, `regressions no depth measure could judge` 0 with 0 post-death surplus acks
— so BUG-0120's unmeasured residue is **zero** here — and the RPO volume bound
was asserted on every kill with 0 over budget and 0 unjudged. Corruption,
time-travel and cross-key all 0; final walk 300 present, 0 missing.

So M2 is one question from its exit, and the question is narrow: what consumes
~3.1 s in roughly 0.5% of promotions when the median is 269 ms.

## Where to start

1. Re-run the soak on the current ops tree; the journal now comes home. At 2
   in 407 a full 800-kill run should catch one or two.
2. Join the breach window (`kill_ms`-`recovered_at_ms`, printed in the FAIL
   note) against the journal's `Detected` / `PromoteIssued` / `Promoted`
   events and attribute the span to a leg.
3. Only then look for a constant. Do not start from the constant: two of the
   three candidates above were killed by evidence that was already in hand,
   and the third matched the arithmetic while being structurally impossible.
