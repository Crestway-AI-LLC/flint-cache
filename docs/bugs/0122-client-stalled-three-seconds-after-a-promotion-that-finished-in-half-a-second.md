# BUG-0122 — the client stalled ~3.1 s after a promotion that finished in ~0.5 s

Status: OPEN — cause ESTABLISHED 2026-09-08; what remains is a decision, not a
fix · Severity: medium — still disqualifying for M2's exit, which requires
RTO <= 3 s in EVERY run. The promotion path is exonerated and the stall is
measured: the harness pays flint-tls's 3 s connect backstop dialling the seat
it has just killed
Found: 2026-09-07, in the soak re-run on the corrected instruments
Component: the DIRECT client discovery path (`flint-chaos` `connect_master` /
`flint_tls::connect`) — originally filed against the promotion path, which the
retained journal has since ruled out. Also a retention gap in
`packaging/aws/chaos-cluster/run.sh` (ops), now fixed

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

**It is not a connect timeout.** ~~`flint_tls::connect` uses
`TcpStream::connect_timeout(.., 3 s)` and the arithmetic is tempting — but a
3 s connect would appear as a 3 s HOLD, and the holds are single-digit
milliseconds.~~ **THIS EXCLUSION WAS UNSOUND — see the correction below.
`max_hold_ms` cannot see a connect at all, so it was never evidence either
way.**

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

## CORRECTED 2026-09-08 — the journal moved the fault, and two of my own claims died

Two soaks now, both with the fleet journal retained (the retention fix above).

### Established: the promotion is not slow, and the remaining time is client-side

Joining each breach against `cp-state.journal`, offsets relative to `kill_ms`:

| offset | iter 10 (stall 3187 ms) | iter 352 (stall 3177 ms) |
|---|---|---|
| `Detected` | +504 ms | +574 ms |
| `PromoteIssued` / `Promoted` | **+511 ms** | **+582 ms** |
| `RejoinStarted` (old master returning) | +1820 ms | +1824 ms |
| `Supervised` | +1952 ms | +1914 ms |

The promotion finished around half a second in; the client did not write
successfully until +3475 / +3457 ms. **Roughly 2.9 s elapsed with a healthy,
writable master already serving.** Detect, fence and promote are all exonerated,
and this bug's original framing pointed at the wrong half of the system.

### Established: the number is a constant, so something is timing out

Five breaches across two independent runs of 407 and 433 master kills:

    3123, 3166, 3177, 3187, 3190 ms   — spread 67 ms on a mean of 3169 (2.1%)

Each with a normal ~700 ms kill dispatch, so none is a dispatch artefact. A value
that repeats within 2% across two runs is a fixed timeout expiring, not a queue
draining or a network varying.

### Corrected: my exclusion of the connect timeout was unsound

`record_hold(shared, sent, answered)` measures a request from SEND to ANSWER. A
blocking `connect` happens **before any request is sent**, so it is structurally
invisible to `max_hold_ms`. The evidence I used to rule the hypothesis out could
not have detected it either way.

Reusable form of the mistake: the number was real and correctly read, and the
inference from it was still unsound. **A measurement's silence is only evidence
once you have shown it was in a position to speak.**

### Refuted: the mechanism I was about to write down

I drafted "the killed seat is restarted immediately, and a restarting seat binds
its listener before it can serve, so the dial hangs for the full 3 s." Checking
it against the code killed it:

- `crates/flint-server/src/main.rs:1862` binds **before** the initial full sync
  and says so — that is what #176 *is*. The listener is open early on purpose.
- It is not a dead listener. `accept_while_loading` (:3126) accepts in a loop and
  **spawns a thread per connection**, and `serve_loading` answers PING and
  FLINTINFO and refuses data commands with `-LOADING`.

So a dial to a restarting seat is accepted and answered, not hung. The mechanism
predicts a hang that the code is specifically built to prevent. Recording it
because it was one edit away from being filed as the cause, and it is wrong.

### Corroborating but not sufficient: the seat correlation

The client dials 7001 first, so only a killed 7001 puts the victim seat in its
path. The completed run's 433 master kills all join 1:1 to a `Detected` and a
`Promoted` event (433/433/433, nothing missed):

| victim | n | stall median | stall p99 | stall max | promote median | over 3 s |
|---|---|---|---|---|---|---|
| 7001 (first dialled) | 217 | 270 ms | **3166 ms** | 3187 ms | 565 ms | **3** |
| 7002 | 216 | 267 ms | **325 ms** | 2920 ms | 556 ms | 0 |

**The bodies of the two distributions are the same and only the tails differ.**
Identical medians rule out "one seat is just slower"; a p99 of 3166 against 325
says the difference is a rare discrete event, not a shift. And every one of the
five breaches ever recorded falls in the first-dialled-victim group.

Stated honestly: 3-vs-0 out of ~217 each is about p≈0.13 alone. It corroborates
a client-side dial cost paid against the seat just killed; the constant is what
carries the argument.

### Open: which 3 s timeout, and the instrument that would say

**Reachability settled 2026-09-08 (BUG-0123).** `connect_within` measured
against TEST-NET-1, which drops SYNs, spends its budget to the millisecond:
251 ms on a 250 ms budget, 2001 ms on 2000 ms, both `TimedOut`. So a dial that
meets an unanswered SYN really does burn the full timeout — the mechanism is
no longer hypothetical. What is still unshown is that this fleet produced that
condition; a blackholed peer and a dead port are different things, and a dead
port answers at once.

`TcpStream::connect_timeout(.., 3 s)` at `crates/flint-tls/src/lib.rs:614` is the
**only 3 s constant anywhere on this path** — the chaos client and cluster code
contain none, and the controller's is 500 ms. But reaching its full value
requires SYNs to go **unanswered**, not refused, and nothing measured so far
shows that happening. The candidate fits the number and lacks a mechanism.

The gap this whole bug is a record of is that **the harness cannot see its own
connect phase**. `record_hold` starts at the send.

**That measure now exists** (`record_connect`, `crates/flint-chaos/src/writer.rs`).
It times `Client::connect_addr` from entry to return **whether the dial succeeds
or fails** — the failure path being the entire point, since a `connect_timeout`
that expires returns `Err` after its full budget and the loop used to discard
that with a bare `continue`. `max_connect_ms`, `max_connect_at_ms` and
`connect_failures` reset per kill and print in the per-iteration line beside
`max_hold_ms`, and the summary says outright when a dial lands near 3 s that
SYNs went unanswered rather than refused.

Two unit tests hold it to the two ways it could inherit the original blindness:
a failed dial must be measured and counted, and a dial outside the kill window
must be ignored — the latter asserted with a positive control, because checking
only for the zero would pass against a function that records nothing at all.

So the next soak answers this. Either a breach carries a ~3 s
`max_connect_ms` with a failed dial, which names the cause outright, or it does
not, which refutes the last candidate standing and sends this back to first
principles with one more phase eliminated. **Both outcomes are progress; neither
requires another plausible story.**

### What this means for M2

**These breaches do not implicate the product's failover.** Promotion completed
in ~0.5 s and the fleet had a writable master for the remaining 2.9 s of the
recorded outage. Whatever the 3 s is, it is being spent by the direct-path client
after the fleet was already healthy.

Real clients do not take this path. They reach the fleet through the proxy, whose
address is fixed and outlives every failover — that is the point of it, and the
edge path is what M3's exit measured at ~472 ms client-observed.

So M2's open question is not "why is promotion slow" but **"is the direct path
the right thing to judge the exit on"** — a decision for Jeff, not a fix.

## SETTLED 2026-09-08 — the instrument answered on its first run

`soak-20260908T020420Z`, 800 kills / 421 promotions on rc.69, the first run
carrying `record_connect`. One kill breached, and it carries the answer in the
line itself:

    iter 574: RTO 3469ms [max_hold_ms=4 max_connect_ms=3000
                          connect_failures=19 dispatch=660ms client_stall=3221ms]

**The dial took exactly 3000 ms** — the `connect_timeout` expiring — and
`3221 = 3000 + 221`, where 221 ms is the run's ordinary recovery (median
265 ms). The breach is one blown dial plus a normal failover.

### Why this is conclusive rather than suggestive

The dial distribution over 421 master kills is **bimodal with nothing in
between**:

| dial | count |
|---|---|
| 1 ms | 23 |
| 2 ms | 4 |
| 21 / 27 / 29 ms | 1 each |
| **3000 ms** | **1** |

9,473 dials failed after a kill across the run. **9,472 of them cost about a
millisecond** — a dead port answers RST at once — and exactly one cost the full
backstop. A latency problem produces a tail; this produces a cliff, and the
single point at the cliff is the single breach.

The journal agrees, as it did for the earlier breaches: `Detected` +467 ms,
`PromoteIssued`/`Promoted` +474 ms. The fleet had a writable master ~3 s before
the client found it.

The seat prediction also came true, and is now barely needed: the victim
dialled FIRST carries the only dial over 29 ms and the only breach (n=211);
the other victim's worst dial is 29 ms (n=210), with medians 266 vs 264 ms.

### What it was

The harness restarts the killed seat immediately (`kill_master_hot` calls
`a.restart`). `connect_master` then walks endpoints in order and dials that
seat first. For a narrow window the restarting seat neither answers nor
refuses — the SYN goes unanswered — and the dial pays flint-tls's
`CONNECT_BACKSTOP` in full. BUG-0123 measured that behaviour directly against
TEST-NET-1: a dropped SYN spends the budget to the millisecond.

Note what this does NOT vindicate. The mechanism I drafted and refuted — "a
restarting seat binds before it can serve, so the dial hangs" — was still
wrong: #176 binds first on purpose and `serve_loading` answers. The window is
narrower than that story and sits before the listener is up, not after. Being
right about the phase did not make the story about the phase right.

### What it means for M2, and the decision it leaves

**The product's failover is not implicated by any breach in three runs.**
Detection and promotion complete in ~0.5 s every time. What the direct path
adds is the harness's own cost dialling a seat it just restarted.

Two remedies exist and they are not equivalent:

1. **Bound the harness's dial** — `connect_within` from BUG-0123 already
   exists, so this is a one-line change to `connect_master`.
2. **Judge the exit on the edge path**, where a real client lives: the proxy's
   address is fixed and outlives every failover, which is the point of it.

Both would make the exit pass, and that is exactly why **neither is mine to
take**. Changing what a measurement counts so that it clears its own budget
needs to be a deliberate decision by the person who owns the milestone, not a
tidy-up by the person who found the cause. Recorded here and raised; not done.
