# BUG-0047 — failover_drill is not parallel-safe, and it is not the OOM killer

> **Every duration in this document was measured on a clock that printed
> integer seconds, which floored them.** `gates.sh` now times in milliseconds
> (`4880d8c`). The tables below remain as the record of what was believed and
> why, but they are NOT reproducible as measurement, and no number here should
> be averaged with, or compared against, anything measured after that commit.
> Absolute deltas are roughly right; ratios on sub-2s drills are not.

**Status:** FIXED 2026-08-24, confirmed by experiment. The measurement
`tools/gates.sh` asked for has been taken, and the OOM hypothesis it named is
disproven.

## Why this matters

`FLINT_GATE_JOBS` has been opt-in since it landed, for a stated reason:

> the knob stays opt-in until a parallel run has been green for a while,
> because the failure mode of getting this wrong is a gate that reports red
> for reasons that have nothing to do with the change under test.

That is the right bar. This bug is the one thing standing between the gate and
that bar — and closing it takes the gate from **31 minutes to ~16**.

## Measured, on the runner that matters

The 3.23x figure in `gates.sh` was taken on a c7i.4xlarge: 16 vCPU, 32 GB.
CI is `ubuntu-latest`: 4 vCPU, 16 GB. Different machine, different answer, so
the run was repeated there (`workflow_dispatch`, `gate_jobs=3`):

| | serial | P=3 |
|---|---|---|
| wall clock | **31.2 min** | **~16 min** |
| result | green | **1 fail / 108 pass** |
| peak memory | not sampled | **1926 MB of 15989** |
| `failover` | `PASS (5s)` | **`FAIL (4s)`** |

## The OOM hypothesis is disproven

`gates.sh` records that seats have twice been SIGKILLed under parallelism with
no attributable killer, and names the kernel OOM killer as "the remaining
candidate". It added a memory sampler so the next occurrence would "arrive
with a number beside it instead of another round of elimination".

**The number is 1926 MB of 15989 — 12% of the box.** No OOM lines in `dmesg`.
Whatever kills the seat, it is not memory pressure, and that candidate can now
be struck off rather than re-argued.

## What actually fails

`failover_drill.sh` kills its own master on purpose (line 66 — the `Killed`
message there is expected). It then restarts that old master as a **ZOMBIE** on
the same port to demonstrate the split-brain hazard, and asserts the zombie
accepts a write. Under P=3 the zombie is SIGKILLed before that assertion:

    == ZOMBIE: restart the OLD master on its old data dir
    flint-server listening on 127.0.0.1:6326 (plaintext)
    tools/failover_drill.sh: line 101: 6631 Killed  "$BIN" --port "$MPORT" ...

## Eliminated so far

- **Memory / OOM** — 12% peak, no `dmesg` evidence.
- **Port collision** — 6326/6327 appear in no other drill.
- **An unscoped `pkill`** — no drill kills `flint-server` without scoping to
  its own port or data dir; `fleet.sh` has no broad sweep either.
- **`step()`'s leaked-seat reap** — in the parallel path `step_report` replays
  *after every drill has finished*, so it cannot fire mid-drill.

## The lead worth pulling next

The zombie is started with a bare `"$BIN" … &`, outside fleet.sh's tracking,
and its data dir is `$FLINT_DRILL_ROOT/flint-fo-m.XXXXXX` — **not** the scope
the drill declares to the harness:

    fleet_init $FLINT_DRILL_ROOT/flint-failover 6326 6327
    MDIR="$(mktemp -d $FLINT_DRILL_ROOT/flint-fo-m.XXXXXX)"

So the zombie is recognised as the drill's only by its **port**, never by its
directory. Anything that reasons about ownership by scope prefix sees an
untracked Flint server. Making the drill declare the prefix it actually uses is
cheap, testable, and would rule the last structural difference in or out.

## Do not flip the default until this is green

One drill failing for harness reasons is exactly the outcome `gates.sh` warns
about. `workflow_dispatch` now takes a `gate_jobs` input (default `1`, so
nothing changes) precisely so this can be re-measured on the real runner
without betting the gate on it.

## The same mismatch exists in four other drills

A scan of all 113 drills for data dirs matching no scope the drill declares:

| drill | creates | declares |
|---|---|---|
| `chaos_unreadable` | `flint-unreadable` | `flint-chaosunread` |
| `controller` | `flint-ctl-r` | `flint-ctl-m` |
| `lease` | `flint-lease-cp`, `flint-lease-r` | `flint-lease-m` |
| `min_replicas` | `flint-minr-r1`, `flint-minr-r2` | `flint-minr-m` |

**These four pass under P=3 today**, so the mismatch alone is not sufficient to
break a drill — every one of their seats is still recognised by a declared
port. `failover` is distinguished by its zombie being started *outside*
fleet.sh's tracking, leaving the directory as the only other marker it could
have had.

So they are latent, not active, and they are listed here rather than fixed
because a change with no failing test behind it is a guess. The durable fix is
an assertion — a drill's data dirs must live under a scope it declares — which
becomes possible once all five are aligned.

## First candidate fix, and how it gets judged

`failover_drill.sh` now creates `flint-failover-{m,r}` instead of
`flint-fo-{m,r}`, matching its `fleet_init`. Correct regardless of this bug: a
scope the harness is told about should be the scope actually used.

Whether it is *sufficient* is decided by re-running the gate at
`gate_jobs=3`. If failover still fails, the cause is elsewhere and this bug
stays open with one more candidate eliminated.

## Confirmed

The scope alignment was sufficient. Same branch, same runner, `gate_jobs=3`:

| run | wall clock | result | peak memory |
|---|---|---|---|
| serial (the rc.63 cut) | **31m15s** | green | not sampled |
| P=3, before the fix | ~16m | **1 fail** (`failover`) | 1926 / 15989 MB |
| P=3, after the fix | **16m09s** | **109 pass / 0 fail** | 1888 / 15989 MB |

`failover` now reads `PASS (5s)` in parallel — identical to serial.

So the cause was structural, not resource: a seat started outside fleet.sh's
tracking, in a directory the drill had never declared, recognisable only by
port. Nothing about memory, and nothing about the product.

**The gate is green in parallel, and the curve is measured:**

| jobs | wall clock | speedup | result | peak memory |
|---|---|---|---|---|
| 1 (serial) | 31m15s | 1.00x | green | not sampled |
| 3 | 16m09s | **1.93x** | 109 / 0 | 1888 MB (12%) |
| 6 | **11m49s** | **2.65x** | 109 / 0 | 2084 MB (13%) |

Memory barely moves between P=3 and P=6 — 1888 to 2084 MB, both ~12% of a
16 GB box — which is the clearest possible answer to the question that kept
this knob off. The drills are not memory-bound and they are not CPU-bound
either: P=6 on 4 vCPU is 1.5x oversubscribed and still 27% faster than P=3,
so most of the wall clock is waiting, not computing.

Diminishing returns are visible and have a known cause. `ns_escape` alone runs
365s, so ~6 minutes is the floor at any P; at P=6 the gate is already within
twice that. P=8 would buy a little; splitting `ns_escape` would buy more.

## Before flipping the default

`gates.sh` asks for "a parallel run green for a while", and one green run is
not a while. The knob is dispatchable so that bar can be met with evidence
rather than argued about. The remaining four scope mismatches should be
aligned first, and then the durable form of this whole bug is one assertion:
**a drill's data dirs must live under a scope it declares.**

## The default is flipped, and the evidence has a shelf life

Three consecutive green runs at P=6 — 11m49s, 11m43s, and the P=3 pair before
them — so `gate.yml` now defaults to `FLINT_GATE_JOBS: 6`, with
`gate_jobs=1` on a manual dispatch as the rollback.

**But that evidence was taken on a tree that is about to change.** A peer
session has ~11 commits queued for main fixing races that a parallel gate makes
*more* likely, and none of them are in what was measured here:

- a master that BINDS and answers `-LOADING` while still loading, which is
  fatal to a replica's full sync because the retry predicate covers
  `-THROTTLED` and the connection errors but not `-LOADING`
- `fleet_kill` returning on signal delivery rather than on the seat being gone,
  where 53 drills respawn on the same ports after a fixed sub-second sleep
- `PONG` no longer meaning ready, affecting 50 drills that wait on it and then
  issue data commands

Verified against this branch's base (`220364f3`): the only `-LOADING` in
`crates/` is flint-vec's vector-index warming — a different concept wearing the
same string — and `fleet_wait_ping` is still PONG-only.

So 109/109 three times says the parallel path works **on the tree it ran
against**. It does not say P=6 is safe once those land. Rebase onto them and
re-run before treating the default as settled; that is a smaller claim than the
numbers above invite, and the difference is exactly the kind of thing that
turns into a gate going red for reasons unrelated to the change under test.

## Watch durations, not just verdicts

The same peer observed `promote_notice` sitting at exactly 10s for 18
consecutive gates, then going 43 / 8 / 42 while still passing — the variance
arriving before the failure. Checked here across serial / P=3 / P=6:

    promote_notice  10 -> 11 -> 12      (stable; does not reproduce)
    upgrade         56 -> 63 -> 66
    decommission    43 -> 43 -> 47
    cpha_roll       36 -> 36 -> 39
    chaos          115 -> 129 -> 120
    edge_roll       40 -> 41 -> 56      <- +40%, the one to watch

One sample per configuration rules nothing out, so this is a baseline to
compare against, not an all-clear.

**A retracted claim, and why the retraction matters more than the claim.**
This table originally read "mild monotonic inflation rather than instability".
A peer session pointed out that the hedge was applied asymmetrically: the
variance claim was flagged as unsupported by n=1, and the *pattern* claim
sitting beside it — which needs at least as much evidence — was not. The
unhedged one was the reassuring one, and that is the reading that gets adopted
by default.

The data is consistent with mild inflation. It is equally consistent with noise
plus one real regression, and n=1 per cell cannot separate those.

Checking the arithmetic: exactly **one** of six is non-monotonic (`chaos`,
115 -> 129 -> 120). `edge_roll` at 40 -> 41 -> 56 *is* monotonic — an uneven
step, not a reversal. But the step sizes are the real signal, and they make a
better case than monotonicity would have:

    drill            serial->P=3   P=3->P=6
    promote_notice        +1          +1
    upgrade               +7          +3     decelerating
    decommission          +0          +4
    cpha_roll             +0          +3
    edge_roll             +1         +15     accelerating, 15x its first step

Everything else inflates smoothly or decelerates as jobs increase. `edge_roll`
is flat from 1->3 then adds 15s from 3->6. Uniform contention should not switch
on between P=3 and P=6; a threshold — CPU starvation crossing a timing-sensitive
wait, in a rolling-upgrade drill — should.

**Open, with a designed experiment.** `edge_roll` plus five cheap controls via
`FLINT_CORE_ORDER`, at `FLINT_GATE_JOBS=1` and `=6`, five runs each, same set
both arms. Predictions committed before any run:

| | outcome | reading |
|---|---|---|
| A | ~44-46s, tight, controls inflate alike | uniform contention |
| B | ~56s consistently, controls mild | threshold |
| C | bimodal 40/56 across the five | a real wait is being missed |
| **D** | ~40-42s, indistinguishable from P=1 | **the 56s never reproduces** |

**D is the correction, not an addition.** The first version of this list held
only B and C — both of which presuppose the 56s is real, resting on the same
single sample this section had just finished conceding cannot support a
pattern. The identical asymmetry, one paragraph after retracting it, inside the
artefact written to prevent it. Committing to D in advance is what stops the
run being read as confirmation whatever it returns.

The five controls are a hard requirement, not a nicety: without their durations
in both arms, "edge_roll inflated" cannot be distinguished from "everything
inflated", and that falsifier holds whatever the absolute numbers are.

**It must run on the CI runner, not a laptop.** The hypothesis is CPU
starvation crossing a timing-sensitive wait:

    developer box   8 cores  ->  P=6 = 0.75 drills/core
    CI runner       4 vCPU   ->  P=6 = 1.50 drills/core

At 0.75 there is no starvation to cross, so a careful local A/B yields a clean
number answering a different question — and its null would be indistinguishable
from D while actually meaning "the condition was never present". Same
measured-on-a-different-machine trap as the 3.23x figure this bug opened by
refusing to reuse. `gate.yml` therefore takes a `core_order` input so the short
list can be dispatched to the 4 vCPU runner directly.

**Correction: ranking that table by percentage was wrong.** A peer session
pointed out that short drills pay a roughly FIXED contention cost — bring-up,
port waits, the guard's settle — which percentage then ranks as catastrophic
while absolutely they cost little. Confirmed here: 38 drills run <=5s serially
and inflate by a mean of **+4.4s**.

Ranked by absolute seconds added instead, `edge_roll` (+16s) does not make the
top eight at all, and the real cost sits elsewhere:

    client_compat    5s -> 39s   +34s
    anti_affinity    3s -> 30s   +27s
    proxy_registry   6s -> 32s   +26s
    attached_chaos  19s -> 41s   +22s
    fleet_guard     68s -> 88s   +20s

**And the fixed-cost model has a limit.** On the peer's 16 vCPU box at P=4
(0.25 drills/core) short drills paid 11-17s. Here, at P=6 on 4 vCPU (1.5
drills/core), the worst pay 27-34s — large in absolute terms as well as
relative, and top of both rankings. Total drill-time across the suite goes
1828s serial -> 2321s at P=6: **27% more CPU-seconds bought 2.65x wall clock.**

That trade is worth making, but it says the runner is saturated, and it means
the clean fixed-cost model may hold only while there are spare cores. The
ranking advice transfers between boxes; the magnitudes do not.

`promote_notice`'s 43/8/42 swing is also now explained rather than open: the
peer saw it only on trees containing #176, and neither of us sees it without.
Two independent negatives, from different core counts. The variance had a
cause and the cause is fixed — it was not parallelism hiding instability.

## edge_roll: closed. The experiment answered no.

Ten runs on `b0ed2a0`, five per arm, six-drill set, predictions named before
interpreting. **`edge_roll` is not special**: 39.8s -> 49.2s, a 1.24x ratio and
the second-lowest in the set. The 56s in the table above did not reproduce —
max 51. The pre-registered falsifier was "if the controls inflate by the same
proportion there is nothing specific to edge_roll", and they inflated *more*.

It landed in none of A/B/C/D cleanly. Closest to A, but A required the controls
to move together and they ranged 1.10x to 10.67x. The honest summary is that
all four predictions were framed around a subject that turned out not to be the
story.

**The control group was the finding**, and only in that environment.
`coproc_forward` went 1.2s -> 12.8s there. Against the full 109-drill run it
ranks **sixth of 38** short drills by absolute inflation (+7s, mean +4.4s) —
above average, not an outlier — and four of the five above it were absent from
the six-drill set entirely:

    client_compat      5 -> 39   +34s
    anti_affinity      3 -> 30   +27s
    proxy_conformance  3 -> 20   +17s
    family_route_cp    1 -> 15   +14s

Two readings survive and neither can be eliminated: the effect is specific to a
six-drill set, where a drill overlaps five neighbours for its whole life rather
than being one of 109; or the single sample here is simply wrong about it.

## A measurement defect in this document's own table

`family_route` reads **0s** serial here and 10.0s in the five-run P=1 arm. Both
cannot be true, and the 0 is sub-second truncation in `gates.sh`'s integer
seconds — so the 18.00x ratio it produces is an artifact of dividing by a
rounded-down zero. Every drill under ~1s in the duration table has the same
flaw, and that is a meaningful fraction of the 38.

Where the two datasets disagree on these six, **the five-run numbers win**:
n=5 with a control group beats n=1 with a rounding bug. This table's value is
breadth, as a generator of leads, not as measurement.

## A caveat on the method, not the run

A six-drill set is not a small version of the full gate. Concurrency decays as
members finish, and the neighbour mix is unrepresentative — both showed up
here, in opposite directions. Worth remembering before the next quick targeted
comparison.

Next, if anyone spends the runs: `client_compat` and `anti_affinity` — the
drills actually costing wall clock in the environment that ships — at n=5, with
a control set large enough that concurrency does not decay, and
`coproc_forward` included as the one place the two datasets actively disagree.

## The rule this whole thread produced

**The shorter the baseline, the wider a ratio's error bar — and a contention
study puts its headline on the shortest drill in the set, every time.**

Both of us did it, independently, on the same day. A `10.67x` on a 1.2s
baseline was honestly 5.8x-11.5x. An `18.00x` was division by a floored zero.
Neither survived contact with the other's data, and neither was a claim about
the system: both were claims about the clock.

Three consequences worth carrying:

- **Rank by absolute seconds added, not by ratio.** Percentage systematically
  promotes short drills paying a fixed bring-up cost, and buries the drills
  actually costing wall clock. Re-ranking moved `edge_roll` out of the top
  eight and `client_compat` (+34s) into first.
- **Suspicious uniformity is an instrument reading, not a system property.**
  Five runs producing `[42,51,51,51,51]` looked like bimodality. Four identical
  values at one-second resolution meant the resolution, not the behaviour.
- **Fix the instrument rather than annotate its error.** Propagating error bars
  through a bad clock documents the problem for one analysis; the next person
  inherits it. One commit removed the class.

Third time in one day that the instrument, not the system, produced the shape
being read — after a leak check whose pattern encoded its own answer, and a
scan that returned clean because it matched nothing.

## The P value, closed: P=6 does NOT ship

The three green P=6 runs were on a tree without the eleven concurrency
commits. Once those landed, the same gate was re-run at both job counts on the
**same sha** (`9940a50`), so only the job count differed:

| drill | P=1 serial | P=6 parallel | verdict |
|---|---|---|---|
| `reseed` | **PASS** (4.2s) | **FAIL** (4.0s) | parallelism-only — **this is the blocker** |
| `chaos` | FAIL (123.3s) | FAIL (122.7s) | tree-level, not parallelism |

`reseed` fails with *"the marker did not trigger a full sync"* only under
contention. That is the precise failure mode `gates.sh`'s own opt-in note
exists to prevent — a gate going red for reasons unrelated to the change under
test — so **the default is back to 1**.

`chaos` is a separate matter and not this branch's: it panics at
`cluster.rs:1520`, `assert!(wait_for_ready(dead, 15s))`, in a crate this branch
never touches, and **main's own gate is failing** at `aead59d1`. A replacement
replica is not reaching ready inside 15s — consistent with nodes now binding
and answering while loading, where readiness means more than it used to.

**What survives, and makes the next attempt cheap:** `auto`, `core_order`, the
millisecond clock, the drills/core ratio, and both attribution assertions all
stand on their own evidence. Fix `reseed` under contention, dispatch
`gate_jobs=6`, and one number changes.

The honest summary of the whole thread: **2.65x was real and is not available
yet.** Three green runs measured a tree that no longer exists, and the one
experiment that could tell parallelism from tree change is the only reason
that is known rather than assumed.