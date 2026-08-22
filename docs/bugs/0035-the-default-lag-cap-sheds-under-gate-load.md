# BUG-0035: the default lag cap sheds under gate load, and two drills misreport it (drills FIXED; the shed itself OPEN)

Status: OPEN 2026-08-20 · Severity: medium — one half is a documented claim
with a counter-example, the other is a drill that reports a verdict for
assertions it never reached

**Update 2026-08-20, later:** the mechanism is reproduced at shipped defaults
and both false claims are corrected — see "Reproduced at shipped defaults" at
the foot of this file. The design half is answered too ("The design half,
answered", below): the caps are mis-POSITIONED, not mis-shaped, because the
soft cap of 500 ms sits BELOW the 631 ms ordinary operating point. The cause
of that operating point is BUG-0038. Still open: BUG-0038's fix, and gate
21's specific perturbation — now merely unnamed rather than unexplained.

## Symptom

Gate 21 on `flintmigrations-all` at `5e3d81e`: 116 PASS, 1 FAIL.

    FAIL  repl                   (7s)  /tmp/flint-gates/20260820T010414Z-5e3d81e/drill-repl.log
            errors: 20328, replies: 50500
            tools/repl_drill.sh: line 22: 14941 Terminated: 15  "$BIN" --port "$MPORT" ...
            tools/repl_drill.sh: line 22: 14949 Terminated: 15  "$BIN" --port "$RPORT" ...

The drill log is 20328 copies of

    THROTTLED replication lag exceeds limit, retry with backoff

The master shed 40% of the load. No stall was induced, no cap was lowered:
this is the shipped default, `--lag-hard-ms 1000` / `--lag-soft-ms 500`
(`repl_hub.rs:20,37`).

**The commit cannot be the cause.** `7a98b18..5e3d81e` is two docs files and
three shell files (`gates.sh` plus `slot_map_drill.sh` and
`restore_ns_drill.sh`, neither of which `repl` runs). No Rust changed, so the
binary behaves identically to the six preceding gate runs — all of which
recorded `throttled=0, errors: 0` in this drill.

## Half one: a published claim now has a counter-example

`tools/lag_cap_drill.sh`'s header states the reason that drill has to lower
the cap at all:

> Loopback replication acks in ~0.2ms and cross-host was not much slower, so a
> 1000ms cap is simply unreachable under any load the harness generates.

`docs/slo.md`'s RPO table says the same thing as a measurement:

    | stall | lag cap | writes shed -THROTTLED | deepest acked-write loss |
    | none  | 1000 ms | 0                      | 0 ms                     |

That first row is exactly gate 21's configuration — no stall, 1000 ms cap —
and gate 21 shed 20328. The cap is reachable by the harness's own load on
loopback. Whether that is a defect or backpressure working correctly is a
separate question; what is not in doubt is that "unreachable" and "0" are
falsified, and both are load-bearing for how the RPO bound is explained.

## Half two: the drill reports a verdict it never computed

`repl_drill.sh` runs under `set -euo pipefail` and loads through

    { awk ... } | valkey-cli -p "$MPORT" --pipe | tail -1

`valkey-cli --pipe` exits non-zero when it counts errors, `pipefail` promotes
that to the pipeline's status, and `set -e` aborts the drill on the spot. The
EXIT trap then kills both seats, which is the two `Terminated: 15` lines.

So the run ended at the LOAD step. It never reached parity samples, the
full-sync assertion, FLINTINFO roles, idle liveness, READONLY, live tail, or
"replica serves reads with master dead" — the six things the drill exists to
check. `FAIL repl` is indistinguishable from a genuine replication failure and
means only that the master applied backpressure while the drill was writing.

This is the same shape as BUG-0029 and BUG-0030: a step that could not answer
producing output shaped like an answer.

## Reproduction: four attempts, all negative, load measured

Standalone at the same commit, on the same box, immediately after the gate:

| attempt | conditions | idle CPU during load | THROTTLED | result |
|---|---|---|---|---|
| 1 | idle box | — | 0 | PASS |
| 2 | 6 CPU burners on 8 cores | — | 0 | PASS |
| 3 | `restart_drill` then `repl_drill` back to back, as the gate runs them, burners still live | 9.6 / 9.2 / 20.2% | 0 | PASS |
| 4 | two `dd conv=fsync` loops on the same filesystem, burners still live | 2.4 / 6.1 / 14.2% | 0 | PASS |

**Attempts 3 and 4 ran under MORE load than intended**, which is worth stating
plainly because the reason is a mistake of the exact kind this file is about.
Attempt 2's six burners were never killed. The teardown ran

    kill $BURNERS ; echo "burners left: $(jobs -p | wc -l)"

and printed `burners left: 0`, which was read as confirmation. `jobs -p` in a
non-interactive shell had never tracked them, so `BURNERS` was empty, `kill`
received no arguments, and the count of zero meant "this shell knows of no
jobs" — output identical to "all six were killed". They span at ~99% CPU each
for 34 minutes and were found only when the box showed load 9.2 on 8 cores.

So attempts 3 and 4 were run with six spinners plus their own load — 2.4% idle
at the floor — and still did not reach a 1000 ms cap. The mistake made those
two negatives STRONGER, not weaker, and it makes attempt 2 a real negative
rather than the inconclusive one this file first called it.

Verify a kill by asking after the PIDs, not by asking the shell how many jobs
it remembers.

## A hypothesis, tested and dead

The first explanation was that a gate run execs FRESHLY BUILT binaries while
every standalone re-run gets the same binary already assessed and cached, and
that Gatekeeper's first-exec stall (69 min on this box before the Developer
Tools grant) was stalling the replica.

`build.log` from the failing run refutes it:

    Finished `release` profile [optimized] target(s) in 0.30s

Nothing was rebuilt. The commit changed no Rust, so gate 21 ran the SAME
binaries as the six clean runs before it, already assessed. There was no fresh
binary and no first exec. The hypothesis is dead, and it was killed by a log
that was already sitting in the failing run's own directory.

## What is actually left

Not CPU (attempts 2, 3), not disk (4), not the restart-then-repl sequence (3),
not a fresh binary (build.log).

A `flint-kv` `cold-modify` process from another session was on the box two
minutes into gate 21, and another was running an hour later. **That is not
offered as the differentiator**, because the box was not sampled for it during
each of the four attempts — so "present during the gate, absent from the
attempts" is a claim the evidence does not support, and writing it that way
was the first draft of this file. What can be said is narrower: another
session's workload comes and goes on this box unmeasured, and it is the
largest uncontrolled input remaining.

There is precedent for it mattering. Gate 17's ten bootstrap failures were
concurrent peer load and nothing else, and that was confirmed only when the
peer independently reported their own bootstraps failing in the same shape at
the same time — neither session could have concluded it alone.

The way to settle this is to sample the box during the drill rather than
reason about it afterwards: record load average and the non-flint process set
into the drill log at the moment the load phase starts. Then a shed run and a
clean run differ by a recorded fact instead of by a recollection.

**Done.** `fleet_env_note` writes one line — load average plus any sibling
project build/test on the box — from `fleet_guard` (before it decides, so a
REFUSED run still says what it saw) and from `fleet_load_resp` (the load phase
itself, which can be minutes later and is where the shed happens). It reads
`args`, never `comm`, which truncates at 15 characters and silently turns
`flint-controlplane` into `flint-controlpl`. The next sighting will carry its
environment on the adjacent line instead of needing an evening of
reconstruction.

A note on how that was verified, because one control failed in the day's own
manner. The intended positive control copied `/bin/sleep` to
`…/flint-fake/release/probe-harness` and ran it — macOS refused to start a
copied system binary, so the process never existed and the check printed "no
sibling processes" for a box that genuinely had none. That is a control that
could not create its condition, reporting the same output as a working one.
Coverage rests instead on three things that did run: the eight-path table test
with its mutation (BUG-0036), a live `_fleet_sibling` naming a real flint-kv
test binary, and the formatting branch driven with a stubbed detector.

If concurrent load IS the trigger, the finding stands and strengthens rather
than dissolving: a busy host is an ordinary production condition, and it
reached a cap two documents describe as unreachable under any load.

## Gate 22 is not more evidence — it is the same mistake, downstream

The re-run at `6fea7c2` came out 114 PASS / 3 FAIL: `edge_roll` (bootstrap),
`json` ("Could not connect to Valkey at 127.0.0.1:7681: Connection refused"),
and `chaos` (a BUG-0023 lost link). `repl` PASSED in that run.

**Those three are the burners, not the product.** The timeline is
unambiguous: the burners started ~18:31 local and were killed at 19:06; gate
22 ran 18:35-19:03, entirely inside that window, at load 9.2 on 8 cores. Both
new drill failures are startup-timing failures — a seat that did not begin
listening inside its wait budget — which is exactly what a saturated box
produces. None of them is filed as a defect.

Gate 21, the run this file is about, ended at ~18:20, ELEVEN MINUTES BEFORE
the first burner existed. Its `repl` failure is not explained by this and
still stands.

The lesson is about attribution, not about load: an intermittent failure
arriving right after a self-inflicted change to the environment is the easiest
kind of evidence to misread in both directions — as a product bug, or as
"just my load" when a real one is hiding underneath. The discriminator was a
timestamp, and it was available the whole time in the gate log directory name.

## The drills are fixed; the shed is not

Not by raising the caps. `-THROTTLED` means the write was NEVER ACKED, so a
key absent because of it is correctly absent, and the fix is for the drills to
say so rather than to stop the master saying it.

`tools/lib/fleet.sh` gained three helpers:

- `fleet_load_resp` pipes the load, PRINTS what was shed, and does not fail on
  shed alone. It does fail when the load delivered nothing at all, because
  "nothing was written" and "everything was refused" must not look alike.
- `fleet_retry_write` retries one write past `-THROTTLED`.
- `fleet_ensure_keys` repairs, one write at a time, exactly the keys a drill
  asserts on. Everything else the load shed stays absent on purpose.

**A wrong fix, measured, before the right one.** The first version replayed the
whole stream until nothing shed. Against a 5 ms cap the attempts shed 19388,
19469, 19667, 19413 and 19337 of 20000 and it gave up: the replay is itself a
firehose and recreates the lag that caused the shed. A retry whose load
profile equals the load that failed is not a retry. Single writes converge
because they let the replica drain between them.

## Verification, both directions, both drills

| drill | condition | shed | result |
|---|---|---|---|
| `repl` | ordinary load | 0 of 50500 | PASS |
| `repl` | master forced to `--lag-hard-ms 5` | **49229 of 50500** | PASS, every assertion reached |
| `controller` | ordinary load | 2428 of 20000 | PASS |

The `controller` row is the one that matters: 2428 writes shed on a quiet box
in an ordinary standalone run, and the OLD drill would have printed
`FAIL: tail lost` for it. That is this product's most serious claim —
acked-write loss across a failover — being asserted from evidence that the
master had openly refused the write. The positive control was not induced; it
arrived on its own, which is also a third independent sighting of the shed.

## What is still open

The drills no longer lie about it, and that is all that changed. The shipped
1000 ms cap is still being reached by ordinary load on loopback, which
`lag_cap_drill.sh`'s header and `slo.md`'s no-stall row both say cannot
happen. Sightings so far: gate 21 `repl` (20328), gate 23 `repl` (19932),
gate 23 `controller` (356), and a standalone `controller` (2428).

**AND ONE IN PRODUCTION, 2026-08-20, which the four above were not.** Rolling
the playground to rc.59 shed **210 writes on the demoted seat** during a
CONTROLLED failover, with the canary replica at 0 — reported by the ops
session, on the live fleet, at shipped defaults, with nobody trying to
provoke it.

That matters more than any of the drill sightings, for two reasons. It is not
a laptop and not a synthetic firehose: it is the roll procedure this product
runs on purpose. And it is the first time the number was COUNTABLE at all,
because `writes_shed_lag` did not exist until 2026-08-20 — every earlier roll
shed an unknown amount and reported nothing, so "0 shed" in the historical
record means "not counted", not "none".

So the honest state of `slo.md`'s no-stall row is worse than "contradicted by
one laptop observation": the shed happens during an ordinary controlled
failover, and the reason nobody saw it is that nothing counted it.

`slo.md`'s table needs correcting once the trigger is understood — not before,
because "0 shed with no stall" is currently the only written statement that
this contradicts, and replacing it with a vaguer sentence would lose the
contradiction rather than resolve it.

## 2026-08-22 — re-measured at HEAD after BUG-0038's fix: the loopback half no longer reproduces

Every sighting in "What is still open" predates BUG-0038's fix, which moved the
ordinary operating peak from 631 ms to 115 ms. Re-measured on today's HEAD, on
a box at load 3.55 — deliberately not a quiet one — with 100k pipelined 1 KB
writes through a live master/replica pair at shipped defaults:

    caps: soft=500ms hard=1000ms
    batch 1..5  lag_ms_max: 32 -> 84 -> 84 -> 139 -> 139
    writes_delayed_soft: 0      writes_shed_lag: 0

Peak 139 ms against a 500 ms soft cap. Ordinary load does not enter the soft
band, so the claim this bug turns on — "the shipped 1000 ms cap is still being
reached by ordinary load on loopback" — does not reproduce at HEAD.

Second, independent line: the `repl` and `controller` drills are the two that
shed in gates 21 and 23, and both have passed in every 118/0 gate run on Linux
since. They were fixed to report shedding rather than swallow it (this file's
"Half two"), so a pass now means the shed did not happen, which it did not mean
before.

### What this does NOT settle, and it is the more important half

**The production sighting stands untouched.** 210 writes shed on the demoted
seat during a CONTROLLED rc.59 failover, canary replica at 0. That is a
different condition from steady pipelined load: a demoted seat mid-failover,
not a master under traffic. Nothing above reproduces it and nothing above
should be read as clearing it.

So the shape of this bug has changed rather than closed:

- loopback/gate-load half — **not reproducing at HEAD**, on two lines of
  evidence, and attributable to BUG-0038's fix
- failover half — **open and unmeasured**, and it is the one that happened on a
  real fleet during a procedure this product runs on purpose

`slo.md`'s no-stall row still should not be edited. The contradiction it needs
to answer is now the failover case specifically, and softening the row before
that is understood would lose the contradiction rather than resolve it — the
same reasoning this file already gives for not editing it.

Worth keeping beside that: `writes_shed_lag` did not exist until 2026-08-20, so
"0 shed" in any earlier record means "not counted". The measurement above is
worth something only because the counter is real, and its zero is a measured
zero — the distinction BUG-0022 exists for.

## Note

`ReplHub::new` stores `lag_hard_ms.max(lag_soft_ms)`, so any fix must move
both caps together — passing only `--lag-hard-ms` leaves the 500 ms default
soft cap in force. `lag_cap_drill.sh`'s header records this trap already.

## Reproduced at shipped defaults — the margin is ~370 ms, not 1000 ms

The open half is closed as a mechanism, though not as a named culprit for
gate 21 specifically. Two facts, both measured on an idle laptop against a
plain local pair with no config changed:

**1. Ordinary sustained load already sits two thirds of the way to the cap.**
One continuous 500k-write pipe, healthy replica, shipped 500/1000 caps:

    t=1s lag_ms=303  t=2s lag_ms=472  t=3s lag_ms=56  t=4s lag_ms=266  t=5s lag_ms=514
    FINAL lag_ms_max=631  lag_max_gap=40161  shed_lag=0  delayed_soft=217

`lag_ms_max=631`. Nothing was stalled, nothing else was running, and the run
shed zero — but it did so with **369 ms of margin, not 1000 ms**. The soft
band engaged 217 times and held, which is the design working.

**2. Spend that margin and the shed is immediate.** Same pair, same defaults,
replica SIGSTOPped for 1.2 s inside the load:

    t=1.0 mid-load  lag_ms=325   seq_lag=40733   shed=0
    STOP  +0.3s     lag_ms=563   seq_lag=78016   shed=0        delayed=29
          +0.6s     lag_ms=900   seq_lag=78128   shed=0        delayed=143
          +0.9s     lag_ms=1246  seq_lag=78157   shed=99163    delayed=166
          +1.2s     lag_ms=1579  seq_lag=78157   shed=232814   delayed=166
          +1.5s     lag_ms=1914  seq_lag=78157   shed=323862   delayed=166
          +1.8s     lag_ms=none  live=0          shed=323862   (liveness window
                                                                expired; no
                                                                replica, no cap,
                                                                writes flow again)

323862 writes shed at the shipped defaults. **A ~700 ms pause of the replica
process is all it takes**, and a contended box supplies one for free — which
is exactly the largest uncontrolled input this file already identified, now
with a mechanism attached rather than a suspicion.

Note the last two rows. Between the hard cap (1000 ms) and `LIVENESS_WINDOW_MS`
(2000 ms) there is a one-second band where a replica that is *frozen* still
counts as *live*, so the master sheds against it; past 2000 ms it stops
counting at all and the shed stops. The widowed grace exists for that second
half and is what `flintctl` turns on for pair members. A bare pair started by
hand, as here, has it at 0.

## Why five more negatives came first, and the mistake they share

Before the above, a load sweep at shipped defaults — 1, 2, 4, 8, 16 concurrent
writers, 20k each, up to 320000 writes — produced peak lags of 47, 76, 36, 34,
35 ms. Flat. Load-independent. A sixth negative, and it looked conclusive.

It was wrong for the same reason the four attempts above it were: **bursty
load drains between batches.** Every one of those probes wrote in chunks that
finished faster than a backlog could build, so replication caught up in the
gaps and the peak never moved no matter how much total volume went through.
The single continuous pipe reaches 631 ms because nothing ever lets the queue
empty.

The generalisation is the same one `lag_cap_drill.sh`'s header got wrong: a
mean ack latency (~0.2 ms on loopback) says nothing about the depth of a
pipelined queue, and scaling total volume does not create a backlog if the
offered load is still bursty. Only *sustained* offered load does. Both that
header and `slo.md`'s no-stall row are corrected in the same commit as this.

## What this does not settle

No claim that gate 21's specific perturbation is identified. What changes is
that it no longer has to be exotic: the headroom is ~370 ms, and any pause of
the replica past that crosses the cap. `fleet_env_note` will name the
environment at the next sighting, and the master now keeps its own record —
`writes_shed_lag`, `lag_ms_max` and `lag_max_gap` in `FLINTINFO`, so the next
occurrence does not have to be reconstructed from a client's error log.

`lag_max_gap` is the discriminator, and the three signatures are now measured
rather than reasoned about: a gap that GROWS with the lag is a replica slower
than the master; a gap that FREEZES while lag climbs (78157, unmoving, above)
is a replica not running at all; a SMALL gap under a climbing lag would be the
ack path rather than the data path, and has not been seen.

## The design half, answered: the caps are mis-POSITIONED, not mis-shaped

The open question was whether shedding is the right response or whether the
soft band should push back harder first. Measuring the shipper (BUG-0038)
answers it, and the answer is neither — it reverses the intuition this file
started with.

**`--lag-soft-ms` ships at 500 ms and the ordinary operating peak is 631 ms.**
So the soft band is not a margin the system occasionally touches under stress;
it is territory the system sits in during healthy traffic. Two clean no-stall
runs delayed 217 and 423 writes with nothing wrong. `--lag-hard-ms 1000` is
about 1.6x the natural operating point.

That changes what each candidate fix is worth:

- **Fix the shipper (BUG-0038).** **DONE, and it was better than predicted.**
  The prediction here was ~380 ms from halving the cycle. The actual cause was
  not the cycle's structure but the replica burning its CPU on one ACK syscall
  per WAL batch; acking once per read group instead took the operating point to
  **115 ms** and `writes_delayed_soft` to **zero**. The shipped 500/1000 caps
  now have ~885 ms of margin instead of ~370, and ordinary traffic no longer
  touches the soft band. The published RPO is honest without being widened.

- **Raise the caps to match reality** (soft ~1500 / hard ~3000). Cheap, and
  it stops the brake firing on healthy traffic — but it pays for a slow
  shipper with three times the RPO, which is selling the product's headline
  bound to avoid an engineering problem. Defensible only as a stated interim,
  and `slo.md` would have to say that is what it is.

- **Proportional backpressure in the soft band** — a delay rising with depth
  instead of a flat 2 ms. This was the first instinct and it is the WEAKEST of
  the three, because the operating point is already inside the band: a
  stronger brake there throttles ordinary healthy traffic, harder and more
  often, to solve a problem that is not the brake's shape but its position.

**The terminal shed stays either way.** Two reasons, both now measured. A
frozen replica cannot be backpressured — the stall trace above went 563 ->
1914 ms with the master shedding and the replica simply not running, and no
delay curve reaches a stopped process. And the shed is what makes "at most one
lag-cap window's worth is at risk" TRUE rather than aspirational: remove the
refusal and the bound is enforced by nothing, which is the position Redis is
in when it keeps acking and lets a replica fall arbitrarily behind.

What stays open here is now only BUG-0038's fix and gate 21's unnamed
perturbation.

## 2026-08-22 — the roll now has a drill that could see it, and at ordinary rate it sheds nothing

`tools/roll_shed_drill.sh`, in CORE. Four controls: the seats are on the
shipped cap, a roll under load sheds zero by the lag cause, the same roll at a
tightened cap DOES shed, and restoring the cap restores writes.

**The coverage gap it closes is the point.** `upgrade_drill.sh` rolls a fleet
and asserts the build lands on every seat, and contains no reference to lag,
shed, or any counter — checked, not assumed. So the only production defect the
roll procedure has ever produced was invisible to the only drill exercising
that procedure, and every green gate since 2026-08-20 has been silent about it
rather than reassuring.

### What was measured

At a paced ~200 writes/sec through the proxy — modest, and what an operator
would recognise — a controlled roll sheds **0** by the lag cause at the shipped
1000ms cap, on both a laptop and the 16-vCPU Linux gate box. That zero is
measured rather than unobserved: the positive control stalls a replica with
SIGSTOP behind a 50ms cap and the master sheds (250 writes on the gate box,
`writes_shed_widowed` unmoved, so it is the lag gate and not the widowed one).

**The first version of that control was wrong in an instructive way.** It
ramped the cap down and re-rolled at each step, expecting the roll itself to
manufacture lag. On the laptop it armed every time — 50, 350, 95 writes at a
50ms cap. On the Linux gate box it shed NOTHING even at 1ms, and the drill
failed on its own positive control. That failure was correct: a roll sheds
only while a replica is LIVE and BEHIND, and the width of that window belongs
to the machine, not the product. A fast box re-syncs a restarted replica
before any lag accumulates. So the control armed on slow hardware and quietly
stopped arming on fast hardware — where it would have gone green while
testing nothing, while asserting that control 2's zero meant something.

**At an unpaced firehose the same roll shed 20651.** That number is true and
proves nothing about the claim: this file already records that a replay at
firehose rate recreates the lag that caused the shed, and `slo.md`'s no-stall
row makes no promise about a client saturating the pipe. A first cut of the
drill asserted zero against that load and failed by construction — a test that
red-lights on behaviour the docs explicitly permit, which is the shape this
file warns about two sections above. The rate is now a knob
(`ROLL_SHED_BATCH`, `ROLL_SHED_GAP`) precisely because the interesting question
is at WHICH rate a roll begins to shed.

### What this does NOT establish

**The production sighting is not reproduced.** 210 writes shed on the
playground during an ordinary rc.59 roll; the same procedure on loopback at a
comparable rate sheds nothing. The gap is unexplained, and the honest list of
candidates is: a real network RTT the loopback pair does not have, disk
behaviour under a real working set, fleet size, or a playground write rate
materially above the paced default here.

So `slo.md`'s no-stall row stays as written. It is still contradicted by one
production observation, and it is now also supported by a repeatable local
measurement — which is a sharper disagreement than before, not a resolution.
The next step is a rate sweep to find the threshold, and if none is found
below firehose rate, the difference is environmental and the playground is
where it must be measured.

### A defect found on the way

The ramp asked for `lag-hard-ms 200` and the seat reported 500:
`set_lag_hard_ms` clamps to `v.max(lag_soft_ms)` and says nothing. Filed as
BUG-0043. Without the drill's read-back the positive control would have tested
one threshold five times while reporting five.

## 2026-08-22, later — the sweep's axis was mislabelled, and the real variable is burst size

A rate sweep was run on the 16-vCPU gate box to answer "at WHICH rate does a
roll begin to shed at the shipped cap". Nominal 200, 500, 1000, 2000, 5000 and
10000 writes/sec: **all six shed zero.**

**That table's rate axis does not mean what it says, and the finding is the
instrument, not the result.** The load generator sends BATCH commands through
one `valkey-cli --pipe`, sleeps GAP, and repeats. Nominal rate was computed as
BATCH/GAP — but each cycle also pays a process spawn, a TCP connect and an
auth, and that overhead is not in the arithmetic. Delivered throughput is
`BATCH / (pipe_time + spawn + GAP)`, which was never measured. Varying BATCH
with GAP fixed therefore varied the BURST SIZE far more than the rate.

Three follow-ups on the laptop, at GAP=0 so no sleep confounds it:

| burst, one `--pipe` | shed at the shipped cap |
|---|---|
| 50 | 0 |
| 500 | 0 |
| 4000 | **416** |

So the threshold is a single uninterrupted pipeline burst somewhere between
500 and 4000 commands. Small batches never shed however tightly they are
looped, because the per-invocation overhead is itself a throttle that lets the
replica drain. **The gap was not the variable either** — GAP=0 at BATCH=50
sheds nothing, which killed that hypothesis as soon as it was tested.

Magnitude is highly variable at a fixed shape: the same BATCH=4000/GAP=0 run
shed 416 here and 20651 earlier the same day. That is what a threshold
phenomenon looks like — once lag crosses the cap, how much sheds depends on
how long it stays crossed — and it means single-run magnitudes should not be
compared.

### The cell that decides it, and is not yet filled

**Large burst on LINUX has not been tested.** Every shed observed so far is on
macOS; every Linux run used a paced generator whose overhead throttled it. The
playground is Linux, so until loopback-Linux is driven at a 4000-command burst
the production 210 has not been given a fair chance to reproduce.

If Linux sheds there, rate/burst explains the playground and `slo.md`'s
no-stall row needs a qualifier about sustained bursts. If Linux does NOT shed
at any burst, the shed is macOS-only on loopback — the rc.15 class, where a
platform difference hid a whole behaviour — and the production sighting is
environmental (real network RTT, disk, fleet size), leaving the playground the
only place it can be measured.

## 2026-08-22, decisive — LINUX does not shed at any burst, and every shed so far was macOS

The cell the previous section named as empty is now filled. On the 16-vCPU
Linux gate box, `GAP=0` at the shipped 1000ms cap:

| burst, one `--pipe` | shed |
|---|---|
| 500 | 0 |
| 4000 | 0 |
| 20000 | 0 |

macOS sheds at burst 4000. Linux sheds at nothing, including a single
uninterrupted 20000-command pipeline.

**So every loopback shed ever recorded against this bug is macOS.** That is
the rc.15 class again — a platform difference that hides an entire behaviour —
though pointing the other way: rc.15 was macOS PERMITTING something Linux
refused, and here macOS EXHIBITS a shed Linux does not. The playground is
Linux.

### What follows, and what does not

**Follows:** the production 210 is not a burst-or-rate phenomenon that loopback
can reproduce. Loopback-Linux replication keeps up with anything this
generator can produce, so the lag cap never engages, so there is nothing on
that machine for rate to explain. The remaining candidates are environmental —
a real network RTT between seats, disk under a real working set, fleet size —
and the playground is the only place they exist.

**Does NOT follow: that Linux cannot be made to shed.** This experiment cannot
distinguish "the server kept up" from "the client could not push hard enough".
Delivered throughput was never measured — the same gap that made the earlier
rate sweep measure burst size instead of rate — and `valkey-cli --pipe` is a
single-connection generator competing for the same CPUs. A saturating client
(`valkey-benchmark -c`, several connections) might well reach the cap. The
claim supported is the narrow one: **at these load shapes, on this machine,
the lag gate does not engage.**

### The consequence for the drill

`roll_shed_drill.sh`'s control 2 cannot fail from load on the Linux gate box,
because no load reaches the cap there. Its regression value on that machine is
limited to catching a change that makes a roll shed for some OTHER reason.

**Control 3 is what carries the drill on Linux**, which is exactly why forcing
the condition with SIGSTOP rather than provoking it with load was necessary
rather than tidy. Had the positive control stayed load-based, the drill would
now be green on the gate box while exercising nothing at all — the failure it
was written to prevent, arrived at by a different road.
