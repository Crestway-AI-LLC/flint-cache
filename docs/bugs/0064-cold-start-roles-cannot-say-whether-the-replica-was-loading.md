# BUG-0064: `cold_start_roles` cannot say whether the replica was loading or absent

Status: **OPEN in its original half; the release-blocking half is FIXED
2026-09-04.** Found 2026-08-27 while confirming BUG-0014 had not fired.

- FIXED: the verify-after-topology race that reddened the **rc.68 release
  gate** in `decommission` (`f9c194d` waits for what `verify` asserts rather
  than a proxy of it), and `36435a8` audited all 18 drills that call `verify`
  for the same shape and found no further instances.
- STILL OPEN: the finding this bug was filed for, which no commit can close.
  `cold_start_roles` still cannot say WHICH of the two faults it saw. The
  assertion is deliberately unchanged; only the next firing, now instrumented,
  will say. Closing this on the strength of the `decommission` fix would be
  recording an answer that was never obtained.

Severity: unknown, and that is the finding — the drill's failure text asserts
a **product** fault ("the pair is storing one copy") for a condition it cannot
distinguish from a **timing** one.

## What is established

`cold_start_roles` has failed **2 of the last 60** gate runs on `main`, with
byte-identical output both times:

    == now the real cold start
    FAIL: live_replicas 0 after cold start — the pair is storing one copy

| run | when | commit |
|---|---|---|
| `33019737250` | 2026-08-26T22:27Z | `631600f2` |
| `33101102664` | 2026-08-27T17:58Z | `60e09b4c` |

**Not caused by BUG-0061's teardown reorder.** `cold_start_roles` is one of
the 46 drills that reorder touched, so it was the first suspect. The 08-26
failure predates the reorder (`7430aeb` is not an ancestor of `631600f2`) and
the text is identical on both sides of it. Ruled out by date, not by argument.

## Why the failure text cannot be trusted as stated

The assertion is a 40-second poll:

    for _ in $(seq 1 80); do [ "$(replicas_of "$MASTER")" = "1" ] && break; sleep 0.5; done

`replicas_of` reads `live_replicas` from the MASTER. **A replica still inside
its load counts as an absent one there.** So two entirely different faults
produce that identical line:

- the budget expired while the replica was still loading — a timing fault on a
  shared runner, and the fleet is fine;
- the replica finished loading and did not attach — the silent single-copy
  fleet of **BUG-0008**, which is marked RESOLVED 2026-08-08 at severity high.

The drill already knows this distinction exists. Twelve lines above the
assertion, `role_decided` carries the comment:

> A ROLE IS DECIDED, NOT MERELY PRESENT. Since #176 a node binds and answers
> FLINTINFO from INSIDE its load, and `role:` reads `loading` there.

The lesson was learned for `role_of` and never carried to `replicas_of`, in
the same file — the same shape as OPS-0037, where a functional probe was
written for one lane and `is-active` left three lines later for the next.

It is also OPS-0045 one layer over: the ops agent read a re-seeding seat as a
dead one because `world::NodeState` had no readiness field. Here a drill reads
a loading replica as an absent one because the assertion never asks.

## What was changed here, and what was not

**The assertion is unchanged.** A genuine replication failure still fails, and
widening the budget to make CI quiet would be the exact move that hides a
BUG-0008 regression.

What changed is what a failure PRINTS. On timeout the drill now dumps each
seat's `role`, `loading`, `live_replicas` and `seq_lag`, and states the reading
rule outright:

    loading=1 on the replica     -> the budget expired mid-load; a timing fault
    loading=0, live_replicas=0   -> it finished loading and did not attach;
                                    that is BUG-0008's single-copy fleet

Same move as BUG-0014's instrument fix, for the same reason: at ~3% the next
firing is the next evidence, and it is worth more than any local reproduction.
Costing another read of the same ambiguous line is the avoidable waste.

## Not established

- Which of the two it is. Two samples, no diagnostic — that is precisely why
  this is filed rather than fixed.
- Whether BUG-0008 regressed. Nothing here shows that; it shows the drill
  cannot rule it out.

## Context worth carrying: the gate on main is red ~15% of runs

Nine of the last sixty, and nobody is triaging them:

| drill | failures |
|---|---|
| `promote_notice` | 4 (all predate `afbed3d`, the BUG-0054/0055 drill fix — see below) |
| `cold_start_roles` | 2 (this bug) |
| `upgrade` | 1 |
| `slot_cutover_recovery` + `snapshot_restore` | 1 |
| no `GATES FAILED` line at all | 1 |

BUG-0014 recorded why these go unread: a run that is re-run green reports as
`success`, and the failure survives only as `attempt=1`. **These nine were not
re-run — they were simply never opened.** That is a second way for a firing to
go unexamined, and it needs no re-run to happen.

## Measured 2026-08-27: the promote_notice fix held

Four of the nine red runs were `promote_notice`. Anchored on `afbed3d` — the
commit that actually changed the drill ("assert the mechanism, not a
difference of two spawns"), not the bug's headline SHA, which a first pass got
wrong — the split is:

| | runs | promote_notice failures |
|---|---|---|
| pre-fix | 24 | 4 (~17%) |
| post-fix (contain `afbed3d`) | 36 | **0** |

At the pre-fix rate, 36 clean runs has probability ~(0.83)^36 ≈ 0.1%. That is
evidence the wall-clock replacement worked, not proof — but it means four of
the nine unread failures are already explained and need no further triage.
Remaining unexplained: the two `cold_start_roles` firings this bug is about,
one `upgrade`, one `slot_cutover_recovery`+`snapshot_restore`, and one run
with no `GATES FAILED` line at all.

## All nine triaged, 2026-08-27 — the backlog is now zero

| runs | drill | verdict |
|---|---|---|
| 4 | `promote_notice` | pre-fix; fix measured effective (0/36 since) |
| 2 | `cold_start_roles` | this bug; diagnostic now in the drill |
| 1 | `upgrade` | `THROTTLED no live replica` on the post-upgrade round trip — the replica was still catching up after the roll and the quorum gate throttled the verify write. The readiness family again (#176, OPS-0045, this bug), one sample; file on recurrence |
| 1 | `slot_cutover_recovery` + `snapshot_restore` | dest seat `Connection refused` mid-recovery, TWO drills red in one run — smells environmental (loaded runner / cross-drill), one sample; file on recurrence |
| 1 | — | **cancelled** at 15 min, both jobs — superseded by a later push, not a failure at all |

So the honest restatement of "red ~15% and none triaged": **8 real failures in
59 completed runs (~14%)**, of which four are already fixed, two carry this
bug's diagnostic, and two are single-sample observations recorded here to be
filed if they recur. The number that was alarming was partly an artifact of
nobody looking — which was the point of writing it down.

## 2026-09-03 — the backlog re-formed, and it is the parallel gate, not eleven bugs

The section above closed with "the backlog is now zero" on 2026-08-27. Six gate
failures have landed since and none had been opened. Over the last 120 runs the
rate is **17 failures / 120 ≈ 14%**, unchanged from the number that section
called alarming — so the triage was a one-off, not a habit.

Triaged, all six:

| run | when (UTC) | failed | commit under test |
|---|---|---|---|
| `33722919171` | 09-03 06:22 | `backup_seat` | **docs only** (an ADR write-up) |
| `33702745608` | 09-03 01:12 | `restart`, `roll_shed` | a bug file + one Rust change |
| `33700917180` | 09-03 00:46 | `failover`, `roll_shed` | **docs only** (two bug headers) |
| `33667073809` | 09-02 18:25 | `failover` | docs only |
| `33660742234` | 09-02 17:23 | `tenant_quota`, `client_compat`, `proxy_registry` | a bug file + a test |
| `33658796083` | 09-02 17:04 | `decommission`, `ctl_cpha`, `pipeline_nodelay` | a drill fix |
| `33888567605` | 09-04 15:17 | `decommission` | **the v0.1.0-rc.68 RELEASE gate** — and the same sha was green on `main` hours earlier |

**Eleven distinct drills across six runs**, with only `failover` and
`roll_shed` appearing twice. Nothing is failing consistently.

**Three of the six are commits that changed no code at all** — bug-file text
and an ADR. A docs-only commit cannot break `backup_seat`'s bootstrap or
`failover`. That is not an inference about probability; it is the whole
diagnosis.

**And the signature is in the log.** `backup_seat`, which failed bootstrap in
3.7 s:

    env [guard]: load 2.47, 2.11, 0.98 | no sibling processes
    (3 seat(s) belong to 4 live peer drill(s) in this suite -- not foreign)
    (FLINT_DRILL_FORCE=1: proceeding despite 2 other flint process(es))
    == bootstrap: the inventory's backup keys must bring the seat up
    FAIL: bootstrap

Four peer drills live, two other flint processes, `FLINT_DRILL_FORCE=1`
overriding the guard that would otherwise have refused, and a bootstrap that
loses in under four seconds.

### So the finding is one thing, not eleven

**These are not eleven flaky drills. They are one parallel gate, and which
drill loses is close to arbitrary** — that is why the failures scatter and why
no single drill looks broken enough to chase.

**Not "starvation", and the first draft of this line said so wrongly.**
`gate.yml` records the runner as **4 vCPU** and sets `FLINT_GATE_JOBS: 4`, so
the gate runs at **one drill per core** — deliberately conservative; the file
notes P=6 would be 1.5 drills/core. And `backup_seat`'s own guard line reads
`load 2.47` against those four cores, roughly 62% busy. That is not a starved
box. Whatever the interference is, raw CPU exhaustion is not established and
the word was reached for because it sounded like an explanation.

Two consequences worth stating plainly:

- **Triaging drill-by-drill is the wrong unit of work**, and this file spent an
  evening doing it on 2026-08-27. The nine it triaged then were the same
  phenomenon; the four `promote_notice` firings really were a drill bug and
  really were fixed, which is exactly what made the rest look like more of the
  same rather than a different thing.
- **A ~14% red gate trains people to re-run**, which BUG-0014 already recorded
  as how a real firing goes unread. Every one of these six was a candidate for
  that, and none of them were re-run — they were simply never opened, for the
  second time in a week.

### Not established

Whether the contention is CPU, ports, or something the existing overlap checks
do not cover. This project has assert_no_port_overlap, assert_no_scope_overlap,
assert_no_used_path_overlap, the kill-pattern check and the binding-argument
check — all built because parallel drills interfere — and the gate is still red
at 14%. So either the remaining interference is a dimension none of them look
at, or it is plain resource starvation on the runner. Those need different
fixes and this does not distinguish them.

The cheap next measurement is `FLINT_GATE_JOBS=1` on a few runs: if the red
rate collapses it is contention and the question becomes which dimension; if it
does not, the parallelism is exonerated and the cause is somewhere nobody has
looked.

**And the workflow already has the controls for it**, which is worth knowing
before anyone builds a harness: `workflow_dispatch` takes `gate_jobs` (the
description says *"Set 1 here for the serial gate; that is also the rollback"*)
and `core_order`, for *"a TARGETED experiment instead of a 12-minute full
gate"* — added because *"contention hypotheses have to be tested on the machine
that has the contention"*. The experiment this file wants is a dispatch with
two inputs, not a new tool.

A caveat on reading the result: at ~14%, telling P=1 from P=4 needs enough runs
to separate the rates. Three green serial runs would be weak evidence — at 14%
the chance of three clean runs by luck alone is about 64%.

## 2026-09-03, later — two contention proxies measured, neither separates, and the planned experiment is now ambiguous

The section above leaves "CPU, ports, or something else" open and proposes a
`gate_jobs=1` dispatch. Before spending ~20 gate runs on a binary outcome, two
proxies that cost nothing were read out of artifacts already on disk — five
failing runs against four passing ones.

**Proxy 1: runner load.** Every guarded drill prints `load` at its own start,
so a run yields ~119 samples.

| | median load | max load |
|---|---|---|
| failing runs | 2.48 – 2.84 | 4.12 – 5.66 |
| passing runs | 2.58 – 2.67 | 4.32 – 4.59 |

Fully overlapping, and one **failing** run peaked at 4.12 — lower than every
passing run. Per-drill is no better: `backup_seat` failed at load 2.47, inside
the passing range of 1.75 – 2.57.

**Proxy 2: concurrent flint processes the guard could not attribute.** CI sets
`FLINT_DRILL_FORCE: "1"` (`gate.yml:155`), so the guard reports and proceeds
rather than refusing — necessary for a parallel gate, and it means every run
logs how many processes it went ahead despite.

| | drills forced | max unattributed processes |
|---|---|---|
| failing runs | 22 – 26 | 4 – 10 |
| passing runs | 23 – 27 | 4 – 10 |

Identical ranges. Both proxies rule out a LARGE effect on n=9; neither rules out
a small one.

### The consequence: "not the commit" was read as "the parallelism"

The section above establishes something real — three failing commits changed no
code, so the COMMIT is not the cause, and eleven drills across six runs means no
single drill is broken. Both are sound. **Neither shows that parallelism is the
cause.** They eliminate per-commit regressions and per-drill bugs and leave
"environmental", and parallelism is one candidate in that class, not the class
itself. The write-up moves from the first to the second in one sentence, and the
two proxies above are the first evidence bearing on it — both negative.

**A competing candidate, with support that landed today.** BUG-0088: `failover`
failed twice by projecting a write wait of **2017 ms and 2033 ms against a
2000 ms deadline** — margins of 0.85% and 1.65%. That is a timing threshold
being missed by about one percent, which a slower-than-usual shared runner
produces without any help from Flint's own parallelism. The same drill on an
unloaded laptop peaks at 18–29 ms against that deadline, so the CI figure is
roughly two orders of magnitude out.

**Which makes the proposed experiment ambiguous as designed.** `gate_jobs=1`
lowers contention AND lowers timing pressure at once. If the red rate collapses,
that result cannot distinguish "parallel drills interfere" from "the runner is
slow and fewer things were racing the clock" — and those need different fixes,
which is exactly the distinction this file said it could not make.

### A cheaper and sharper version of the same experiment

BUG-0088's `write_wait_peak_ms` landed today and every gate run now records a
continuous margin instead of a pass/fail bit. That changes the arithmetic: the
three-green-runs problem (64% likely by luck at 14%) exists only because the
outcome is binary. Comparing peak distributions between P=1 and P=4 needs far
fewer runs than comparing failure rates, and it answers the sharper question —
whether serial running moves the timing margin at all, which is what separates
the two hypotheses.

So the recommendation is unchanged in shape and cheaper in cost: let the
instrument accumulate a few dozen P=4 peaks from ordinary gate runs, which costs
nothing because those runs happen anyway, then dispatch a handful at P=1 and
compare the distributions rather than the verdicts.

## 2026-09-03, later still — this bug's shape is in 23 drills, and fixing the instrument diagnosed an excluded drill in one run

Reading the actual FAIL line out of every failing drill across the six red runs,
rather than counting drill names:

| failure | count |
|---|---|
| `FAIL: bootstrap` (bare, no reason) | 3 |
| replica still `loading` when verify ran -> `SINGLE-COPY` | 2 of those 3, once captured |
| `FAIL connect ... Error 111` | 1 |
| `FAIL: with the replica stalled 1500ms behind a 50ms cap, the master shed` | 2 |
| `FAIL: master unchanged`, `FAIL: arm A client` | 2 |

**Seat bring-up is the largest cluster, and it was the least diagnosable.** Two
drills that captured `bootstrap`'s output named the cause on sight — a replica
still `loading` when `verify` ran, which is this bug's subject firing outside
`cold_start_roles`. A third printed `FAIL: bootstrap` and nothing else.

### Why: 23 drills sent the reason to /dev/null

- **9 drills** ran `bootstrap >/dev/null 2>&1 || { echo "FAIL: bootstrap"; exit 1; }`.
  The reason existed and was discarded one character before it could be printed.
- **14 drills** were worse: `bootstrap >/dev/null 2>&1` with **no exit check at
  all**, under `set -u` rather than `set -e`. A refused or failed bootstrap did
  not stop them. They ran on into their assertions and reported whichever
  tripped first — `cold_start_roles` announcing **"FAIL: no replication after
  bootstrap"**, a product claim, for what may simply have been "bootstrap failed
  and the reason went to /dev/null".

That last line is this bug's thesis verbatim, and this file was named after the
one drill it was noticed in. It is in 23.

All 23 now capture and report, and `assert_bootstrap_failures_say_why` in
`tools/gates.sh` keeps them that way — tri-state (zero drills scanned is a
FAILURE, not a pass) and mutation-tested by reintroducing a single discard.

### The change paid for itself on its first run

`stop_sweep` has sat in the gate's `EXCLUDED` list with this note: *"FAILS in
setup: 'fleet B did not start'. It declares eight ports across two fleets, so a
collision is the first thing to check."*

**It was not a collision.** With the output captured, one run said so:

    flintctl: refusing `bootstrap`: this flintctl reports build "0.0.1", which
    is not a release, and the inventory does not declare `disposable on`.

`$INVA` declares `disposable on`; `$INVB` never did. flintctl refused to
bootstrap fleet B, the refusal went to `/dev/null`, the exit status was ignored,
and the only symptom anyone ever saw was a later assertion counting fleet B's
processes and finding none. A one-line inventory omission, recorded as a
suspected port collision for as long as that note has existed.

Fixed. Both fleets now start (`A=5 procs, B=5 procs`) and the drill reaches its
real test, where it fails differently and honestly: **"FAIL: second start did
not re-record pids"** — a claim about `start` over a live fleet, not about
setup. It stays EXCLUDED on that, and the note now records the true symptom so
the next person does not start from the port theory again.

### What this says about the 14% red gate

It does not explain all of it, and it is not the parallelism theory either. What
it does is remove the largest source of *undiagnosable* reds: the next
`FAIL: bootstrap` in any of these 23 drills will arrive with the reason attached.
That is worth more than another hypothesis, because every hypothesis this file
has entertained failed for want of evidence that was being discarded at the
moment it was produced.

### A third instance, found the same way: `decommission`

`decommission` failed on the gate 2026-09-02 and again 2026-09-03 with
`FAIL: master unchanged (127.0.0.1:7221 -> 127.0.0.1:7221)`. Its own log
contradicts it two lines earlier:

    127.0.0.1:7221 demoted + drained; 127.0.0.1:7222 promoted at (0,3)
    == failover complete: 127.0.0.1:7222 is master; 127.0.0.1:7221 rejoined as replica
    == new master: 127.0.0.1:7221
    FAIL: master unchanged (127.0.0.1:7221 -> 127.0.0.1:7221)

The handoff worked. The read after it did not. The drill did `sleep 1` and then
took ONE sample, through `master() { status | awk '/master/{print $3; exit}'; }`
— first matching line wins, and the ex-master is listed first. A demoted master
RESTARTS and boots as **master from its durable role** before the demotion is
applied, which `flint-server` announces in its own log, so one second later the
old node can still legitimately be calling itself master.

So the message asserts a product fault — the failover did not move the master —
for a read taken during a documented transient. This file's thesis for the third
time, in a third drill.

Replaced with a bounded poll (30s) for a master that differs, and three
outcomes instead of two: no master at all, a master that genuinely never moved
(which now dumps `status`), and success. Passes locally, twice.

**Not claimed:** that the race is reproduced on demand. It is inferred from the
log's own contradiction plus the documented durable-role boot. What is certain
is that a single sample one second after a restart cannot distinguish the two,
which is enough to fix regardless of how often it bites.

### A fourth, and this one had already built the instrument it did not use

`roll_shed` failed twice on the gate with:

    FAIL: with the replica stalled 1500ms behind a 50ms cap, the master shed
          NOTHING by the lag cause.

Unlike the others this drill is careful — that IS a positive control failing to
arm, and it says so ("that is not a pass — it means control 2 above proves
nothing"). It names two causes: `writes_shed_lag` is not wired, or the stall
never reached the master's view of lag.

**There is a third, and it is the likely one on a contended runner: no write was
ever offered.** A gate cannot refuse what nobody sent, so a zero has a reading
that is not about the product at all — and the writer only has a 1.5 s window in
which to be scheduled.

The drill had already solved this and not connected it. `writer_stop` computes
`DELIVERED`, and its own header explains why it was written: *"any burst could
not be separated from this client cannot push hard enough to make it, because
nobody knew what the client actually delivered."* Control 3 never read it.

Now it does. A passing run reports the denominator that was missing —
**300 delivered, 250 shed by lag** — and a failing one splits in two:
`DELIVERED == 0` reports a harness/machine result that says nothing about the
gate (and warns that control 2's zero rests on it), while writes offered and
none shed keeps the original product-facing message, now carrying the count.
Both branches mutation-tested; still a FAILURE either way, because a control
that cannot arm is not a pass.

Four drills, one shape: `cold_start_roles`, `stop_sweep`, `decommission`,
`roll_shed`. In every case the instrument existed or was one line away, and the
verdict was reported as a fact about the product.

### `pipeline_nodelay` is a different class, and it points at memory

Unlike the four above, this drill's failure IS diagnosable — the log carries the
client traceback already:

    ConnectionResetError: [Errno 104] Connection reset by peer
    FAIL: arm A client
    line 27: 166405 Killed   $B --port 6407 --engine rocks --data-dir "$D/a"

**Its server was SIGKILLed in the middle of the run**, with 4 peer drills live.
`Killed` is signal 9, and `fleet_kill` is the only thing here that sends it — but
that path was checked and is properly scoped, boundary-tested on the path and
digit-anchored on the port, so a peer drill killing this seat is not the
explanation.

**The candidate is the OOM killer**, and it fits three things at once: it sends
SIGKILL; it is a MEMORY event, so the load figure printed beside it can be
unremarkable — which matches load having failed to separate five failing runs
from four passing ones above; and BUG-0060 established that every resource limit
in this product is per-unit with nothing bounding the aggregate. A gate running
`FLINT_GATE_JOBS: 4` drills at once, each with several RocksDB seats, is exactly
where an unbounded aggregate would be reached first.

**This is a hypothesis and the instrument is the response, not an argument.**
`fleet_env_note` now prints available memory beside the load on every guarded
drill, so each gate run records it — UNREADABLE on macOS rather than a
fabricated figure from `vm_stat`, since the gate runs on Linux and a local
invention would later be compared against real numbers.

Nothing is claimed until those accumulate. What can be said now is that the one
axis on which these failures could plausibly differ is the one nothing was
recording, which is the same position BUG-0088 was in this morning.

## 2026-09-03 — the P=1 experiment, DESIGN AND PREDICTION RECORDED BEFORE THE RESULT

This file has wanted a `gate_jobs=1` comparison since 2026-08-27 and never took
it, because at a 14% failure rate telling P=1 from P=4 needed ~20 runs to
separate the rates. **BUG-0088's `write_wait_peak_ms` removes that cost**: the
outcome is now a continuous margin rather than a pass/fail bit, so
distributions can be compared instead of rates.

**Baseline, and it was free.** Seven P=4 peaks from ordinary pushes:
124, 324, 495, 557, 763, 840, 1322 ms — median **557**, min **124**, max
**1322**.

**Arms.** Four `workflow_dispatch` runs at `gate_jobs=1`, full CORE list, all at
commit `6771559`. Runs `33822293332`, `33822297593`, `33822301564`,
`33822305667`. No matched P=4 arm is needed because the baseline above is the
same workflow on the same runner class.

**Recorded before looking, because this file has twice reached a conclusion the
evidence did not carry:**

- If **all four** P=1 peaks fall below the P=4 minimum (124 ms), parallel
  contention is the dominant term and the 14% red rate has a mechanism.
- If the P=1 **median lands inside the P=4 middle range** (~324-840 ms),
  parallelism is exonerated as the dominant term and the cause is the runner
  itself — which is what the 10.7x spread and the load/memory nulls already
  suggest.
- **Anything between those is inconclusive at n=4** and must be reported as
  such rather than argued into one of the two.

**Known confound, stated now:** the seven baseline peaks come from seven
different commits, all on `main` within a few hours and none touching the write
path, while the four dispatches are one commit. That biases toward the baseline
looking *more* variable than a single-commit sample would, so a P=1 arm that
merely looks tighter proves nothing; only its LOCATION relative to the baseline
range counts.

## 2026-09-03 — RESULT: parallelism is the dominant term, and the OOM hypothesis is dead

Four dispatches at `gate_jobs=1`, all green, all at commit `6771559`.

| arm | n | peaks (ms) | median | min | max |
|---|---|---|---|---|---|
| **P=1** | 4 | 48, 61, 69, 71 | **65** | 48 | **71** |
| **P=4** | 9 | 90, 124, 324, 495, 557, 763, 840, 955, 1322 | **557** | **90** | 1322 |

**The pre-registered condition is met:** every P=1 peak falls below the P=4
minimum, so parallel contention is the dominant term in the write-wait margin.
Medians differ **8.6x**.

**Two corrections to how that was set up, both against my own case.**

*The baseline was stale.* The prediction was written against n=7 with a minimum
of 124 ms. Two further P=4 runs had already landed (`51e4f68` at 955 ms,
`6771559` at 90 ms) and are included above. The P=4 minimum is therefore **90,
not 124**, and the gap to the P=1 maximum of 71 is **1.27x**, not 1.7x. The
condition still passes, on a much narrower margin than the original framing
would have suggested. Recorded because the threshold was fixed in advance
precisely so it could not be adjusted afterwards — and the honest report is that
the baseline moved underneath it, not that the margin was comfortable.

*The separation is carried by the TAIL, not the floor.* A P=4 run can be as
quiet as 90 ms — `6771559` was — so the two arms are adjacent at the low end.
What P=1 never produces is the upper half: 495 to 1322 ms, **8x to 19x** the P=1
maximum. Since the tail is what crosses the 2000 ms deadline, that is the part
that matters, but "P=1 is uniformly faster" would overstate it. P=4 is
*sometimes* as fast; it is sometimes fifteen times slower.

### The OOM hypothesis is refuted, by the instrument added for it

Lowest free memory recorded in each arm, on a 15.6 GiB runner:

| arm | lowest free |
|---|---|
| P=4 | 13.8 - 13.9 GiB |
| P=1 | 14.1 - 14.3 GiB |

The gate uses **under 1.8 GiB** at four-way parallelism and never approaches
pressure. The `pipeline_nodelay` server that was SIGKILLed mid-run was not
OOM-killed, and BUG-0060's unbounded aggregate is not what this gate is hitting.
That question is closed with a number a day after it was raised, which is what
the instrument was for.

### What this settles, and what it does not

**Settled:** the parallel gate, not the runner, is the dominant source of write-
wait pressure. The earlier reading here — that load and unattributed-process
counts failed to separate failing from passing runs, therefore the runner rather
than parallelism — was measuring the wrong quantity. Load does not separate
them; the write-wait margin separates them cleanly. A null on a proxy said
nothing about the thing the proxy stood for.

**Not settled:** which resource. Not memory, now measured. CPU remains open —
`FLINT_GATE_JOBS: 4` on 4 vCPU is one drill per core, and the file already
records a clean run at load 8.04, so raw exhaustion is not it either. Ports,
disk and lock contention are untested. The mechanism by which four drills triple
a write-wait margin is still unknown.

**Also not settled: whether this explains the 14% red rate.** It explains the
`failover` refusals, whose margin is now measured on both arms. It says nothing
yet about the bootstrap cluster, which was diagnosable-in-principle and is only
now being reported properly.

### Which resource: the terms are now recorded, with a prediction

Parallelism is established as the dominant term; the resource is not. `est_ms`
is `inflight x service_us`, so the peak now carries BOTH factors rather than
their product, and the next gate runs will say which one four-way parallelism
inflates:

- **inflight** rising means more offered load than the seat can take -- the
  client and proxy pushing harder, a queueing effect.
- **service_us** rising means the seat itself got slower per write -- disk,
  lock, or CPU steal.

They imply different fixes and the product cannot distinguish them.

**Baseline, this laptop, serial:** `peak 24ms (inflight 256 x service 97us)`,
and 256 x 97us = 24.8ms, so the packing is arithmetically coherent.

**Prediction, recorded before the CI data:** inflight is bounded by the client's
pipeline depth and by `max-conns`, so reaching the observed P=4 peak of 1322 ms
by queue depth alone would need an inflight around 13 600 — implausible. If
inflight instead stays near 256, **service_us must be doing the work: ~5 200us
per write against 97us here, a ~53x slowdown per write.** That would mean the
seat is being starved rather than over-offered, and would point at disk or
scheduler contention between parallel drills rather than at load arriving faster.

If the next runs instead show inflight climbing with service_us flat, this
prediction is wrong and the effect is queueing — which would be the more
comfortable answer and is not the one the arithmetic favours.

**Implementation note.** One packed atomic, `est_ms:16 | inflight:16 |
service_us:32`, so `fetch_max` orders by the peak and the surviving value
carries its OWN terms. Three separate atomics would let a later write's inflight
land beside an earlier write's peak — a torn reading indistinguishable from
data. Three tests cover it, including that a larger peak with minimal terms
still beats a smaller peak with maximal ones, which is the property the layout
exists for.

### The prediction held: service time moved, queue depth did not

First CI run carrying the terms (`9620d72`, P=4). Both refusals:

    THROTTLED write would wait ~2002ms (inflight 244 x service 8206us)
    THROTTLED write would wait ~2009ms (inflight 218 x service 9216us)

Laptop, serial, two runs: `inflight 256 x service 97us` and
`inflight 65 x service 885us`.

**Inflight did not inflate.** 218 and 244 sit inside the serial range of 65-256
— if anything lower than the higher serial sample. **Service time did**:
8206 and 9216us against a serial 97-885us.

Stated with the right precision: the multiple depends on which serial sample you
compare against, so it is "an order of magnitude or more per write", not a
figure to two significant digits — n=2 on the serial side. The qualitative
result is what matters and it is unambiguous: **the term that moves under
parallelism is per-write service time, not queue depth.**

So the seat is **starved, not over-offered**. Four drills do not make the client
push harder; they make each write take far longer to serve. That excludes
queueing and admission-side explanations and points at disk or scheduler
contention. It does not yet separate those two.

**Gap worth naming:** the four P=1 runs predate the terms instrument, so there
is no serial CI sample — only a laptop one, on different hardware. A P=1
dispatch now would produce the matched arm and is four runs.

### The instrument aborted the run it was diagnosing, and that was my defect

The peak line never printed on that run: the log stops after
`errors: 2, replies: 20000`, with no FAIL and no diagnosis.

`failover_drill.sh` runs under `set -euo pipefail`, and the added line was
`INFO=$(valkey-cli ... | tr -d '\r')`. The master was refusing writes and had
just lost its replication link, so FLINTINFO returned non-zero, `pipefail`
carried it through the substitution, and `set -e` exited the script mid-run —
**silently**. A diagnostic that can abort the run it is diagnosing is worse than
no diagnostic, and it is precisely the class this file has spent the day
removing. The two other drills edited today (`decommission`, `roll_shed`) use
`set -u` only and were checked rather than assumed.

Fixed with `|| true` and three outcomes, since two of them are different faults:
the seat not answering is transient and now reports UNREADABLE and **continues**
to the real assertions, while INFO answering *without* the fields is the build
having lost the instrument and still fails. Both branches mutation-tested.

### Narrowing the resource: fsync is periodic, so it is a poor candidate

The service-time result needs the WAL fsync excluded or implicated, so the drill
now reports both the cadence and the count during the load:

    == wal fsync: cadence 500ms (periodic, not per-write), 0 fsync(s) during the load of 20000 writes

**The fsync here is on a timer, not on the write.** A write pays for one only if
it lands on a tick, which makes a uniform per-write service inflation an
unlikely thing for it to cause directly. It does not eliminate the disk — the
same device serves compaction and the memtable flush — but it does mean the
mechanism is not "each write now waits for a durable sync".

**Two wrong readings were nearly shipped from this one spot in a single
sitting**, and both are worth recording because each looked finished:

1. A **count alone**, dismissed as structurally zero on the grounds that
   `WAL_FSYNC_MS` defaults to 0. The static does default to 0; the running seat
   reports **500ms**. The number was real and the reasoning about it was not.
2. Then a **cadence alone**, worded as "fsync IS part of the service time
   above" — which reads as per-write and is false for a timer.

The first would have deleted a live measurement; the second would have
misattributed the service inflation to durability. Both numbers, with the
periodic caveat, or neither.

**Still open:** disk contention from compaction, CPU scheduling, and RocksDB
internal locking are all consistent with what is now measured. Separating them
needs either a serial CI arm carrying these fields (four dispatches) or a
per-write breakdown the seat does not currently expose.

### The next fork is free: RocksDB already says whether it is throttling

`FLINTINFO` carries the engine's own backpressure signals and the drill already
reads INFO, so this needed no server change:

    == engine backpressure at read: write_stopped=0 delayed_write_rate=0 l0_files=1 pending_compaction_bytes=0

**Serial baseline: the engine is entirely quiet** at 179us service time. Nothing
stopped, nothing delayed, one L0 file, no compaction debt.

That makes the P=4 comparison a clean fork, with both branches meaningful:

- **Loud** — `write_stopped`, a non-zero `delayed_write_rate`, or growing
  `l0_files`/`pending_compaction_bytes` — means RocksDB is throttling the write
  path deliberately, and the ~10x service inflation is compaction debt from four
  drills sharing one disk. That is a capacity problem with known knobs, and it
  connects directly to BUG-0013.
- **Quiet** — all of them near zero while service time is still ~8000us — means
  the engine believes it is healthy and the time is going somewhere it cannot
  see: the OS descheduling the process, or contention below the filesystem. That
  is not a Flint tuning problem at all, and would make the gate's flakiness an
  artefact of running four seat-heavy drills on a 4-vCPU shared runner.

**Asymmetry worth stating in advance:** the reading is instantaneous at INFO
time, not at the peak, so debt can drain in between. A LOUD reading is therefore
conclusive and a QUIET one is only suggestive — the fork is not symmetric and a
quiet P=4 result will need the reading moved next to the peak before it can
carry the second conclusion.

## 2026-09-03 — MATCHED ARMS at one commit: the engine is not the bottleneck

Four `gate_jobs=1` dispatches at `afb4eb1`, against that same commit's own P=4
push run. Same code, same workflow, same runner class — the earlier comparison
spanned commits and this one does not.

| | P=1 (n=4) | P=4 (n=1, same commit) |
|---|---|---|
| peak | 65, 88, 73, 79 ms | **843 ms** |
| **inflight** | **256, 256, 256, 256** | **256** |
| service_us | 255, 346, 287, 309 | **3293** |
| write_stopped | 0, 0, 0, 0 | 0 |
| delayed_write_rate | 0, 0, 0, 0 | 0 |
| l0_files | 1, 1, 1, 1 | 1 |
| pending_compaction_bytes | 0, 0, 0, 0 | 0 |
| fsyncs during load | 0, 0, 0, 0 | 0 |

**Inflight is identical — 256 in every run of both arms.** Not "similar":
identical. Queue depth is definitively not the mechanism, which retires the
whole class of admission-side and offered-load explanations.

**Per-write service time is ~11x higher** (median 298us serial against 3293us),
and it is the only column that moves.

**Every engine-visible signal is byte-identical between the arms.** RocksDB is
not stopping writes, not delaying them, holds one L0 file, carries no compaction
debt, and syncs nothing. It reports itself perfectly healthy in both arms while
taking eleven times longer per write in one of them.

Corroborated by the two refusals from the previous commit, which sampled the
same fields at the moment of failure rather than at a peak: inflight 244 and
218 — inside the serial range — with service 8206us and 9216us.

### So the time is going where the engine cannot see it

Not queueing, not admission, not compaction, not fsync, not memory (13.8+ GiB
free throughout). What remains is beneath or beside Flint: the OS descheduling
the process, or contention below the filesystem, on a 4 vCPU shared runner
asked to run four seat-heavy drills at once.

**That makes the gate's flakiness a property of the runner shape rather than a
Flint defect** — which is worth stating plainly because three investigations
have now looked for a Flint defect and this file spent an evening triaging
drills one at a time on that assumption.

### The caveat that was registered in advance, honoured

The backpressure figures above are read AFTER the load, and debt can drain in
between — recorded in this file before the first result, not after. So a quiet
reading is strong here and not yet conclusive, on that axis alone. The
during-load sampler landed in `59a1c58`, *after* these runs, and takes a maximum
across the whole load window; the next P=4 run carries it, and a quiet maximum
there closes the last gap.

**Also thin:** the matched P=4 arm is a single run. The P=1 arm is four. The
peak distribution has n=9 on the P=4 side but the TERMS have n=1 at this commit
plus the two refusal samples above. Nothing here rests on the P=4 service figure
being 3293 rather than some other large number — only on it being an order of
magnitude above 298 with inflight unchanged, which all three P=4 samples agree
on.

### The during-load sampler was added, measured, and removed

It was added to close the caveat above — a backpressure maximum taken across the
load window cannot have drained by the time it is read. It does not work, for a
reason the arithmetic gives directly.

**It yields one sample.** At 256 in flight and ~6ms service the seat sustains
~41 000 writes/s, so a 20 000-key load lasts about **half a second**. A 200ms
poller sees one or two ticks of it. The 200ms interval had been chosen against
an assumed multi-second load; the load is not multi-second.

**And tightening it costs the metric under study.** Each sample is a FLINTINFO,
which queries RocksDB properties. The two runs carrying the sampler report
service **6353us and 6149us**; the three without it report **3282, 3293 and
4362us**. On the PEAK there is no visible effect at all — the sampler runs are
412ms and 1574ms, the lowest and the highest of the five — so this is n=2 and
not a demonstrated bias. But it is a plausible bias in the exact number the
investigation turns on, bought for one sample.

Removed. **The after-load caveat therefore stands**, and closing it needs
in-process sampling on the server rather than an external poller: at half a
second of wall clock there is no external polling rate that is both frequent
enough to characterise the window and cheap enough not to enter the result.

### What running the gate serially would actually cost

Since the finding points at the runner, the obvious response is to lower
`FLINT_GATE_JOBS`. Measured, rather than assumed:

| | wall clock |
|---|---|
| P=4 push gates (n=5) | 11m32s, 11m41s, 11m46s, 11m48s, 12m22s — median **11m46s** |
| P=1 dispatches (n=4) | 32m36s, 33m02s, 33m31s, 33m36s — median **33m17s** |

**2.8x slower**, about 21 minutes added per gate. That is the price of the
serial arm, and it is not obviously worth paying for a ~14% flake rate on a
suite whose failures are now largely diagnosable. An intermediate P (2 or 3) is
untested and would cost two more dispatch batches to characterise.

This is a decision about how much gate latency the project will spend to buy
determinism, which is not a decision this file should make on its own.


## 2026-09-04 — it stopped a release, and the ops repo has the same shape

`decommission` failed the **release gate** for `v0.1.0-rc.68`
(run `33888567605`), on `ce04511` — a sha whose gate was **green on `main`**
a few hours earlier. Same bytes, opposite verdicts, which is this bug's
thesis stated by the release process instead of by a drill.

    FAIL  decommission (6.4s)
          FAIL: verify still red after the member came back
          FAIL pair 0 replicating  SINGLE-COPY: every member up, but 0 of 1
               streaming from 127.0.0.1:7221 — one disk holds the only copy

`release.sh` then refused to build: *"public gate is 'failure' at ce04511 —
rc.53 was tagged on a red-gate commit. Do not repeat it."* The cut is
**stopped pending this bug** (Jeff, 2026-09-04) rather than re-run to green,
because the script keys on the LATEST run at that sha, so retrying would flip
the verdict by mechanism rather than by evidence.

### The same signature, measured in the ops repo the night before

Worth having because it puts numbers on the load dependence this bug
suspects. `flint-cache-ops`' `saas_fulfillment` produced the identical line
once its `flintctl bootstrap` output stopped being discarded (OPS-0117):

    pair 0    127.0.0.1:7103  loading epoch  build unstamped

| where | load | result |
|---|---|---|
| laptop, a peer's suite also running | 2.9–3.6 | **2 failures in 8** |
| idle gate box (c7i.xlarge) | 0.36 | **0 failures in 12** |

So the race opens under contention and shuts when the box is quiet, which
matches "the runner is 4 vCPU at one drill per core" exactly.

### What the ops side did about it, if the pattern is useful here

`fleet_bootstrap` in the ops repo distinguishes *not yet* from *broken*,
which is the distinction this bug says the drills cannot make. A failure whose
log shows `loading` is re-verified on a bounded budget and **says that it
waited**; every other failure stays immediately fatal with its reason.
Retrying until it passes would recreate the defect in a new form — waiting for
a convergence condition that is named in the log does not. Its positive
control had to be built separately, because twelve clean runs on an idle box
never exercised the wait at all.

Offered, not applied: these four drills are this repo's, and the shape may or
may not fit them.

### Audited for more of the same, and there are none

The rc.68 release gate died on `decommission` asserting `verify` green one
attempt after restarting a downed member — waiting on liveness, asserting
streaming. Since that is the fifth instance of this family, the obvious question
is how many more exist.

**Method.** Every `verify` call in every drill, checked for a topology-changing
`flintctl` command (`start`, `restart`, `expand`, `add-replica`, `swap-node`,
`roll-node`, `decommission-node`, `upgrade`, `promote`) within the ten lines
above it, and for a bounded retry within six lines around it. 18 drills call
`verify`; most without a retry, which is correct — a `verify` that fails from a
settled cluster SHOULD fail, and adding retries everywhere would mask exactly
the failures these drills exist to catch.

**Result: two hits, both false positives, both in `decommission` itself.** The
topology command near them is the negative control at line 203 — a
`decommission-node` that must be REFUSED — so nothing changed, and by that point
the cluster has already settled through the retry added at line 161.

**No further instances.** The race that reddened the release was the only one of
its shape.

### Why no gate check for it

The scan cannot distinguish a topology command that RAN from one asserted to be
refused, which is precisely the two false positives above. Shipping it would
need an exclusion list, and BUG-0086 already recorded what that costs: "a list
saying these are fine is a second declaration to keep in sync with use." The
same reasoning that stopped a port check shipping there stops this one here.

What is checkable, and already checked, is the narrower thing: a drill that
waits on `nodes_live` and then asserts `verify`. That pattern appears in exactly
one drill and it is now fixed.

