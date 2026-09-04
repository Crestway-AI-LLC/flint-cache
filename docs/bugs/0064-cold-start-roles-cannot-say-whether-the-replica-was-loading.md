# BUG-0064: `cold_start_roles` cannot say whether the replica was loading or absent

Status: OPEN, found 2026-08-27 while confirming BUG-0014 had not fired ·
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

