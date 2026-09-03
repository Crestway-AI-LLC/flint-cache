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

**These are not eleven flaky drills. They are one parallel gate that
occasionally starves whichever drill is bootstrapping.** Which drill loses is
close to arbitrary — that is why the failures scatter and why no single drill
looks broken enough to chase.

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
rate collapses, it is contention and the question becomes which dimension; if
it does not, the runner is simply too small and no amount of overlap-checking
will help.
