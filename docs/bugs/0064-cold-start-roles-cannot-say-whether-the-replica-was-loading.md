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
