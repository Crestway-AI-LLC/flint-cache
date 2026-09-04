# BUG-0089 — a failed bring-up is not fatal, and its reason is discarded

**Status:** FIXED 2026-09-04. Library half 2026-09-03; all 77 spawns
converted the next day, once the scope turned out to make per-drill paths
unnecessary.
**Area:** `tools/lib/fleet.sh` and the drills that stand up a cluster.

## The asymmetry

`fleet_wait_ping` **exits** when a seat never comes up. `fleet_wait_listen`,
in the same file, **returned**. The drills run under `set -u`, not `set -e`,
so a failed listen printed its line and execution continued into the
assertions below it — dying on whichever tripped first, which is a claim
about the product for what was really a seat that never started.

Measured across `tools/*_drill.sh`:

| | count |
|---|---|
| unchecked `fleet_wait_listen` calls (return ignored) | **131**, across 60 drills |
| bring-up spawns discarding stderr with `2>/dev/null &` | **77**, across 31 drills |

Two functions side by side, one returning and one exiting, reads as two
contracts rather than one oversight — and neither half is visible from a
passing run.

## Fixed

`fleet_wait_listen` exits, like its sibling. All three bring-up failure
paths — `wait_listen`, `wait_ping`, and `wait_ready`'s both branches — now
call `fleet_why_not_up`, which prints the non-empty logs in `FLEET_SCOPE`
and, when there are none, says the reason was **discarded rather than
absent**. A node stuck in `loading` has a log saying how far its sync got,
which is the next question in that branch too.

## What is left, and why it was not done here

The 77 discarding spawns. In the ops repo the same conversion was mechanical
because every drill there names its scratch directory `$D`; here the
convention varies per drill — `MDIR`, `RDIR`, `mktemp -d $FLINT_DRILL_ROOT/…`
— so each one needs its own log path and its own reading. That is 31 files of
careful edits, worth doing and not worth doing blind in one pass.

Until then `fleet_why_not_up` is honest about it: it names the discard as the
reason the cause is missing, instead of printing a symptom over silence.

## Found by

Porting the ops repo's [OPS-0117] and asking whether the same class lives
here. It does, three times over. The ops side also carries a standing note —
*"local flint-server lacks rocks, drills fail at bring-up looking like a
product bug"* — which was this defect recorded as a fact about the
environment rather than a bug in the harness.

The neighbouring class was already fixed here in `b7b74a9`: 23 drills that
ran `bootstrap >/dev/null` and reported a bare failure. That commit found
seat bring-up to be the largest and least diagnosable failure cluster across
six red gate runs, and fixed the bootstrap half of it. This is the seat-spawn
half.

## 2026-09-04 — `fleet_why_not_up` looked one directory too high

The library half shipped a reader that scans `"$FLEET_SCOPE"/*.log`. `flintctl`
writes seat logs to **`<statedir>/logs/<name>.log`**
(`crates/flint-ctl/src/main.rs:1438`), and for the **11 drills whose
`FLEET_SCOPE` is the inventory statedir** — `backup_seat`, `build_stamp`,
`cold_start_roles`, `config_drift` and seven more — that is exactly one level
below the glob.

So on those drills a failed bring-up scanned the top level, found nothing, and
printed *"the reason is gone, not absent"* while flintctl's log sat in `logs/`.
**"Cannot look" reported as "absent", inside the function written to stop that.**
Fixed by globbing both levels; verified by planting a log in `logs/` and
watching it surface.

**The zero-case message also asserted a cause it cannot know.** It said the
spawn "sent stderr to /dev/null" — one explanation of several. The scope may not
be a directory, the seat may have died before writing, or the logs may be
somewhere the function does not know about, which is the case for the **29
drills whose FLEET_SCOPE is not their statedir**. It now names where it looked
and leaves the conclusion to the reader.

**This does not touch the 77 spawns**, which remain as this file records them.
It makes the reader correct first, so that converting them can be checked
against something that works.

## 2026-09-04 — all 77, and two defects found doing it

### `FLEET_SCOPE` is a prefix, and the helper assumed a directory

The library half shipped with `fleet_why_not_up` testing
`[ -d "$FLEET_SCOPE" ]`. In this repo the scope is a **prefix** —
`fleet_init $FLINT_DRILL_ROOT/flint-bloom-` — and the lock beside it is
`${FLEET_SCOPE}.lock`, a sibling file. So the test was never true, the helper
found nothing every time, and it then printed *"this drill's spawn sent
stderr to /dev/null"*.

**It asserted a cause it had not established, inside the function written to
stop exactly that.** It globs both forms now, and when it finds nothing it
says what it looked for rather than why it failed — "the spawn discarded it"
and "this drill logs somewhere else" are indistinguishable from there.

### The conversion needed no per-drill paths after all

The reason given for deferring it was that the scratch-directory convention
varies — `MDIR`, `RDIR`, `D1`, `SDIR`, `STATE`, and six drills with none.
That was true and irrelevant: every drill that spawns a seat has already
called `fleet_init`, so **every one has `$FLEET_SCOPE`**. All 77 now write
`${FLEET_SCOPE}<name>.log`, which is also exactly where the helper looks.

31 drills, 77 spawns, zero remaining.

### Last run's logs are not this run's evidence

Nothing removed those logs, so a re-run would find the previous failure's
output still sitting there and report it as the cause of today's — a stale
cause read as a fresh one, which is worse than having none. `fleet_init`
clears `${FLEET_SCOPE}*.log` at the start of every run. Not in a trap: the
drills install their own `EXIT` traps and the last one installed wins.

Verified by planting a stale log, running the drill, and confirming it was
gone.
### Both halves were found independently, and each missed the other's

Two sessions fixed `fleet_why_not_up` the same afternoon without knowing.
One found that `flintctl` writes to `<statedir>/logs/` and the glob looked one
level too high; the other found that for most drills `FLEET_SCOPE` is a
**prefix**, so the `[ -d "$d" ]` guard is false and the scan never runs at
all. Each fix left the other's case reporting "nothing found".

Resolved as the union — `"$d"*.log`, `"$d"/*.log`, `"$d"/logs/*.log`, and no
`[ -d ]` gate, since an unmatched glob is skipped by the `-f` test and the
gate is what hid the prefix case. Worth recording because the two diagnoses
read as contradictory ("it is a directory one level up" against "it is not a
directory") and are both true, of different drills.

### Two counts, 54 and 77, and both are right

A concurrent re-count arrived at **54 spawns across 30 drills**; this pass
converted 77 across 31. Neither is wrong — they scope the word "spawn"
differently. Of the 77 lines changed:

| what is backgrounded | count |
|---|---|
| a seat binary (`$B`) | 54 |
| a control plane (`$CP`) | 14 |
| a proxy (`$PX`) | 9 |

The 54 is exactly the seat-binary subset. The other 23 are control planes and
proxies, which are bring-up too and fail just as opaquely — a proxy that never
binds discards its reason the same way a node does — so they went in the same
pass. Recorded because two numbers for one job, left standing side by side,
reads as a disagreement about the WORK when it is only a disagreement about
the noun.

### The 8 spawns with no redirect stay as they are

Also from that re-count, and it holds: 5 in `failover`, 2 in `restart`, 1 in
`repl` background a seat with **no** redirect at all, so their stderr is
INHERITED by the drill and already reaches the drill log and the gate
artifact — `failover`'s CI log carries `flint-server listening on ...` inline,
which is that inheritance visible. Converting them would move output from a
place that works to a place that also works. Left alone deliberately, and said
so here, so that a later sweep counting `2>` redirects does not "finish" them.
