# BUG-0089 — a failed bring-up is not fatal, and its reason is discarded

**Status:** library half FIXED 2026-09-03; **77 spawns across 31 drills still
discard their stderr** — see "What is left".
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

