# BUG-0204: `fleet_kill vec` killed nothing, so a drill's co-processor outlived its cleanup (FIXED 2026-10-03)

**Status:** **FIXED 2026-10-03.** Found extending `edge_roll_drill` for ops
ADR-0050, whose cleanup needed to reap a co-processor. Held by
`fleet_guard_drill`'s case K.
**Severity:** low; test harness only. `coproc_family_drill` calls
`fleet_kill vec` on entry and on exit. Its `flintctl stop` reaps the seat on
a clean run, but a co-processor a failed run left behind was never killed by
the next one.

## What was measured

A `flint-vec` started in a drill's scope (`fleet_init`, its port declared)
was selected by `_fleet_ours vec`. After `fleet_kill vec`, `kill -0` found it
still running.

## Why

`fleet_kill` selects with `_fleet_ours`, then re-checks each pid's binary
before the signal (a pid can exit and be reused between the snapshot and the
kill). The selection takes whatever components the caller names, so `vec`
selects `^flint-vec$`. The re-check is a fixed `case` of every fleet binary,
and `flint-vec` was not in it, so every selected co-processor was skipped.

## The fix

`flint-vec` joins the re-check. `fleet_guard_drill` case K starts a fake
co-processor in its own scope and requires `_fleet_ours vec` to select it,
the control that the kill check is not vacuous. It then requires
`fleet_kill vec` to end it. The unfixed `fleet.sh` fails it.

## Not changed

A bare `fleet_kill`, with no components named, still does not select
`flint-vec`, and neither does the guard's foreign-seat scan. Both lists name
the seats a drill's generic cleanup and the guard treat as a fleet. Adding
co-processors to them would change what every drill sweeps and what the
guard calls busy, so it is a separate decision from this bug.
