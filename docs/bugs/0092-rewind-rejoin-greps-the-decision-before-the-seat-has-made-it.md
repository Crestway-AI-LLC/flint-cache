# BUG-0092 — `rewind_rejoin` greps the rejoin decision before the seat has made it

**Status:** FIXED 2026-09-04 · **Severity:** medium — one arm fails
intermittently and names the product; its sibling passes for the same reason
and names nothing.

## The firing

A gate run on 2026-09-04 (`08d908e`):

    FAIL  rewind_rejoin  (8.8s)
        FAIL: arm E — a quarantined snapshot was not reconsidered under a LOWER fence.

The drill passes locally, repeatedly. It is a race, not a regression.

## The race

Every arm spawns the rejoining seat, calls `fleet_wait_ready`, then greps its
log on the next line:

    $B --port 6405 ... --replica-of 127.0.0.1:6406 --rewind-snaps ... &
    fleet_wait_ready 6405
    grep -q "rewound to" "$D/qe-a2.log" || { echo "FAIL: arm E — ..."; }

`fleet_wait_ready` returns when the LOCAL load finishes — `loading:1` is gone.
**The rewind-versus-reseed choice is a REPLICATION event**, made after the seat
has contacted its master, so it is logged some time after readiness. The wait
and the assertion are about different things.

That is [BUG-0064](0064-cold-start-roles-cannot-say-whether-the-replica-was-loading.md)'s
family exactly: waiting on a proxy reached earlier than the condition being
asserted. It is the fourth instance this week, after `decommission` (which
reddened the rc.68 release gate), `cold_start_roles`, and BUG-0090's
write-timeout.

## The half that is worse than the failure

Arm E asserts a marker is PRESENT, so it fails loudly and blames the product —
"a quarantined snapshot was not reconsidered" — for an unwritten log.

**Arm F asserts the same marker is ABSENT**, and an unwritten log satisfies
that. It has therefore been passing whenever it lost the race, proving nothing
about BUG-0062's livelock staying closed. Arm B is the same shape twice over.

A visible flake and an invisible pass, from one defect. The flake is the lucky
half: it is the only reason anybody looked.

## The fix

`fleet_wait_log` already exists for this, and its own header says so — *"eight
sites across six drills spelled 'wait until the seat has logged what it
decided' as a fixed sleep"*. This drill was not one of the eight.

A seat logs exactly one of three rejoin decisions, so `wait_rejoin` blocks
until whichever it reaches appears — `warm rejoin at seq`, `rewound to`, or
`full sync: received` — and every arm's existing greps stay as the verdict,
unchanged. Waiting for one specific marker would be wrong: three of the six
assertion groups are about a marker being ABSENT.

A log that never decides times out with a named failure, rather than letting
the arm read an empty file as an answer.

## Verified

The full drill could not be run on this machine at the time: a peer session's
`wal_window_drill` held a seat, and `FLINT_DRILL_FORCE=1` would have destroyed
a fleet this session does not own. So `wait_rejoin` was driven directly — a
marker arriving 1.2 s late is waited for and found, all three decisions are
accepted, and **a log that never decides times out rather than passing**, which
is the control that keeps the other three assertions meaningful. The full drill
runs in CI, which has no siblings.
