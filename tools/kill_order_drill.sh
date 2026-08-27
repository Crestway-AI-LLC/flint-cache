#!/usr/bin/env bash
# Copyright (c) 2026 Crestway AI LLC. All rights reserved.
#
# BUG-0061 — no cleanup may kill a supervised seat before its supervisor.
#
# WHY THIS EXISTS. The controller's job is to notice a dead node and respawn
# it. Killing nodes while their supervisor still runs is a race with the
# supervisor, and the drills were winning it for the wrong reason: fleet_kill
# returned within milliseconds of `kill -9`, so the controller died before it
# could react. BUG-0051 makes fleet_kill wait for the deaths — which its own
# comment always claimed it did — and the window opens to the length of the
# wait. What leaks is the node the controller put back.
#
# Two drills lost that race on the first run carrying BUG-0051
# (controller_multipair, snapshot_restore). Forty-six files had the same
# shape and differed only in how long their controller took to notice.
#
# THE GATE'S LEAK DETECTOR ALREADY CATCHES THIS — but only when the race is
# actually lost, which is why 44 of the 46 looked fine. This asserts the
# ORDER, so all of them are covered deterministically and a new drill written
# from an old template fails here rather than intermittently, months later,
# on someone else's change.
#
# It is a source assertion on purpose: the runtime version is "run 46 fleet
# drills and see if anything leaks", which is the gate, and the gate is what
# was already too slow to attribute.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${FLINT_KILL_ORDER_GLOB:-$ROOT/tools/*.sh}"

python3 - "$TARGET" <<'PY'
import glob, re, sys

STMT  = re.compile(r"^\s*fleet_kill\s+\w+\s*$")
SLEEP = re.compile(r"^\s*sleep\s+[\d.]+\s*$")

def parse_line(l):
    """A line made only of fleet_kill statements (optionally ending in a
    sleep). Returns the kinds it kills, or None if it is anything else —
    which is what ends a block."""
    parts = l.strip().split(";")
    kinds = []
    for i, p in enumerate(parts):
        if STMT.match(p):
            kinds.append(p.strip().split()[1])
        elif SLEEP.match(p) and i == len(parts) - 1:
            pass
        else:
            return None
    return kinds or None

SUPERVISOR, SUPERVISED = "controller", "server"
bad = []
files = sorted(glob.glob(sys.argv[1]))
if not files:
    print(f"KILL ORDER DRILL FAILED: no files matched {sys.argv[1]} — this check would pass vacuously")
    sys.exit(1)

for path in files:
    lines = open(path).read().split("\n")
    i = 0
    while i < len(lines):
        if parse_line(lines[i]) is None:
            i += 1
            continue
        # a maximal run of kill-only lines is ONE teardown block; ordering
        # only means anything inside a block, which is why this is not a
        # whole-file scan (that pairs one block's server with the next
        # block's controller and reports a race that cannot happen)
        kinds, start, j = [], i + 1, i
        while j < len(lines):
            k = parse_line(lines[j])
            if k is None:
                break
            kinds.extend(k)
            j += 1
        if SUPERVISOR in kinds and SUPERVISED in kinds:
            if kinds.index(SUPERVISED) < kinds.index(SUPERVISOR):
                bad.append((path, start, " ".join(kinds)))
        i = j

if bad:
    print(f"KILL ORDER DRILL FAILED: {len(bad)} teardown block(s) kill the supervised seat first")
    for p, ln, order in bad:
        print(f"   {p}:{ln}  {order}")
    print("   The controller respawns dead nodes. Kill it FIRST or it puts one back (BUG-0061).")
    sys.exit(1)

print(f"   ok — {len(files)} files, no block kills 'server' before 'controller'")
PY
RC=$?
[ $RC -eq 0 ] || exit $RC
echo "KILL ORDER DRILL PASSED"
