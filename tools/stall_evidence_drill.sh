#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0270: when the per-drill cap fires, the stalled fleet's evidence is taken
# BEFORE the kill.
#
# A probe drill starts a busy stand-in seat that holds a listening socket,
# writes a log under its fleet_init scope, and then stalls. capped_drill at a
# 3-second cap must report the cap and then the evidence:
# - the probe's process tree, seat included;
# - the seat's threads, and a stack or the tools that could not attach;
# - its socket;
# - the log's last lines.
# The probe's EXIT trap deletes that log, as a real drill's does, so reading
# the log's marker in the evidence shows the evidence came before the kill.
# Afterwards nothing of the probe may be left running.
set -uo pipefail
REPO=$(cd "$(dirname "$0")/.." && pwd)
. "$REPO/tools/lib/drill-timeout.sh" || { echo "FAIL: cannot source tools/lib/drill-timeout.sh"; exit 1; }
W=$(mktemp -d "${TMPDIR:-/tmp}/flint-stallev.XXXXXX") || { echo "FAIL: mktemp"; exit 1; }
trap 'rm -rf "$W"' EXIT
mkdir -p "$W/tools" "$W/root"

# The probe's fleet_init line is printed, not written here at the start of a
# line: the gate's leak check reads every `^fleet_init` in THIS file as this
# drill's scope and ports, and would take the probe's for its own.
{ echo 'fleet_init() { :; }'
  printf '%s %s\n' fleet_init '$FLINT_DRILL_ROOT/flint-stallprobe'
} > "$W/tools/stallprobe_drill.sh"
cat >> "$W/tools/stallprobe_drill.sh" <<'PROBE'
S=$FLINT_DRILL_ROOT/flint-stallprobe; mkdir -p "$S"
# Busy and listening, the shape of the seats BUG-0270 saw.
python3 -c 'import socket
s = socket.socket(); s.bind(("127.0.0.1", 0)); s.listen()
while True: pass' &
SEAT=$!
echo "probe seat log: the marker line STALLPROBE-7f3a" > "$S/seat.log"
trap 'kill $SEAT 2>/dev/null; rm -rf "$S"' EXIT
wait
PROBE

echo "== a probe drill that stalls, under a 3 s cap"
start=$(date +%s)
out=$(cd "$W" && FLINT_DRILL_TIMEOUT_S=3 FLINT_DRILL_ROOT="$W/root" capped_drill stallprobe 2>&1)
rc=$?
secs=$(( $(date +%s) - start ))
fail() { echo "FAIL: $1"; echo "$out" | sed 's/^/    | /' | head -120; exit 1; }
[ "$rc" = 124 ] || fail "capped_drill returned $rc, not 124 (the cap's status)"
grep -q "^FAIL: killed at the 3s per-drill cap" <<< "$out" || fail "the cap's FAIL line is missing"
grep -q "^== stall evidence for stallprobe, taken before the cap's kill" <<< "$out" || fail "no stall evidence"
grep -q "^== end of stall evidence for stallprobe" <<< "$out" || fail "the evidence did not finish"
seat=$(sed -n 's/^-- pid \([0-9][0-9]*\): .*[Pp]ython.*/\1/p' <<< "$out" | awk 'NR==1')
[ -n "$seat" ] || fail "the evidence does not name the probe's seat"
grep -qE "stacks \((eu-stack|gdb|sample)|no stack tool could attach" <<< "$out" \
  || fail "the seat has neither a stack nor the reason it has none"
if command -v lsof >/dev/null 2>&1 || command -v ss >/dev/null 2>&1; then
  grep -q "LISTEN" <<< "$out" || fail "the seat's listening socket is not listed"
else
  grep -q "neither lsof nor ss" <<< "$out" || fail "no socket section"
fi
grep -q "STALLPROBE-7f3a" <<< "$out" \
  || fail "the fleet log's last lines are missing: the evidence came after the EXIT trap deleted them"
echo "  the cap fired; the evidence named seat $seat, its threads and socket, and the log its EXIT trap then deleted"

echo "== nothing of the probe is left"
i=0; while kill -0 "$seat" 2>/dev/null && [ "$i" -lt 10 ]; do sleep 1; i=$((i + 1)); done
kill -0 "$seat" 2>/dev/null && { kill -9 "$seat" 2>/dev/null; fail "the probe's seat $seat outlived its drill"; }
[ -e "$W/root/flint-stallprobe/seat.log" ] && fail "the probe's EXIT trap did not run"
[ "$secs" -lt 60 ] || fail "the capped probe took ${secs}s; evidence must not hold the cap for long"
echo "  the seat is gone and the probe's EXIT trap ran (${secs}s in all)"

echo "PASS: a drill killed at the per-drill cap leaves its evidence first: the process tree, each process's threads and stack or why there is none, its sockets, and the fleet's logs before its EXIT trap removes them (BUG-0270)"
