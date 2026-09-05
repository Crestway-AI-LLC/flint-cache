#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
#
# BUG-0051 — when fleet_kill returns, the seats it killed must be GONE.
#
# WHY THIS EXISTS. fleet_kill ends with a loop that waits for each killed pid
# to disappear, wrapped in a long comment explaining that `kill -9` is delivery
# and not death, that #176 made a node bind within milliseconds of exec, and
# that 21 drills respawn on the ports it just freed. The loop iterated
# `_killed_pids`, and nothing in fleet_kill ever appended to it — the sole
# append in the tree was in fleet_signal, a different function that SIGSTOPs
# and has no wait block. So the protection had never executed once.
#
# A wait that does not happen is indistinguishable from a wait that finished
# instantly: no output either way, and the failure it prevents is
# intermittent. That is why the fix needed a check rather than a one-line
# append, and why this drill asserts the POSTCONDITION rather than looking for
# the loop in the source.
#
# The postcondition discriminates, measured before this was written: a real
# flint-server with rocks open is still present for 29-83 poll iterations
# after kill -9, five runs of five. Without the fix, fleet_kill returns inside
# that window.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init "$FLINT_DRILL_ROOT/flint-killrel" 6955
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-killrel; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; sleep 0.3
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT

P=6955
fail() { echo "FAIL: $*"; exit 1; }

cargo build --release -q -p flint-server --features flint-server/rocks \
  || fail "build"

echo "== a seat on $P, killed, then respawned on the SAME port with no sleep"
$B --port $P --engine rocks --data-dir "$D/n" 2> "$D/n.log" &
FIRST=$!
fleet_wait_listen $P || { tail -5 "$D/n.log" | sed 's/^/    /'; fail "first seat never listened"; }
echo "  first seat up, pid $FIRST"

fleet_kill server

# THE ASSERTION. No sleep between fleet_kill returning and this check: the
# whole claim is that fleet_kill does not return until the process is gone.
if kill -0 "$FIRST" 2>/dev/null; then
  fail "fleet_kill returned while pid $FIRST was still alive — the wait-for-death
      loop did not run, so every caller that respawns on these ports is racing
      the socket release (BUG-0051)"
fi
echo "  fleet_kill returned only after pid $FIRST was gone"

# And the consequence that race actually produces: the replacement takes
# EADDRINUSE and exits, which presents as "nothing listening" much later and
# nowhere near this line.
$B --port $P --engine rocks --data-dir "$D/n2" 2> "$D/n2.log" &
SECOND=$!
sleep 0.6
if ! kill -0 "$SECOND" 2>/dev/null; then
  echo "    --- replacement stderr ---"; tail -5 "$D/n2.log" | sed 's/^/    /'
  fail "the replacement on port $P exited immediately — EADDRINUSE, which is
      exactly what the release wait exists to prevent"
fi
fleet_wait_listen $P || { tail -5 "$D/n2.log" | sed 's/^/    /'; fail "replacement never listened on $P"; }
[ "$FIRST" != "$SECOND" ] || fail "same pid twice — the seat was never actually replaced, so this proves nothing"
echo "  replacement (pid $SECOND) bound $P immediately after the kill"

echo "PASS: fleet_kill waits for death — the killed pid was gone on return, and a
      replacement bound the same port with no sleep in between"
