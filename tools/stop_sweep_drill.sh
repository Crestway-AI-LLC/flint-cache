#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# `flintctl stop` must leave NOTHING running — including processes the
# pidfiles never learned about.
#
# The bug this guards (found on the production playground 2026-07-26): a
# `start` that runs while a fleet is already up overwrites the pidfiles, so
# the earlier set becomes invisible to `stop` and survives forever. A
# controller orphan is benign (epoch fencing makes concurrent controllers
# safe by design), but an orphaned NODE holds its port and data dir, and
# the next start then fails or — worse — two servers share a data dir.
#
# Also asserts the sweep is SCOPED: a second fleet with its own statedir on
# the same host must be untouched, and a non-flint process merely mentioning
# the statedir (an editor, a tail) must survive.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-sweep-a 6317 6318 6319 6320 6321 7820 7879 7889
fleet_guard
A=$FLINT_DRILL_ROOT/flint-sweep-a; B=$FLINT_DRILL_ROOT/flint-sweep-b
INVA=$FLINT_DRILL_ROOT/flint-sweep-a.flint; INVB=$FLINT_DRILL_ROOT/flint-sweep-b.flint
CTL=./target/release/flintctl
fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  $CTL -f "$INVA" stop >/dev/null 2>&1; $CTL -f "$INVB" stop >/dev/null 2>&1
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane
  kill "${TAIL_PID:-0}" 2>/dev/null
  rm -rf "$A" "$B" "$INVA" "$INVB"
}
trap cleanup EXIT
rm -rf "$A" "$B" "$INVA" "$INVB"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

cat > "$INVA" <<EOF
disposable on
statedir $A
bins ./target/release
tls on
cp 127.0.0.1:6321
pair 127.0.0.1:6317,127.0.0.1:6318
proxy 127.0.0.1:7879
controller on
EOF
cat > "$INVB" <<EOF
disposable on
statedir $B
bins ./target/release
tls on
cp 127.0.0.1:7820
pair 127.0.0.1:6319,127.0.0.1:6320
proxy 127.0.0.1:7889
controller on
EOF

echo "== fleet A and fleet B (separate statedirs) both up"
# Captured and checked, per fleet. These discarded both bootstraps' output
# and ignored both exit statuses, so a fleet that failed to come up was
# reported as whichever later assertion tripped -- and with TWO fleets the
# message could not even say which one was missing (BUG-0064).
$CTL -f "$INVA" bootstrap >"$A-boot.log" 2>&1 || {
  echo "FAIL: bootstrap fleet A"; tail -25 "$A-boot.log"; exit 1; }
$CTL -f "$INVB" bootstrap >"$B-boot.log" 2>&1 || {
  echo "FAIL: bootstrap fleet B"; tail -25 "$B-boot.log"; exit 1; }
count_a() { ps -eo args | grep -c "[f]lint-.*$A" ; }
count_b() { ps -eo args | grep -c "[f]lint-.*$B" ; }
[ "$(count_a)" -ge 4 ] || { echo "FAIL: fleet A did not start"; exit 1; }
[ "$(count_b)" -ge 4 ] || { echo "FAIL: fleet B did not start"; exit 1; }
echo "  A=$(count_a) procs, B=$(count_b) procs"

echo "== reproduce the bug: a second start over the live fleet A"
# This overwrites A's pidfiles; the first set becomes invisible to stop.
BEFORE_PIDS=$(cat "$A"/pids/controller.pid)
$CTL -f "$INVA" start >/dev/null 2>&1
AFTER_PIDS=$(cat "$A"/pids/controller.pid)
[ "$BEFORE_PIDS" != "$AFTER_PIDS" ] || { echo "FAIL: second start did not re-record pids"; exit 1; }
kill -0 "$BEFORE_PIDS" 2>/dev/null || { echo "FAIL: expected the first controller to still be alive"; exit 1; }
echo "  pidfile now $AFTER_PIDS; orphan $BEFORE_PIDS still running (the bug)"

echo "== a bystander that merely mentions the statedir must survive"
tail -f "$INVA" >/dev/null 2>&1 &
TAIL_PID=$!
sleep 0.3

echo "== stop A: pidfile kills PLUS the orphan sweep"
$CTL -f "$INVA" stop 2>&1 | sed 's/^/  /'
sleep 1
kill -0 "$BEFORE_PIDS" 2>/dev/null && { echo "FAIL: orphan $BEFORE_PIDS survived stop"; exit 1; }
[ "$(count_a)" = "0" ] || { echo "FAIL: $(count_a) fleet-A processes still running"; ps -eo args | grep "[f]lint-.*$A" | head -3; exit 1; }
echo "  every fleet-A process gone, orphan included"

echo "== fleet B untouched (scoping holds)"
[ "$(count_b)" -ge 4 ] || { echo "FAIL: the sweep killed fleet B ($(count_b) left)"; exit 1; }
echo "  B still $(count_b) procs"

echo "== the bystander survived (not a flint binary)"
kill -0 "$TAIL_PID" 2>/dev/null || { echo "FAIL: the sweep killed a non-flint process"; exit 1; }
echo "  tail still alive"

echo "== stop is idempotent on an already-stopped fleet"
$CTL -f "$INVA" stop >/dev/null 2>&1 || { echo "FAIL: second stop errored"; exit 1; }
echo "  clean"

echo "PASS: stop sweeps orphaned fleet processes, scoped to this statedir and to flint binaries"
