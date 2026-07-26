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
A=/tmp/flint-sweep-a; B=/tmp/flint-sweep-b
INVA=/tmp/flint-sweep-a.flint; INVB=/tmp/flint-sweep-b.flint
CTL=./target/release/flintctl
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
pkill -9 -f flint-controlplane 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null
sleep 0.4
cleanup() {
  $CTL -f "$INVA" stop >/dev/null 2>&1; $CTL -f "$INVB" stop >/dev/null 2>&1
  pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
  pkill -9 -f flint-controlplane 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null
  kill "${TAIL_PID:-0}" 2>/dev/null
  rm -rf "$A" "$B" "$INVA" "$INVB"
}
trap cleanup EXIT
rm -rf "$A" "$B" "$INVA" "$INVB"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks

cat > "$INVA" <<EOF
statedir $A
bins ./target/release
tls on
cp 127.0.0.1:7810
pair 127.0.0.1:7501,127.0.0.1:7502
proxy 127.0.0.1:7879
controller on
EOF
cat > "$INVB" <<EOF
statedir $B
bins ./target/release
tls on
cp 127.0.0.1:7820
pair 127.0.0.1:7601,127.0.0.1:7602
proxy 127.0.0.1:7889
controller on
EOF

echo "== fleet A and fleet B (separate statedirs) both up"
$CTL -f "$INVA" bootstrap >/dev/null 2>&1
$CTL -f "$INVB" bootstrap >/dev/null 2>&1
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
