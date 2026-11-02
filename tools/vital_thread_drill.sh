#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0065: a seat whose vital thread panics ends itself, and one whose
# reporting thread panics serves on. With the test-only
# FLINT_TEST_PANIC_THREAD, on real seats:
#   - a master's WAL fsync thread: the seat exits with status 70, naming the
#     thread; started again on the same data, every write it acknowledged
#     is there
#   - a replica's replication thread: the replica exits with status 70;
#     started again, it catches up on the writes the master took while it
#     was down
#   - a reporting thread (the engine-write sampler): the seat serves on, and
#     FLINTINFO counts it in thread_panics_total; the master beside it,
#     started without the variable, counts none (the control)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-vital- 6551 6552 6553
fleet_guard
fleet_kill server; sleep 0.4
B=./target/release/flint-server
D=$(mktemp -d $FLINT_DRILL_ROOT/flint-vital-d.XXXXXX)
cleanup() {
  fleet_kill server
  rm -rf "$D"
}
trap cleanup EXIT

info() { valkey-cli -p "$1" FLINTINFO 2>/dev/null | tr -d '\r' | sed -n "s/^$2://p"; }
# exited <pid> <log> <thread>: the seat ends itself within 15 s, status 70,
# and its log names the thread.
exited() {
  local pid=$1 log=$2 thread=$3 rc
  for _ in $(seq 1 150); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
  if kill -0 "$pid" 2>/dev/null; then
    echo "FAIL (ADR-0065): the seat is still up 15 s after its $thread thread panicked:"
    sed 's/^/    /' "$log"; exit 1
  fi
  wait "$pid"; rc=$?
  [ "$rc" = 70 ] || { echo "FAIL (ADR-0065): the seat exited $rc, expected 70:"; sed 's/^/    /' "$log"; exit 1; }
  grep -q "vital thread $thread panicked" "$log" \
    || { echo "FAIL (ADR-0065): the exit does not name the $thread thread:"; sed 's/^/    /' "$log"; exit 1; }
}

echo "== a master's WAL fsync thread panics: the seat ends itself, and keeps what it acknowledged"
FLINT_TEST_PANIC_THREAD=wal-fsync:6000 $B --port 6551 --engine rocks --data-dir "$D/m" 2>"$D/m1.log" & MP=$!
fleet_wait_listen 6551; fleet_wait_ping 6551
for i in $(seq 1 50); do
  R="$(valkey-cli -p 6551 SET "k$i" "v$i" 2>&1)"
  [ "$R" = "OK" ] || { echo "FAIL: SET k$i before the panic: $R"; exit 1; }
done
exited "$MP" "$D/m1.log" wal-fsync
$B --port 6551 --engine rocks --data-dir "$D/m" 2>"$D/m2.log" &
fleet_wait_listen 6551; fleet_wait_ping 6551
for i in $(seq 1 50); do
  [ "$(valkey-cli -p 6551 GET "k$i" 2>&1)" = "v$i" ] || { echo "FAIL (ADR-0065): k$i is not there after the restart"; exit 1; }
done
echo "  exited 70 naming wal-fsync; restarted on the same data, all 50 writes there"

echo "== a replica's replication thread panics: the replica ends itself, and catches up when started again"
FLINT_TEST_PANIC_THREAD=replication:4000 $B --port 6552 --engine rocks --data-dir "$D/r" \
  --replica-of 127.0.0.1:6551 2>"$D/r1.log" & RP=$!
# Not waited on: it need not be ready before its thread panics.
exited "$RP" "$D/r1.log" replication
for i in $(seq 1 20); do
  [ "$(valkey-cli -p 6551 SET "after$i" "w$i" 2>&1)" = "OK" ] || { echo "FAIL: SET after$i while the replica was down"; exit 1; }
done
$B --port 6552 --engine rocks --data-dir "$D/r" --replica-of 127.0.0.1:6551 2>"$D/r2.log" &
fleet_wait_listen 6552; fleet_wait_ping 6552
CAUGHT=0
for _ in $(seq 1 150); do
  [ "$(valkey-cli -p 6552 GET after20 2>&1)" = "w20" ] && [ "$(valkey-cli -p 6552 GET k50 2>&1)" = "v50" ] && { CAUGHT=1; break; }
  sleep 0.1
done
[ "$CAUGHT" = 1 ] || { echo "FAIL (ADR-0065): the restarted replica did not catch up:"; sed 's/^/    /' "$D/r2.log"; exit 1; }
echo "  exited 70 naming replication; restarted, it caught up on the writes taken while it was down"

echo "== a reporting thread panics: the seat serves on and counts it"
FLINT_TEST_PANIC_THREAD=engine-sampler:500 $B --port 6553 --engine rocks --data-dir "$D/s" 2>"$D/s.log" & SP=$!
fleet_wait_listen 6553; fleet_wait_ping 6553
COUNTED=0
for _ in $(seq 1 50); do [ "$(info 6553 thread_panics_total)" = 1 ] && { COUNTED=1; break; }; sleep 0.1; done
[ "$COUNTED" = 1 ] || { echo "FAIL (ADR-0065): thread_panics_total [$(info 6553 thread_panics_total)], expected 1:"; sed 's/^/    /' "$D/s.log"; exit 1; }
kill -0 "$SP" 2>/dev/null || { echo "FAIL (ADR-0065): the seat exited on a reporting thread's panic"; exit 1; }
[ "$(valkey-cli -p 6553 SET s v 2>&1)" = "OK" ] && [ "$(valkey-cli -p 6553 GET s 2>&1)" = "v" ] \
  || { echo "FAIL (ADR-0065): the seat does not serve after a reporting thread's panic"; exit 1; }
[ "$(info 6551 thread_panics_total)" = 0 ] || { echo "FAIL: the control master counts [$(info 6551 thread_panics_total)] thread panics"; exit 1; }
echo "  thread_panics_total 1, the seat up and serving; the control master counts 0"

echo "PASS: a vital thread's panic ends the seat with status 70, naming the thread, and the restart loses"
echo "      nothing it acknowledged (a master) and catches up (a replica); a reporting thread's panic is"
echo "      counted in thread_panics_total while the seat serves on (ADR-0065)."
