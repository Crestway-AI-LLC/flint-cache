#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0057 D5 and D7: a rollback below the release that introduced consumer
# groups keeps a stream readable and its groups intact.
#
# A seat of THIS build stores a stream with two groups, pending entries and a
# consumer, and stops. The previous release's seat (FLINT_PREV_SERVER, built
# from its tag) opens the same data directory: the entries read back, the
# stream takes new ones, and the group commands it does not know are refused.
# THIS build then opens it again: the groups, their pending entries and their
# consumer are as they were, and `>` delivers what the previous release added.
#
# Not in the gate: it needs a second build. Run it by hand when touching how
# streams or groups are stored, with the release a rollback would go to:
#   git worktree add --detach ../prev v0.1.0-rc.82
#   (cd ../prev && cargo build --release -p flint-server --features rocks)
#   FLINT_PREV_SERVER=../prev/target/release/flint-server tools/stream_groups_rollback_drill.sh
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"

PREV=${FLINT_PREV_SERVER:-}
if [ -z "$PREV" ] || [ ! -x "$PREV" ]; then
  echo "SKIP: set FLINT_PREV_SERVER to the previous release's flint-server (built with rocks)"
  exit 0
fi
fleet_init "$FLINT_DRILL_ROOT/flint-sgroll" 7944
fleet_guard
PORT=7944
D=$FLINT_DRILL_ROOT/flint-sgroll
fleet_kill server; sleep 0.3
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

cargo build --release -q -p flint-server --features rocks || { echo "FAIL: build"; exit 1; }
NOW=./target/release/flint-server
C="valkey-cli -p $PORT"
start() {   # start <binary> <flags...>
  local bin="$1"; shift
  # In a subshell, so this shell owns no job and announces no death.
  ( "$bin" --port "$PORT" --engine rocks --data-dir "$D/data" "$@" >>"$D/seat.log" 2>&1 & )
  fleet_wait_listen "$PORT"
  fleet_wait_ready "$PORT"
}
stop() {
  fleet_signal_port "$PORT" -TERM
  for _ in $(seq 1 100); do
    valkey-cli -p "$PORT" PING >/dev/null 2>&1 || return 0
    sleep 0.1
  done
  echo "FAIL: the seat on $PORT did not stop"; exit 1
}
# The parts of a group that do not move with the clock.
groups() { $C XINFO GROUPS s | tr -d '\r' | paste -sd' ' -; }
pending() { $C XPENDING s g - + 100 | tr -d '\r' | awk 'NR%4!=3' | paste -sd' ' -; }

echo "== this build: a stream, two groups, pending entries, a consumer"
start "$NOW" --streams --stream-groups
for i in $(seq 1 10); do $C XADD s "$i-0" n "$i" >/dev/null; done
[ "$($C XGROUP CREATE s g 0)" = "OK" ] || { echo "FAIL: XGROUP CREATE g"; exit 1; }
[ "$($C XGROUP CREATE s h '$')" = "OK" ] || { echo "FAIL: XGROUP CREATE h"; exit 1; }
$C XREADGROUP GROUP g c1 COUNT 4 STREAMS s '>' >/dev/null
[ "$($C XACK s g 1-0)" = "1" ] || { echo "FAIL: XACK"; exit 1; }
G0=$(groups); P0=$(pending)
case "$P0" in "2-0 c1 1 3-0 c1 1 4-0 c1 1") ;; *) echo "FAIL (control): pending is '$P0'"; exit 1 ;; esac
stop

echo "== the previous release on the same data: entries read, groups refused, XADD works"
start "$PREV" --streams
[ "$($C TYPE s)" = "stream" ] || { echo "FAIL: TYPE s is '$($C TYPE s)' under $PREV"; exit 1; }
[ "$($C XLEN s)" = "10" ] || { echo "FAIL: XLEN s is '$($C XLEN s)' under $PREV, expected 10"; exit 1; }
IDS=$($C XRANGE s - + | awk 'NR%3==1' | paste -sd' ' -)
[ "$IDS" = "1-0 2-0 3-0 4-0 5-0 6-0 7-0 8-0 9-0 10-0" ] || {
  echo "FAIL: XRANGE under $PREV read '$IDS': a group's rows must not appear as entries"; exit 1; }
case "$($C XREADGROUP GROUP g c1 STREAMS s '>' 2>&1)" in
  *"unknown command"*) ;;
  *) echo "FAIL: $PREV answered XREADGROUP; this drill needs the release before groups"; exit 1 ;;
esac
[ "$($C XADD s 11-0 n 11)" = "11-0" ] || { echo "FAIL: XADD under $PREV"; exit 1; }
stop
echo "  10 entries read back, XREADGROUP refused, 11-0 added"

echo "== this build again: the groups are as they were, and > delivers 11-0"
start "$NOW" --streams --stream-groups
P1=$(pending)
[ "$P1" = "$P0" ] || { echo "FAIL: pending was '$P0', now '$P1'"; exit 1; }
G1=$(groups)
for f in "name g consumers 1 pending 3 last-delivered-id 4-0" "name h consumers 0 pending 0 last-delivered-id 10-0"; do
  case "$G1" in *"$f"*) ;; *) echo "FAIL: XINFO GROUPS lost '$f': before '$G0', after '$G1'"; exit 1 ;; esac
done
GOT=$($C XREADGROUP GROUP g c1 STREAMS s '>' | awk 'NR%3==2' | paste -sd' ' -)
[ "$GOT" = "5-0 6-0 7-0 8-0 9-0 10-0 11-0" ] || { echo "FAIL: g read '$GOT' after the roll forward"; exit 1; }
GOT=$($C XREADGROUP GROUP h c2 STREAMS s '>' | awk 'NR%3==2' | paste -sd' ' -)
[ "$GOT" = "11-0" ] || { echo "FAIL: h read '$GOT' after the roll forward, expected 11-0"; exit 1; }
stop

echo "PASS: stream_groups_rollback — the previous release reads a stream with groups and adds to it, and the roll forward finds the groups, their pending entries and their consumer as they were"
