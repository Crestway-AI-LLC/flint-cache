#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Manual failover drill: master + replica, load, kill -9 the master,
# FLINTPROMOTE the replica with a higher role epoch, verify it accepts
# writes with data intact — and that stale/equal epochs are FENCED.
# (The meta trio will automate the decision; the mechanics are these.)
set -euo pipefail
. "$(dirname "$0")/lib/fleet.sh"
# Declared so the SET of drills can be checked for port collisions —
# fleet_init only records the scope, it changes no behaviour here. A
# drill that declares nothing is invisible to assert_no_port_overlap,
# which is how failover and controller came to share 6440/6441 and
# reseed and lag_cap to share 6471/6472, unseen.
fleet_init $FLINT_DRILL_ROOT/flint-failover 6326 6327

KEYS="${1:-20000}"
MPORT="${2:-6326}"
RPORT="${3:-6327}"
MDIR="$(mktemp -d $FLINT_DRILL_ROOT/flint-fo-m.XXXXXX)"
RDIR="$(mktemp -d $FLINT_DRILL_ROOT/flint-fo-r.XXXXXX)"
BIN="$(dirname "$0")/../target/release/flint-server"

cleanup() {
  pkill -f "flint-server --port $MPORT" 2>/dev/null || true
  pkill -f "flint-server --port $RPORT" 2>/dev/null || true
  rm -rf "$MDIR" "$RDIR"
}
trap cleanup EXIT

echo "== master :$MPORT, replica :$RPORT"
"$BIN" --port "$MPORT" --engine rocks --data-dir "$MDIR" &
fleet_wait_listen "$MPORT"
sleep 0.4
"$BIN" --port "$RPORT" --engine rocks --data-dir "$RDIR" --replica-of "127.0.0.1:$MPORT" &
fleet_wait_listen "$RPORT"
sleep 0.6

echo "== loading $KEYS keys"
awk -v n="$KEYS" 'BEGIN {
  for (i = 0; i < n; i++) {
    k = sprintf("key:%07d", i); v = sprintf("value-%07d", i)
    printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, length(v), v
  }
}' | valkey-cli -p "$MPORT" --pipe | tail -1

echo "== waiting for replica catch-up"
LAST="key:$(printf '%07d' $((KEYS - 1)))"
CAUGHT=0
for i in $(seq 1 100); do
  if [ "$(valkey-cli -p "$RPORT" GET "$LAST" 2>/dev/null || true)" = "value-$(printf '%07d' $((KEYS - 1)))" ]; then
    CAUGHT=1; break
  fi
  sleep 0.1
done
[ "$CAUGHT" = "1" ] || { echo "FAIL: replica never caught up"; valkey-cli -p "$RPORT" FLINTINFO | tr '\r' ' '; exit 1; }

echo "== stale-epoch promotion must be FENCED (current role epoch is (0,1))"
F1=$(valkey-cli -p "$RPORT" FLINTPROMOTE 0 1 2>&1 || true)
echo "$F1" | grep -q "FENCED" || { echo "FAIL: equal epoch not fenced: $F1"; exit 1; }
F2=$(valkey-cli -p "$RPORT" FLINTPROMOTE 0 0 2>&1 || true)
echo "$F2" | grep -q -E "FENCED|ERR" || { echo "FAIL: zero epoch accepted: $F2"; exit 1; }
echo "fencing OK: $F1"

echo "== kill -9 the master"
pkill -9 -f "flint-server --port $MPORT"
sleep 0.3

echo "== promote replica at role epoch (0,2)"
P=$(valkey-cli -p "$RPORT" FLINTPROMOTE 0 2)
echo "$P" | grep -q "OK promoted" || { echo "FAIL: promotion refused: $P"; exit 1; }
echo "$P"

echo "== promoted node accepts writes and kept the data"
W=$(valkey-cli -p "$RPORT" SET after-failover works)
[ "$W" = "OK" ] || { echo "FAIL: write after promotion: $W"; exit 1; }
[ "$(valkey-cli -p "$RPORT" GET after-failover)" = "works" ] || { echo "FAIL: read-back"; exit 1; }
[ "$(valkey-cli -p "$RPORT" GET key:0000000)" = "value-0000000" ] || { echo "FAIL: pre-failover data lost"; exit 1; }
[ "$(valkey-cli -p "$RPORT" GET "$LAST")" = "value-$(printf '%07d' $((KEYS - 1)))" ] || { echo "FAIL: tail data lost"; exit 1; }

echo "== role epoch is durable and visible"
valkey-cli -p "$RPORT" FLINTINFO | tr '\r' ' ' | grep -o "role:[a-z]* " | head -1
valkey-cli -p "$RPORT" FLINTINFO | tr '\r' ' ' | grep -o "role_epoch:[^ ]*" | head -1

echo "== re-promotion at the same epoch is FENCED (no double promotion)"
F3=$(valkey-cli -p "$RPORT" FLINTPROMOTE 0 2 2>&1 || true)
echo "$F3" | grep -q "FENCED" || { echo "FAIL: double promotion accepted: $F3"; exit 1; }

echo "== restart the promoted node WITH stale --replica-of: manifest must win"
pkill -f "flint-server --port $RPORT"
sleep 0.4
"$BIN" --port "$RPORT" --engine rocks --data-dir "$RDIR" --replica-of "127.0.0.1:$MPORT" &
fleet_wait_listen "$RPORT"
sleep 0.6
W2=$(valkey-cli -p "$RPORT" SET after-restart still-master)
[ "$W2" = "OK" ] || { echo "FAIL: promoted role lost after restart: $W2"; exit 1; }
[ "$(valkey-cli -p "$RPORT" GET after-failover)" = "works" ] || { echo "FAIL: post-promotion data lost"; exit 1; }

echo "== ZOMBIE: restart the OLD master on its old data dir"
"$BIN" --port "$MPORT" --engine rocks --data-dir "$MDIR" &
fleet_wait_listen "$MPORT"
sleep 0.6
# Hazard demonstrated: it still believes it is master (accepts a write).
Z=$(valkey-cli -p "$MPORT" SET zombie-write bad 2>&1)
[ "$Z" = "OK" ] || { echo "FAIL: expected the zombie hazard (write accepted), got: $Z"; exit 1; }
echo "zombie accepts writes (hazard confirmed; the trio's lease will close this window)"

echo "== fence the zombie with FLINTDEMOTE at a higher epoch"
CUR=$(valkey-cli -p "$RPORT" FLINTINFO | tr '\r' ' ' | grep -oE 'role_epoch:\([0-9]+,[0-9]+\)' | grep -oE '[0-9]+\)' | tr -d ')')
NEXT=$((CUR + 1))
D=$(valkey-cli -p "$MPORT" FLINTDEMOTE 0 "$NEXT")
echo "$D" | grep -q "OK demoted" || { echo "FAIL: demotion refused: $D"; exit 1; }
echo "$D"
RO=$(valkey-cli -p "$MPORT" SET should-fail x 2>&1 || true)
echo "$RO" | grep -q "READONLY" || { echo "FAIL: zombie still writable after demote: $RO"; exit 1; }

echo "== stale demotion epoch is FENCED"
F4=$(valkey-cli -p "$MPORT" FLINTDEMOTE 0 "$NEXT" 2>&1 || true)
echo "$F4" | grep -q "FENCED" || { echo "FAIL: equal-epoch demotion accepted: $F4"; exit 1; }

echo "== demotion survives restart (durable fencing)"
pkill -f "flint-server --port $MPORT"
sleep 0.4
"$BIN" --port "$MPORT" --engine rocks --data-dir "$MDIR" &
fleet_wait_listen "$MPORT"
sleep 0.6
RO2=$(valkey-cli -p "$MPORT" SET should-fail x 2>&1 || true)
echo "$RO2" | grep -q "READONLY" || { echo "FAIL: zombie writable again after restart: $RO2"; exit 1; }
echo "demoted role held across restart"

echo "PASS: epoch-fenced promotion + demotion, durable roles, zombie fenced, data intact"
