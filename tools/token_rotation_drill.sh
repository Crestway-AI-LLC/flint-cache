#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Token rotation drill: zero-downtime dual-version token rotation with
# per-version usage metrics.
#   - a tenant rotates its token: OLD and NEW both authenticate (no downtime)
#   - the proxy counts AUTHs per token — the operator watches the OLD token's
#     count go flat (clients migrated) before retiring it
#   - CPDROPPREV retires the old token; it then gets WRONGPASS, NEW still works
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-rot-state 6750 7550 6323
fleet_guard
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; sleep 0.4
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
STATE=/tmp/flint-rot-state
cleanup() {
  pkill -9 -f "flint-server --port 675" 2>/dev/null
  fleet_kill proxy
  fleet_kill controlplane
  rm -rf /tmp/flint-rot-* "$STATE" "$STATE.tmp"
}
trap cleanup EXIT
rm -f "$STATE"

$B --port 6750 --engine rocks --data-dir /tmp/flint-rot-data 2>/dev/null &
fleet_wait_listen 6750
sleep 0.6
$CP --port 7550 --state "$STATE" 2>/tmp/flint-rot-cp.log &
fleet_wait_listen 7550
sleep 0.4
fleet_cp 7550 CPADDPROXY 127.0.0.1:6323
fleet_cp 7550 CPADDPAIR 127.0.0.1:6750
fleet_cp 7550 CPADDTENANT acme tok-v1 acme 1
$PX --port 6323 --control-plane 127.0.0.1:7550 --advertise 127.0.0.1:6323 2>/tmp/flint-rot-px.log &
fleet_wait_listen 6323
sleep 1.2

a() { valkey-cli -p 6323 -a "$1" --no-auth-warning "${@:2}"; }
echo "== tenant on token v1: write + read work"
[ "$(a tok-v1 SET k hello)" = "OK" ] || { echo "FAIL: v1 auth pre-rotation"; exit 1; }
[ "$(a tok-v1 GET k)" = "hello" ] || { echo "FAIL: v1 read"; exit 1; }
echo "  v1 serves"

echo "== rotate to v2: BOTH tokens authenticate (zero downtime)"
R=$(valkey-cli -p 7550 CPROTATETOKEN acme tok-v2)
echo "  $R"
echo "$R" | grep -q "rotated" || { echo "FAIL: rotate rejected: $R"; exit 1; }
sleep 1.2   # let the snapshot push carry the new token set
[ "$(a tok-v2 GET k)" = "hello" ] || { echo "FAIL: NEW token v2 not accepted after rotate"; exit 1; }
[ "$(a tok-v1 GET k)" = "hello" ] || { echo "FAIL: OLD token v1 stopped working (downtime!)"; exit 1; }
echo "  v1 AND v2 both serve — no downtime"

echo "== per-version usage: proxy counts AUTHs per token"
# Drive some traffic on each token, then read the counters.
for i in $(seq 1 5); do a tok-v1 PING >/dev/null; done
for i in $(seq 1 8); do a tok-v2 PING >/dev/null; done
C1=$(valkey-cli -p 6323 PROXYAUTHCOUNT tok-v1)
C2=$(valkey-cli -p 6323 PROXYAUTHCOUNT tok-v2)
echo "  auth counts: v1=$C1 v2=$C2"
[ "$C1" -ge 5 ] && [ "$C2" -ge 8 ] || { echo "FAIL: counters wrong (v1=$C1 v2=$C2)"; exit 1; }

echo "== drain check: old token's count goes FLAT as clients migrate"
BEFORE=$(valkey-cli -p 6323 PROXYAUTHCOUNT tok-v1)
# Simulate: clients now only use v2.
for i in $(seq 1 10); do a tok-v2 PING >/dev/null; done
AFTER=$(valkey-cli -p 6323 PROXYAUTHCOUNT tok-v1)
[ "$AFTER" = "$BEFORE" ] || { echo "FAIL: v1 count still climbing ($BEFORE -> $AFTER)"; exit 1; }
echo "  v1 count flat at $AFTER while v2 traffic continued — safe to retire"

echo "== retire the old token (CPDROPPREV): v1 -> WRONGPASS, v2 still serves"
valkey-cli -p 7550 CPDROPPREV acme >/dev/null
sleep 1.2
X=$(a tok-v1 GET k 2>&1)
echo "$X" | grep -q "WRONGPASS" || { echo "FAIL: retired token v1 still accepted: $X"; exit 1; }
[ "$(a tok-v2 GET k)" = "hello" ] || { echo "FAIL: v2 broke after dropping v1"; exit 1; }
echo "  v1 rejected, v2 serves"

echo "== rotation state is durable (CP restart preserves current token)"
fleet_kill controlplane; sleep 0.4
$CP --port 7550 --state "$STATE" 2>>/tmp/flint-rot-cp.log &
fleet_wait_listen 7550
sleep 1.5
[ "$(a tok-v2 GET k)" = "hello" ] || { echo "FAIL: v2 lost across CP restart"; exit 1; }
echo "  current token survived CP restart"

echo "PASS: dual-version token rotation — zero downtime, per-token usage metric, durable"
