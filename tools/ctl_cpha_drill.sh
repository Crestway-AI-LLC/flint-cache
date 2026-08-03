#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# flintctl + 3-seat Raft CP: the inventory contract "3 cp lines = a Raft
# group" — previously a comment reading "3 = Raft, later".
#
# controlplane_ha_drill proves the Raft CP itself (election, redirect,
# leader kill, durability). THIS drill proves flintctl can be its OPERATOR:
# bootstrap starts all three seats with derived raft args, registration
# routes to the leader, tenant mutations FOLLOW a -LEADER redirect (via
# call_cp), and — the part that matters in production — a mutation issued
# AFTER the leader is killed still lands, because call_cp walks to the new
# leader instead of reporting "the CP rejected the command".
#
# The production stake: the 4-host Limited-mode env runs a 3-seat CP
# co-located on existing hosts. Without this tooling those seats would be
# hand-managed, which is how one of them quietly stops being restarted.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-cpha-ctl 6930 6931 7561 7562 7563 7861
fleet_guard
CTL=./target/release/flintctl
D=/tmp/flint-cpha-ctl; INV=$D/cluster.flint
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; sleep 0.4
cleanup() {
  $CTL -f "$INV" stop 2>/dev/null
  fleet_kill server; fleet_kill proxy; fleet_kill controlplane
  rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

cat > "$INV" <<EOF
statedir $D/state
bins ./target/release
disposable on
cp 127.0.0.1:7561
cp 127.0.0.1:7562
cp 127.0.0.1:7563
pair 127.0.0.1:6930,127.0.0.1:6931
proxy 127.0.0.1:7861
EOF

echo "== bootstrap: three seats, one leader, registration lands"
$CTL -f "$INV" bootstrap >"$D/boot.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -15 "$D/boot.log"; exit 1; }
grep -q "3 raft seats up, leader elected" "$D/boot.log" \
  || { echo "FAIL: bootstrap did not report an elected raft leader"; tail -5 "$D/boot.log"; exit 1; }

echo "== all three seats visible in status"
ST=$($CTL -f "$INV" status 2>&1)
N=$(echo "$ST" | grep -c "^cp .*up")
[ "$N" = "3" ] || { echo "FAIL: status shows $N/3 CP seats up"; echo "$ST"; exit 1; }
echo "  3/3 seats up"

echo "== a tenant mutation lands regardless of which seat leads"
$CTL -f "$INV" tenant add acme tok-acme acme 1 >/dev/null 2>&1 \
  || { echo "FAIL: tenant add against the raft CP"; exit 1; }
sleep 1.5
[ "$(valkey-cli -p 7861 -a tok-acme --no-auth-warning SET k1 v1 2>/dev/null)" = "OK" ] \
  || { echo "FAIL: tenant not serving through the proxy"; exit 1; }
echo "  tenant added and serving"

echo "== KILL THE LEADER: the next mutation must still land"
# Find the leader by asking any seat; kill exactly that seat's process.
LEADER_ID=$(valkey-cli -p 7561 CPINFO 2>/dev/null | tr -d '\r' | grep '^leader:' | cut -d: -f2)
[ -n "$LEADER_ID" ] || LEADER_ID=$(valkey-cli -p 7562 CPINFO 2>/dev/null | tr -d '\r' | grep '^leader:' | cut -d: -f2)
[ -n "$LEADER_ID" ] || { echo "FAIL: no seat reports a leader"; exit 1; }
# Match on the seat's unique STATE DIR, not on flag order: cp_seat_args
# emits --state before --raft, so "--node-id N .*statedir" never matches.
pkill -9 -f "flint-controlplane.*cp-state-n$LEADER_ID " \
  || { echo "FAIL: could not kill leader seat n$LEADER_ID"; exit 1; }
echo "  killed leader seat n$LEADER_ID"
sleep 2   # election

# The mutation that used to fail here with "the CP rejected the command":
# flintctl still dials cp[0] first; if that WAS the leader it is now dead,
# and if it was a follower it now redirects somewhere new. Either way
# call_cp must walk to the survivor quorum's leader.
$CTL -f "$INV" tenant add globex tok-glx globex 1 >"$D/add2.log" 2>&1 \
  || { echo "FAIL: tenant add after leader kill"; cat "$D/add2.log"; exit 1; }
sleep 1.5
[ "$(valkey-cli -p 7861 -a tok-glx --no-auth-warning SET k2 v2 2>/dev/null)" = "OK" ] \
  || { echo "FAIL: post-failover tenant not serving"; exit 1; }
echo "  mutation landed on the NEW leader; tenant serving"

echo "== the dead seat shows as DOWN, the survivors as up"
ST=$($CTL -f "$INV" status 2>&1)
UP=$(echo "$ST" | grep -c "^cp .*up"); DOWN=$(echo "$ST" | grep -c "^cp .*DOWN")
[ "$UP" = "2" ] && [ "$DOWN" = "1" ] \
  || { echo "FAIL: expected 2 up / 1 DOWN, got $UP up / $DOWN DOWN"; echo "$ST"; exit 1; }
echo "  status is honest: 2 up, 1 DOWN"

echo "PASS: flintctl bootstraps a 3-seat raft CP, follows the leader through a redirect, survives a leader kill mid-operation, and reports seat health honestly"
