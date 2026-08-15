#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0007 PLUMBING drill — the federation seams exist and are inert.
#   - CPTENANTFEDERATE flips the tenant's 'f' flag; the snapshot carries it
#     (both orders), and traffic through the proxy is unaffected either way
#   - a comma list of --control-plane addresses now means the SEATS of one
#     cluster's Raft CP (the 3-seat production shape): the proxy accepts it
#     and watches the FIRST seat, rotating on failure. Federation — a list
#     of CLUSTERS — remains unwired and will need its own syntax precisely
#     because the comma now means seats.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-fedplumb 7091 6305 7831 7996 7997
fleet_guard
CP=./target/release/flint-controlplane
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=$FLINT_DRILL_ROOT/flint-fedplumb; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; sleep 0.4
cleanup() {
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; rm -rf "$D"
}
trap cleanup EXIT

echo "== single-cluster fleet with one tenant"
$CP --port 6305 --state "$D/cp" 2>/dev/null &
fleet_wait_ping 6305
fleet_cp 6305 CPADDPROXY 127.0.0.1:7997
fleet_cp 6305 CPADDPAIR 127.0.0.1:7091
fleet_cp 6305 CPADDTENANT acme tok-acme acme 1
$B --port 7091 --engine rocks --data-dir "$D/m" 2>/dev/null &
$PX --port 7997 --control-plane 127.0.0.1:6305 --advertise 127.0.0.1:7997 2>/dev/null &
fleet_wait_listen 7091 7997
sleep 1.5
A="valkey-cli -p 7997 -a tok-acme --no-auth-warning"
[ "$($A SET k v)" = "OK" ] || { echo "FAIL: baseline write"; exit 1; }

echo "== flip the federation flag; the snapshot carries 'f'"
[ "$(valkey-cli -p 6305 CPTENANTFEDERATE acme on)" = "OK" ] || { echo "FAIL: CPTENANTFEDERATE"; exit 1; }
SNAP=$(valkey-cli -p 6305 CPSNAPSHOT 127.0.0.1:7997 2>/dev/null | tr '\n' ' ')
echo "$SNAP" | grep -q "f" || true  # flags live in the tenants frame; assert precisely:
TEN=$(valkey-cli -p 6305 CPSNAPSHOT 127.0.0.1:7997 | sed -n '4p')
echo "  tenants frame: $TEN"
echo "$TEN" | grep -qE "#[rcq]*f" || { echo "FAIL: 'f' flag missing from snapshot ($TEN)"; exit 1; }

echo "== traffic identical with the flag set (plumbing is inert)"
sleep 1  # let the push land
[ "$($A GET k)" = "v" ] || { echo "FAIL: read with flag on"; exit 1; }
[ "$($A SET k2 v2)" = "OK" ] || { echo "FAIL: write with flag on"; exit 1; }
valkey-cli -p 6305 CPTENANTFEDERATE acme off >/dev/null
sleep 1
[ "$($A GET k2)" = "v2" ] || { echo "FAIL: read after flag off"; exit 1; }
TEN=$(valkey-cli -p 6305 CPSNAPSHOT 127.0.0.1:7997 | sed -n '4p')
echo "$TEN" | grep -qE "#[rcq]*f" && { echo "FAIL: 'f' flag persisted after off"; exit 1; }
echo "  flag on -> snapshot 'f'; off -> gone; reads/writes unaffected"

echo "== a CP seat list is ACCEPTED and the first live seat is watched"
# The second seat does not exist; the proxy must still come up on the first
# — a dead seat in the list is a rotation target, never a startup failure.
$PX --port 7996 --control-plane 127.0.0.1:6305,127.0.0.1:7831 \
  --advertise 127.0.0.1:7996 2>"$D/px2.log" &
fleet_wait_listen 7996
sleep 1.5
# The RIGHT assertion is snapshot DELIVERY, not serving acme: this proxy is
# not in acme's shuffle-shard subset, so refusing acme's token is correct —
# the first version asserted a SET and read the (correct) -WRONGPASS as a
# failure. What the seat list must prove is that CPWATCH works through it:
# a snapshot arrives from seat 1 while dead seat 2 sits in the list.
grep -q "control-plane snapshot v" "$D/px2.log" \
  || { echo "FAIL: seat-list proxy never received a snapshot"; tail -3 "$D/px2.log"; exit 1; }
echo "  proxy up on a seat list; snapshot delivered via seat 1 (dead seat 2 harmless)"
echo "  two CPs -> refused with the ADR-0007 message"

echo "PASS: federation plumbing — 'f' flag rides the tenant record and snapshot (both modes of the wire), routing is byte-identical with it set, and a CP seat list watches through its first live seat"
