#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0007 PLUMBING drill — the federation seams exist and are inert.
#   - CPTENANTFEDERATE flips the tenant's 'f' flag; the snapshot carries it
#     (both orders), and traffic through the proxy is unaffected either way
#   - multiple --control-plane addresses are rejected with an honest error
#     (multi-cluster subscription arrives with the fleet-map work)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-fedplumb 7091 7830 7831 7996 7997
fleet_guard
CP=./target/release/flint-controlplane
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=/tmp/flint-fedplumb; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; sleep 0.4
cleanup() {
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; rm -rf "$D"
}
trap cleanup EXIT

echo "== single-cluster fleet with one tenant"
$CP --port 7830 --state "$D/cp" 2>/dev/null &
for i in $(seq 1 30); do [ "$(valkey-cli -p 7830 PING 2>/dev/null)" = "PONG" ] && break; sleep 0.2; done
valkey-cli -p 7830 CPADDPROXY 127.0.0.1:7997 >/dev/null
valkey-cli -p 7830 CPADDPAIR 127.0.0.1:7091 >/dev/null
valkey-cli -p 7830 CPADDTENANT acme tok-acme acme 1 >/dev/null
$B --port 7091 --engine rocks --data-dir "$D/m" 2>/dev/null &
$PX --port 7997 --control-plane 127.0.0.1:7830 --advertise 127.0.0.1:7997 2>/dev/null &
fleet_wait_listen 7091 7997
sleep 1.5
A="valkey-cli -p 7997 -a tok-acme --no-auth-warning"
[ "$($A SET k v)" = "OK" ] || { echo "FAIL: baseline write"; exit 1; }

echo "== flip the federation flag; the snapshot carries 'f'"
[ "$(valkey-cli -p 7830 CPTENANTFEDERATE acme on)" = "OK" ] || { echo "FAIL: CPTENANTFEDERATE"; exit 1; }
SNAP=$(valkey-cli -p 7830 CPSNAPSHOT 127.0.0.1:7997 2>/dev/null | tr '\n' ' ')
echo "$SNAP" | grep -q "f" || true  # flags live in the tenants frame; assert precisely:
TEN=$(valkey-cli -p 7830 CPSNAPSHOT 127.0.0.1:7997 | sed -n '4p')
echo "  tenants frame: $TEN"
echo "$TEN" | grep -qE "#[rcq]*f" || { echo "FAIL: 'f' flag missing from snapshot ($TEN)"; exit 1; }

echo "== traffic identical with the flag set (plumbing is inert)"
sleep 1  # let the push land
[ "$($A GET k)" = "v" ] || { echo "FAIL: read with flag on"; exit 1; }
[ "$($A SET k2 v2)" = "OK" ] || { echo "FAIL: write with flag on"; exit 1; }
valkey-cli -p 7830 CPTENANTFEDERATE acme off >/dev/null
sleep 1
[ "$($A GET k2)" = "v2" ] || { echo "FAIL: read after flag off"; exit 1; }
TEN=$(valkey-cli -p 7830 CPSNAPSHOT 127.0.0.1:7997 | sed -n '4p')
echo "$TEN" | grep -qE "#[rcq]*f" && { echo "FAIL: 'f' flag persisted after off"; exit 1; }
echo "  flag on -> snapshot 'f'; off -> gone; reads/writes unaffected"

echo "== multiple control planes are rejected honestly"
OUT=$($PX --port 7996 --control-plane 127.0.0.1:7830,127.0.0.1:7831 2>&1 || true)
echo "$OUT" | grep -q "federation (ADR-0007)" || { echo "FAIL: multi-CP not rejected with the ADR message ($OUT)"; exit 1; }
echo "  two CPs -> refused with the ADR-0007 message"

echo "PASS: federation plumbing — 'f' flag rides the tenant record and snapshot (both modes of the wire), routing is byte-identical with it set, and multi-CP input fails loudly until the fleet-map work lands"
