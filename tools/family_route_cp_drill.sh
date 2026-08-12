#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0010 D1: the co-processor family route table, PROPAGATED FROM THE CONTROL
# PLANE (not the static --families flag). family_route_drill proves the proxy
# CONSUMES a family table; this proves the CP PRODUCES one — CPFAMILY registers
# a family, the CPWATCH snapshot carries it as element 7, and a proxy started
# with NO --families flag learns to route the prefix. Without this the whole
# co-processor feature is unreachable on a CP-driven fleet (the playground/prod
# shape); the mechanism existed only on the consumer side.
#
# The claims:
#   - a proxy in CP mode with NO --families routes NOTHING to a co-processor
#     until the CP says so (VEC.SET reaches the master as an unknown command)
#   - CPFAMILY <prefix> <endpoint> propagates: VEC.SET now answers
#     -COPROCUNAVAIL (registered, endpoint dead) — proof the table arrived
#   - only the REGISTERED prefix routes (an unregistered one still hits the
#     master), so element 7 is applied precisely, not as a blanket
#   - CPFAMILYCLEAR propagates too: the empty element 7 CLEARS the table and
#     VEC.SET falls back to the master — the always-emit contract, proven
#   - the ordinary data path through the CP-driven proxy is untouched (control)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-famcp 6698 6699 6683
fleet_guard
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
D=/tmp/flint-famcp; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; sleep 0.4
cleanup() { fleet_kill server; fleet_kill proxy; fleet_kill controlplane; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-controlplane -p flint-proxy --features flint-server/rocks

# A DEAD co-processor endpoint: nothing listens on port 1, so a registered
# family whose endpoint is down answers -COPROCUNAVAIL. That is exactly the
# signal we want — "the family is registered and routed" — without standing up
# a stand-in co-processor (coproc_forward_drill already proves the live path).
DEAD="127.0.0.1:1"

echo "== cluster: CP + master + one CP-driven proxy (NO --families flag)"
$CP --port 6683 --state "$D/cp" 2>/dev/null &
fleet_wait_ping 6683
fleet_cp 6683 CPADDPROXY 127.0.0.1:6699
fleet_cp 6683 CPADDPAIR 127.0.0.1:6698
fleet_cp 6683 CPADDTENANT t1 tok1 ns1 1
$B --port 6698 --engine rocks --data-dir "$D/m" 2>/dev/null &
fleet_wait_listen 6698
$PX --port 6699 --control-plane 127.0.0.1:6683 --advertise 127.0.0.1:6699 2>"$D/px.log" &
fleet_wait_listen 6699

A="valkey-cli -p 6699 -a tok1 --no-auth-warning"
# Wait until the proxy has its first snapshot (a tenant command works).
for _ in $(seq 1 100); do
  [ "$($A SET __probe__ 1 2>&1)" = "OK" ] && break
  sleep 0.1
done
# wait_reply <expect-substr> <args...> : retry the family command until the CP
# push lands (no fixed sleep — poll for the state we pushed).
wait_reply() {
  local want="$1"; shift
  local i out
  for i in $(seq 1 100); do
    out="$($A "$@" 2>&1 | tr -d '\r')"
    case "$out" in *"$want"*) echo "$out"; return 0 ;; esac
    sleep 0.1
  done
  echo "$out"; return 1
}

echo "== before any CPFAMILY: VEC.SET is unregistered -> reaches the master (NOT a co-processor)"
R=$($A VEC.SET k v 2>&1 | tr -d '\r')
echo "$R" | grep -qi "COPROCUNAVAIL" \
  && { echo "FAIL: VEC. routed to a co-processor with no family registered anywhere: $R"; exit 1; }
echo "  VEC.SET -> $R  (master's unknown-command, as expected)"

echo "== CPFAMILY VEC. propagates from the CP to the flag-less proxy"
valkey-cli -p 6683 CPFAMILY VEC. "$DEAD" >/dev/null || { echo "FAIL: CPFAMILY rejected"; exit 1; }
R=$(wait_reply COPROCUNAVAIL VEC.SET k v) \
  || { echo "FAIL: CPFAMILY never reached the proxy (VEC.SET never became COPROCUNAVAIL): $R"; exit 1; }
echo "  VEC.SET -> $R  (registered via CP, endpoint dead)"
echo "  CPFAMILIES on the CP:"; valkey-cli -p 6683 CPFAMILIES | sed 's/^/    /'

echo "== only the REGISTERED prefix routes: an unregistered one still hits the master"
E=$($A MAT.MUL x 2>&1 | tr -d '\r')
echo "$E" | grep -qi "COPROCUNAVAIL" \
  && { echo "FAIL: an unregistered prefix routed to a co-processor: $E"; exit 1; }
echo "  MAT.MUL -> $E  (still the master; element 7 applied precisely)"

echo "== CPFAMILYCLEAR propagates: the emptied element 7 CLEARS the table"
valkey-cli -p 6683 CPFAMILYCLEAR VEC. >/dev/null || { echo "FAIL: CPFAMILYCLEAR rejected"; exit 1; }
# Success = VEC.SET stops being COPROCUNAVAIL (falls back to the master).
ok=0
for _ in $(seq 1 100); do
  case "$($A VEC.SET k v 2>&1 | tr -d '\r')" in *COPROCUNAVAIL*) sleep 0.1 ;; *) ok=1; break ;; esac
done
[ "$ok" = 1 ] || { echo "FAIL: CPFAMILYCLEAR did not propagate — VEC.SET still COPROCUNAVAIL"; exit 1; }
echo "  VEC.SET -> back to the master (clear propagated via empty element 7)"

echo "== CONTROL: the ordinary data path through the CP-driven proxy is intact"
cli_ok $A SET real value
[ "$($A GET real)" = "value" ] || { echo "FAIL (control): tenant SET/GET broke"; exit 1; }
echo "  tenant SET/GET still works"

echo "PASS: the CP produces the family route table — CPFAMILY registers it,"
echo "      CPWATCH element 7 carries it, a proxy with no --families flag learns"
echo "      to route the prefix, an unregistered prefix does not, and"
echo "      CPFAMILYCLEAR's empty element 7 clears it back to the master."
