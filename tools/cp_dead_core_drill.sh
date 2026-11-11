#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0282: a control-plane seat whose Raft core stopped went on answering as
# the leader. openraft stops its core on a storage error, a flush that fails
# on a full or read-only volume; the process served on with its metrics
# frozen, so a seat that was leader still said `leader:` itself, and CPLEASE,
# leader-only so that a fenced master hears it is fenced, answered OK to a
# master the real leader had fenced. A master renewing against that seat
# first wrote on beside its successor, with no bound.
#
# Asserts, on a real three-seat Raft control plane:
#   - the leader's flush failing ends its process, status 70, naming why
#   - the others elect a leader, which records a promotion (CPFENCE)
#   - no seat still serving answers OK to the fenced master's renewal; the
#     successor's renewal is answered OK (the control)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
# 6614-6616 are names in the registry only: a pair and a proxy nothing serves.
fleet_init $FLINT_DRILL_ROOT/flint-cpdead- 6608 6609 6610 6611 6612 6613 6614 6615 6616
fleet_guard
CP=./target/release/flint-controlplane
D=$(mktemp -d $FLINT_DRILL_ROOT/flint-cpdead-d.XXXXXX)
fleet_kill controlplane; sleep 0.3
cleanup() {
  chmod -R u+w "$D" 2>/dev/null
  fleet_kill controlplane
  rm -rf "$D"
}
trap cleanup EXIT
# A read-only directory is how this drill fails a flush, and root writes
# through one.
[ "$(id -u)" = 0 ] && { echo "SKIP: running as root, which a read-only directory does not stop"; exit 0; }

PEERS="1=127.0.0.1:6611,2=127.0.0.1:6612,3=127.0.0.1:6613"
CLIENTS="1=127.0.0.1:6608,2=127.0.0.1:6609,3=127.0.0.1:6610"
port() { echo $((6607 + $1)); }
lead() { valkey-cli -p "$(port "$1")" CPINFO 2>/dev/null | tr '\r' '\n' | sed -n 's/^leader://p'; }
for id in 1 2 3; do
  mkdir -p "$D/s$id"
  ( $CP --raft --node-id $id --port "$(port $id)" --raft-port $((6610 + id)) \
      --peers "$PEERS" --client-addrs "$CLIENTS" --state "$D/s$id/state" >"$D/n$id.log" 2>&1
    echo $? >"$D/rc$id" ) &
done
fleet_wait_listen 6608 6609 6610
L=""
for _ in $(seq 1 60); do L=$(lead 1); [ -n "$L" ] && [ "$L" != none ] && break; sleep 0.5; done
[ -n "$L" ] && [ "$L" != none ] || { echo "FAIL: no leader elected"; tail -3 "$D"/n*.log; exit 1; }
A=127.0.0.1:6614; B=127.0.0.1:6615

echo "== a pair, its master holding the lease, leader seat $L"
[ "$(valkey-cli -p "$(port $L)" CPADDPAIR $A,$B | tr -d '\r')" = OK ] || { echo "FAIL: CPADDPAIR"; exit 1; }
[ "$(valkey-cli -p "$(port $L)" CPLEASE $A | tr -d '\r')" = OK ] || { echo "FAIL: the master could not take its lease"; exit 1; }
echo "  $A holds the lease"

echo "== the leader's flush fails: its process ends, status 70"
chmod a-w "$D/s$L"
valkey-cli -p "$(port $L)" CPADDPROXY 127.0.0.1:6616 >/dev/null 2>&1
for _ in $(seq 1 100); do [ -f "$D/rc$L" ] && break; sleep 0.1; done
if [ ! -f "$D/rc$L" ]; then
  echo "FAIL (BUG-0282): seat $L's Raft core stopped and the seat serves on, saying leader:$(lead $L)"
  echo "  and answering the master's renewal: $(valkey-cli -p "$(port $L)" CPLEASE $A 2>&1 | tr -d '\r')"
  exit 1
fi
[ "$(cat "$D/rc$L")" = 70 ] || { echo "FAIL: seat $L exited $(cat "$D/rc$L"), not 70"; tail -3 "$D/n$L.log"; exit 1; }
grep -q 'Raft core stopped' "$D/n$L.log" || { echo "FAIL (BUG-0282): the exit does not say why"; tail -3 "$D/n$L.log"; exit 1; }
echo "  exit 70: $(grep -o 'the Raft core stopped ([^)]*)' "$D/n$L.log" | head -1)"

echo "== a successor is promoted at the new leader; nothing still serving says OK to the fenced master"
L2=""
for _ in $(seq 1 60); do
  for id in 1 2 3; do
    [ "$id" = "$L" ] && continue
    x=$(lead $id); [ -n "$x" ] && [ "$x" != none ] && [ "$x" != "$L" ] && L2=$x
  done
  [ -n "$L2" ] && break; sleep 0.5
done
[ -n "$L2" ] || { echo "FAIL: the two remaining seats elected no leader"; exit 1; }
case "$(valkey-cli -p "$(port $L2)" CPFENCE $B | tr -d '\r')" in
  OK*) ;; *) echo "FAIL: CPFENCE at the new leader"; exit 1 ;;
esac
for id in 1 2 3; do
  R=$(valkey-cli -p "$(port $id)" CPLEASE $A 2>&1 | tr -d '\r')
  [ "$R" = OK ] && { echo "FAIL (BUG-0282): seat $id renewed the fenced master's lease: OK"; exit 1; }
  echo "  seat $id, the fenced master: $R"
done
[ "$(valkey-cli -p "$(port $L2)" CPLEASE $B | tr -d '\r')" = OK ] \
  || { echo "FAIL: the successor's renewal at the new leader was not OK"; exit 1; }
echo "  the successor $B renews: OK"

echo "PASS: a control-plane seat whose Raft core stops ends itself, status 70, and no seat still serving"
echo "      renews a fenced master's lease (BUG-0282)."
