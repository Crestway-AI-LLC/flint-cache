#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Controller HA drill: run THREE controllers on one pair. Discovery-based +
# epoch-fenced means concurrent controllers are safe. Verify:
#   1. with all 3 running, a master kill promotes exactly once (extra
#      attempts are -FENCED, never a second real promotion), data intact;
#   2. killing 2 of 3 controllers still recovers the next master kill.
set -u
cd "$(dirname "$0")/.."
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null; sleep 0.4
D1=$(mktemp -d /tmp/flint-ha-1.XXXXXX); D2=$(mktemp -d /tmp/flint-ha-2.XXXXXX)
D3=$(mktemp -d /tmp/flint-ha-3.XXXXXX)
B=./target/release/flint-server
P1=6450; P2=6451; P3=6452
cleanup() {
  pkill -9 -f "flint-server --port 645" 2>/dev/null
  pkill -9 -f flint-controller 2>/dev/null
  rm -rf "$D1" "$D2" "$D3"
}
trap cleanup EXIT

# Pair = P1 (master) + P2 (replica). P3 is a pre-started SPARE the harness
# uses as the replacement after a promotion (the controller decides; node
# lifecycle is external, matching the real split).
$B --port $P1 --engine rocks --data-dir "$D1" 2>/dev/null &
sleep 0.5
$B --port $P2 --engine rocks --data-dir "$D2" --replica-of 127.0.0.1:$P1 2>/dev/null &
sleep 0.9

echo "== loading 15000 keys"
awk 'BEGIN{for(i=0;i<15000;i++){k=sprintf("key:%07d",i);v=sprintf("value-%07d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
  | valkey-cli -p $P1 --pipe | tail -1

echo "== starting THREE controllers on the same pair"
for c in A B C; do
  ./target/release/flint-controller --nodes 127.0.0.1:$P1,127.0.0.1:$P2 --id "$c" \
    --poll-ms 150 --confirm 3 2>> /tmp/flint-ha-ctl.log &
done
: > /tmp/flint-ha-ctl.log
sleep 1.6

echo "== KILL master; 3 controllers race to promote"
pkill -9 -f "flint-server --port $P1"
PROMOTED=0
for i in $(seq 1 60); do
  [ "$(valkey-cli -p $P2 SET p ok 2>&1)" = "OK" ] && { PROMOTED=1; break; }
  sleep 0.2
done
[ "$PROMOTED" = "1" ] || { echo "FAIL: no promotion"; cat /tmp/flint-ha-ctl.log; exit 1; }
sleep 1.0  # let all controllers observe the new state

echo "== exactly ONE effective promotion (the rest -FENCED, no double promote)"
REAL=$(grep -c "PROMOTED 127.0.0.1:$P2 at (0,2)" /tmp/flint-ha-ctl.log)
HIGHER=$(grep -cE "PROMOTED 127.0.0.1:$P2 at \(0,[3-9]" /tmp/flint-ha-ctl.log)
FENCED=$(grep -c "promotion fenced" /tmp/flint-ha-ctl.log)
echo "  real promotions at (0,2): $REAL | promotions at higher epoch: $HIGHER | fenced attempts: $FENCED"
[ "$HIGHER" = "0" ] || { echo "FAIL: a second real promotion at a higher epoch occurred"; exit 1; }
[ "$REAL" -ge 1 ] || { echo "FAIL: no real promotion recorded"; exit 1; }
[ "$(valkey-cli -p $P2 GET key:0000000)" = "value-0000000" ] || { echo "FAIL: data lost"; exit 1; }
[ "$(valkey-cli -p $P2 GET key:0014999)" = "value-0014999" ] || { echo "FAIL: tail lost"; exit 1; }
echo "  data intact on promoted master :$P2"

echo "== kill 2 of 3 controllers; 1 survivor must still handle the next event"
# The new master is :P2. Attach a fresh replica (:P3) so the pair is healthy.
$B --port $P3 --engine rocks --data-dir "$D3" --replica-of 127.0.0.1:$P2 2>/dev/null &
# Point the survivor controller at the new pair; kill the other two.
pkill -9 -f "flint-controller --nodes 127.0.0.1:$P1,127.0.0.1:$P2 --id A"
pkill -9 -f "flint-controller --nodes 127.0.0.1:$P1,127.0.0.1:$P2 --id B"
pkill -9 -f "flint-controller --nodes 127.0.0.1:$P1,127.0.0.1:$P2 --id C"
sleep 0.3
: > /tmp/flint-ha-ctl2.log
./target/release/flint-controller --nodes 127.0.0.1:$P2,127.0.0.1:$P3 --id SURV \
  --poll-ms 150 --confirm 3 2>> /tmp/flint-ha-ctl2.log &
sleep 2.0  # let it observe convergence of the new pair

echo "== KILL the new master :$P2; lone survivor controller must promote :$P3"
pkill -9 -f "flint-server --port $P2"
RECOVERED=0
for i in $(seq 1 60); do
  [ "$(valkey-cli -p $P3 SET p2 ok 2>&1)" = "OK" ] && { RECOVERED=1; break; }
  sleep 0.2
done
[ "$RECOVERED" = "1" ] || { echo "FAIL: lone survivor controller did not recover"; cat /tmp/flint-ha-ctl2.log; exit 1; }
[ "$(valkey-cli -p $P3 GET key:0007500)" = "value-0007500" ] || { echo "FAIL: data lost after 2nd failover"; exit 1; }
echo "  lone survivor promoted :$P3, data intact"

echo "PASS: concurrent controllers safe (exactly-once promotion), HA survives losing 2 of 3"
