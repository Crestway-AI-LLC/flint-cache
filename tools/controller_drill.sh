#!/usr/bin/env bash
# Controller drill: a master/replica pair plus flint-controller. Kill the
# master and verify the controller AUTOMATICALLY promotes the survivor —
# no manual FLINTPROMOTE — with data intact. Then bring the old master back
# and verify the controller fences it (FLINTDEMOTE).
set -u
cd "$(dirname "$0")/.."
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null; sleep 0.4
MDIR=$(mktemp -d /tmp/flint-ctl-m.XXXXXX); RDIR=$(mktemp -d /tmp/flint-ctl-r.XXXXXX)
B=./target/release/flint-server
MPORT=6440; RPORT=6441
cleanup() {
  pkill -9 -f "flint-server --port 644" 2>/dev/null
  pkill -9 -f flint-controller 2>/dev/null
  rm -rf "$MDIR" "$RDIR"
}
trap cleanup EXIT

$B --port $MPORT --engine rocks --data-dir "$MDIR" 2>/dev/null &
sleep 0.5
$B --port $RPORT --engine rocks --data-dir "$RDIR" --replica-of 127.0.0.1:$MPORT 2>/dev/null &
sleep 0.9

echo "== loading 20000 keys"
awk 'BEGIN{for(i=0;i<20000;i++){k=sprintf("key:%07d",i);v=sprintf("value-%07d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
  | valkey-cli -p $MPORT --pipe | tail -1

echo "== starting controller"
./target/release/flint-controller --nodes 127.0.0.1:$MPORT,127.0.0.1:$RPORT --id ctl \
  --poll-ms 150 --confirm 3 2> /tmp/flint-ctl.log &
sleep 1.5   # let it observe convergence

echo "== KILL master (no manual promotion — the controller must act)"
pkill -9 -f "flint-server --port $MPORT"

echo "== waiting for the controller to auto-promote the replica"
PROMOTED=0
for i in $(seq 1 60); do
  W=$(valkey-cli -p $RPORT SET ctl-probe ok 2>&1)
  if [ "$W" = "OK" ]; then PROMOTED=1; break; fi
  sleep 0.2
done
[ "$PROMOTED" = "1" ] || { echo "FAIL: controller did not auto-promote in 12s"; echo "--- controller log:"; cat /tmp/flint-ctl.log; exit 1; }
echo "auto-promotion OK ($(( i * 200 ))ms after kill)"

echo "== data intact on the new master"
[ "$(valkey-cli -p $RPORT GET key:0000000)" = "value-0000000" ] || { echo "FAIL: head lost"; exit 1; }
[ "$(valkey-cli -p $RPORT GET key:0019999)" = "value-0019999" ] || { echo "FAIL: tail lost"; exit 1; }
[ "$(valkey-cli -p $RPORT GET ctl-probe)" = "ok" ] || { echo "FAIL: post-promotion write lost"; exit 1; }
echo "$(grep -oE 'PROMOTED .* at \(0,[0-9]+\)' /tmp/flint-ctl.log | head -1)"

echo "== bring the OLD master back; controller must fence it"
$B --port $MPORT --engine rocks --data-dir "$MDIR" 2>/dev/null &
sleep 2.0
FENCED=0
for i in $(seq 1 40); do
  RO=$(valkey-cli -p $MPORT SET zombie bad 2>&1 || true)
  if echo "$RO" | grep -q "READONLY"; then FENCED=1; break; fi
  sleep 0.2
done
[ "$FENCED" = "1" ] || { echo "FAIL: controller did not fence the returned master"; echo "--- controller log:"; tail -8 /tmp/flint-ctl.log; exit 1; }
echo "$(grep -oE 'FENCED zombie .* at \(0,[0-9]+\)' /tmp/flint-ctl.log | head -1)"

echo "PASS: hands-free failover + automatic zombie fencing, data intact"
