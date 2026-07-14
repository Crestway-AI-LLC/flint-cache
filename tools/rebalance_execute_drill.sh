#!/usr/bin/env bash
# Rebalance EXECUTION drill: three masters with badly unbalanced fills; the
# controller (planner + executor) must move slots via real FLINTMIGRATEIN
# cutovers until the group is balanced within the deadband — hands-free.
# Asserts: fills converge, total keys conserved (no loss, no duplication),
# moved slots answer -MOVED on the old owner and serve on the new one.
set -u
cd "$(dirname "$0")/.."
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null; sleep 0.4
B=./target/release/flint-server
P0=6600; P1=6601; P2=6602
DIRS=""
cleanup() {
  pkill -9 -f "flint-server --port 660" 2>/dev/null
  pkill -9 -f flint-controller 2>/dev/null
  rm -rf /tmp/flint-rb-*
}
trap cleanup EXIT

for p in $P0 $P1 $P2; do
  d="/tmp/flint-rb-$p"; rm -rf "$d"
  $B --port $p --engine rocks --data-dir "$d" 2>/dev/null &
done
sleep 0.8
for p in $P0 $P1 $P2; do [ "$(valkey-cli -p $p PING)" = "PONG" ] || { echo "FAIL: :$p down"; exit 1; }; done

# g0 heavily loaded across 6 distinct hash-tag slots; g1/g2 lightly loaded.
echo "== seed: g0 = 6 tags x 4000 keys = 24000; g1 = g2 = 2000"
for t in mv0 mv1 mv2 mv3 mv4 mv5; do
  awk -v tag="$t" 'BEGIN{for(i=0;i<4000;i++){k=sprintf("{%s}:key%05d",tag,i);v=sprintf("%s-%05d",tag,i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
    | valkey-cli -p $P0 --pipe >/dev/null
done
awk 'BEGIN{for(i=0;i<2000;i++){k=sprintf("{g1}:key%05d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n",length(k),k}}' | valkey-cli -p $P1 --pipe >/dev/null
awk 'BEGIN{for(i=0;i<2000;i++){k=sprintf("{g2}:key%05d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n",length(k),k}}' | valkey-cli -p $P2 --pipe >/dev/null
T0=$(valkey-cli -p $P0 DBSIZE); T1=$(valkey-cli -p $P1 DBSIZE); T2=$(valkey-cli -p $P2 DBSIZE)
TOTAL=$((T0+T1+T2))
echo "  fills: g0=$T0 g1=$T1 g2=$T2 total=$TOTAL"

echo "== start controller with --rebalance-execute (deadband 0.2, 2 slots/cycle)"
./target/release/flint-controller \
  --pairs "127.0.0.1:$P0;127.0.0.1:$P1;127.0.0.1:$P2" \
  --id RBX --poll-ms 200 --rebalance-deadband 0.2 --rebalance-execute --max-slots-per-cycle 2 \
  2>/tmp/flint-rbx.log &

# Wait for convergence: balanced-within-deadband logged AFTER at least one
# EXECUTE, or fills numerically balanced.
BALANCED=0
for i in $(seq 1 120); do
  D0=$(valkey-cli -p $P0 DBSIZE); D1=$(valkey-cli -p $P1 DBSIZE); D2=$(valkey-cli -p $P2 DBSIZE)
  MAX=$D0; [ "$D1" -gt "$MAX" ] && MAX=$D1; [ "$D2" -gt "$MAX" ] && MAX=$D2
  MEAN=$(( (D0+D1+D2) / 3 ))
  if [ "$MEAN" -gt 0 ] && [ $((MAX*100)) -le $((MEAN*125)) ] && grep -q "rebalance EXECUTE" /tmp/flint-rbx.log; then
    BALANCED=1; break
  fi
  sleep 1
done
D0=$(valkey-cli -p $P0 DBSIZE); D1=$(valkey-cli -p $P1 DBSIZE); D2=$(valkey-cli -p $P2 DBSIZE)
echo "  fills after: g0=$D0 g1=$D1 g2=$D2 (executes: $(grep -c "rebalance EXECUTE" /tmp/flint-rbx.log), slot moves: $(grep -c "MIGRATEIN-OK" /tmp/flint-rbx.log))"
[ "$BALANCED" = "1" ] || { echo "FAIL: did not converge to balance"; tail -15 /tmp/flint-rbx.log; exit 1; }

echo "== conservation: every key exactly once across the group"
AFTER=$((D0+D1+D2))
[ "$AFTER" = "$TOTAL" ] || { echo "FAIL: total keys changed: $TOTAL -> $AFTER"; exit 1; }

echo "== moved tags: -MOVED on g0, served with correct value on the new owner"
MOVED_TAGS=0
for t in mv0 mv1 mv2 mv3 mv4 mv5; do
  k="{$t}:key00042"; want="$t-00042"
  R0=$(valkey-cli -p $P0 GET "$k" 2>&1)
  if echo "$R0" | grep -q "^MOVED"; then
    MOVED_TAGS=$((MOVED_TAGS+1))
    DEST=$(echo "$R0" | awk "{print \$3}"); DP=${DEST##*:}
    GOT=$(valkey-cli -p $DP GET "$k")
    [ "$GOT" = "$want" ] || { echo "FAIL: $k on new owner $DEST = '$GOT' (want $want)"; exit 1; }
  else
    [ "$R0" = "$want" ] || { echo "FAIL: unmoved $k wrong on g0: '$R0'"; exit 1; }
  fi
done
[ "$MOVED_TAGS" -ge 2 ] || { echo "FAIL: expected >=2 tags moved, got $MOVED_TAGS"; exit 1; }
echo "  $MOVED_TAGS of 6 tags relocated, all keys correct on their owners"

echo "PASS: controller planned AND executed rebalancing to convergence — no loss, correct -MOVED routing"
