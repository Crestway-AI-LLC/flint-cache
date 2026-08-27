#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Rebalance EXECUTION drill: three masters with badly unbalanced fills; the
# controller (planner + executor) must move slots via real FLINTMIGRATEIN
# cutovers until the group is balanced within the deadband — hands-free.
# Asserts: fills converge, total keys conserved (no loss, no duplication),
# moved slots answer -MOVED on the old owner and serve on the new one.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-rb- 6600 6601 6602
fleet_guard
fleet_kill controller; fleet_kill server; sleep 0.4
B=./target/release/flint-server
P0=6600; P1=6601; P2=6602
DIRS=""
cleanup() {
  pkill -9 -f "flint-server --port 660" 2>/dev/null
  fleet_kill controller
  rm -rf $FLINT_DRILL_ROOT/flint-rb-*
}
trap cleanup EXIT

for p in $P0 $P1 $P2; do
  d="$FLINT_DRILL_ROOT/flint-rb-$p"; rm -rf "$d"
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
  2>$FLINT_DRILL_ROOT/flint-rbx.log &

# Wait for convergence: balanced-within-deadband logged AFTER at least one
# EXECUTE, or fills numerically balanced.
# DBSIZE can answer -TRYAGAIN (the stale-read fence, when a replica is
# behind), and an error string is not a number. Feeding one to `$(( ))` is
# worse than a type error: bash arithmetic dereferences BARE WORDS as
# variable names, so "TRYAGAIN replica out of sync" evaluates `sync`, and
# under `set -u` the drill died with "line 91: sync: unbound variable" — a
# transient replica lag reported as a shell fault, naming nothing about the
# product.
#
# Guarding inside the poll loop was only half the fix, because the guard
# `continue`s and leaves the ERROR STRING in D0/D1/D2. On the timeout path
# the loop then falls out with those strings still set, and the report and
# the conservation check below both consume them. So: keep the raw reply for
# diagnostics, and promote to N0/N1/N2 only once proven numeric. Nothing
# downstream touches anything but N*.
is_num() { case "${1:-}" in '' | *[!0-9]*) return 1 ;; *) return 0 ;; esac; }
BALANCED=0
N0=""; N1=""; N2=""; LASTREPLY="(no reply at all)"
for i in $(seq 1 120); do
  D0=$(valkey-cli -p $P0 DBSIZE 2>&1); D1=$(valkey-cli -p $P1 DBSIZE 2>&1); D2=$(valkey-cli -p $P2 DBSIZE 2>&1)
  if ! is_num "$D0" || ! is_num "$D1" || ! is_num "$D2"; then
    LASTREPLY="g0='$D0' g1='$D1' g2='$D2'"
    sleep 0.5; continue
  fi
  N0=$D0; N1=$D1; N2=$D2
  MAX=$D0; [ "$D1" -gt "$MAX" ] && MAX=$D1; [ "$D2" -gt "$MAX" ] && MAX=$D2
  MEAN=$(( (D0+D1+D2) / 3 ))
  # CONSERVATION IS PART OF "SETTLED", not a separate check afterwards.
  #
  # A slot move copies to the destination and only then drops the source, so
  # mid-flight the same keys are counted on BOTH pairs and every DBSIZE is
  # inflated. The balance criterion is purely distributional, so it can go
  # true during that window — and the conservation check immediately below
  # then reads the duplicates and reports "total keys changed: 28000 ->
  # 29171", which looks exactly like the group having invented data.
  #
  # It is a race in the ruler, not a defect in the product: the drill was
  # asking "are the sizes balanced yet" when it meant "has the migration
  # finished". Requiring the total to be intact makes the loop wait for the
  # cutover, and turns the check below into a confirmation instead of a
  # coin toss.
  SUM=$((D0+D1+D2))
  if [ "$MEAN" -gt 0 ] && [ $((MAX*100)) -le $((MEAN*125)) ] \
     && [ "$SUM" -eq "$TOTAL" ] && grep -q "rebalance EXECUTE" $FLINT_DRILL_ROOT/flint-rbx.log; then
    BALANCED=1; break
  fi
  sleep 1
done
# Never re-read here: that re-opens the -TRYAGAIN window the loop just closed.
# If not one poll in 120s produced three numbers, say exactly that and show
# the last reply — the group being permanently fenced is a real finding, and
# it is not "did not converge to balance".
if [ -z "$N0" ]; then
  echo "FAIL: no numeric DBSIZE from the group in 120s — every poll was fenced or errored"
  echo "      last reply: $LASTREPLY"
  tail -15 $FLINT_DRILL_ROOT/flint-rbx.log; exit 1
fi
echo "  fills after: g0=$N0 g1=$N1 g2=$N2 (executes: $(grep -c "rebalance EXECUTE" $FLINT_DRILL_ROOT/flint-rbx.log), slot moves: $(grep -c "MIGRATEIN-OK" $FLINT_DRILL_ROOT/flint-rbx.log))"
# Say WHICH half of "settled" never arrived — a conservation failure that
# persists is a product bug, and must not be reported as "did not balance".
[ "$BALANCED" = "1" ] || {
  MOVES=$(grep -c "MIGRATEIN-OK" $FLINT_DRILL_ROOT/flint-rbx.log)
  EXECS=$(grep -c "rebalance EXECUTE" $FLINT_DRILL_ROOT/flint-rbx.log)
  if [ $((N0+N1+N2)) -ne "$TOTAL" ]; then
    # An INFLATED sum with a migration still running is the mid-move window,
    # not invented data: a slot copies to the destination and only then drops
    # from the source, so both count it. Saying "duplicated rows" there
    # accuses the product of the one thing this drill exists to disprove,
    # when what actually happened is that a loaded box did not finish inside
    # 120s. Distinguish, and only cry duplication when nothing is in flight.
    if [ $((N0+N1+N2)) -gt "$TOTAL" ] && [ "$EXECS" -gt 0 ] && [ "$MOVES" -lt "$EXECS" ]; then
      echo "FAIL: did not settle within 120s — a slot move was STILL IN FLIGHT"
      echo "      ($EXECS execute(s), $MOVES completed; sum $TOTAL -> $((N0+N1+N2)) is the"
      echo "      mid-move double count, not duplicated rows). On a loaded box this is a"
      echo "      timeout; re-run on a quiet one before reading it as a product fault."
    else
      echo "FAIL: keys not conserved after 120s: $TOTAL -> $((N0+N1+N2))"
      echo "      a migration that never completed its cutover, or duplicated rows"
      echo "      ($EXECS execute(s), $MOVES completed)"
    fi
  else
    echo "FAIL: did not converge to balance"
  fi
  tail -15 $FLINT_DRILL_ROOT/flint-rbx.log; exit 1
}

echo "== conservation: every key exactly once across the group"
AFTER=$((N0+N1+N2))
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
