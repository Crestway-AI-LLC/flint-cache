#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Controller HA drill: run THREE controllers on one pair. Discovery-based +
# epoch-fenced means concurrent controllers are safe. Verify:
#   1. with all 3 running, a master kill promotes exactly once (extra
#      attempts are -FENCED, never a second real promotion), data intact;
#   2. killing 2 of 3 controllers still recovers the next master kill.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-ha- 6450 6451 6452
fleet_guard
fleet_kill controller; fleet_kill server; sleep 0.4
D1=$(mktemp -d $FLINT_DRILL_ROOT/flint-ha-1.XXXXXX); D2=$(mktemp -d $FLINT_DRILL_ROOT/flint-ha-2.XXXXXX)
D3=$(mktemp -d $FLINT_DRILL_ROOT/flint-ha-3.XXXXXX)
B=./target/release/flint-server
P1=6450; P2=6451; P3=6452
cleanup() {
  # fleet_kill, not "--port 645": that is a substring match, and it stopped
  # being self-contained the moment disk_selffill declared 6458. Scoped to the
  # ports this drill gave fleet_init (6450-6452).
  fleet_kill controller
  fleet_kill server
  rm -rf "$D1" "$D2" "$D3"
}
trap cleanup EXIT

# Pair = P1 (master) + P2 (replica). P3 is a pre-started SPARE the harness
# uses as the replacement after a promotion (the controller decides; node
# lifecycle is external, matching the real split).
$B --port $P1 --engine rocks --data-dir "$D1" 2>/dev/null &
fleet_wait_listen $P1
sleep 0.5
$B --port $P2 --engine rocks --data-dir "$D2" --replica-of 127.0.0.1:$P1 2>/dev/null &
fleet_wait_listen $P2
sleep 0.9

echo "== loading 15000 keys"
awk 'BEGIN{for(i=0;i<15000;i++){k=sprintf("key:%07d",i);v=sprintf("value-%07d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
  | valkey-cli -p $P1 --pipe | tail -1

echo "== starting THREE controllers on the same pair"
# ONE LOG PER CONTROLLER. All three used to append to a single file, and
# concurrent appends interleave WITHIN a line — three processes writing
# "[A] PROMOTED 127.0.0.1:6451 at (0,2)" at the same moment produce
#
#   [B[][Cg0][] PROMOTED g0127.0.0.1:6451] PROMOTED  at (0,127.0.0.1:64512 at (0,)
#
# so `grep -c "PROMOTED 127.0.0.1:$P2 at (0,2)"` counted 0 and the drill
# reported "no real promotion recorded" — on a run where the promotion had
# demonstrably happened, because the loop above had already confirmed :$P2
# accepting writes. Measured 2026-08-10: 3 failures in 8 runs, and every
# failing log was mangled exactly this way. The product was never involved.
#
# Separate files also make the evidence better: which controller promoted
# and which were fenced is now readable rather than inferred.
#
# Truncate BEFORE the process starts, not after. The old `: > log` ran after
# all three had been launched, which is a second race on the same file.
for c in A B C; do
  : > "$FLINT_DRILL_ROOT/flint-ha-ctl-$c.log"
  ./target/release/flint-controller --nodes 127.0.0.1:$P1,127.0.0.1:$P2 --id "$c" \
    --poll-ms 150 --confirm 3 2>> "$FLINT_DRILL_ROOT/flint-ha-ctl-$c.log" &
done
HA_LOGS="$FLINT_DRILL_ROOT/flint-ha-ctl-A.log $FLINT_DRILL_ROOT/flint-ha-ctl-B.log $FLINT_DRILL_ROOT/flint-ha-ctl-C.log"
sleep 1.6

echo "== KILL master; 3 controllers race to promote"
pkill -9 -f "flint-server --port $P1"
PROMOTED=0
for i in $(seq 1 60); do
  [ "$(valkey-cli -p $P2 SET p ok 2>&1)" = "OK" ] && { PROMOTED=1; break; }
  sleep 0.2
done
[ "$PROMOTED" = "1" ] || { echo "FAIL: no promotion"; cat $HA_LOGS; exit 1; }
sleep 1.0  # let all controllers observe the new state

echo "== exactly ONE effective promotion (the rest -FENCED, no double promote)"
# COUNTED NUMERICALLY, AND FOR EVERY SEAT. Both counters used to be lexical.
#
# The higher-epoch one was `grep -cE "... at \(0,[3-9]"` — a string PREFIX test
# wearing the shape of a comparison. It matches (0,3) through (0,9), and also
# (0,30) and (0,300), but NOT (0,10) through (0,29), which begin with 1 or 2.
# A promotion there was counted by neither total and simply disappeared. This
# drill's epochs stay small so it happened to work, and nothing in it said
# when it would stop.
#
# Both also pinned :$P2, the expected survivor, so a promotion of any OTHER
# seat was invisible to both. That is precisely the case ADR-0004's
# bounded-transient allowance excludes — "two controllers promoting different
# survivors" — so the drill could not see the one shape the ADR calls
# dangerous. That is the worse of the two blind spots, and the reason this is
# a third counter rather than a widened regex.
read -r REAL HIGHER OTHER <<EOF
$(grep -h -oE "PROMOTED 127\.0\.0\.1:[0-9]+ at \(0,[0-9]+\)" $HA_LOGS 2>/dev/null \
  | sed -E 's/.*:([0-9]+) at \(0,([0-9]+)\)/\1 \2/' \
  | awk -v p2="$P2" '
      $1 == p2 && $2 == 2 { real++;   next }
      $1 == p2 && $2 >  2 { higher++; next }
                          { other++ }
      END { printf "%d %d %d\n", real+0, higher+0, other+0 }')
EOF
FENCED=$(grep -h -c "promotion fenced" $HA_LOGS | awk '{s+=$1} END {print s+0}')
echo "  real promotions at (0,2): $REAL | promotions at higher epoch: $HIGHER | fenced attempts: $FENCED | promotions of another seat: $OTHER"

# SPLIT-BRAIN IS NOT A BOUNDED TRANSIENT. Every allowance below is about the
# SAME survivor being re-promoted at a higher epoch, which is idempotent for
# data. A promotion of a different seat is the other thing entirely, and it
# gets no allowance at all.
if [ "${OTHER:-0}" -gt 0 ]; then
  echo "FAIL: $OTHER promotion(s) of a seat other than the expected survivor :$P2."
  echo "      Two controllers promoting DIFFERENT survivors is split-brain, and it is"
  echo "      the one case ADR-0004's bounded-transient allowance explicitly excludes."
  grep -h -oE "PROMOTED 127\.0\.0\.1:[0-9]+ at \(0,[0-9]+\)" $HA_LOGS | sort | uniq -c | sed 's/^/        /'
  exit 1
fi

# SAY WHEN THE RACE DID NOT START. `fenced attempts: 0` means no second
# controller ever reached FLINTPROMOTE — one controller got there first and the
# other two observed finished state. The exactly-once property was then NOT
# exercised, and a PASS on that run says nothing about the fence. Observed on an
# 8-core laptop four times in a row while a 16-vCPU box raced on the first try
# (BUG-0042). Not a failure: the drill cannot make the machine faster. But a
# silent pass and a tested pass must not look identical.
[ "$FENCED" = "0" ] && {
  echo "  NOTE: no controller was fenced — the race did not start on this box, so the"
  echo "        exactly-once property was NOT exercised by this run."
}

# A SECOND PROMOTION OF THE SAME SURVIVOR IS A BOUNDED TRANSIENT, NOT A DEFECT,
# and asserting HIGHER=0 asserted something the design does not promise.
# `FLINTPROMOTE 0 next` answers -FENCED only when `next <= current`, so a
# controller whose ROLE view is stale (no master-claimer yet) while its EPOCH
# read is fresh computes next=current+1 and passes the fence by construction.
# ADR-0004 expects exactly that and calls it converged: "epochs are monotonic,
# so colliding controllers cannot cycle — every effective action strictly
# increases the max epoch, and all controllers reconverge within a tick." The
# #168/#171 recovery paths RE-PROMOTE a self-fenced master at a higher epoch on
# purpose, so a blanket "never twice" would forbid the recovery this product
# depends on.
#
# What must never happen is CYCLING: re-promotion that does not settle, which is
# a livelock and the failure mode this drill exists to catch. So bound it rather
# than forbid it, and fail loudly when the bound is exceeded.
HIGHER_MAX=2
if [ "$HIGHER" -gt "$HIGHER_MAX" ]; then
  echo "FAIL: $HIGHER promotions at a higher epoch — controllers are CYCLING, not converging."
  echo "      ADR-0004 permits a bounded transient (a stale role view proposing epoch+1"
  echo "      passes the fence by construction); it does not permit repeated re-promotion."
  cat $HA_LOGS
  exit 1
fi
if [ "$HIGHER" != "0" ]; then
  echo "  NOTE: $HIGHER re-promotion(s) of :$P2 at a higher epoch — bounded transient (BUG-0042)"
  # WHICH PATH PRODUCED IT. BUG-0042 candidate 1 says a second controller sees
  # no master-claimer while the just-promoted survivor already holds the top
  # epoch, and takes the #168/#171 recovery path — which cannot tell a
  # SELF-FENCED master needing recovery from one promoted moments ago whose
  # role claim this controller has not observed yet. Both present identically.
  #
  # The controller names the path it took. Grepping for it turns the next
  # firing into an answer instead of another sighting: the whole cost of this
  # bug so far is that it fires rarely and says nothing when it does.
  R168=$(grep -h -c 'self-fenced, recovering it (#168)' $HA_LOGS 2>/dev/null | awk '{s+=$1} END {print s+0}')
  R171=$(grep -h -c 'recovering it (#171)' $HA_LOGS 2>/dev/null | awk '{s+=$1} END {print s+0}')
  echo "        recovery paths taken: #168 self-fenced=$R168 | #171 remembered-lineage=$R171"
  if [ "$R168" -gt 0 ] || [ "$R171" -gt 0 ]; then
    echo "        -> BUG-0042 candidate 1 SUPPORTED: a recovery path re-promoted the"
    echo "           survivor. Those paths fire on 'no master-claimer + holds top epoch',"
    echo "           which is indistinguishable from a promotion this controller has not"
    echo "           seen land yet. Capture these logs; that is the evidence the bug needs."
  else
    echo "        -> BUG-0042 candidate 1 NOT supported on this run: the higher-epoch"
    echo "           promotion came from the ordinary path, so candidate 2 (fence checked"
    echo "           against a stale epoch read) is where to look next."
  fi
fi

# CONVERGENCE, which is the property ADR-0004 actually promises and which no
# count above tests: the transient must SETTLE. A first draft of this asserted
# "exactly one node claims master" — vacuous here, because P1 is killed and P3
# is not started until the next phase, so the count can only ever be 0 or 1 and
# the assertion cannot fail. Sample the promotion count, wait several poll
# intervals, and require it to stop moving instead.
SETTLE_AFTER=$HIGHER
sleep 1.5   # 10x --poll-ms 150; ADR-0004 claims reconvergence "within a tick"
HIGHER2=$(grep -h -oE "PROMOTED 127\.0\.0\.1:[0-9]+ at \(0,[0-9]+\)" $HA_LOGS 2>/dev/null \
  | sed -E 's/.*:([0-9]+) at \(0,([0-9]+)\)/\1 \2/' \
  | awk -v p2="$P2" '$1 == p2 && $2 > 2 { n++ } END { print n+0 }')
if [ "$HIGHER2" -gt "$SETTLE_AFTER" ]; then
  echo "FAIL: promotions still arriving 1.5s after the race ($SETTLE_AFTER -> $HIGHER2)."
  echo "      ADR-0004 promises the controllers reconverge within a tick; this is a livelock."
  cat $HA_LOGS
  exit 1
fi
echo "  converged: no further promotions in 1.5s (10 poll intervals)"
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
: > $FLINT_DRILL_ROOT/flint-ha-ctl2.log
./target/release/flint-controller --nodes 127.0.0.1:$P2,127.0.0.1:$P3 --id SURV \
  --poll-ms 150 --confirm 3 2>> $FLINT_DRILL_ROOT/flint-ha-ctl2.log &
sleep 2.0  # let it observe convergence of the new pair

echo "== KILL the new master :$P2; lone survivor controller must promote :$P3"
pkill -9 -f "flint-server --port $P2"
RECOVERED=0
for i in $(seq 1 60); do
  [ "$(valkey-cli -p $P3 SET p2 ok 2>&1)" = "OK" ] && { RECOVERED=1; break; }
  sleep 0.2
done
[ "$RECOVERED" = "1" ] || { echo "FAIL: lone survivor controller did not recover"; cat $FLINT_DRILL_ROOT/flint-ha-ctl2.log; exit 1; }
[ "$(valkey-cli -p $P3 GET key:0007500)" = "value-0007500" ] || { echo "FAIL: data lost after 2nd failover"; exit 1; }
echo "  lone survivor promoted :$P3, data intact"

echo "PASS: concurrent controllers safe (exactly-once promotion), HA survives losing 2 of 3"
