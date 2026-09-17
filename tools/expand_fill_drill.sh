#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# THE SEAM BETWEEN TWO PROVEN HALVES. `rebalance_execute_drill` starts every
# pair up front and rebalances among pairs that all already hold data;
# `migrate_slots_drill` expands a pair and then moves slots to it EXPLICITLY.
# Neither covers the join: a pair that arrives holding NOTHING, and a
# rebalancer that has to fill it with no operator command at all.
#
# That join is what `docs/capacity-model.md` promises operators -- "70% fill
# => expand => controller drains the pressured pair" -- and what the roadmap's
# elasticity lane states as the mechanism behind CapacityPressure. It had no
# drill.
#
# Asserts: the new pair starts EMPTY (or the fill proves nothing), the
# rebalancer moves slots to it hands-free, keys are conserved exactly, the
# moved keys serve correct values on the new owner, and the old owner answers
# -MOVED for them.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-xf- 6642 6643
fleet_guard
fleet_kill controller; fleet_kill server; sleep 0.4
B=./target/release/flint-server
P0=6642; P1=6643
cleanup() {
  # Controller FIRST (BUG-0061): it respawns dead nodes, so killing the seats
  # first only gives it something to put back.
  fleet_kill controller
  fleet_kill server
  rm -rf $FLINT_DRILL_ROOT/flint-xf-*
}
trap cleanup EXIT

echo "== the cluster before expansion: ONE pair holding everything"
d0="$FLINT_DRILL_ROOT/flint-xf-$P0"; rm -rf "$d0"
$B --port $P0 --engine rocks --data-dir "$d0" 2>"${FLEET_SCOPE}server.log" &
fleet_wait_listen $P0
[ "$(valkey-cli -p $P0 PING)" = "PONG" ] || { echo "FAIL: :$P0 down"; exit 1; }

# Six hash-tag slots so the planner has something to divide. One tag cannot
# be split, so a single tag would make "balance" unreachable and the drill
# would time out on a plan that was never possible.
TAGS="xf0 xf1 xf2 xf3 xf4 xf5"
PER=2000
# awk-generated RESP through --pipe, the idiom rebalance_execute uses.
# flint-server has no Lua: an EVAL here wrote nothing and the seed assertion
# below was what said so.
# THROUGH fleet_load_resp (BUG-0147). The DBSIZE floor below is a real check
# and it caught the EVAL mistake -- but it can only ever say "short", never
# which of the six tags was refused or why.
_xf_seed_gen() {
  awk -v tag="$t" -v n=$PER 'BEGIN{for(i=0;i<n;i++){k=sprintf("{%s}k%05d",tag,i);v=sprintf("%s:%05d",tag,i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}'
}
for t in $TAGS; do
  fleet_load_resp "$P0" _xf_seed_gen "$PER" || exit 1
done
TOTAL=$(valkey-cli -p $P0 DBSIZE)
echo "  $TOTAL keys on :$P0 across 6 tags"
[ "$TOTAL" -ge $(( PER * 6 )) ] || { echo "FAIL: seed short: $TOTAL"; exit 1; }

echo "== expand: a second pair joins holding NOTHING"
d1="$FLINT_DRILL_ROOT/flint-xf-$P1"; rm -rf "$d1"
$B --port $P1 --engine rocks --data-dir "$d1" 2>>"${FLEET_SCOPE}server.log" &
# READY, NOT MERELY BOUND (BUG-0165). Since #176 a node binds and answers from
# inside its load, so DBSIZE below can come back `-LOADING Flint is loading the
# dataset in memory` -- which lands in EMPTY as a string and fails the control
# that is supposed to prove the pair starts at zero. That is what reddened a
# gate run: a harness race reported as "the joining pair is not empty".
fleet_wait_ping $P1
# THE CONTROL FOR THE WHOLE DRILL. "The new pair ended up with keys" is
# satisfied by a pair that always had them, so prove it starts at zero.
EMPTY=$(valkey-cli -p $P1 DBSIZE)
[ "$EMPTY" = "0" ] || { echo "FAIL: the joining pair is not empty ($EMPTY keys) — a fill would prove nothing"; exit 1; }
echo "  :$P1 joined with 0 keys"

echo "== arm the rebalancer and stand back — no migrate command is issued"
./target/release/flint-controller --pairs "127.0.0.1:$P0;127.0.0.1:$P1" \
  --id XFX --poll-ms 200 --rebalance-deadband 0.2 --rebalance-execute --max-slots-per-cycle 2 \
  > "$FLINT_DRILL_ROOT/flint-xf-ctl.log" 2>&1 &
CTL_PID=$!

DEADLINE=$(( $(date +%s) + ${EXPAND_FILL_BUDGET_S:-90} ))
MOVED=0
while :; do
  N1=$(valkey-cli -p $P1 DBSIZE 2>/dev/null | tr -cd '0-9')
  [ -n "$N1" ] && [ "$N1" -gt 0 ] && { MOVED=$N1; break; }
  [ "$(date +%s)" -ge "$DEADLINE" ] && break
  sleep 1
done
ps -p "$CTL_PID" >/dev/null 2>&1 || {
  echo "FAIL: the controller exited before it could move anything. Its log:"
  tail -20 "$FLINT_DRILL_ROOT/flint-xf-ctl.log" | sed 's/^/  | /'; exit 1; }
[ "$MOVED" -gt 0 ] || {
  echo "FAIL: the joining pair is STILL EMPTY after ${EXPAND_FILL_BUDGET_S:-90}s."
  echo "      Expansion that nobody fills is the capacity loop's last step"
  echo "      missing — see docs/capacity-model.md's '70% fill => expand =>"
  echo "      controller drains the pressured pair'. Controller log:"
  tail -20 "$FLINT_DRILL_ROOT/flint-xf-ctl.log" | sed 's/^/  | /'; exit 1; }
echo "  the rebalancer filled :$P1 with $MOVED key(s), hands-free"

echo "== conservation: every key exactly once across the pair of them"
# Let it settle rather than sampling mid-move: a slot in flight is counted
# nowhere for the instant between the copy and the cutover.
sleep 5
N0=$(valkey-cli -p $P0 DBSIZE); N1=$(valkey-cli -p $P1 DBSIZE)
echo "  :$P0 = $N0   :$P1 = $N1   (seeded $TOTAL)"
[ $(( N0 + N1 )) = "$TOTAL" ] || { echo "FAIL: keys not conserved: $TOTAL -> $(( N0 + N1 ))"; exit 1; }

echo "== the moved keys serve on the NEW owner, and the old one redirects"
CHECKED=0
for t in $TAGS; do
  OWNER_REPLY=$(valkey-cli -p $P0 GET "{$t}k00001" 2>&1)
  case "$OWNER_REPLY" in
    *MOVED*)
      GOT=$(valkey-cli -p $P1 GET "{$t}k00001")
      [ "$GOT" = "$t:00001" ] || { echo "FAIL: {$t}k00001 moved but :$P1 serves '$GOT' (want '$t:00001')"; exit 1; }
      CHECKED=$(( CHECKED + 1 )) ;;
    "$t:00001") ;;                       # still on the old owner, correct
    *) echo "FAIL: {$t}k00001 on :$P0 answered '$OWNER_REPLY'"; exit 1 ;;
  esac
done
[ "$CHECKED" -ge 1 ] || { echo "FAIL: no tag was redirected, so nothing was verified on the new owner"; exit 1; }
echo "  $CHECKED tag(s) redirected from :$P0 and served correctly by :$P1"

echo "PASS: a pair that joins EMPTY is filled by the rebalancer with no operator command, keys conserved"
