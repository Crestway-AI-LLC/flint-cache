#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Slow-master drill (#164): a master that is ALIVE but unresponsive must NOT be
# flapped to death. SIGSTOP freezes the app while the KERNEL keeps accepting TCP
# on its port — exactly a CPU-starved master (the scale rehearsal's 2-vCPU nodes
# flapped this way: the controller logged 25+ "master unreachable" and promoted
# until both pairs were all-replica).
#
# Proves the slow-vs-dead distinction:
#   1. a BRIEF stall (< --slow-promote-ms) rides through — no promotion, the
#      master is retained, its epoch is unchanged;
#   2. a SUSTAINED stall (> --slow-promote-ms) still escalates to a real
#      promotion, so a genuinely hung-but-listening master is not tolerated
#      forever.
# A killed master (the other drills) refuses the socket and still promotes fast;
# that RTO-critical path is unchanged.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-slow- 6386 6387
fleet_guard
fleet_kill controller; fleet_kill server; sleep 0.4
D1=$FLINT_DRILL_ROOT/flint-slow-1; D2=$FLINT_DRILL_ROOT/flint-slow-2
rm -rf "$D1" "$D2" "$D1.log" "$D2.log"
P1=6386; P2=6387

cleanup() {
  # Controller first (it respawns), then UNFREEZE any stopped seat so the kill
  # lands cleanly, then the seats. A seat left SIGSTOPped would outlive the run.
  fleet_kill controller
  pkill -CONT -f "flint-server --port $P1" 2>/dev/null
  pkill -CONT -f "flint-server --port $P2" 2>/dev/null
  fleet_kill server
  rm -rf "$D1" "$D2"
}
trap cleanup EXIT

# Short slow-promote so the drill is quick; dead-path confirm stays fast. Lease
# and max-stale generous enough that a brief stall neither self-fences the master
# nor closes the degraded window before the escape-hatch promotion.
echo "== start managed controller (--slow-promote-ms 2000)"
./target/release/flint-controller --manage-slots "$P1:$D1,$P2:$D2" --id SLOW \
  --poll-ms 150 --confirm 3 --slow-promote-ms 2000 --max-stale-ms 8000 \
  2>$FLINT_DRILL_ROOT/flint-slow.log &
for i in $(seq 1 60); do
  fleet_ready $P1 && fleet_ready $P2 && break
  sleep 0.2
done
fleet_ready $P1 || { echo "FAIL: controller never bootstrapped a READY master"; cat $FLINT_DRILL_ROOT/flint-slow.log; exit 1; }

master_port() {
  for p in $P1 $P2; do
    valkey-cli -p $p FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -q "role:master" && { echo $p; return; }
  done
}
role_of()  { valkey-cli -p $1 FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -oE "role:[a-z]+"; }
epoch_of() { valkey-cli -p $1 FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -oE "role_epoch:\([0-9]+,[0-9]+\)"; }

# Let the pair converge so auto-failover is armed.
for i in $(seq 1 60); do
  M=$(master_port); [ -n "$M" ] && [ "$(valkey-cli -p $M FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -oE 'live_replicas:[0-9]+')" = "live_replicas:1" ] && break
  sleep 0.2
done
M=$(master_port); [ -n "$M" ] || { echo "FAIL: no master after bootstrap"; cat $FLINT_DRILL_ROOT/flint-slow.log; exit 1; }
OTHER=$([ "$M" = "$P1" ] && echo $P2 || echo $P1)
echo "converged: master :$M, replica :$OTHER"

# ---- Test 1: BRIEF stall must NOT promote --------------------------------
EBEFORE=$(epoch_of $M)
echo "== test 1: SIGSTOP master :$M for 1.2s (< 2s slow-promote) — expect NO promotion"
pkill -STOP -f "flint-server --port $M" || { echo "FAIL: could not SIGSTOP :$M"; exit 1; }
sleep 1.2
R=$(role_of $OTHER)
pkill -CONT -f "flint-server --port $M"
[ "$R" = "role:replica" ] || { echo "FAIL: replica :$OTHER became '$R' during a brief stall — the slow master was FLAPPED"; tail -15 $FLINT_DRILL_ROOT/flint-slow.log; exit 1; }
# Master resumes; its epoch must be unchanged (no promotion happened anywhere).
for i in $(seq 1 40); do [ "$(role_of $M)" = "role:master" ] && break; sleep 0.2; done
[ "$(role_of $M)" = "role:master" ] || { echo "FAIL: master :$M did not resume after SIGCONT"; tail -15 $FLINT_DRILL_ROOT/flint-slow.log; exit 1; }
EAFTER=$(epoch_of $M)
[ "$EAFTER" = "$EBEFORE" ] || { echo "FAIL: epoch moved $EBEFORE -> $EAFTER — a promotion occurred during the stall"; exit 1; }
echo "  brief stall rode through: :$OTHER stayed replica, :$M retained at $EBEFORE"

# Re-converge before test 2.
for i in $(seq 1 60); do [ "$(valkey-cli -p $M FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -oE 'live_replicas:[0-9]+')" = "live_replicas:1" ] && break; sleep 0.2; done

# ---- Test 2: SUSTAINED stall must ESCALATE — the point is it is NOT held
#      forever like a brief one. Escalation is promote (if the replica is still
#      fresh) OR page (if the outage outran the degraded window, max-stale);
#      both prove the hold is bounded. Test 1 (no page/promote) is the anti-flap
#      half; this is the liveness half.
M=$(master_port); OTHER=$([ "$M" = "$P1" ] && echo $P2 || echo $P1)
echo "== test 2: SIGSTOP master :$M sustained (> 2s slow-promote) — expect escalation (promote or page)"
pkill -STOP -f "flint-server --port $M" || { echo "FAIL: could not SIGSTOP :$M"; exit 1; }
# Test 1's brief stall never escalates, so any REFUSING/promotion below is THIS stall's.
ESCALATED=0; WHY=""
for i in $(seq 1 120); do   # up to ~24s: observe of a frozen node is timeout-bound, so ticks are slow
  [ "$(role_of $OTHER)" = "role:master" ] && { ESCALATED=1; WHY="promoted :$OTHER"; break; }
  grep -q "REFUSING" $FLINT_DRILL_ROOT/flint-slow.log 2>/dev/null && { ESCALATED=1; WHY="paged (degraded window)"; break; }
  sleep 0.2
done
pkill -CONT -f "flint-server --port $M"   # unfreeze the old master (fenced if a promotion happened)
[ "$ESCALATED" = "1" ] || { echo "FAIL: a hung-but-listening master was held forever — no promote, no page"; tail -15 $FLINT_DRILL_ROOT/flint-slow.log; exit 1; }
echo "  sustained stall escalated: $WHY"

echo "PASS: a listening-but-slow master is retained on a brief stall and escalated only when sustained"
