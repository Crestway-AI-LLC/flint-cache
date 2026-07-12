#!/usr/bin/env bash
# Multi-pair controller drill: ONE controller manages THREE pairs (a group).
# Each pair fails over INDEPENDENTLY — killing the master of pair B must
# promote pair B's survivor and respawn its replacement without touching
# pairs A or C. Proves the group generalization: N pairs, one controller,
# per-pair state. The drill only KILLS; the controller does everything.
set -u
cd "$(dirname "$0")/.."
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null; sleep 0.4
# Three pairs on fixed ports; roles float within each pair.
declare -a A=(6500 6501) B=(6510 6511) C=(6520 6521)
DIRS=()
for p in 6500 6501 6510 6511 6520 6521; do DIRS+=("/tmp/flint-mp-$p"); rm -rf "/tmp/flint-mp-$p" "/tmp/flint-mp-$p.log"; done
cleanup() {
  pkill -9 -f "flint-server --port 65" 2>/dev/null
  pkill -9 -f flint-controller 2>/dev/null
  for p in 6500 6501 6510 6511 6520 6521; do rm -rf "/tmp/flint-mp-$p"; done
}
trap cleanup EXIT

echo "== one controller manages 3 pairs (--manage-pairs g0;g1;g2)"
./target/release/flint-controller \
  --manage-pairs "6500:/tmp/flint-mp-6500,6501:/tmp/flint-mp-6501;6510:/tmp/flint-mp-6510,6511:/tmp/flint-mp-6511;6520:/tmp/flint-mp-6520,6521:/tmp/flint-mp-6521" \
  --id MP --poll-ms 150 --confirm 3 --lease-ttl-ms 3000 2>/tmp/flint-mp.log &

# Wait for all six nodes to come up (controller bootstraps each pair).
for i in $(seq 1 80); do
  UP=0
  for p in 6500 6501 6510 6511 6520 6521; do
    [ "$(valkey-cli -p $p PING 2>/dev/null)" = "PONG" ] && UP=$((UP+1))
  done
  [ "$UP" = "6" ] && break
  sleep 0.2
done
[ "$UP" = "6" ] || { echo "FAIL: controller did not bootstrap all 3 pairs (up=$UP)"; tail -20 /tmp/flint-mp.log; exit 1; }
echo "all 3 pairs bootstrapped"

# Which port in a pair currently holds master?
master_of() {  # args: two ports
  for p in "$@"; do
    valkey-cli -p $p FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -q "role:master" && { echo $p; return; }
  done
}

# Seed each pair's master with a distinct key so we can prove no cross-pair damage.
for pair in "A" "B" "C"; do
  eval "ports=(\${$pair[@]})"
  m=$(master_of ${ports[@]})
  valkey-cli -p $m SET "owner" "$pair" >/dev/null
  valkey-cli -p $m SET "seed-$pair" "v-$pair" >/dev/null
done
sleep 2.0  # let replicas converge and the controller observe each pair

# Kill ONLY pair B's master. A and C must be untouched.
BM=$(master_of ${B[@]})
BO=$([ "$BM" = "${B[0]}" ] && echo ${B[1]} || echo ${B[0]})
echo "== KILL pair B master :$BM (expect promote :$BO, respawn :$BM; A & C untouched)"
AM_BEFORE=$(master_of ${A[@]}); CM_BEFORE=$(master_of ${C[@]})
pkill -9 -f "flint-server --port $BM"

# Pair B recovers.
REC=0
for i in $(seq 1 80); do
  valkey-cli -p $BO FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -q "role:master" && { REC=1; break; }
  sleep 0.2
done
[ "$REC" = "1" ] || { echo "FAIL: pair B did not fail over"; tail -20 /tmp/flint-mp.log; exit 1; }
# Wait for B to reconverge (replacement respawned + acking).
for i in $(seq 1 80); do
  SL=$(valkey-cli -p $BO FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -oE "seq_lag:[a-z0-9]+")
  LR=$(valkey-cli -p $BO FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -oE "live_replicas:[0-9]+")
  [ "$SL" = "seq_lag:0" ] && [ "$LR" = "live_replicas:1" ] && break
  sleep 0.2
done
echo "  pair B promoted :$BO and respawned :$BM"

# Pair B data intact.
[ "$(valkey-cli -p $BO GET owner)" = "B" ] || { echo "FAIL: pair B lost its data / cross-pair bleed"; exit 1; }
[ "$(valkey-cli -p $BO GET seed-B)" = "v-B" ] || { echo "FAIL: pair B seed lost"; exit 1; }

# A and C: same master as before (no spurious failover), data intact, no B bleed.
AM_AFTER=$(master_of ${A[@]}); CM_AFTER=$(master_of ${C[@]})
[ "$AM_AFTER" = "$AM_BEFORE" ] || { echo "FAIL: pair A failed over spuriously ($AM_BEFORE -> $AM_AFTER)"; exit 1; }
[ "$CM_AFTER" = "$CM_BEFORE" ] || { echo "FAIL: pair C failed over spuriously ($CM_BEFORE -> $CM_AFTER)"; exit 1; }
[ "$(valkey-cli -p $AM_AFTER GET owner)" = "A" ] || { echo "FAIL: pair A data wrong"; exit 1; }
[ "$(valkey-cli -p $CM_AFTER GET owner)" = "C" ] || { echo "FAIL: pair C data wrong"; exit 1; }
[ -z "$(valkey-cli -p $AM_AFTER GET seed-B)" ] || { echo "FAIL: pair B key leaked into pair A"; exit 1; }
echo "  pairs A (:$AM_AFTER) and C (:$CM_AFTER) untouched, no cross-pair bleed"

# Now kill pair A's master too — two independent failovers, different pairs.
echo "== KILL pair A master :$AM_AFTER (independent second failover)"
AO=$([ "$AM_AFTER" = "${A[0]}" ] && echo ${A[1]} || echo ${A[0]})
pkill -9 -f "flint-server --port $AM_AFTER"
REC=0
for i in $(seq 1 80); do
  valkey-cli -p $AO FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -q "role:master" && { REC=1; break; }
  sleep 0.2
done
[ "$REC" = "1" ] || { echo "FAIL: pair A did not fail over"; tail -20 /tmp/flint-mp.log; exit 1; }
[ "$(valkey-cli -p $AO GET owner)" = "A" ] || { echo "FAIL: pair A data lost after its failover"; exit 1; }
# C still steady.
[ "$(master_of ${C[@]})" = "$CM_AFTER" ] || { echo "FAIL: pair C disturbed by pair A failover"; exit 1; }
echo "  pair A promoted :$AO; pair C still steady on :$CM_AFTER"

echo "PASS: one controller drives 3 pairs with independent per-pair failover, no cross-pair effects"
