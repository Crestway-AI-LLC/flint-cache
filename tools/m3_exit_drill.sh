#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# M3 exit drill (roadmap): 50 namespaces packed on ONE group; failover under
# multi-tenant load.
#   - 50 tenants, each with its own token, share 2 replicated pairs behind
#     one proxy endpoint
#   - every tenant seeds keys through the AUTHED proxy (range-routed across
#     both pairs)
#   - a master is killed while several tenants are actively writing; the
#     controller promotes, the proxy chases
#   - afterwards: every one of the 50 tenants reads its sampled keys correct
#     and can write — no tenant lost, no tenant bled into another
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-m3- 6669 6710 6711 6720 6721
fleet_guard
fleet_kill server; fleet_kill controller; fleet_kill proxy; sleep 0.4
B=./target/release/flint-server
KEYS_PER_TENANT=300
cleanup() {
  pkill -9 -f "flint-server --port 67" 2>/dev/null
  fleet_kill controller
  fleet_kill proxy
  rm -rf $FLINT_DRILL_ROOT/flint-m3-* $FLINT_DRILL_ROOT/m3-stop
}
trap cleanup EXIT

echo "== group: 2 replicated pairs (6710/6711, 6720/6721)"
for spec in "6710:" "6711:127.0.0.1:6710" "6720:" "6721:127.0.0.1:6720"; do
  p=${spec%%:*}; m=${spec#*:}
  d="$FLINT_DRILL_ROOT/flint-m3-$p"; rm -rf "$d"
  if [ -n "$m" ]; then
    $B --port $p --engine rocks --data-dir "$d" --replica-of "$m" 2>/dev/null &
  else
    $B --port $p --engine rocks --data-dir "$d" 2>/dev/null &
  fi
  sleep 0.4
done
sleep 0.8

TENANTS=$(python3 -c 'print(",".join(f"tok{i:02d}=tenant{i:02d}" for i in range(50)))')
./target/release/flint-controller --pairs "127.0.0.1:6710,127.0.0.1:6711;127.0.0.1:6720,127.0.0.1:6721" \
  --id M3 --poll-ms 150 --confirm 3 2>$FLINT_DRILL_ROOT/flint-m3-ctl.log &
./target/release/flint-proxy --port 6669 \
  --pairs "127.0.0.1:6710,127.0.0.1:6711;127.0.0.1:6720,127.0.0.1:6721" \
  --tenants "$TENANTS" 2>$FLINT_DRILL_ROOT/flint-m3-proxy.log &
fleet_wait_listen 6669
sleep 1.2

echo "== seed: 50 tenants x $KEYS_PER_TENANT keys through the authed proxy"
for i in $(seq -w 0 49); do
  awk -v t="$i" -v n="$KEYS_PER_TENANT" 'BEGIN{for(j=0;j<n;j++){k=sprintf("data:%05d",j);v=sprintf("t%s-%05d",t,j);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
    | valkey-cli -p 6669 -a "tok$i" --no-auth-warning --pipe >/dev/null
done
# Spot-verify pack: 5 tenants' DBSIZE (all 50 checked in the final sweep).
for i in 00 13 27 38 49; do
  D=$(valkey-cli -p 6669 -a "tok$i" --no-auth-warning DBSIZE)
  [ "$D" = "$KEYS_PER_TENANT" ] || { echo "FAIL: tenant$i DBSIZE=$D after seed"; exit 1; }
done
EXPECT_ROWS=$((50 * KEYS_PER_TENANT))
TOTAL_ROWS=$(( $(valkey-cli -p 6710 FLINTSLOTSTATS | awk '{s+=$2} END{print s+0}') + $(valkey-cli -p 6720 FLINTSLOTSTATS | awk '{s+=$2} END{print s+0}') ))
echo "  50 tenants seeded; group holds $TOTAL_ROWS rows across 2 pairs"
[ "$TOTAL_ROWS" = "$EXPECT_ROWS" ] || {
  # CARRY THE EVIDENCE OUT (#174). This mismatch is intermittent — it does not
  # reproduce on a quiet box, and every occurrence so far has been on a
  # contended one — so the count alone has never been enough to diagnose it.
  # Two numbers and no breakdown is exactly the failure shape that costs a
  # whole run per attempt, so print WHICH namespaces and slots hold the
  # surplus, and what the nodes think is in flight, at the moment it fires.
  echo "FAIL: expected $EXPECT_ROWS rows (50 tenants x $KEYS_PER_TENANT), got $TOTAL_ROWS"
  echo "  --- per-namespace totals (slot count ns) ---"
  for P in 6710 6720; do
    echo "  node $P:"
    valkey-cli -p $P FLINTSLOTSTATS | tr -d '\r' \
      | awk '{n[$3] += $2; s[$3]++} END {for (k in n) printf "    %-12s %8d rows in %4d slot(s)\n", k, n[k], s[k]}' \
      | sort
    # A slot mid-move is counted on one side and suppressed on the other, so
    # an in-flight migration is the first thing to rule in or out.
    echo "    migrations in flight: $(valkey-cli -p $P FLINTMIGRATIONS | tr -d '\r' | tr '\n' ';')"
  done
  # Per-tenant DBSIZE through the proxy: says whether the surplus is visible
  # to a tenant (real keys nobody asked for) or only to the row counter.
  echo "  --- tenants whose DBSIZE is not $KEYS_PER_TENANT ---"
  for i in $(seq -w 0 49); do
    D=$(valkey-cli -p 6669 -a "tok$i" --no-auth-warning DBSIZE 2>/dev/null | tr -d '\r')
    [ "$D" = "$KEYS_PER_TENANT" ] || echo "    tenant$i: $D"
  done
  exit 1
}

# Let replicas converge before inducing failure (steady-state failover).
for m in 6710 6720; do
  for i in $(seq 1 50); do
    SL=$(valkey-cli -p $m FLINTINFO | tr '\r' ' ' | grep -oE "seq_lag:[a-z0-9]+")
    [ "$SL" = "seq_lag:0" ] && break; sleep 0.2
  done
done

echo "== multi-tenant load: 6 tenants write continuously; KILL pair-0 master"
rm -f $FLINT_DRILL_ROOT/m3-stop
for i in 01 09 17 25 33 41; do
  # RECORD WHAT WAS ACKED, not what was attempted. `j` used to be incremented
  # unconditionally with the reply discarded, so it counted ATTEMPTS — and
  # the check at the end took the MIDPOINT of 1000..j and demanded that key
  # be readable. During the failover this drill deliberately causes, writes
  # are refused; those attempts still advanced the counter, so the midpoint
  # could land squarely in the hole and the drill reported
  #
  #     FAIL: writer tenant17 live:1869 = ''
  #
  # which reads as a cache losing an acked write — the most serious claim
  # this product can make about itself — for a write that was never acked.
  # Green on a fast laptop, red on an 8-vCPU box, because what changes is the
  # width of the outage relative to the write rate.
  #
  # Writes are still allowed to fail here: racing a kill is the point. Only
  # the BOOKKEEPING has to be honest about which ones landed.
  ( j=1000; last_acked=""
    while [ ! -f $FLINT_DRILL_ROOT/m3-stop ]; do
      [ "$(valkey-cli -p 6669 -a "tok$i" --no-auth-warning SET "live:$j" "L$i-$j" 2>/dev/null)" = "OK" ] \
        && last_acked=$j
      j=$((j+1))
    done
    echo "${last_acked:-none}" > "$FLINT_DRILL_ROOT/flint-m3-writer$i" ) &
done
sleep 2
pkill -9 -f "flint-server --port 6710"
KILL_T=$(python3 -c 'import time;print(int(time.time()*1000))')

# Recovery probe: a pair-0-range key writable again via the proxy.
PROBE=$(python3 -c '
def c(d):
 p=0x1021;x=0
 for b in d:
  x^=b<<8
  for _ in range(8): x=((x<<1)^p)&0xffff if x&0x8000 else (x<<1)&0xffff
 return x
i=0
while True:
    k=f"probe{i}"
    if c(k.encode())%16384 < 8192:
        print(k); break
    i+=1')
REC=0
for i in $(seq 1 100); do
  [ "$(valkey-cli -p 6669 -a tok00 --no-auth-warning SET "$PROBE" ok 2>&1)" = "OK" ] && { REC=1; break; }
  sleep 0.2
done
NOW_T=$(python3 -c 'import time;print(int(time.time()*1000))')
[ "$REC" = "1" ] || { echo "FAIL: writes did not recover after master kill"; tail -8 $FLINT_DRILL_ROOT/flint-m3-ctl.log; exit 1; }
echo "  failover recovered; client-observed write outage ~$((NOW_T - KILL_T))ms"
sleep 2   # let writers churn on the promoted master
touch $FLINT_DRILL_ROOT/m3-stop; sleep 1

echo "== full 50-tenant integrity sweep through the proxy"
for i in $(seq -w 0 49); do
  D=$(valkey-cli -p 6669 -a "tok$i" --no-auth-warning DBSIZE)
  [ "$D" -ge "$KEYS_PER_TENANT" ] || { echo "FAIL: tenant$i DBSIZE=$D (< $KEYS_PER_TENANT)"; exit 1; }
  for j in 00000 00150 00299; do
    G=$(valkey-cli -p 6669 -a "tok$i" --no-auth-warning GET "data:$j")
    [ "$G" = "t$i-$j" ] || { echo "FAIL: tenant$i data:$j = '$G' (want t$i-$j)"; exit 1; }
  done
  W=$(valkey-cli -p 6669 -a "tok$i" --no-auth-warning SET postcheck ok 2>&1)
  [ "$W" = "OK" ] || { echo "FAIL: tenant$i cannot write post-failover: $W"; exit 1; }
done
echo "  all 50 tenants: seeded keys correct, DBSIZE sane, writes OK"

echo "== the mid-failover writers' data landed under the right tenants"
for i in 01 09 17; do
  # The LAST key this writer saw acked — a key we watched the cluster accept,
  # rather than the midpoint of a range that includes everything it refused
  # mid-failover. "A write we were told landed is still there" is the claim
  # worth making, and it is the claim an operator cares about.
  LAST=$(cat "$FLINT_DRILL_ROOT/flint-m3-writer$i" 2>/dev/null || echo none)
  [ "$LAST" != "none" ] || { echo "FAIL: writer tenant$i never had a single write acked"; exit 1; }
  G=$(valkey-cli -p 6669 -a "tok$i" --no-auth-warning GET "live:$LAST")
  [ "$G" = "L$i-$LAST" ] || { echo "FAIL: writer tenant$i live:$LAST = '$G' — an ACKED write is missing"; exit 1; }
done
echo "  live-writer keys verified on their tenants"

echo "PASS: M3 exit — 50 namespaces on one group, failover under multi-tenant load, all tenants intact"
