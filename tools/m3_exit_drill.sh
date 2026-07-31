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
fleet_init /tmp/flint-m3- 6669 6710 6711 6720 6721
fleet_guard
fleet_kill server; fleet_kill controller; fleet_kill proxy; sleep 0.4
B=./target/release/flint-server
KEYS_PER_TENANT=300
cleanup() {
  pkill -9 -f "flint-server --port 67" 2>/dev/null
  fleet_kill controller
  fleet_kill proxy
  rm -rf /tmp/flint-m3-* /tmp/m3-stop
}
trap cleanup EXIT

echo "== group: 2 replicated pairs (6710/6711, 6720/6721)"
for spec in "6710:" "6711:127.0.0.1:6710" "6720:" "6721:127.0.0.1:6720"; do
  p=${spec%%:*}; m=${spec#*:}
  d="/tmp/flint-m3-$p"; rm -rf "$d"
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
  --id M3 --poll-ms 150 --confirm 3 2>/tmp/flint-m3-ctl.log &
./target/release/flint-proxy --port 6669 \
  --pairs "127.0.0.1:6710,127.0.0.1:6711;127.0.0.1:6720,127.0.0.1:6721" \
  --tenants "$TENANTS" 2>/tmp/flint-m3-proxy.log &
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
TOTAL_ROWS=$(( $(valkey-cli -p 6710 FLINTSLOTSTATS | awk '{s+=$2} END{print s+0}') + $(valkey-cli -p 6720 FLINTSLOTSTATS | awk '{s+=$2} END{print s+0}') ))
echo "  50 tenants seeded; group holds $TOTAL_ROWS rows across 2 pairs"
[ "$TOTAL_ROWS" = "15000" ] || { echo "FAIL: expected 15000 rows, got $TOTAL_ROWS"; exit 1; }

# Let replicas converge before inducing failure (steady-state failover).
for m in 6710 6720; do
  for i in $(seq 1 50); do
    SL=$(valkey-cli -p $m FLINTINFO | tr '\r' ' ' | grep -oE "seq_lag:[a-z0-9]+")
    [ "$SL" = "seq_lag:0" ] && break; sleep 0.2
  done
done

echo "== multi-tenant load: 6 tenants write continuously; KILL pair-0 master"
rm -f /tmp/m3-stop
for i in 01 09 17 25 33 41; do
  ( j=1000
    while [ ! -f /tmp/m3-stop ]; do
      valkey-cli -p 6669 -a "tok$i" --no-auth-warning SET "live:$j" "L$i-$j" >/dev/null 2>&1
      j=$((j+1))
    done
    echo $j > "/tmp/flint-m3-writer$i" ) &
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
[ "$REC" = "1" ] || { echo "FAIL: writes did not recover after master kill"; tail -8 /tmp/flint-m3-ctl.log; exit 1; }
echo "  failover recovered; client-observed write outage ~$((NOW_T - KILL_T))ms"
sleep 2   # let writers churn on the promoted master
touch /tmp/m3-stop; sleep 1

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
  LAST=$(cat "/tmp/flint-m3-writer$i" 2>/dev/null || echo 1000)
  MID=$(( (1000 + LAST) / 2 ))
  G=$(valkey-cli -p 6669 -a "tok$i" --no-auth-warning GET "live:$MID")
  [ "$G" = "L$i-$MID" ] || { echo "FAIL: writer tenant$i live:$MID = '$G'"; exit 1; }
done
echo "  live-writer keys verified on their tenants"

echo "PASS: M3 exit — 50 namespaces on one group, failover under multi-tenant load, all tenants intact"
