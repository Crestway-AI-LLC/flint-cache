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
# The writer subshells below loop until a stop FILE appears, and this cleanup
# used to DELETE that file — so the teardown that is supposed to guarantee
# they stop was removing their only stop signal. On the `exit 1` path (the
# recovery probe failing) they were never signalled at all.
#
# Both leave six subshells looping forever at ppid=1, each forking a
# valkey-cli per iteration. That is not a tidiness problem: a runnable
# process adds 1 to load average, so six of them add SIX on an 8-core box,
# and the next gate's m3_exit then fails its own 20 s recovery probe and
# leaks six more. Observed three gates running: 681 s, 696 s, 668 s, each
# leaving exactly six, each slowing the run that followed it.
#
# So the writers are killed by PID, which holds on every exit path. WRITERS
# is read at call time, so it being empty before the loop runs is fine.
cleanup() {
  # `wait` with NO arguments waits for every background job — including the
  # four seats, the controller and the proxy, which this function does not
  # kill until the lines below. That deadlocks: the drill printed PASS and
  # then hung for 29 minutes. Wait for the WRITER pids specifically.
  if [ -n "${WRITERS:-}" ]; then
    kill -9 $WRITERS 2>/dev/null
    wait $WRITERS 2>/dev/null
  fi
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
TOTAL_ROWS=$(( $(valkey-cli -p 6710 FLINTSLOTSTATS | awk '{s+=$2} END{print s+0}') + $(valkey-cli -p 6720 FLINTSLOTSTATS | awk '{s+=$2} END{print s+0}') ))
echo "  50 tenants seeded; group holds $TOTAL_ROWS rows across 2 pairs"
[ "$TOTAL_ROWS" = "15000" ] || { echo "FAIL: expected 15000 rows, got $TOTAL_ROWS"; exit 1; }

# Let replicas converge before inducing failure (steady-state failover), and
# PROVE the controller could have observed it.
#
# The controller records a lineage holder only for a master satisfying
# `live_replicas >= 1 && seq_lag == Some(0)` (insync_lineage_holder), and it
# needs --confirm consecutive polls at --poll-ms to commit that memory. A fresh
# controller that never saw such a moment REFUSES to promote when the master
# dies -- "no member was ever observed holding the lineage ... PAGE" -- which is
# CORRECT, and which this drill then reported as
#
#     FAIL: writes did not recover after master kill
#
# i.e. as a failover defect. It is not one. It is this setup step never having
# established the precondition the assertion depends on.
#
# The previous loop could not catch that. It sampled seq_lag ONCE, never looked
# at live_replicas -- half the predicate the controller actually uses -- and,
# decisively, fell through SILENTLY after 50 tries. A setup step that cannot
# fail hands the kill whatever state happens to exist. On a loaded box, where
# the replica trails the 6 continuous writers, that state is "never in sync",
# and the drill failed 5 of 12 runs while the controller was behaving exactly
# as designed.
INSYNC_HOLDS=6            # x 0.2s = 1.2s > --poll-ms 150 * --confirm 3
for m in 6710 6720; do
  held=0
  for i in $(seq 1 100); do
    INFO=$(valkey-cli -p $m FLINTINFO | tr '\r' ' ')
    SL=$(printf '%s' "$INFO" | grep -oE 'seq_lag:[a-z0-9]+')
    LR=$(printf '%s' "$INFO" | grep -oE 'live_replicas:[0-9]+' | cut -d: -f2)
    if [ "$SL" = "seq_lag:0" ] && [ "${LR:-0}" -ge 1 ]; then
      held=$((held+1))
      [ "$held" -ge "$INSYNC_HOLDS" ] && break
    else
      held=0
    fi
    sleep 0.2
  done
  if [ "$held" -lt "$INSYNC_HOLDS" ]; then
    echo "FAIL: master $m never held live_replicas>=1 with seq_lag:0 for $((INSYNC_HOLDS*200))ms"
    echo "      (last seen: ${SL:-<none>} live_replicas:${LR:-<none>})."
    echo "      The controller cannot have observed a lineage holder, so it will"
    echo "      correctly REFUSE to promote when the master is killed. This is a"
    echo "      SETUP failure -- the precondition was never established -- and NOT"
    echo "      evidence that failover is broken. Do not read it as data loss."
    exit 1
  fi
done

echo "== multi-tenant load: 6 tenants write continuously; KILL pair-0 master"
rm -f $FLINT_DRILL_ROOT/m3-stop
WRITERS=""
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
  WRITERS="$WRITERS $!"
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
