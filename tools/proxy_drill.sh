#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Proxy drill: a client that knows ONLY the proxy endpoint keeps working
# while, underneath it: (1) slots migrate between pairs (client must never
# see -MOVED — the proxy absorbs and chases redirects), and (2) a master is
# killed and the controller promotes its replica (the proxy rediscovers the
# new master; the client sees at most a latency blip). This is the routing
# plane's product promise: zero cluster awareness for clients.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-px- 6630 6631 6640 6641 6666
fleet_guard
fleet_kill server; fleet_kill controller; fleet_kill proxy; sleep 0.4
B=./target/release/flint-server
# Two pairs: (6630 master, 6631 replica) and (6640 master, 6641 replica).
cleanup() {
  pkill -9 -f "flint-server --port 66" 2>/dev/null
  fleet_kill controller
  fleet_kill proxy
  rm -rf /tmp/flint-px-*
}
trap cleanup EXIT

for spec in "6630:" "6631:127.0.0.1:6630" "6640:" "6641:127.0.0.1:6640"; do
  p=${spec%%:*}; m=${spec#*:}
  d="/tmp/flint-px-$p"; rm -rf "$d"
  if [ -n "$m" ]; then
    $B --port $p --engine rocks --data-dir "$d" --replica-of "$m" 2>/dev/null &
  else
    $B --port $p --engine rocks --data-dir "$d" 2>/dev/null &
  fi
  sleep 0.4
done
sleep 0.8

echo "== start controller (failover) and proxy (routing)"
./target/release/flint-controller --pairs "127.0.0.1:6630,127.0.0.1:6631;127.0.0.1:6640,127.0.0.1:6641" \
  --id PX --poll-ms 150 --confirm 3 2>/tmp/flint-px-ctl.log &
./target/release/flint-proxy --port 6666 --pairs "127.0.0.1:6630,127.0.0.1:6631;127.0.0.1:6640,127.0.0.1:6641" 2>/tmp/flint-px-proxy.log &
fleet_wait_listen 6666
sleep 1.2
[ "$(valkey-cli -p 6666 PING)" = "PONG" ] || { echo "FAIL: proxy not up"; exit 1; }

echo "== client (proxy-only) writes 8000 keys across the keyspace"
awk 'BEGIN{for(i=0;i<8000;i++){k=sprintf("user:%06d",i);v=sprintf("val-%06d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
  | valkey-cli -p 6666 --pipe | tail -1
[ "$(valkey-cli -p 6666 DBSIZE)" = "8000" ] || { echo "FAIL: DBSIZE via proxy != 8000"; exit 1; }
echo "  8000 keys via proxy; DBSIZE fan-out agrees"

# The load above is RESP-encoded, which is why it never exercised the INLINE
# parser. `redis-cli --pipe` forwards whatever it is fed verbatim, and the
# mass-insert recipe in Redis's own documentation feeds it plain text. The
# proxy rejected that and closed the connection while --pipe still printed
# "All data transferred" — a 200k-key load that reported success and wrote
# nothing. Same tool, same flag, plain-text input: that is the whole bug.
echo "== the SAME --pipe load as plain-text INLINE commands"
awk 'BEGIN{for(i=0;i<500;i++)printf "SET inline:%06d v-%06d\r\n",i,i}' \
  | valkey-cli -p 6666 --pipe | tail -1
[ "$(valkey-cli -p 6666 DBSIZE)" = "8500" ] || {
  echo "FAIL: inline --pipe load silently dropped (DBSIZE $(valkey-cli -p 6666 DBSIZE), want 8500)"; exit 1; }
[ "$(valkey-cli -p 6666 GET inline:000499)" = "v-000499" ] || { echo "FAIL: inline write not readable"; exit 1; }
# A bare inline command on its own connection, unbatched.
[ "$(printf 'PING\r\n' | nc -w 2 127.0.0.1 6666 | tr -d '\r\n+')" = "PONG" ] \
  || { echo "FAIL: bare inline PING refused by the proxy"; exit 1; }
echo "  500 inline writes landed and read back; bare inline PING answered"

echo "== migrate a busy slot between pairs; client reads via proxy must not notice"
# Find where 'user:000042' lives: its slot's default owner, then move that
# slot to the OTHER pair directly (admin path), and read via proxy.
SLOT=$(python3 -c '
def c(d):
 p=0x1021;x=0
 for b in d:
  x^=b<<8
  for _ in range(8): x=((x<<1)^p)&0xffff if x&0x8000 else (x<<1)&0xffff
 return x
print(c(b"user:000042")%16384)')
NPAIRS=2
PAIRIDX=$((SLOT * NPAIRS / 16384))
if [ "$PAIRIDX" = "0" ]; then SRC=6630; DST=6640; else SRC=6640; DST=6630; fi
echo "  key user:000042 -> slot $SLOT, default pair $PAIRIDX (:$SRC); moving to :$DST"
RES=$(valkey-cli -p $DST FLINTMIGRATEIN "127.0.0.1:$SRC" "$SLOT" "127.0.0.1:$DST" 2>&1)
echo "$RES" | grep -q "MIGRATEIN-OK.*cutover" || { echo "FAIL: migration failed: $RES"; exit 1; }
# The client keeps using the proxy; the first read hits the old owner, gets
# -MOVED internally, and must still come back correct.
GOT=$(valkey-cli -p 6666 GET "user:000042")
[ "$GOT" = "val-000042" ] || { echo "FAIL: read after migration via proxy: '$GOT'"; exit 1; }
GOT2=$(valkey-cli -p 6666 GET "user:000042")
[ "$GOT2" = "val-000042" ] || { echo "FAIL: cached-route read: '$GOT2'"; exit 1; }
W=$(valkey-cli -p 6666 SET "user:000042" "val-000042" 2>&1)
[ "$W" = "OK" ] || { echo "FAIL: write to migrated slot via proxy: $W"; exit 1; }
echo "  migrated slot served through proxy: reads + writes OK, no -MOVED leaked"

echo "== kill pair-0 master mid-traffic; controller promotes; proxy chases"
pkill -9 -f "flint-server --port 6630"

# ORDER MATTERS HERE, and it is the whole point of this section.
#
# Wait for the promotion by asking the NODE, not the proxy, so that the
# fan-out below is the FIRST thing the proxy is asked after the topology
# moved. Keyed traffic re-resolves a dead backend as a routing miss and
# repairs the shared master table as a side effect — so any keyed read done
# first would hide a fan-out that cannot rediscover on its own. Checking
# fan-out after the keyed recovery loop passes against the buggy proxy;
# checking it before does not. The bug was permanent, not slow: DBSIZE and
# SCAN stayed pointed at the corpse until someone restarted the proxy.
PROMOTED=0
for _ in $(seq 1 100); do
  valkey-cli -p 6631 FLINTINFO 2>/dev/null | tr '\r' ' ' | grep -q "role:master" \
    && { PROMOTED=1; break; }
  sleep 0.2
done
[ "$PROMOTED" = "1" ] || { echo "FAIL: controller never promoted 6631"; exit 1; }

echo "== fan-out rediscovers on its own, with no keyed traffic to repair it"
FN=0
for _ in $(seq 1 60); do
  N=$(valkey-cli -p 6666 DBSIZE 2>&1)
  if [ "$N" -ge 8500 ] 2>/dev/null && valkey-cli -p 6666 SCAN 0 COUNT 10 >/dev/null 2>&1; then
    FN=1; break
  fi
  sleep 0.2
done
[ "$FN" = "1" ] || {
  echo "FAIL: fan-out never recovered after promotion (DBSIZE -> [$N]); stale master table"
  exit 1; }
echo "  DBSIZE $N and SCAN answered by the promoted master, unaided"

OK=0
for i in $(seq 1 100); do
  R=$(valkey-cli -p 6666 GET "user:000001" 2>&1)   # slot in pair 0's range
  if [ "$R" = "val-000001" ]; then OK=1; break; fi
  sleep 0.2
done
[ "$OK" = "1" ] || { echo "FAIL: reads did not recover after failover"; tail -6 /tmp/flint-px-proxy.log; exit 1; }
echo "  reads recovered after failover ($((i*200))ms observed via client)"
W2=$(valkey-cli -p 6666 SET post-failover ok 2>&1)
[ "$W2" = "OK" ] || echo "  (write settling: $W2)"
for i in $(seq 1 50); do
  [ "$(valkey-cli -p 6666 SET post-failover ok 2>&1)" = "OK" ] && break; sleep 0.2
done
[ "$(valkey-cli -p 6666 GET post-failover)" = "ok" ] || { echo "FAIL: writes did not recover"; exit 1; }
echo "  writes recovered; client never saw the topology"

echo "== final consistency: sampled keys correct via proxy"
for k in 000000 000042 004321 007999; do
  [ "$(valkey-cli -p 6666 GET "user:$k")" = "val-$k" ] || { echo "FAIL: user:$k wrong after all events"; exit 1; }
done

echo "PASS: one endpoint, zero client cluster-awareness — migration and failover both absorbed by the proxy"
