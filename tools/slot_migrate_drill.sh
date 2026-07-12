#!/usr/bin/env bash
# Slot-move drill: ship one slot's data from a live SOURCE to a live
# DESTINATION in a SINGLE pass, while writes to that slot arrive AFTER the
# migration's snapshot — so those writes can ONLY reach the destination via
# the replication tail, not the bulk copy. Proves the data-shipping half of
# intra-group rebalancing catches mid-copy writes. Ownership cutover is a
# separate step, not exercised here.
set -u
cd "$(dirname "$0")/.."
pkill -9 -f flint-server 2>/dev/null; sleep 0.4
SDIR=$(mktemp -d /tmp/flint-mig-src.XXXXXX); DDIR=$(mktemp -d /tmp/flint-mig-dst.XXXXXX)
B=./target/release/flint-server
SPORT=6530; DPORT=6531
# Small base so the bulk snapshot ships quickly, then a large continuous
# mid-write burst that keeps the source head ahead of the migrate cursor for
# the whole tail phase — guaranteeing the tail (not the bulk) carries them.
BASE=20000        # keys present before the move (the bulk snapshot)
MID_LO=20000; MID_HI=220000  # keys written AFTER the snapshot (tail-only)
cleanup() {
  pkill -9 -f "flint-server --port 653" 2>/dev/null
  rm -rf "$SDIR" "$DDIR"
}
trap cleanup EXIT

$B --port $SPORT --engine rocks --data-dir "$SDIR" 2>/dev/null &
$B --port $DPORT --engine rocks --data-dir "$DDIR" 2>/dev/null &
sleep 0.8
for p in $SPORT $DPORT; do
  [ "$(valkey-cli -p $p PING 2>/dev/null)" = "PONG" ] || { echo "FAIL: node :$p not up"; exit 1; }
done

# Hash tag forces every key into one known slot.
TAG="{mover}"
SLOT=$(python3 - <<'PY'
def crc16(d):
    poly=0x1021; crc=0
    for b in d:
        crc^=b<<8
        for _ in range(8):
            crc=((crc<<1)^poly)&0xffff if crc&0x8000 else (crc<<1)&0xffff
    return crc
print(crc16(b"mover")%16384)
PY
)
echo "== slot for tag $TAG is $SLOT; base=$BASE keys, mid=$((MID_HI-MID_LO)) keys (tail-only)"

echo "== seed $BASE base keys on the SOURCE"
awk -v tag="$TAG" -v n="$BASE" 'BEGIN{for(i=0;i<n;i++){k=sprintf("%s:key%06d",tag,i);v=sprintf("base-%06d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
  | valkey-cli -p $SPORT --pipe | tail -1

# Background writer: one CONTINUOUS pipe of the mid keys, started just before
# the migrate. The bulk (small) ships while this burst is still streaming, so
# the source head stays ahead of the migrate cursor through the tail phase —
# the migrate cannot drain (and return) until the burst finishes. Every mid
# key is written after the snapshot, so it can reach the destination ONLY via
# the tail.
( awk -v tag="$TAG" -v lo="$MID_LO" -v hi="$MID_HI" 'BEGIN{for(i=lo;i<hi;i++){k=sprintf("%s:key%06d",tag,i);v=sprintf("mid-%06d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
    | valkey-cli -p $SPORT --pipe >/dev/null ) &
WRITER=$!
sleep 0.05

echo "== single FLINTMIGRATEIN pass (bulk snapshot + tail of post-snapshot writes)"
RES=$(valkey-cli -p $DPORT -t 180 FLINTMIGRATEIN "127.0.0.1:$SPORT" "$SLOT" 2>&1)
wait $WRITER 2>/dev/null
echo "  result: $RES"
echo "$RES" | grep -q "MIGRATEIN-OK" || { echo "FAIL: migration did not complete: $RES"; exit 1; }
APPLIED=$(echo "$RES" | grep -oE '[0-9]+$')

# The pass must have applied MORE than the bulk alone — i.e. the tail shipped
# post-snapshot writes. (If it equalled BASE, the tail did nothing.)
[ "$APPLIED" -gt "$BASE" ] || { echo "FAIL: applied=$APPLIED not greater than bulk=$BASE — tail shipped nothing"; exit 1; }
echo "  applied=$APPLIED > bulk=$BASE — the tail shipped mid-copy writes"

echo "== verify destination holds base AND tail-only keys, with exact values"
MISS=0
for i in 000000 000001 019999 020000 120000 219999; do
  k="$TAG:key$i"
  got=$(valkey-cli -p $DPORT GET "$k")
  [ -n "$got" ] || { echo "  MISSING on destination: $k"; MISS=$((MISS+1)); }
done
[ "$MISS" = "0" ] || { echo "FAIL: $MISS sampled keys missing on destination"; exit 1; }
# A tail-only key must carry its post-snapshot value, proving it came via tail.
[ "$(valkey-cli -p $DPORT GET "$TAG:key120000")" = "mid-120000" ] || { echo "FAIL: tail-only key wrong/absent on destination"; exit 1; }

SRC_N=$(valkey-cli -p $SPORT DBSIZE); DST_N=$(valkey-cli -p $DPORT DBSIZE)
echo "  source DBSIZE=$SRC_N  destination DBSIZE=$DST_N"
[ "$DST_N" = "$SRC_N" ] || { echo "FAIL: destination count $DST_N != source $SRC_N"; exit 1; }

echo "PASS: single-pass slot move shipped bulk + tail; destination holds the full slot ($DST_N keys), tail-only keys present with post-snapshot values"
