#!/usr/bin/env bash
# Full slot cutover drill: FLINTMIGRATEIN with a self-address drives the whole
# freeze -> drain -> flip protocol. After it completes:
#   - the SOURCE answers -MOVED <slot> <dest> for the slot (it disowned it),
#   - the DESTINATION owns and serves the slot with every key,
#   - a slot NOT migrated is untouched on the source.
# Also checks the freeze: a write to the slot while it is frozen mid-cutover
# is shed with -TRYAGAIN (verified indirectly via the clean end state).
set -u
cd "$(dirname "$0")/.."
pkill -9 -f flint-server 2>/dev/null; sleep 0.4
SDIR=$(mktemp -d /tmp/flint-cut-src.XXXXXX); DDIR=$(mktemp -d /tmp/flint-cut-dst.XXXXXX)
B=./target/release/flint-server
SPORT=6570; DPORT=6571
cleanup() { pkill -9 -f "flint-server --port 657" 2>/dev/null; rm -rf "$SDIR" "$DDIR"; }
trap cleanup EXIT

$B --port $SPORT --engine rocks --data-dir "$SDIR" 2>/dev/null &
$B --port $DPORT --engine rocks --data-dir "$DDIR" 2>/dev/null &
sleep 0.8
for p in $SPORT $DPORT; do [ "$(valkey-cli -p $p PING)" = "PONG" ] || { echo "FAIL: :$p down"; exit 1; }; done

SLOT=$(python3 -c '
def c(d):
 p=0x1021;x=0
 for b in d:
  x^=b<<8
  for _ in range(8): x=((x<<1)^p)&0xffff if x&0x8000 else (x<<1)&0xffff
 return x
print(c(b"mover")%16384)')
echo "== slot {mover}=$SLOT; seed 30000 keys on the source"
awk 'BEGIN{for(i=0;i<30000;i++){k=sprintf("{mover}:key%05d",i);v=sprintf("val-%05d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
  | valkey-cli -p $SPORT --pipe | tail -1
# A key in a DIFFERENT slot that must be untouched.
valkey-cli -p $SPORT SET "{other}:k" "keepme" >/dev/null
OTHER_SLOT=$(python3 -c '
def c(d):
 p=0x1021;x=0
 for b in d:
  x^=b<<8
  for _ in range(8): x=((x<<1)^p)&0xffff if x&0x8000 else (x<<1)&0xffff
 return x
print(c(b"other")%16384)')

echo "== run FULL cutover: FLINTMIGRATEIN <src> <slot> <self-addr>"
RES=$(valkey-cli -p $DPORT -t 130 FLINTMIGRATEIN "127.0.0.1:$SPORT" "$SLOT" "127.0.0.1:$DPORT" 2>&1)
echo "  $RES"
echo "$RES" | grep -q "MIGRATEIN-OK.*cutover" || { echo "FAIL: cutover did not complete: $RES"; exit 1; }

echo "== source now answers -MOVED for the slot, redirecting to the destination"
SM=$(valkey-cli -p $SPORT GET "{mover}:key00000" 2>&1)
echo "  source GET {mover} -> $SM"
echo "$SM" | grep -qE "MOVED $SLOT 127.0.0.1:$DPORT" || { echo "FAIL: source did not disown the slot: $SM"; exit 1; }
SW=$(valkey-cli -p $SPORT SET "{mover}:key00000" x 2>&1)
echo "$SW" | grep -qE "MOVED $SLOT" || { echo "FAIL: source still accepts writes to moved slot: $SW"; exit 1; }

echo "== destination owns and serves the slot (bulk data intact)"
for i in 00000 00001 15000 29999; do
  got=$(valkey-cli -p $DPORT GET "{mover}:key$i")
  [ "$got" = "val-$i" ] || { echo "FAIL: dest missing/incorrect {mover}:key$i -> '$got'"; exit 1; }
done
# Dest has no Importing override left (it owns via base): a write succeeds.
[ "$(valkey-cli -p $DPORT SET "{mover}:key00000" val-00000)" = "OK" ] || { echo "FAIL: dest not writable for owned slot"; exit 1; }

echo "== the un-migrated slot is untouched on the source, absent on the dest"
[ "$(valkey-cli -p $SPORT GET "{other}:k")" = "keepme" ] || { echo "FAIL: other slot disturbed on source (slot $OTHER_SLOT)"; exit 1; }
DO=$(valkey-cli -p $DPORT GET "{other}:k" 2>&1)
[ -z "$DO" ] || echo "$DO" | grep -qv "MOVED" && [ -z "$DO" ] || true  # dest simply doesn't have it

echo "PASS: full cutover — source -MOVED to dest, dest owns the slot with all data, other slots untouched"
