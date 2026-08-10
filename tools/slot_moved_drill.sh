#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# -MOVED enforcement drill: mark one slot as handed off (FLINTSLOTMOVED) and
# verify per-slot ownership on the command path — a key in the moved slot is
# redirected with -MOVED <slot> <addr>, a key in any other slot still serves,
# and the override is DURABLE (survives a restart, since it lives in the
# manifest). This is step 2 of the migration cutover; the freeze/drain/flip
# protocol that sets this state automatically is next.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-moved 6560 7000
fleet_guard
fleet_kill server; sleep 0.4
DIR=$(mktemp -d /tmp/flint-moved.XXXXXX)
B=./target/release/flint-server
PORT=6560
cleanup() { pkill -9 -f "flint-server --port 6560" 2>/dev/null; rm -rf "$DIR"; }
trap cleanup EXIT

$B --port $PORT --engine rocks --data-dir "$DIR" 2>/dev/null &
fleet_wait_listen $PORT
sleep 0.7
[ "$(valkey-cli -p $PORT PING)" = "PONG" ] || { echo "FAIL: node not up"; exit 1; }

# Two hash-tagged key families in known, distinct slots.
SLOT_A=$(python3 -c 'import sys;
def c(d):
 p=0x1021;x=0
 for b in d:
  x^=b<<8
  for _ in range(8): x=((x<<1)^p)&0xffff if x&0x8000 else (x<<1)&0xffff
 return x
print(c(b"alpha")%16384)')
SLOT_B=$(python3 -c '
def c(d):
 p=0x1021;x=0
 for b in d:
  x^=b<<8
  for _ in range(8): x=((x<<1)^p)&0xffff if x&0x8000 else (x<<1)&0xffff
 return x
print(c(b"beta")%16384)')
echo "== slot for {alpha}=$SLOT_A, {beta}=$SLOT_B"

cli_ok valkey-cli -p $PORT SET "{alpha}:k" "va"
cli_ok valkey-cli -p $PORT SET "{beta}:k"  "vb"

echo "== before any move: both keys serve normally"
[ "$(valkey-cli -p $PORT GET "{alpha}:k")" = "va" ] || { echo "FAIL: alpha not served pre-move"; exit 1; }
[ "$(valkey-cli -p $PORT GET "{beta}:k")"  = "vb" ] || { echo "FAIL: beta not served pre-move"; exit 1; }

echo "== hand off slot $SLOT_A to 127.0.0.1:7000 (FLINTSLOTMOVED)"
RES=$(valkey-cli -p $PORT FLINTSLOTMOVED "$SLOT_A" "127.0.0.1:7000")
echo "  $RES"
echo "$RES" | grep -q "moved to" || { echo "FAIL: FLINTSLOTMOVED rejected: $RES"; exit 1; }

echo "== key in the moved slot is redirected; key in the other slot still serves"
MOVED=$(valkey-cli -p $PORT GET "{alpha}:k" 2>&1)
echo "  alpha GET -> $MOVED"
echo "$MOVED" | grep -qE "MOVED $SLOT_A 127.0.0.1:7000" || { echo "FAIL: expected -MOVED for alpha, got: $MOVED"; exit 1; }
WMOVED=$(valkey-cli -p $PORT SET "{alpha}:k" "x" 2>&1)
echo "$WMOVED" | grep -qE "MOVED $SLOT_A" || { echo "FAIL: write to moved slot not redirected: $WMOVED"; exit 1; }
[ "$(valkey-cli -p $PORT GET "{beta}:k")" = "vb" ] || { echo "FAIL: beta disturbed by alpha's move"; exit 1; }
echo "  alpha redirected, beta served"

echo "== restart the node; the override is durable (manifest-backed)"
pkill -9 -f "flint-server --port 6560"; sleep 0.5
$B --port $PORT --engine rocks --data-dir "$DIR" 2>/dev/null &
fleet_wait_listen $PORT
sleep 0.7
MOVED2=$(valkey-cli -p $PORT GET "{alpha}:k" 2>&1)
echo "  after restart, alpha GET -> $MOVED2"
echo "$MOVED2" | grep -qE "MOVED $SLOT_A 127.0.0.1:7000" || { echo "FAIL: override lost across restart: $MOVED2"; exit 1; }
[ "$(valkey-cli -p $PORT GET "{beta}:k")" = "vb" ] || { echo "FAIL: beta lost across restart"; exit 1; }

echo "PASS: per-slot -MOVED enforcement works and survives restart; unrelated slots unaffected"
