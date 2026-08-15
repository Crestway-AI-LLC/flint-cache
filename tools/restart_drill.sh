#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Warm-restart drill: load data into flint (rocks engine), kill -9 the
# server, time restart-to-first-PONG, and verify the data survived.
#
# Requires: a release build with --features rocks, valkey-cli on PATH.
# Usage: tools/restart_drill.sh [KEYS] [PORT]
set -euo pipefail

KEYS="${1:-100000}"
PORT="${2:-6410}"
DIR="$(mktemp -d ${FLINT_DRILL_ROOT:-/tmp}/flint-drill.XXXXXX)"
BIN="$(dirname "$0")/../target/release/flint-server"

cleanup() { pkill -f "flint-server --port $PORT" 2>/dev/null || true; rm -rf "$DIR"; }
trap cleanup EXIT

echo "== drill: $KEYS keys, port $PORT, dir $DIR"

"$BIN" --port "$PORT" --engine rocks --data-dir "$DIR" &
# WAIT for the port, do not guess at it. This was `sleep 0.5`, which is a bet
# that the machine can start a just-linked binary in half a second — and the
# first run after a build loses that bet, because the first exec pays for
# signature validation and a cold page cache. The 100k-key load below then
# hit a socket nobody was listening on and the drill died in one second with
# no diagnosis, on a build that was fine. The restart phase further down
# already polls for PONG; this start had no reason to differ.
for _ in $(seq 1 200); do
  [ "$(valkey-cli -p "$PORT" PING 2>/dev/null)" = "PONG" ] && break
  sleep 0.05
done
[ "$(valkey-cli -p "$PORT" PING 2>/dev/null)" = "PONG" ] \
  || { echo "FAIL: server never came up on $PORT"; exit 1; }

echo "== loading $KEYS strings + 1000 hashes via valkey-cli --pipe"
# Emit proper RESP arrays (flint speaks RESP only; inline commands are a
# compatibility-backlog item).
{
  awk -v n="$KEYS" 'BEGIN {
    for (i = 0; i < n; i++) {
      k = sprintf("key:%07d", i); v = sprintf("value-%07d", i)
      printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, length(v), v
    }
  }'
  awk 'BEGIN {
    for (i = 0; i < 1000; i++) {
      k = sprintf("hash:%04d", i); v1 = sprintf("v%d", i); v2 = sprintf("w%d", i)
      printf "*6\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n$2\r\nf1\r\n$%d\r\n%s\r\n$2\r\nf2\r\n$%d\r\n%s\r\n", length(k), k, length(v1), v1, length(v2), v2
    }
  }'
} | valkey-cli -p "$PORT" --pipe | tail -1

BEFORE_SAMPLE=$(valkey-cli -p "$PORT" GET "key:0042000" || true)
echo "== sample before kill: key:0042000 = $BEFORE_SAMPLE"

echo "== WAL fsync cadence is live (bounded host-loss window)"
sleep 1.2   # > two 500 ms ticks
WFT=$(valkey-cli -p "$PORT" FLINTINFO | tr -d '\r' | grep '^wal_fsync_total:' | cut -d: -f2)
[ "${WFT:-0}" -ge 1 ] || { echo "FAIL: wal_fsync_total never advanced ($WFT)"; exit 1; }
echo "== wal_fsync_total=$WFT (cadence $(valkey-cli -p "$PORT" FLINTINFO | tr -d '\r' | grep '^wal_fsync_ms:' | cut -d: -f2) ms)"

echo "== kill -9"
pkill -9 -f "flint-server --port $PORT"
sleep 0.3

echo "== restarting (timing to first successful PING)"
START_NS=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')
"$BIN" --port "$PORT" --engine rocks --data-dir "$DIR" &
until valkey-cli -p "$PORT" PING 2>/dev/null | grep -q PONG; do sleep 0.02; done
END_NS=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')
echo "== restart-to-PONG: $(( (END_NS - START_NS) / 1000000 )) ms"

echo "== verification after restart"
AFTER_SAMPLE=$(valkey-cli -p "$PORT" GET "key:0042000")
HASH_SAMPLE=$(valkey-cli -p "$PORT" HGET "hash:0777" f1)
[ "$AFTER_SAMPLE" = "value-0042000" ] || { echo "FAIL: string lost ($AFTER_SAMPLE)"; exit 1; }
[ "$HASH_SAMPLE" = "v777" ] || { echo "FAIL: hash lost ($HASH_SAMPLE)"; exit 1; }
echo "PASS: data survived kill -9 (string + hash verified)"
