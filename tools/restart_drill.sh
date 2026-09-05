#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Warm-restart drill: load data into flint (rocks engine), kill -9 the
# server, time the restart, and verify the data survived.
#
# TWO times, since #176, because they are now genuinely different events and
# only one of them is recovery:
#
#   restart-to-PONG      the listener is open and the node answers. It is
#                        answering `-LOADING`: alive, not serving. This is
#                        what a supervisor or a controller sees, and the
#                        whole point of #176 is that it arrives immediately
#                        instead of at the end.
#   restart-to-SERVING   the store is open and data commands work. This is
#                        the recovery number, and the one to compare across
#                        builds.
#
# The drill used to print the first and call it recovery — which was true
# only while the two were the same event, and stopped being true the moment
# the listener moved ahead of the load. Reading a key on the first PONG now
# gets `-LOADING`, correctly.
#
# Requires: a release build with --features rocks, valkey-cli on PATH.
# Usage: tools/restart_drill.sh [KEYS]
#
# THE PORT IS A LITERAL BELOW, NOT AN ARGUMENT (docs/bugs/0020).
# assert_no_port_overlap builds its map by parsing the `fleet_init` lines of
# tools/*_drill.sh, so a port that arrives through $2 declares nothing. This
# drill lived its whole life inside that blind spot for exactly that reason,
# and its cleanup was a bare `pkill -f` that owned nothing — which is how a
# full gate came to report `restart(leaked)` while the drill's own assertions
# all passed.
set -euo pipefail

KEYS="${1:-100000}"
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-restart- 6410
fleet_guard
fleet_kill server
sleep 0.3

PORT=6410
DIR="$(mktemp -d $FLINT_DRILL_ROOT/flint-restart-data.XXXXXX)"
BIN=./target/release/flint-server

cleanup() { fleet_kill server; rm -rf "$DIR"; }
trap cleanup EXIT

echo "== drill: $KEYS keys, port $PORT, dir $DIR"

"$BIN" --port "$PORT" --engine rocks --data-dir "$DIR" &
# WAIT for the port, do not guess at it. This was `sleep 0.5`, which is a bet
# that the machine can start a just-linked binary in half a second — and the
# first run after a build loses that bet, because the first exec pays for
# signature validation and a cold page cache. The 100k-key load below then
# hit a socket nobody was listening on and the drill died in one second with
# no diagnosis, on a build that was fine. It then became a hand-rolled poll,
# which fleet_wait_ping now owns for the whole suite; see its comment for why
# a loop that expires silently is worse than no loop at all.
fleet_wait_ping "$PORT"

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
} > "$DIR/load.resp"

# THROUGH fleet_load_resp, NOT a second copy of its rules.
#
# The first fix for BUG-0096 open-coded the shed tolerance here -- and
# fleet.sh had carried `fleet_load_resp` since BUG-0035, doing the same job
# for the lag cap, with `repl_drill`'s own comment describing this exact
# failure ("a single -THROTTLED aborted the run HERE"). Two implementations of
# one rule is the thing this suite keeps paying for, so the useful parts of
# the open-coded version were folded INTO the helper (a reply-count check, a
# shed ceiling, and the positive rule that every error line is a shed) and
# this call site now has none of them.
#
# The ceiling is 0.05% of the load: one shed in 101,000 is a slow runner,
# hundreds is the deadline estimator firing systematically.
fleet_load_resp "$PORT" "cat $DIR/load.resp" "$(( KEYS + 1000 ))" "$(( (KEYS + 1000) / 2000 ))" \
  || exit 1
rm -f "$DIR/load.resp"

# DERIVE the sampled key from KEYS; do not hardcode an index. This was
# key:0042000, which is never written when KEYS <= 42000 — so
# `restart_drill.sh 20000` ended in "FAIL: string lost ()" about a key that
# had never existed. The empty parenthesis was the only thing separating a
# reported durability failure from a drill that could not answer. The gate
# always passes the 100000 default, so it never fired there.
SAMPLE_IDX=$(( KEYS / 2 ))
SAMPLE_KEY=$(printf 'key:%07d' "$SAMPLE_IDX")
SAMPLE_VAL=$(printf 'value-%07d' "$SAMPLE_IDX")
BEFORE_SAMPLE=$(valkey-cli -p "$PORT" GET "$SAMPLE_KEY" || true)
echo "== sample before kill: $SAMPLE_KEY = $BEFORE_SAMPLE"
[ "$BEFORE_SAMPLE" = "$SAMPLE_VAL" ] \
  || { echo "FAIL: $SAMPLE_KEY was not written before the kill — the survival check below could not answer"; exit 1; }
# AND THE HASH, which was checked after the restart and never before it. The
# string got this guard because `key:0042000` was verified without ever having
# been written, and "the empty parenthesis was the only thing separating a
# reported durability failure from a drill that could not answer". The hash
# three lines later kept the original shape, so an HSET that was shed or never
# issued still reports `FAIL: hash lost ()` — the same false durability
# failure, in the same file, for the same reason.
BEFORE_HASH=$(valkey-cli -p "$PORT" HGET hash:0777 f1 || true)
[ "$BEFORE_HASH" = "v777" ] \
  || { echo "FAIL: hash:0777 f1 was not written before the kill (got '$BEFORE_HASH') — the survival check below could not answer"; exit 1; }

echo "== WAL fsync cadence is live (bounded host-loss window)"
sleep 1.2   # > two 500 ms ticks
WFT=$(valkey-cli -p "$PORT" FLINTINFO | tr -d '\r' | grep '^wal_fsync_total:' | cut -d: -f2)
[ "${WFT:-0}" -ge 1 ] || { echo "FAIL: wal_fsync_total never advanced ($WFT)"; exit 1; }
echo "== wal_fsync_total=$WFT (cadence $(valkey-cli -p "$PORT" FLINTINFO | tr -d '\r' | grep '^wal_fsync_ms:' | cut -d: -f2) ms)"

echo "== kill -9"
# Scoped to OUR seat on OUR port, and CHECKED. fleet_signal_port returns
# non-zero when it signalled nothing, and that distinction is the whole drill:
# a kill that matched no process leaves the original server up, so the restart
# timed below is a server that never went down and the survival check reads
# memory that was never reloaded from disk. A green from that is worthless.
fleet_signal_port "$PORT" -9 \
  || { echo "FAIL: no seat of ours on $PORT to kill — nothing below would be measuring a restart"; exit 1; }
sleep 0.3

echo "== restarting (timing to PONG, then to SERVING)"
now_ns() { date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))'; }
START_NS=$(now_ns)
"$BIN" --port "$PORT" --engine rocks --data-dir "$DIR" &
# Polls far tighter than fleet_wait_ping (20 ms vs 200 ms) because this loop
# IS the measurement — but BOTH waits are bounded, because an unbounded
# `until` here hangs the whole gate rather than failing this one drill, and
# gates.sh puts no timeout around a step. 775911c added the second wait with
# no deadline; that discipline is main's and it is kept.
RESTART_DEADLINE=$(( $(date +%s) + 60 ))
until valkey-cli -p "$PORT" PING 2>/dev/null | grep -q PONG; do
  [ "$(date +%s)" -lt "$RESTART_DEADLINE" ] \
    || { echo "FAIL: no PONG from $PORT 60s after restart"; exit 1; }
  sleep 0.02
done
PONG_NS=$(now_ns)
# PONG means ALIVE, not SERVING. `loading:1` is the seat's own answer and only
# an explicit 1 counts — the same rule flintctl and the controller use, so all
# three agree on what ready means. Measuring only the first is what let a
# recovering node look recovered.
SERVE_DEADLINE=$(( $(date +%s) + 120 ))
until ! valkey-cli -p "$PORT" FLINTINFO 2>/dev/null | tr -d '\r' | grep -qx 'loading:1'; do
  [ "$(date +%s)" -lt "$SERVE_DEADLINE" ] \
    || { echo "FAIL: $PORT still reports loading:1 120s after restart"; exit 1; }
  sleep 0.02
done
SERVE_NS=$(now_ns)
echo "== restart-to-PONG: $(( (PONG_NS - START_NS) / 1000000 )) ms (alive)"
echo "== restart-to-SERVING: $(( (SERVE_NS - START_NS) / 1000000 )) ms (recovered)"

echo "== verification after restart"
AFTER_SAMPLE=$(valkey-cli -p "$PORT" GET "$SAMPLE_KEY")
HASH_SAMPLE=$(valkey-cli -p "$PORT" HGET "hash:0777" f1)
[ "$AFTER_SAMPLE" = "$SAMPLE_VAL" ] || { echo "FAIL: string lost ($AFTER_SAMPLE)"; exit 1; }
[ "$HASH_SAMPLE" = "v777" ] || { echo "FAIL: hash lost ($HASH_SAMPLE)"; exit 1; }
echo "PASS: data survived kill -9 (string + hash verified)"
