#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Run the JSON conformance corpus against the REAL RedisJSON module and
# assert that the only cases which fail are the ones we deliberately chose
# to differ on.
#
# Why this exists: the JSON family is flint-only in the corpus (stock
# Redis/Valkey have no JSON type), so a green run against our own engines
# proves self-consistency and nothing about compatibility. This script is
# the missing oracle. It cannot run in CI — RedisJSON is a separate module
# that has to be built from source — so it is an on-demand gate, run when
# JSON semantics change.
#
# Usage:
#   REJSON_MODULE=/path/to/librejson.dylib tools/redisjson_compare.sh
#
# To produce the module:
#   git clone https://github.com/RedisJSON/RedisJSON && cd RedisJSON
#   cargo build --release      # -> target/release/librejson.{dylib,so}
# and a Redis/Valkey server binary that can load modules (redis 8.x).
set -u
cd "$(dirname "$0")/.."

PORT=${PORT:-6390}
SERVER=${REDIS_SERVER:-$(command -v redis-server || echo /opt/homebrew/opt/redis@8.2/bin/redis-server)}
MODULE=${REJSON_MODULE:-}
CLI=${REDIS_CLI:-$(command -v valkey-cli || command -v redis-cli)}

if [ -z "$MODULE" ] || [ ! -f "$MODULE" ]; then
  echo "SKIP: set REJSON_MODULE to a built librejson.dylib/.so (see header)"
  exit 0
fi
if [ ! -x "$SERVER" ]; then
  echo "SKIP: no module-capable redis-server (set REDIS_SERVER)"
  exit 0
fi

# The cases we KNOWINGLY answer differently. Each is a decision recorded in
# docs/command-support.md, not an accident — so the gate is "exactly these
# three fail", in both directions: a NEW failure is a regression, and a
# disappearing one means the divergence was quietly dropped.
EXPECTED_DIVERGENCES=(
  "TYPE doc"            # we answer "json"; RedisJSON answers "ReJSON-RL"
  "JSON.SET d \$.a[3]"  # index == len appends here, RedisJSON refuses
  "JSON.GET d \$..b"    # multi-match paths are UNSUPPORTED in our v1
)

cleanup() { [ -n "${SRV_PID:-}" ] && kill "$SRV_PID" 2>/dev/null; }
trap cleanup EXIT

cargo build --release -p flint-conformance >/dev/null 2>&1 || {
  echo "FAIL: could not build flint-conformance"; exit 1; }

"$SERVER" --port "$PORT" --save '' --loadmodule "$MODULE" >/tmp/rejson-oracle.log 2>&1 &
SRV_PID=$!
for _ in $(seq 1 40); do
  [ "$($CLI -p "$PORT" PING 2>/dev/null)" = "PONG" ] && break; sleep 0.25
done
if [ "$($CLI -p "$PORT" PING 2>/dev/null)" != "PONG" ]; then
  echo "FAIL: server did not come up"; tail -5 /tmp/rejson-oracle.log; exit 1
fi
$CLI -p "$PORT" JSON.SET __probe '$' '{}' >/dev/null 2>&1 || {
  echo "FAIL: module loaded but JSON.SET is unknown"; exit 1; }
$CLI -p "$PORT" FLUSHALL >/dev/null

OUT=$(./target/release/flint-conformance --target "127.0.0.1:$PORT" 2>&1)
echo "$OUT" | grep -E '^  json |^overall'

FAILS=$(echo "$OUT" | grep '^  \[json\]')
N=$(printf '%s' "$FAILS" | grep -c . )
echo "--- divergences from RedisJSON ($N) ---"
echo "$FAILS"

RC=0
[ "$N" = "${#EXPECTED_DIVERGENCES[@]}" ] || {
  echo "FAIL: expected ${#EXPECTED_DIVERGENCES[@]} divergences, saw $N"; RC=1; }
for want in "${EXPECTED_DIVERGENCES[@]}"; do
  echo "$FAILS" | grep -qF "$want" || {
    echo "FAIL: the documented divergence '$want' no longer appears"; RC=1; }
done

[ "$RC" = 0 ] && echo "PASS: RedisJSON agrees with Flint everywhere except the ${#EXPECTED_DIVERGENCES[@]} documented divergences"
exit $RC
