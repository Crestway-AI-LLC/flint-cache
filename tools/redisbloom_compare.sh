#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Run the Bloom conformance corpus against the REAL RedisBloom module and
# assert that the only cases which fail are the ones we deliberately chose
# to differ on (ADR-0016 D7).
#
# Why this exists: the `bloom` family is flint-only in the corpus (stock
# Redis/Valkey have no BF.* commands — they come from a module), so a green
# run against our own engines proves self-consistency and NOTHING about
# compatibility. Without this script, "RedisBloom-compatible" means "matches
# the contract we wrote". It cannot run in CI — RedisBloom has to be built
# from source — so it is an on-demand gate, run when BF semantics change.
#
# Usage:
#   REBLOOM_MODULE=/path/to/redisbloom.so tools/redisbloom_compare.sh
#
# To produce the module:
#   git clone --recursive https://github.com/RedisBloom/RedisBloom
#   cd RedisBloom && make          # -> bin/<platform>/redisbloom.so
# and a Redis/Valkey server binary that can load modules (redis 8.x).
set -u
cd "$(dirname "$0")/.."

PORT=${PORT:-6391}
SERVER=${REDIS_SERVER:-$(command -v redis-server || echo /opt/homebrew/opt/redis@8.2/bin/redis-server)}
MODULE=${REBLOOM_MODULE:-}
CLI=${REDIS_CLI:-$(command -v valkey-cli || command -v redis-cli)}

if [ -z "$MODULE" ] || [ ! -f "$MODULE" ]; then
  echo "SKIP: set REBLOOM_MODULE to a built redisbloom.so (see header)"
  exit 0
fi
if [ ! -x "$SERVER" ]; then
  echo "SKIP: no module-capable redis-server (set REDIS_SERVER)"
  exit 0
fi

# The cases we KNOWINGLY answer differently, each a decision in ADR-0016 D7
# and docs/command-support.md. The gate is "exactly these fail", in BOTH
# directions: a new failure is a regression, and a disappearing one means a
# divergence was quietly dropped — or, worse, that the corpus stopped
# exercising it.
#
# NOTE the asymmetry in what a failure MEANS here. For the two entries
# below, RedisBloom is "right" and we differ on purpose. Anything else that
# fails is a place where a real client would behave differently against
# Flint than against RedisBloom, which is the whole thing this family
# exists to avoid.
EXPECTED_DIVERGENCES=(
  "TYPE bf"           # we answer "bloom"; RedisBloom answers "MBbloom--"
  "BF.SCANDUMP i 0"   # refused here: our block layout is not theirs (D7.2)
)

cleanup() { [ -n "${SRV_PID:-}" ] && kill "$SRV_PID" 2>/dev/null; }
trap cleanup EXIT

cargo build --release -p flint-conformance >/dev/null 2>&1 || {
  echo "FAIL: could not build flint-conformance"; exit 1; }

"$SERVER" --port "$PORT" --save '' --loadmodule "$MODULE" >/tmp/rebloom-oracle.log 2>&1 &
SRV_PID=$!
for _ in $(seq 1 40); do
  [ "$($CLI -p "$PORT" PING 2>/dev/null)" = "PONG" ] && break; sleep 0.25
done
if [ "$($CLI -p "$PORT" PING 2>/dev/null)" != "PONG" ]; then
  echo "FAIL: server did not come up"; tail -5 /tmp/rebloom-oracle.log; exit 1
fi
# Capability assert, not a version string: prove the module actually serves
# BF.ADD before trusting a run against it. A server that answers PING while
# the module failed to load would otherwise report every case as a
# divergence and look like a catastrophic incompatibility.
$CLI -p "$PORT" BF.ADD __probe x >/dev/null 2>&1 || {
  echo "FAIL: server is up but BF.ADD is unknown — module not loaded"; exit 1; }
$CLI -p "$PORT" FLUSHALL >/dev/null

OUT=$(./target/release/flint-conformance --target "127.0.0.1:$PORT" 2>&1)
echo "$OUT" | grep -E '^  bloom |^overall'

FAILS=$(echo "$OUT" | grep '^  \[bloom\]')
N=$(printf '%s' "$FAILS" | grep -c . )
echo "--- divergences from RedisBloom ($N) ---"
echo "$FAILS"

RC=0
[ "$N" = "${#EXPECTED_DIVERGENCES[@]}" ] || {
  echo "FAIL: expected ${#EXPECTED_DIVERGENCES[@]} divergences, saw $N"; RC=1; }
for want in "${EXPECTED_DIVERGENCES[@]}"; do
  echo "$FAILS" | grep -qF "$want" || {
    echo "FAIL: the documented divergence '$want' no longer appears"; RC=1; }
done

[ "$RC" = 0 ] && echo "PASS: RedisBloom agrees with Flint everywhere except the ${#EXPECTED_DIVERGENCES[@]} documented divergences"
exit $RC
