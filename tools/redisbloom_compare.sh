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
# Usage — EASIEST FIRST, because this is the one that has actually been run:
#
#   docker run -d --name flint-rebloom -p 6391:6379 redis/redis-stack-server
#   REBLOOM_ADDR=127.0.0.1:6391 tools/redisbloom_compare.sh
#   docker rm -f flint-rebloom
#
# `redis-server` on a Homebrew Mac is usually VALKEY (check `redis-server
# --version`), which will not load a RedisBloom .so — so the container is
# not merely convenient, it is the path that works on the machine this is
# normally typed on.
#
# Or, against a module you built and a server that can load it:
#   REBLOOM_MODULE=/path/to/redisbloom.so tools/redisbloom_compare.sh
#   # git clone --recursive https://github.com/RedisBloom/RedisBloom
#   # cd RedisBloom && make      -> bin/<platform>/redisbloom.so
set -u
cd "$(dirname "$0")/.."

PORT=${PORT:-6391}
SERVER=${REDIS_SERVER:-$(command -v redis-server || echo /opt/homebrew/opt/redis@8.2/bin/redis-server)}
MODULE=${REBLOOM_MODULE:-}
ADDR=${REBLOOM_ADDR:-}
CLI=${REDIS_CLI:-$(command -v valkey-cli || command -v redis-cli)}

if [ -n "$ADDR" ]; then
  # An oracle someone else is running (a container, a remote box). We do
  # not start or stop it; we only refuse to trust it before the capability
  # assert below.
  HOST=${ADDR%:*}; PORT=${ADDR##*:}
elif [ -n "$MODULE" ] && [ -f "$MODULE" ]; then
  HOST=127.0.0.1
  if [ ! -x "$SERVER" ]; then
    echo "SKIP: no module-capable redis-server (set REDIS_SERVER)"
    exit 0
  fi
else
  echo "SKIP: set REBLOOM_ADDR to a running RedisBloom, or REBLOOM_MODULE to"
  echo "      a built redisbloom.so (see the header for a docker one-liner)"
  exit 0
fi

# The cases we KNOWINGLY answer differently, each a decision in ADR-0016 D7
# and docs/command-support.md. The gate is "exactly these fail", in BOTH
# directions: a new failure is a regression, and a disappearing one means a
# divergence was quietly dropped — or, worse, that the corpus stopped
# exercising it.
#
# Matched on the CASE NAME, and each divergence has a case to itself in the
# corpus. That is not tidiness. `run_case` stops a case at its first failing
# step, so a divergence sitting in the middle of a long case silences every
# step after it: the first real run of this script had `TYPE` failing at
# step 6 of the lifecycle case, which meant `BF.SCANDUMP` — listed right
# here as a thing we were checking — had never been sent to RedisBloom at
# all. A gate that cannot reach half its own assertions is not a gate.
#
# NOTE the asymmetry in what a failure MEANS. On D7.1-D7.3 RedisBloom is
# "right" and we differ on purpose. On D7.4 the reverse: RedisBloom accepts
# garbage and we refuse it. Anything NOT listed here that fails is a place
# where a real client behaves differently against Flint than against
# RedisBloom, which is the whole thing this family exists to avoid.
EXPECTED_DIVERGENCES=(
  "DIVERGENCE D7.1"   # TYPE: we answer "bloom", RedisBloom "MBbloom--"
  "DIVERGENCE D7.2"   # BF.SCANDUMP refused: our block layout is not theirs
  "DIVERGENCE D7.3"   # BF.INFO SIZE counts materialised, not reserved, bytes
  "DIVERGENCE D7.4"   # we refuse unknown BF.RESERVE options; they ignore them
)

cleanup() { [ -n "${SRV_PID:-}" ] && kill "$SRV_PID" 2>/dev/null; }
trap cleanup EXIT

cargo build --release -p flint-conformance >/dev/null 2>&1 || {
  echo "FAIL: could not build flint-conformance"; exit 1; }

if [ -z "$ADDR" ]; then
  "$SERVER" --port "$PORT" --save '' --loadmodule "$MODULE" >/tmp/rebloom-oracle.log 2>&1 &
  SRV_PID=$!
fi
for _ in $(seq 1 40); do
  [ "$($CLI -h "$HOST" -p "$PORT" PING 2>/dev/null)" = "PONG" ] && break; sleep 0.25
done
if [ "$($CLI -h "$HOST" -p "$PORT" PING 2>/dev/null)" != "PONG" ]; then
  echo "FAIL: oracle at $HOST:$PORT did not answer PING"
  [ -z "$ADDR" ] && tail -5 /tmp/rebloom-oracle.log
  exit 1
fi
# Capability assert, not a version string: prove the module actually serves
# BF.ADD before trusting a run against it. A server that answers PING while
# the module failed to load would otherwise report every case as a
# divergence and look like a catastrophic incompatibility.
$CLI -h "$HOST" -p "$PORT" BF.ADD __probe x >/dev/null 2>&1 || {
  echo "FAIL: server is up but BF.ADD is unknown — module not loaded"; exit 1; }
# Name the oracle in the output. A run that silently compared against the
# wrong build is the failure mode this whole file is guarding against, and
# a version in the log is what makes an old PASS re-checkable later.
#
# MODULE LIST prints flat through a non-tty cli: name/<mod>/ver/<n>/path/...
# so the version is two lines past the module name, not one.
BF_VER=$($CLI -h "$HOST" -p "$PORT" MODULE LIST | tr -d '\r' | grep -A2 '^bf$' | tail -1)
# BF.ADD answering is NOT enough to prove this is RedisBloom — Flint serves
# BF.ADD too, so the assert above passes when the script is pointed at our
# own server by mistake, and the run then "compares Flint against Flint"
# and fails only on the divergence count. Demand the MODULE.
case "${BF_VER:-}" in
  ''|*[!0-9]*)
    echo "FAIL: no 'bf' module reported at $HOST:$PORT — this is not a"
    echo "      RedisBloom oracle. (Flint answers BF.ADD as well, so the"
    echo "      capability probe alone cannot tell the two apart.)"
    exit 1 ;;
esac
echo "oracle: RedisBloom (bf) v$BF_VER at $HOST:$PORT"
$CLI -h "$HOST" -p "$PORT" FLUSHALL >/dev/null

OUT=$(./target/release/flint-conformance --target "$HOST:$PORT" 2>&1)
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
