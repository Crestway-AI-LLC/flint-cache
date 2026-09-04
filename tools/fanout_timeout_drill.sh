#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The O(keys) admin class gets its OWN clock, and keyed traffic keeps its.
#
# WHY THIS DRILL EXISTS. Scale run 19 (2026-08-14) loaded 100 GB across four
# pairs, killed nothing, and still reported FAILED: every structural check
# passed — masters elected, replicas streaming, write/read round trip on all
# four pairs — and the single failure was
#
#   DBSIZE fan-out: ERR fan-out to 172.31.79.45:7001: Resource temporarily
#   unavailable (os error 11)
#
# DBSIZE walks every metadata row on the node it asks, so its honest cost
# grows with the keyspace; the proxy fanned it out on a socket carrying
# KEYED traffic's 5 s timeout. ~1.6M keys per pair exceeded it, so
# `flintctl verify --probe` — the command that answers "is my fleet whole?"
# — cried wolf on a healthy fleet. Above a few hundred GB that trains an
# operator to ignore their own verify, which is worse than having none.
#
# The fix is a separate budget for that class (`--fanout-timeout-ms`), NOT a
# blanket raise: a client waiting on GET still wants to fail over fast. So
# this drill asserts BOTH halves, because a fix that slowed keyed traffic
# down to make DBSIZE survive would pass a one-sided test.
#
# It is a POSITIVE CONTROL, not a smoke test: the corpus is large enough
# that the scan takes real milliseconds, and the SAME command is shown to
# fail under a 1 ms budget and succeed under a generous one. If DBSIZE were
# secretly O(1) here, step 2 would pass when it must fail, and the drill
# fails loudly rather than reporting a green it did not earn.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-fanout 7181 7182 7183 7184
fleet_guard
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
D=$FLINT_DRILL_ROOT/flint-fanout; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; sleep 0.4
cleanup() {
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; rm -rf "$D"
}
trap cleanup EXIT

echo "== cluster: CP + master + proxy (tenant acme)"
$CP --port 7181 --state "$D/cp" 2>"${FLEET_SCOPE}cp.log" &
fleet_wait_ping 7181
fleet_cp 7181 CPADDPROXY 127.0.0.1:7183
fleet_cp 7181 CPADDPAIR 127.0.0.1:7182
fleet_cp 7181 CPADDTENANT acme tok-acme acme 1
$B --port 7182 --engine rocks --data-dir "$D/m" 2>"${FLEET_SCOPE}server.log" &
fleet_wait_listen 7182
sleep 0.7

# The corpus is the instrument. DBSIZE decodes one metadata header per key,
# so this is what makes the scan take long enough to be governed by a
# millisecond-scale budget at all — with a handful of keys both budgets
# would pass and the drill would prove nothing.
KEYS=${FLINT_FANOUT_DRILL_KEYS:-300000}

start_proxy() { # $1 = --fanout-timeout-ms value ("" = compiled default)
  local extra=""
  [ -n "$1" ] && extra="--fanout-timeout-ms $1"
  # shellcheck disable=SC2086
  $PX --port 7183 --control-plane 127.0.0.1:7181 --advertise 127.0.0.1:7183 \
      $extra 2>>"$D/px.log" &
  fleet_wait_listen 7183
  sleep 1.2
}
A="valkey-cli -p 7183 -a tok-acme --no-auth-warning"

start_proxy ""
echo "== seed $KEYS keys through the proxy (the scan needs something to scan)"
python3 - "$KEYS" <<'PY' | valkey-cli -p 7183 -a tok-acme --no-auth-warning --pipe >/dev/null
import sys
n = int(sys.argv[1])
out = sys.stdout.buffer
buf = bytearray()
for i in range(n):
    k = ("fk:%09d" % i).encode()
    buf += b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$2\r\nvv\r\n" % (len(k), k)
    if len(buf) >= 1 << 20:
        out.write(buf); del buf[:]
out.write(buf); out.flush()
PY

echo "== baseline: DBSIZE succeeds on the default budget, and is MEASURABLY slow"
T0=$(date +%s%N)
N=$($A DBSIZE | tr -d '\r')
T1=$(date +%s%N)
MS=$(( (T1 - T0) / 1000000 ))
[ "$N" = "$KEYS" ] || { echo "FAIL: DBSIZE returned '$N', expected $KEYS"; exit 1; }
echo "  DBSIZE = $N in ${MS}ms"
# The instrument check. If the scan is faster than the small budget below,
# step 2 cannot distinguish a working knob from a broken one — say so
# instead of reporting a green that measured nothing.
[ "$MS" -ge 5 ] || {
  echo "FAIL: DBSIZE took only ${MS}ms — corpus too small to test a 1ms budget"
  echo "      on this machine; raise FLINT_FANOUT_DRILL_KEYS above $KEYS."
  exit 1
}

echo "== the knob BINDS: same command, 1ms fan-out budget, must fail"
fleet_kill proxy; sleep 0.5
start_proxy 1
OUT=$($A DBSIZE 2>&1 | tr -d '\r')
case "$OUT" in
  *fan-out*|*ERR*|*error*) echo "  DBSIZE refused as designed: $OUT" ;;
  "$KEYS") echo "FAIL: DBSIZE returned $KEYS under a 1ms budget — the knob is not wired"; exit 1 ;;
  *) echo "FAIL: unexpected DBSIZE reply under a 1ms budget: '$OUT'"; exit 1 ;;
esac

echo "== and it is SCOPED: keyed traffic on the same proxy is untouched"
# The whole point of a separate budget. A GET must still work — and still
# work through a connection the failed fan-out just dropped.
$A SET keyed hello >/dev/null || { echo "FAIL: SET refused under a 1ms fan-out budget"; exit 1; }
[ "$($A GET keyed | tr -d '\r')" = "hello" ] || {
  echo "FAIL: keyed GET broke under a 1ms fan-out budget — the budget leaked"
  exit 1
}
echo "  SET + GET still served (the 1ms budget never touched the keyed path)"

echo "== restoring a sane budget makes the SAME command work again"
fleet_kill proxy; sleep 0.5
start_proxy 60000
N2=$($A DBSIZE | tr -d '\r')
[ "$N2" = "$((KEYS + 1))" ] || { echo "FAIL: DBSIZE returned '$N2', expected $((KEYS + 1))"; exit 1; }
echo "  DBSIZE = $N2 (the extra key is the keyed SET above)"

echo "PASS: the O(keys) admin class has its own budget — 1ms refuses DBSIZE while keyed GET keeps serving, 60s serves both, and the scan is real (${MS}ms for $KEYS keys)"
