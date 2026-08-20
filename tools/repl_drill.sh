#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Replication drill: master + replica (rocks), mass-load, verify parity,
# verify -READONLY, kill the master, confirm the replica keeps serving.
# Requires a release build with --features rocks and valkey-cli.
set -euo pipefail
. "$(dirname "$0")/lib/fleet.sh"
# Declared so the SET of drills can be checked for port collisions —
# fleet_init only records the scope, it changes no behaviour here. A
# drill that declares nothing is invisible to assert_no_port_overlap,
# which is how failover and controller came to share 6440/6441 and
# reseed and lag_cap to share 6471/6472, unseen.
fleet_init $FLINT_DRILL_ROOT/flint-repl 6420 6421

KEYS="${1:-50000}"
MPORT="${2:-6420}"
RPORT="${3:-6421}"
MDIR="$(mktemp -d $FLINT_DRILL_ROOT/flint-repl-m.XXXXXX)"
RDIR="$(mktemp -d $FLINT_DRILL_ROOT/flint-repl-r.XXXXXX)"
BIN="$(dirname "$0")/../target/release/flint-server"

cleanup() {
  pkill -f "flint-server --port $MPORT" 2>/dev/null || true
  pkill -f "flint-server --port $RPORT" 2>/dev/null || true
  rm -rf "$MDIR" "$RDIR" "${RLOG:-}"
}
trap cleanup EXIT

echo "== master :$MPORT, replica :$RPORT, $KEYS keys"
"$BIN" --port "$MPORT" --engine rocks --data-dir "$MDIR" &
fleet_wait_listen "$MPORT"
sleep 0.4
RLOG="$(mktemp $FLINT_DRILL_ROOT/flint-replica-log.XXXXXX)"
"$BIN" --port "$RPORT" --engine rocks --data-dir "$RDIR" --replica-of "127.0.0.1:$MPORT" 2> "$RLOG" &
fleet_wait_listen "$RPORT"
sleep 0.6

echo "== loading $KEYS strings + 500 hashes into the master"
# Loaded through fleet_load_resp, which replays whatever the master sheds.
# The previous form piped once under `set -euo pipefail`; valkey-cli --pipe
# exits non-zero when it counts errors, so a single -THROTTLED aborted the
# run HERE and the EXIT trap killed both seats. The drill then reported
# "FAIL repl" for parity, full sync, roles, idle liveness, READONLY, live
# tail and survivor reads — none of which it had reached (BUG-0035).
_repl_load_gen() {
  awk -v n="$KEYS" 'BEGIN {
    for (i = 0; i < n; i++) {
      k = sprintf("key:%07d", i); v = sprintf("value-%07d", i)
      printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, length(v), v
    }
  }'
  awk 'BEGIN {
    for (i = 0; i < 500; i++) {
      k = sprintf("hash:%04d", i); v = sprintf("v%d", i)
      printf "*4\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n$2\r\nf1\r\n$%d\r\n%s\r\n", length(k), k, length(v), v
    }
  }'
}
fleet_load_resp "$MPORT" _repl_load_gen || exit 1
# Repair exactly what the parity checks below sample. Anything else the load
# shed stays absent on purpose: it was never acked, and the drill asserts
# nothing about it.
_P0="key:$(printf '%07d' 0)"
_PM="key:$(printf '%07d' $((KEYS / 2)))"
_PL="key:$(printf '%07d' $((KEYS - 1)))"
fleet_ensure_keys "$MPORT" \
  "$_P0=value-$(printf '%07d' 0)" \
  "$_PM=value-$(printf '%07d' $((KEYS / 2)))" \
  "$_PL=value-$(printf '%07d' $((KEYS - 1)))" || exit 1
fleet_retry_write "$MPORT" HSET hash:0250 f1 v250 || exit 1

echo "== waiting for replica catch-up"
# WAIT ON A SENTINEL WRITTEN LAST, not on the last STRING key.
#
# The probe used to be key:<KEYS-1>, which is written before the 500 hashes
# and before the repairs above. Replication is ordered, so seeing that key
# proves only that the stream reached THAT point -- the hashes were still in
# flight, and the parity check below reads one. On an idle box they landed
# inside the sampling interval and it passed for months; under a loaded box it
# fails as "hash mismatch ('v250' vs '')", which reads as replication losing
# a write rather than as the drill asking the wrong question.
#
# A sentinel written after everything else makes the readiness check cover
# exactly what the assertions read: ordered delivery means its arrival implies
# all of it.
fleet_retry_write "$MPORT" SET repl-drill-sentinel ready || exit 1
CAUGHT_UP=0
for i in $(seq 1 200); do
  SAMPLE=$(valkey-cli -p "$RPORT" GET repl-drill-sentinel 2>/dev/null || true)
  [ "$SAMPLE" = "ready" ] && { CAUGHT_UP=1; break; }
  sleep 0.1
done
[ "$CAUGHT_UP" = "1" ] || {
  echo "FAIL: replica never reached the sentinel in 20s — it is not caught up,"
  echo "      so every parity result below would be about timing, not parity."
  exit 1
}

echo "== parity samples"
for probe in 0 $((KEYS / 2)) $((KEYS - 1)); do
  K="key:$(printf '%07d' "$probe")"
  M=$(valkey-cli -p "$MPORT" GET "$K")
  R=$(valkey-cli -p "$RPORT" GET "$K")
  [ "$M" = "$R" ] || { echo "FAIL: mismatch on $K ('$M' vs '$R')"; exit 1; }
done
HM=$(valkey-cli -p "$MPORT" HGET hash:0250 f1)
HR=$(valkey-cli -p "$RPORT" HGET hash:0250 f1)
[ "$HM" = "$HR" ] || { echo "FAIL: hash mismatch ('$HM' vs '$HR')"; exit 1; }
echo "parity OK (strings + hashes)"

echo "== full sync path used by the fresh replica"
grep -q "full sync: received" "$RLOG" || { echo "FAIL: replica did not full-sync"; cat "$RLOG"; exit 1; }
grep "full sync" "$RLOG" | head -2

echo "== FLINTINFO"
MINFO=$(valkey-cli -p "$MPORT" FLINTINFO)
RINFO=$(valkey-cli -p "$RPORT" FLINTINFO)
echo "$MINFO" | grep -q "role:master" || { echo "FAIL: master role"; exit 1; }
echo "$MINFO" | grep -q "live_replicas:1" || { echo "FAIL: master does not see live replica"; echo "$MINFO"; exit 1; }
echo "$RINFO" | grep -q "role:replica" || { echo "FAIL: replica role"; exit 1; }
echo "$MINFO" | tr '\r' ' '

echo "== idle liveness: healthy-but-idle replica must stay live"
sleep 3
IDLE=$(valkey-cli -p "$MPORT" FLINTINFO | tr '\r' ' ')
echo "$IDLE" | grep -q "live_replicas:1" || { echo "FAIL: idle replica declared dead"; echo "$IDLE"; exit 1; }
echo "idle liveness OK"

echo "== replica rejects writes"
RO=$(valkey-cli -p "$RPORT" SET should-fail x 2>&1 || true)
echo "$RO" | grep -q "READONLY" || { echo "FAIL: expected READONLY, got '$RO'"; exit 1; }
echo "READONLY OK"

echo "== live tail: write to master, read from replica"
valkey-cli -p "$MPORT" SET live-tail-probe hello > /dev/null
for i in $(seq 1 50); do
  V=$(valkey-cli -p "$RPORT" GET live-tail-probe 2>/dev/null || true)
  [ "$V" = "hello" ] && break
  sleep 0.05
done
[ "$V" = "hello" ] || { echo "FAIL: live tail did not propagate"; exit 1; }
echo "live tail OK"

echo "== kill master; replica must keep serving reads"
pkill -9 -f "flint-server --port $MPORT"
sleep 0.5
SURVIVOR=$(valkey-cli -p "$RPORT" GET "key:0000000")
[ "$SURVIVOR" = "value-0000000" ] || { echo "FAIL: replica lost reads after master death"; exit 1; }
echo "PASS: replica serves reads with master dead"
