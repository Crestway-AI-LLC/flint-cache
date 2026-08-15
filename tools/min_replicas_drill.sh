#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# min-replicas-to-write drill: a master with --min-replicas-to-write 1 sheds
# writes (-THROTTLED) the moment its last replica dies, and resumes as soon
# as a replacement starts acking. This closes the widowed-master hole: the
# lag cap alone cannot bound loss when there is no replica to measure lag
# against (e.g. a partition strands the master with a lease-renewing
# controller while the other side promotes). Reads stay available throughout.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-minr-m 6480 6481 6482
fleet_guard
fleet_kill server; sleep 0.4
MDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-minr-m.XXXXXX); R1DIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-minr-r1.XXXXXX)
R2DIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-minr-r2.XXXXXX)
B=./target/release/flint-server
MPORT=6480; R1PORT=6481; R2PORT=6482
cleanup() {
  pkill -9 -f "flint-server --port 648" 2>/dev/null
  rm -rf "$MDIR" "$R1DIR" "$R2DIR"
}
trap cleanup EXIT

echo "== master with min-replicas-to-write=1, plus one replica"
$B --port $MPORT --engine rocks --data-dir "$MDIR" --min-replicas-to-write 1 2>/dev/null &
fleet_wait_listen $MPORT
sleep 0.5

echo "== gate engaged BEFORE any replica exists (no unbounded window at birth)"
OUT=$(valkey-cli -p $MPORT SET early bad 2>&1 || true)
echo "$OUT" | grep -q "THROTTLED" || { echo "FAIL: writes allowed with no replica ever: $OUT"; exit 1; }
echo "throttled as expected"

$B --port $R1PORT --engine rocks --data-dir "$R1DIR" --replica-of 127.0.0.1:$MPORT 2>/dev/null &
OPEN=0
for i in $(seq 1 40); do
  [ "$(valkey-cli -p $MPORT SET k1 v1 2>&1)" = "OK" ] && { OPEN=1; break; }
  sleep 0.2
done
[ "$OPEN" = "1" ] || { echo "FAIL: gate never lifted after replica attached"; exit 1; }
echo "== gate lifted once the replica acks; writes flow"

echo "== KILL the only replica: master is widowed — writes must shed fast"
pkill -9 -f "flint-server --port $R1PORT"
SHED=0
for i in $(seq 1 40); do   # unregister on teardown should make this near-instant
  OUT=$(valkey-cli -p $MPORT SET widow bad 2>&1 || true)
  if echo "$OUT" | grep -q "THROTTLED"; then SHED=1; break; fi
  sleep 0.1
done
[ "$SHED" = "1" ] || { echo "FAIL: widowed master kept accepting writes"; exit 1; }
echo "shed after ~$(( i * 100 ))ms"

echo "== reads stay available while writes are shed (degraded, not down)"
[ "$(valkey-cli -p $MPORT GET k1)" = "v1" ] || { echo "FAIL: reads broken while gated"; exit 1; }

echo "== attach a replacement replica: writes resume on first ack (no full-resync wait)"
$B --port $R2PORT --engine rocks --data-dir "$R2DIR" --replica-of 127.0.0.1:$MPORT 2>/dev/null &
RESUMED=0
for i in $(seq 1 60); do
  [ "$(valkey-cli -p $MPORT SET recovered ok 2>&1)" = "OK" ] && { RESUMED=1; break; }
  sleep 0.2
done
[ "$RESUMED" = "1" ] || { echo "FAIL: writes did not resume after replacement attached"; exit 1; }
echo "resumed after ~$(( i * 200 ))ms"
valkey-cli -p $MPORT FLINTINFO | tr '\r' '\n' | grep -E "min_replicas_to_write|live_replicas"

echo "PASS: min-replicas-to-write bounds the widowed-master window (shed fast, resume on first ack, reads unaffected)"
