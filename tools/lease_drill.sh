#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Lease drill: a master lease-managed by a controller self-fences when
# renewals stop (the controller can no longer reach it). Proves the master
# stops accepting writes on TTL expiry WITHOUT anyone sending FLINTDEMOTE —
# the partition-split-brain guard.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-lease-m 6308 6309
fleet_guard
fleet_kill server; fleet_kill controller; sleep 0.4
MDIR=$(mktemp -d /tmp/flint-lease-m.XXXXXX); RDIR=$(mktemp -d /tmp/flint-lease-r.XXXXXX)
B=./target/release/flint-server
MPORT=6308; RPORT=6309
cleanup() {
  pkill -9 -f "flint-server --port 644" 2>/dev/null
  fleet_kill controller
  rm -rf "$MDIR" "$RDIR"
}
trap cleanup EXIT

$B --port $MPORT --engine rocks --data-dir "$MDIR" 2>/dev/null &
fleet_wait_listen $MPORT
sleep 0.5
$B --port $RPORT --engine rocks --data-dir "$RDIR" --replica-of 127.0.0.1:$MPORT 2>/dev/null &
fleet_wait_listen $RPORT
sleep 0.9
valkey-cli -p $MPORT SET k v >/dev/null

echo "== controller manages the master with a 1.5s lease TTL"
./target/release/flint-controller --nodes 127.0.0.1:$MPORT,127.0.0.1:$RPORT --id L \
  --poll-ms 150 --lease-ttl-ms 1500 2>/tmp/flint-lease.log &
sleep 1.0

echo "== master is writable while the lease is renewed"
[ "$(valkey-cli -p $MPORT SET before ok 2>&1)" = "OK" ] || { echo "FAIL: master not writable under lease"; exit 1; }
echo "writable OK"

echo "== stop renewals (kill the controller — simulates the master being"
echo "   partitioned from every controller); master must self-fence on TTL"
fleet_kill controller
FENCED=0
for i in $(seq 1 40); do   # up to 8s; TTL is 1.5s
  RO=$(valkey-cli -p $MPORT SET after bad 2>&1 || true)
  if echo "$RO" | grep -q "READONLY"; then FENCED=1; break; fi
  sleep 0.2
done
[ "$FENCED" = "1" ] || { echo "FAIL: master did not self-fence after lease expiry"; exit 1; }
echo "self-fenced after ~$(( i * 200 ))ms of no renewal (TTL 1500ms)"
grep -q "self-fenced" "$MDIR.log" 2>/dev/null && echo "  (server logged the self-fence)"

echo "== data still readable on the self-fenced master (read-only, not down)"
[ "$(valkey-cli -p $MPORT GET k)" = "v" ] || { echo "FAIL: reads broken after self-fence"; exit 1; }

echo "== self-fence is NOT auto-undone by a later renewal (no resurrection)"
valkey-cli -p $MPORT FLINTLEASE 5000 >/dev/null
sleep 0.3
RO2=$(valkey-cli -p $MPORT SET after2 bad 2>&1 || true)
echo "$RO2" | grep -q "READONLY" || { echo "FAIL: renewal resurrected a self-fenced master: $RO2"; exit 1; }
echo "stays read-only despite renewal (recovery requires FLINTDEMOTE + resync)"

echo "PASS: lease self-fencing closes the partition window without reaching the node"
