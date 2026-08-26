#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0005 D7 drill: read scaling via per-shard replica count (two knobs).
#
# Replicas mirror the master, so a correctly-served replica read returns the
# SAME value as the master — invisible by value in steady state. The clean
# observable is master-down: a read routed to the (dead) master FAILS, a read
# served by a replica SUCCEEDS. So the same dead-master cluster, toggling only
# the tenant flag, flips read success — that is the whole D7 routing contract.
#   - dead-replica fallback (master alive): reads fall back to the master
#   - flag OFF, master dead: reads FAIL (reads are master-only — untouched)
#   - flag ON,  master dead: reads SUCCEED (a replica serves them)
#   - writes always FAIL with no master (writes never touch a replica)
# No controller runs, so a killed master is NOT promoted — the master-down
# state is stable for the toggle.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-d7 6311 6312 6972 7640 6313
fleet_guard
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
D=$FLINT_DRILL_ROOT/flint-d7; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; sleep 0.4
cleanup() {
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; rm -rf "$D"
}
trap cleanup EXIT

$CP --port 7640 --state "$D/cp" 2>/dev/null &
fleet_wait_ping 7640
fleet_cp 7640 CPADDPROXY 127.0.0.1:6313
fleet_cp 7640 CPADDPAIR 127.0.0.1:6311,127.0.0.1:6312,127.0.0.1:6972
fleet_cp 7640 CPADDTENANT acme tok-acme acme 1
start_replica() { $B --port "$1" --engine rocks --data-dir "$D/$2" --replica-of 127.0.0.1:6311 2>/dev/null & }
$B --port 6311 --engine rocks --data-dir "$D/m" 2>/dev/null &
fleet_wait_listen 6311
sleep 0.7
start_replica 6312 r1
start_replica 6972 r2
$PX --port 6313 --control-plane 127.0.0.1:7640 --advertise 127.0.0.1:6313 2>/dev/null &
# Gate on BOTH replicas being live before any test relies on them.
for i in $(seq 1 50); do
  fleet_ready 6312 && fleet_ready 6972 && break
  sleep 0.2
done
fleet_ready 6312 || { echo "FAIL: replica 6312 never became READY"; exit 1; }
fleet_ready 6972 || { echo "FAIL: replica 6972 never became READY"; exit 1; }
sleep 1
a() { valkey-cli -p 6313 -a tok-acme --no-auth-warning "$@"; }
[ "$(a SET rk masterval)" = "OK" ] || { echo "FAIL: seed"; exit 1; }
for i in $(seq 1 40); do
  [ "$(valkey-cli -p 6311 FLINTINFO | tr '\r' '\n' | grep '^seq_lag:' | cut -d: -f2)" = "0" ] && break
  sleep 0.3
done

echo "== opt in (CPTENANTREADS acme on) — the routing knob"
valkey-cli -p 7640 CPTENANTREADS acme on >/dev/null
sleep 1.5   # snapshot push carries the flag

echo "== dead-replica fallback (master alive): kill BOTH replicas, reads survive"
pkill -9 -f "flint-server --port 6312"; pkill -9 -f "flint-server --port 6972"; sleep 0.5
OK=1
for i in $(seq 1 12); do [ "$(a GET rk)" = "masterval" ] || OK=0; done
[ "$OK" = "1" ] || { echo "FAIL: reads did not fall back to master when replicas died"; exit 1; }
echo "  replicas dead -> reads fell back to the master, none failed"
# Bring the replicas back and reconverge for the master-down phase.
start_replica 6312 r1b; start_replica 6972 r2b
for i in $(seq 1 50); do
  fleet_ready 6312 && fleet_ready 6972 \
    && { L=$(valkey-cli -p 6311 FLINTINFO | tr '\r' '\n' | grep '^live_replicas:' | cut -d: -f2); [ "$L" -ge 2 ] 2>/dev/null && break; }
  sleep 0.2
done

echo "== kill the MASTER (no controller -> stays down): the flag flips read success"
pkill -9 -f "flint-server --port 6311"; sleep 0.6
# Flag ON: reads must SUCCEED (a replica serves them).
ON=$(a GET rk)
[ "$ON" = "masterval" ] || { echo "FAIL: flag-on read did not survive master death (got: $ON)"; exit 1; }
echo "  flag ON  + master dead -> GET rk = masterval (a replica served it)"
# Writes must FAIL — writes never touch a replica.
W=$(a SET wk v 2>&1)
echo "$W" | grep -qi "master" && echo "  writes still require the master (SET -> ${W:0:40})" || { echo "FAIL: write unexpectedly succeeded with no master: $W"; exit 1; }

echo "== flag OFF, same dead-master cluster: reads now FAIL (master-only)"
valkey-cli -p 7640 CPTENANTREADS acme off >/dev/null
sleep 1.5
OFF=$(a GET rk 2>&1)
echo "$OFF" | grep -qi "master" || { echo "FAIL: flag-off read did not fail on the dead master (got: $OFF)"; exit 1; }
echo "  flag OFF + master dead -> GET rk = error (reads are master-only): ${OFF:0:40}"

echo "PASS: D7 replica reads — opt-in flag flips read routing (replica vs master), writes master-only, dead-replica fallback; flag-off path untouched"
