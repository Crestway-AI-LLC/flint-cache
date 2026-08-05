#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# `backup-to` in the inventory IS the backup seat (ADR-0011 D8's last
# wire): flintctl bootstrap starts flint-backup in schedule mode with
# flags derived from the inventory, supervises it like the controller, and
# stop/start manage it by pidfile like every other seat.
#
#   0. CAPABILITY — bootstrap with backup keys brings the seat up; sets
#      appear in the destination on cadence and the status file reports a
#      healthy backup job. On a TLS fleet — the seat must dial the mesh
#      with the fleet's certs, or the master query fails before the first
#      checkpoint.
#   1. LIFECYCLE — `flintctl stop` takes the seat down with the fleet (a
#      backup seat that outlives its fleet backs up nothing and holds the
#      cp-state path open); `start` brings it back; a second `start` does
#      NOT double it (the seat_alive gate).
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-bkseat 7121 7122 7123 7124
fleet_guard
STATE=/tmp/flint-bkseat
INV=/tmp/flint-bkseat.flint
CTL=./target/release/flintctl
fleet_kill server; fleet_kill proxy; fleet_kill controlplane
fleet_kill controller; fleet_kill backup
sleep 0.4
cleanup() {
  $CTL -f "$INV" stop 2>/dev/null
  fleet_kill server; fleet_kill proxy; fleet_kill controlplane
  fleet_kill controller; fleet_kill backup
  rm -rf "$STATE" "$INV" /tmp/flint-bkseat-sets
}
trap cleanup EXIT
rm -rf "$STATE" "$INV" /tmp/flint-bkseat-sets

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-ctl --features flint-server/rocks,flint-backup/rocks -p flint-backup

cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
tls on
cp 127.0.0.1:7124
pair 127.0.0.1:7121,127.0.0.1:7122
proxy 127.0.0.1:7123
backup-to /tmp/flint-bkseat-sets
backup-every 3s
backup-keep 2
EOF

echo "== bootstrap: the inventory's backup keys must bring the seat up"
$CTL -f "$INV" bootstrap >"$STATE-boot.log" 2>&1 || {
  echo "FAIL: bootstrap"; tail -20 "$STATE-boot.log"; exit 1; }
[ -f "$STATE/pids/backup.pid" ] || { echo "FAIL: no backup pidfile after bootstrap"; exit 1; }
kill -0 "$(cat "$STATE/pids/backup.pid")" 2>/dev/null || {
  echo "FAIL: backup pidfile is stale"; exit 1; }

echo "== sets on cadence with a healthy status"
# No corpus: the fleet is mTLS, plaintext valkey-cli cannot dial a node
# port, and the seat's behavior is what is under test — an empty keyspace
# checkpoints exactly like a full one.
SETS=0
for _ in $(seq 1 90); do
  SETS=$(ls /tmp/flint-bkseat-sets 2>/dev/null | grep -c '^backup-')
  RUNS=$(grep '^job backup' "$STATE/logs/backup-status" 2>/dev/null | awk '{print $4}')
  [ "${SETS:-0}" -ge 2 ] && [ "${RUNS:-0}" -ge 2 ] && break
  sleep 0.5
done
[ "${SETS:-0}" -ge 2 ] || { echo "FAIL: seat produced $SETS set(s)"; cat "$STATE/logs/backup-status" 2>/dev/null; tail -5 "$STATE/logs/backup.log" 2>/dev/null; exit 1; }
FAILS=$(grep '^job backup' "$STATE/logs/backup-status" | awk '{print $6}')
[ "${FAILS:-1}" = "0" ] || { echo "FAIL: backup job reports $FAILS failure(s) on a TLS fleet"; cat "$STATE/logs/backup-status"; exit 1; }
echo "  $SETS set(s), backup job healthy ($RUNS runs, 0 failures), through the mesh certs"

echo "== stop takes the seat down with the fleet"
$CTL -f "$INV" stop >/dev/null 2>&1
sleep 0.5
if pgrep -f "flint-backup schedule.*$STATE" >/dev/null; then
  echo "FAIL: the backup seat survived flintctl stop"; exit 1
fi
echo "  seat stopped by pidfile"

echo "== start brings it back; a second start does not double it"
$CTL -f "$INV" start >"$STATE-start.log" 2>&1 || {
  echo "FAIL: start"; tail -20 "$STATE-start.log"; exit 1; }
sleep 0.5
N1=$(pgrep -f "flint-backup schedule.*$STATE" | wc -l | tr -d ' ')
[ "$N1" = "1" ] || { echo "FAIL: $N1 backup seat(s) after start"; exit 1; }
$CTL -f "$INV" start >/dev/null 2>&1
sleep 0.5
N2=$(pgrep -f "flint-backup schedule.*$STATE" | wc -l | tr -d ' ')
[ "$N2" = "1" ] || {
  echo "FAIL: $N2 backup seat(s) after a second start — two schedules cut two checkpoints"; exit 1; }
echo "  one seat after start, still one after a second start"

echo
echo "PASS: backup-to in the inventory is the seat — bootstrapped with the fleet's certs, producing sets on cadence, stopped and started by pidfile, and never doubled"
