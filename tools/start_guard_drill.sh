#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# `flintctl start` must never replace a seat that already has a process.
#
# SERVING is proved by dialling a seat. NOT serving is not proved by a failed
# dial: a node doing wipe + full sync holds a live process and an unbound
# port for as long as the sync takes, and looks exactly like a dead one. The
# node branch of `start` WIPES the data dir before respawning, so a start
# issued during that window deletes the sync in progress — and a start issued
# once a minute never converges.
#
# That is not hypothetical. A supervise timer running `flintctl start` on the
# playground restarted a killed replica four times in four minutes; the node
# never reached serving and recovered on the first attempt once the timer was
# stopped and one start ran alone (#139). Supervision cannot be switched on
# until this holds.
#
# The window is made deterministic with SIGSTOP rather than raced: a stopped
# process is alive, owns its port, and answers nothing — the same shape as a
# seat mid-sync, and the same shape `start` used to misread.
#
# Requires a release build with --features rocks.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-startguard 6963 6964 7655 7895
fleet_guard
STATE=/tmp/flint-startguard
INV=/tmp/flint-startguard.flint
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; sleep 0.3
cleanup() {
  ./target/release/flintctl -f "$INV" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill proxy; fleet_kill controlplane
  rm -rf "$STATE" "$INV"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-ctl --features flint-server/rocks

cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
tls on
cp 127.0.0.1:7655
pair 127.0.0.1:6963,127.0.0.1:6964
proxy 127.0.0.1:7895
EOF

CTL=./target/release/flintctl
echo "== bootstrap a pair behind a proxy"
$CTL -f "$INV" bootstrap >/dev/null 2>&1 || { echo "FAIL: bootstrap"; exit 1; }
REPLICA=$($CTL -f "$INV" status 2>/dev/null | awk '/replica/{print $3; exit}')
[ -n "$REPLICA" ] || { echo "FAIL: no replica after bootstrap"; exit 1; }
RPORT=${REPLICA##*:}
echo "  replica is $REPLICA"

# A sentinel INSIDE the data dir: if `start` takes the wipe branch, the whole
# directory goes and the file with it. Cheaper and more direct than reading
# the log for what the wipe printed.
SENTINEL="$STATE/node-$RPORT/.start-guard-sentinel"
touch "$SENTINEL"
BEFORE_PID=$(pgrep -f "flint-server --port $RPORT" | head -1)
[ -n "$BEFORE_PID" ] || { echo "FAIL: no replica process to stop"; exit 1; }

echo "== freeze the replica: alive, owns its port, answers nothing"
fleet_signal_port "$RPORT" -STOP || { echo "FAIL: could not SIGSTOP $REPLICA"; exit 1; }
sleep 1
# Positive control on the SETUP: if it still answers, the rest proves nothing.
$CTL -f "$INV" status 2>/dev/null | grep -q "$REPLICA  *DOWN" \
  || { echo "FAIL: frozen replica still reads as up — the window was not created"; \
       $CTL -f "$INV" status; fleet_signal_port "$RPORT" -CONT; exit 1; }
echo "  status reports it DOWN while its process is alive — the misreadable state"

echo "== start must leave it alone"
OUT=$($CTL -f "$INV" start 2>&1)
fleet_signal_port "$RPORT" -CONT   # unfreeze before any assertion can exit
echo "$OUT" | grep -q "node-$RPORT STARTING" \
  || { echo "FAIL: start did not recognise the seat as starting:"; echo "$OUT" | sed 's/^/    /'; exit 1; }
echo "$OUT" | grep -q "started node-$RPORT" \
  && { echo "FAIL: start respawned a seat whose process was alive"; echo "$OUT" | sed 's/^/    /'; exit 1; }
[ -f "$SENTINEL" ] \
  || { echo "FAIL: start WIPED the data dir of a live seat (sentinel gone)"; exit 1; }
AFTER_PID=$(pgrep -f "flint-server --port $RPORT" | head -1)
[ "$AFTER_PID" = "$BEFORE_PID" ] \
  || { echo "FAIL: replica process changed ($BEFORE_PID -> $AFTER_PID)"; exit 1; }
# `pgrep -c` is Linux-only — BSD pgrep prints a usage message and no count,
# so the drill read an empty string as zero and failed a passing product.
# Count the lines instead; it means the same thing on both.
N=$(pgrep -f "flint-server --port $RPORT" | wc -l | tr -d ' ')
[ "${N:-0}" -eq 1 ] || { echo "FAIL: $N processes on port $RPORT — start spawned a duplicate"; exit 1; }
echo "  left alone: same pid, one process, data dir intact"

echo "== and it recovers on its own once unfrozen"
for _ in $(seq 1 40); do
  $CTL -f "$INV" status 2>/dev/null | grep -q "$REPLICA.*replica" && break
  sleep 0.5
done
$CTL -f "$INV" status 2>/dev/null | grep -q "$REPLICA.*replica" \
  || { echo "FAIL: replica did not return to service after SIGCONT"; $CTL -f "$INV" status; exit 1; }
echo "  serving again"

echo "PASS: start never replaces a seat that already has a process (#139)"
