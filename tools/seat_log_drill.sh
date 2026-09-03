#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A seat's log must survive the seat being restarted.
#
# `spawn` opened the log with File::create, which truncates. Every respawn
# therefore erased the previous run's output — so a seat that starts, runs
# briefly and dies destroys its own evidence on the way back up, and the
# more it crash-loops the less there is to read.
#
# That is not a hypothetical cost. docs/bugs/0005 is a replica exiting silently ~90
# seconds after a clean start, and every attempt to diagnose it read a log
# holding only the newest attempt, which showed a healthy boot every time.
# The crash output existed and was overwritten before anyone looked.
#
# Two properties, both asserted here:
#   - a marker written by run N is still present after run N+1 starts
#   - each run is separated by a banner, so the file can be read as a
#     sequence rather than a smear
#
# Also covers stdout, which used to go to /dev/null: a seat that reported
# something there was silenced entirely, and half the evidence never existed.
#
# Requires a release build with --features rocks.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-seatlog 6966 6967 7656 7896
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-seatlog
INV=$FLINT_DRILL_ROOT/flint-seatlog.flint
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; sleep 0.3
cleanup() {
  ./target/release/flintctl -f "$INV" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill proxy; fleet_kill controlplane
  rm -rf "$STATE" "$INV"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-ctl --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
tls on
cp 127.0.0.1:7656
pair 127.0.0.1:6966,127.0.0.1:6967
proxy 127.0.0.1:7896
EOF

CTL=./target/release/flintctl
echo "== bootstrap"
$CTL -f "$INV" bootstrap >"$STATE-boot.log" 2>&1 || {
  # The reason bootstrap failed is in ITS OWN output, and this line
  # used to send that to /dev/null and then report a bare failure --
  # so the largest cluster of gate reds ("FAIL: bootstrap") could not
  # be diagnosed from the artifact at all. Two drills that captured it
  # showed the actual cause immediately: a replica still `loading`
  # when verify ran (BUG-0064).
  echo "FAIL: bootstrap"; tail -25 "$STATE-boot.log"; exit 1; }

REPLICA=$($CTL -f "$INV" status 2>/dev/null | awk '$4=="replica"{print $3; exit}')
[ -n "$REPLICA" ] || { echo "FAIL: no replica after bootstrap"; exit 1; }
RPORT=${REPLICA##*:}
LOG="$STATE/logs/node-$RPORT.log"
[ -f "$LOG" ] || { echo "FAIL: no log at $LOG"; exit 1; }

# A marker that could only have come from the FIRST run. Appended to the
# seat's own log, which is exactly what a truncating respawn destroys.
MARK="drill-marker-$$-$(date +%s)"
echo "$MARK" >> "$LOG"
BANNERS_BEFORE=$(grep -c '^=== flintctl start ' "$LOG" || true)
BANNERS_BEFORE="${BANNERS_BEFORE//[^0-9]/}"

echo "== restart the replica (the act that used to erase the evidence)"
fleet_signal_port "$RPORT" -9 || { echo "FAIL: could not kill $REPLICA"; exit 1; }
for _ in $(seq 1 20); do
  $CTL -f "$INV" status 2>/dev/null | grep -q "$REPLICA  *DOWN" && break
  sleep 0.3
done
$CTL -f "$INV" start >/dev/null 2>&1 || { echo "FAIL: start after kill"; exit 1; }
for _ in $(seq 1 40); do
  $CTL -f "$INV" status 2>/dev/null | grep -q "$REPLICA.*replica" && break
  sleep 0.5
done

grep -q "$MARK" "$LOG" \
  || { echo "FAIL: the previous run's output was erased by the restart"; exit 1; }
echo "  the earlier run's marker survived"

BANNERS_AFTER=$(grep -c '^=== flintctl start ' "$LOG" || true)
BANNERS_AFTER="${BANNERS_AFTER//[^0-9]/}"
[ "${BANNERS_AFTER:-0}" -gt "${BANNERS_BEFORE:-0}" ] \
  || { echo "FAIL: no new start banner — runs cannot be told apart ($BANNERS_BEFORE -> $BANNERS_AFTER)"; exit 1; }
echo "  runs are separated by banners ($BANNERS_BEFORE -> $BANNERS_AFTER)"

# The seat must actually be serving again — a log that survives because
# nothing restarted would pass the checks above and prove nothing.
$CTL -f "$INV" status 2>/dev/null | grep -q "$REPLICA.*replica" \
  || { echo "FAIL: replica did not come back, so the restart never happened"; \
       $CTL -f "$INV" status; exit 1; }
echo "  and the seat is serving again (the restart was real)"

echo "PASS: a seat's log outlives the seat (docs/bugs/0005 needs this to be debuggable)"
