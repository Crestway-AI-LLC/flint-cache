#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0159: `verify` must be able to see a MISSING controller — and must not
# cry wolf about one that simply has not registered yet.
#
# The controller has no listener by design, so verify cannot probe it. The only
# evidence it exists is the row it pushes to the CP every 30s, and the CP holds
# that registry in memory only (`#[serde(skip)]`), so a control plane that
# restarts knows of no controller until the next tick. `verify_after` runs
# immediately following upgrade/expand/swap/roll — exactly inside that window.
#
# So the check has two ways to be useless and this drill closes both:
#
#   ARM 1  a fleet with a live controller verifies clean
#   ARM 2  CP restarted, registry empty, CONTROLLER STILL ALIVE -> verify must
#          still pass, because it waits the registration window out. Without
#          the wait this is a red verify after every roll.
#   ARM 3  CP restarted, registry empty, CONTROLLER KILLED -> verify must FAIL
#          and name the controller.
#
# Arms 2 and 3 differ by ONE fact: whether the controller process is alive.
# Same inventory, same CP restart, same empty registry, opposite verdicts —
# which is what makes arm 3 a positive control rather than a hope.
#
# Requires: a release build with --features rocks, valkey-cli on PATH.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-vctl- 7460 7461 7462 7463
fleet_guard

STATE=$FLINT_DRILL_ROOT/flint-vctl-state
INV=$FLINT_DRILL_ROOT/flint-vctl.flint
A=127.0.0.1:7460
B=127.0.0.1:7461
PROXY=127.0.0.1:7462
CP=127.0.0.1:7463
OUT=$FLINT_DRILL_ROOT/flint-vctl.verify

fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
  rm -rf "$STATE" "$INV" "$OUT"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV" "$OUT"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
cp $CP
pair $A,$B
proxy $PROXY
controller on
EOF

CTL="./target/release/flintctl -f $INV"

echo "== bootstrap: a fleet that declares a controller"
$CTL bootstrap >"$STATE-boot.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -25 "$STATE-boot.log"; exit 1; }

# The controller registers at boot and then every 30s, so this is prompt. It
# is waited for rather than assumed, because arm 1 asserting "verify passes"
# before any row existed would pass for the wrong reason.
controller_rows() { valkey-cli -p "${CP##*:}" CPINFO 2>/dev/null | tr -d '\r' | grep -c '^controller:'; }
for _ in $(seq 1 60); do
  [ "$(controller_rows)" != "0" ] && break
  sleep 0.5
done
[ "$(controller_rows)" != "0" ] && echo "  the CP has a controller row" || {
  echo "FAIL: no controller row ever appeared, so the rest of this drill would"
  echo "  be asserting against a fleet that never had a controller at all"
  exit 1; }

echo "== ARM 1: with a live controller, verify passes and says which one"
$CTL verify >"$OUT" 2>&1 || { echo "FAIL: verify refused a healthy fleet"; tail -20 "$OUT"; exit 1; }
grep -q "a controller is reporting" "$OUT" || {
  echo "FAIL: verify passed without reporting on the controller at all --"
  echo "  which is BUG-0159 itself: the section is missing, not merely quiet"
  tail -20 "$OUT"; exit 1; }
echo "  verify OK and named the controller"

# The registry is in-memory, so restarting the CP is how this drill produces
# the state a roll produces: a reachable control plane that knows of no
# controller yet. Restarted BY HAND rather than through `flintctl start`,
# which would respawn the controller too and defeat arm 3.
restart_cp_alone() {
  fleet_kill controlplane
  sleep 0.3
  # `disown` so the shell does not print `Killed: 9` when cleanup or the next
  # arm takes this seat down: a job-control notice in a drill's output reads
  # like a fault to whoever finds the gate log.
  ./target/release/flint-controlplane --port "${CP##*:}" --state "$STATE/cp-state" \
    >>"$STATE-cp.log" 2>&1 &
  disown
  for _ in $(seq 1 60); do
    [ "$(valkey-cli -p "${CP##*:}" PING 2>/dev/null)" = "PONG" ] && return 0
    sleep 0.5
  done
  echo "FAIL: the control plane did not come back after a manual restart"; exit 1
}

echo "== ARM 2: empty registry, controller ALIVE — verify must still pass"
restart_cp_alone
[ "$(controller_rows)" = "0" ] || echo "  note: a row was already back before verify ran"
$CTL verify >"$OUT" 2>&1 || {
  echo "FAIL: verify refused a fleet whose controller is alive and had simply"
  echo "  not re-registered yet. This is the false alarm after every roll that"
  echo "  the registration window exists to prevent."
  tail -20 "$OUT"; exit 1; }
grep -q "a controller is reporting" "$OUT" || {
  echo "FAIL: verify passed but never saw a controller row"; tail -20 "$OUT"; exit 1; }
echo "  verify waited the window out and passed"

echo "== ARM 3: empty registry, controller KILLED — verify must FAIL"
fleet_kill controller
sleep 0.4
restart_cp_alone
if $CTL verify >"$OUT" 2>&1; then
  echo "FAIL: verify PASSED a fleet with no controller. That is the defect"
  echo "  BUG-0159 filed: pairs up, CP reachable, nothing to promote a master."
  tail -20 "$OUT"; exit 1
fi
# The RIGHT reason. A verify that failed because the proxy fell over would
# satisfy a bare exit-code assertion and prove nothing about this check.
grep -q "a controller is registered" "$OUT" || {
  echo "FAIL: verify failed, but not with the controller finding -- so this arm"
  echo "  says nothing about the check it exists to exercise:"
  tail -20 "$OUT"; exit 1; }
grep -q "does NOT fail over" "$OUT" || {
  echo "FAIL: the controller failure did not state the consequence"; tail -5 "$OUT"; exit 1; }
echo "  verify FAILED and named the controller"

echo "PASS: verify sees a live controller, waits out an empty registry while one is alive, and refuses a fleet that has none"
