#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# An IDLE control plane must not make the proxy rotate its CP seat (BUG-0081).
#
# `watch_control_plane` sets a 30s read timeout deliberately, and the control
# plane sends NOTHING while idle: its `watch` pushes only when the version
# advances past what the proxy has ACKed, waiting on a condvar in 500ms slices
# with no keepalive on the wire. So on a quiet fleet that read times out every
# 30s -- and the rotation loop treated ANY error as a dead seat, so it logged
# "trying next seat" and re-subscribed every ~31s, forever, for no change.
#
# The rotation exists for a real failure (a proxy pinned to a killed seat,
# reconnecting to a corpse while quorum stayed healthy). "This seat has nothing
# to say yet" is not that failure, and the two were indistinguishable.
#
# WHAT THIS ASSERTS: leave a healthy fleet completely idle for longer than one
# read timeout, and the proxy must not rotate even once.
#
# The window is deliberately just over 30s. Before the fix a rotation appears at
# ~31s, so a shorter idle would pass against the broken code and prove nothing;
# the drill would be green about a period in which nothing could have happened.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-cpwi-state 6521 6522 6523
fleet_guard
D=$FLINT_DRILL_ROOT/flint-cpwi; STATE=$FLINT_DRILL_ROOT/flint-cpwi-state
INV=$D/cluster.flint
IDLE_S="${FLINT_CPWI_IDLE_S:-40}"
rm -rf "$D" "$STATE"; mkdir -p "$D"
fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop >/dev/null 2>&1
  fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
  [ -n "${KEEP:-}" ] || rm -rf "$D" "$STATE"
}
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<INVEOF
disposable on
statedir $STATE
bins ./target/release
cp 127.0.0.1:6522
pair 127.0.0.1:6521
proxy 127.0.0.1:6523
proxy-advertise 127.0.0.1:6523
INVEOF

CTL=./target/release/flintctl
$CTL -f "$INV" bootstrap >"$D/bootstrap.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -5 "$D/bootstrap.log"; exit 1; }
PLOG="$STATE/logs/proxy-6523.log"
[ -f "$PLOG" ] || { echo "FAIL: no proxy log at $PLOG — nothing to assert against"; exit 1; }
echo "  fleet up; watching $PLOG"

# CONTROL: the watch must actually be RUNNING, or "no rotations" is trivially
# true of a proxy that never subscribed. A subscribed proxy applies the initial
# snapshot and says so.
for _ in $(seq 1 30); do
  grep -q "control-plane snapshot v" "$PLOG" && break
  sleep 1
done
grep -q "control-plane snapshot v" "$PLOG" || {
  echo "FAIL: the proxy never applied a control-plane snapshot, so it never"
  echo "      subscribed — 'it did not rotate' would be true and meaningless."
  tail -5 "$PLOG" | sed 's/^/    /'
  exit 1; }
echo "  control: the proxy is subscribed (snapshot applied)"

# `grep -c` PRINTS 0 and EXITS 1 when there are no matches, so `|| echo 0`
# appends a second zero and the comparison below becomes `[: 0\n0: integer
# expression expected` -- an error, not a failure, so the FAIL branch could
# never fire. The first run of this drill passed that way.
count_rotations() { c=$(grep -c "trying next seat" "$PLOG" 2>/dev/null | tr -d '[:space:]'); printf '%s' "${c:-0}"; }
BEFORE=$(count_rotations)
echo "== idle for ${IDLE_S}s — longer than the 30s read timeout"
sleep "$IDLE_S"
AFTER=$(count_rotations)

if [ "$AFTER" -gt "$BEFORE" ]; then
  echo "FAIL: the proxy rotated its control-plane seat $((AFTER - BEFORE)) time(s)"
  echo "      while the fleet was completely idle. An idle control plane is not"
  echo "      a dead one; each rotation is a fresh CPWATCH and a fresh filtered"
  echo "      snapshot bought for nothing (BUG-0081)."
  grep "trying next seat" "$PLOG" | tail -3 | sed 's/^/    /'
  exit 1
fi

# And it must still be ALIVE, not merely quiet: a proxy that died would also
# log no rotations.
grep -q "control-plane snapshot v" "$PLOG" || { echo "FAIL: lost the snapshot marker"; exit 1; }
./target/release/flintctl -f "$INV" status 2>/dev/null | grep -q "^proxy .* up " || {
  echo "FAIL: the proxy is not up after the idle window — 'no rotations' would"
  echo "      then just mean 'no proxy'."
  exit 1; }

echo "PASS: ${IDLE_S}s idle, zero control-plane rotations, proxy still serving — an idle CP no longer reads as a dead one (BUG-0081)"
