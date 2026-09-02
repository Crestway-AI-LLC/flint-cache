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
# read timeout, and the proxy must not rotate even once -- and must not apply a
# further filtered snapshot either.
#
# THREE SEATS, ON PURPOSE. BUG-0081 said of the three-seat case: "it would walk
# the seats once a second forever, paying a fresh CPWATCH and a fresh filtered
# snapshot per rotation on an idle fleet", and then said that was an inference
# from reading the loop rather than running it, "worth a drill that counts
# CPWATCH subscriptions on an idle fleet before believing it". This is that
# drill. One seat exercises the same trigger, but only three exercise the
# WALK, and the snapshot count is the cost the prediction was about.
#
# The window is deliberately just over 30s. Before the fix a rotation appears at
# ~31s, so a shorter idle would pass against the broken code and prove nothing;
# the drill would be green about a period in which nothing could have happened.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
# Moved off 6521 (BUG-0086): controller_multipair_drill uses it as pair C's
# second seat without declaring it, so the two drills shared a port and
# assert_no_port_overlap -- which reads declarations only -- saw nothing.
fleet_init $FLINT_DRILL_ROOT/flint-cpwi-state 6603 6604 6605 6606 6607
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
cp 127.0.0.1:6604
cp 127.0.0.1:6605
cp 127.0.0.1:6606
pair 127.0.0.1:6603
proxy 127.0.0.1:6607
proxy-advertise 127.0.0.1:6607
INVEOF

CTL=./target/release/flintctl
$CTL -f "$INV" bootstrap >"$D/bootstrap.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -5 "$D/bootstrap.log"; exit 1; }
PLOG="$STATE/logs/proxy-6607.log"
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
# THE COST, NOT JUST THE EVENT. BUG-0081 predicted that on three seats the
# proxy "would walk the seats once a second forever, paying a fresh CPWATCH and
# a fresh filtered snapshot per rotation" -- and the write-up said that was an
# inference from reading the loop rather than running it. This counts the
# snapshots, which is what a rotation actually costs, and it is a SECOND
# observation of the same property: a churn has to show up here even if the
# rotation log line is ever renamed away from "trying next seat".
count_snapshots() { c=$(grep -c "control-plane snapshot v" "$PLOG" 2>/dev/null | tr -d '[:space:]'); printf '%s' "${c:-0}"; }
BEFORE=$(count_rotations)
SNAP_BEFORE=$(count_snapshots)
[ "$SNAP_BEFORE" -ge 1 ] || { echo "FAIL: no snapshot counted before the idle window, so the count below can only go up from nothing"; exit 1; }
echo "== idle for ${IDLE_S}s — longer than the 30s read timeout"
sleep "$IDLE_S"
AFTER=$(count_rotations)
SNAP_AFTER=$(count_snapshots)

if [ "$SNAP_AFTER" -gt "$SNAP_BEFORE" ]; then
  echo "FAIL: the proxy applied $((SNAP_AFTER - SNAP_BEFORE)) further control-plane"
  echo "      snapshot(s) across a window in which the fleet did not change. A"
  echo "      filtered snapshot per idle interval is the cost BUG-0081 predicted"
  echo "      for the three-seat case, and this fleet has three seats."
  grep "control-plane snapshot v" "$PLOG" | tail -3 | sed 's/^/    /'
  exit 1
fi

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

echo "PASS: ${IDLE_S}s idle on THREE control-plane seats, zero rotations, zero extra snapshots ($SNAP_AFTER total), proxy still serving — an idle CP no longer reads as a dead one, and the three-seat churn is measured rather than predicted (BUG-0081)"
