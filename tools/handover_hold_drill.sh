#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A planned handover must not race the controller (BUG-0207).
#
# THE BUG THIS PINS, measured on the playground at every roll from rc.76 to
# rc.79. `flintctl`'s controlled failover (the `failover` verb, and `upgrade`'s
# masters phase) demotes the old master, drains, commits CPFENCE and only then
# promotes the new one, so for the length of the drain the pair has no master.
# The controller promotes after `confirm` empty ticks. Every roll's drain
# outlasted them, so the controller promoted the just-demoted old master into
# the gap and the roll then promoted the new one at the SAME role epoch:
#
#   controller.log  [ctl][g0] PROMOTED 172.31.64.94:7001 at (0,82): OK promoted at (0,82)
#   roll output     pair 0: 172.31.64.94:7001 demoted + drained; 172.31.64.94:7002 promoted at (0,82)
#
# The fix: the handover takes a hold on the CP (`CPHANDOVER`) before it
# demotes and refreshes it while the drain makes progress; the controller asks
# for it before announcing an outage, and stands down while it is live.
#
# ON A LAPTOP THE DRAIN IS TOO FAST TO RACE, so the gap is held open with
# FLINT_HANDOVER_DRAIN_FLOOR_MS (a drill knob, like FLINT_ROLL_GRACE_MS). The
# controller runs at the playground's own `poll-ms 100 confirm 3`.
#
# Two arms:
#   1. A planned handover: the controller must say it is HOLDING (which it does
#      only after `confirm` empty ticks -- the proof the gap was long enough to
#      race), must not promote, and the pair must end with one master.
#   2. A `flintctl` that dies mid-handover: the hold must lapse, and the
#      controller must take the pair within a few seconds -- a hold may delay
#      a real outage by one hold, never block it.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-handover 7568 7569 7570
fleet_guard
D=$FLINT_DRILL_ROOT/flint-handover; INV=$D/cluster.flint
CTL=./target/release/flintctl
FLOOR_MS=2000

fleet_kill controller; fleet_kill server; fleet_kill controlplane
sleep 0.4
cleanup() {
  $CTL -f "$INV" stop >/dev/null 2>&1
  fleet_kill controller; fleet_kill server; fleet_kill controlplane
  rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

cargo build --release -q -p flint-server -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:7570
pair 127.0.0.1:7568,127.0.0.1:7569
controller on
poll-ms 100
confirm 3
EOF

echo "== bootstrap (CP, 1 pair, controller; poll-ms 100 confirm 3, as the playground)"
$CTL -f "$INV" bootstrap >"$D/bootstrap.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -20 "$D/bootstrap.log" | sed 's/^/  | /'; exit 1; }
$CTL -f "$INV" verify >"$D/verify.log" 2>&1 \
  || { echo "FAIL: fleet did not verify before the test"; tail -15 "$D/verify.log" | sed 's/^/  | /'; exit 1; }

st()        { $CTL -f "$INV" status 2>/dev/null; }
masters()   { st | awk '$1=="pair" && $4=="master"' | wc -l | tr -d ' '; }
master()    { st | awk '$1=="pair" && $4=="master"{print $3; exit}'; }
epoch_of()  { st | awk -v a="$1" '$1=="pair" && $3==a {print $6; exit}'; }
role_of()   { st | awk -v a="$1" '$1=="pair" && $3==a {print $4; exit}'; }
clog()      { ls -t "$D"/state/logs/*ontroller* 2>/dev/null | head -1; }
show()      { st | grep -E '^pair|^controller' | sed 's/^/    /'; c=$(clog); [ -n "$c" ] && tail -15 "$c" | sed 's/^/  | /'; }

for _ in $(seq 1 60); do [ "$(masters)" = 1 ] && break; sleep 0.5; done
[ "$(masters)" = 1 ] || { echo "FAIL: expected 1 master before the test"; show; exit 1; }
CLOG=$(clog); [ -n "$CLOG" ] || { echo "FAIL: no controller log under $D/state/logs"; exit 1; }

# ---------------------------------------------------------------- arm 1
M=$(master)
echo "== arm 1: planned handover of $M with the drain held open ${FLOOR_MS}ms"
MARK=$(wc -l < "$CLOG" | tr -d ' ')
FLINT_HANDOVER_DRAIN_FLOOR_MS=$FLOOR_MS $CTL -f "$INV" failover "$M" >"$D/failover1.log" 2>&1 \
  || { echo "FAIL: failover of $M"; tail -15 "$D/failover1.log" | sed 's/^/  | /'; show; exit 1; }
sleep 1
NEW=$(master)
DURING=$(tail -n +"$((MARK + 1))" "$CLOG")
if ! grep -q 'holding: a planned handover is in progress' <<<"$DURING"; then
  echo "FAIL: the controller never said it was holding -- either the gap did not outlast"
  echo "      confirm ticks (the drill proves nothing) or it did not ask the CP for the hold"
  show; exit 1
fi
echo "  ok    the controller saw the gap past confirm ticks and held"
if grep -q "PROMOTED $M " <<<"$DURING"; then
  echo "FAIL: the controller promoted the demoted old master $M into the planned gap (BUG-0207)"
  grep "PROMOTED" <<<"$DURING" | sed 's/^/  | /'; show; exit 1
fi
echo "  ok    the controller did not promote the old master into the gap"
[ "$(masters)" = 1 ] && [ -n "$NEW" ] && [ "$NEW" != "$M" ] \
  || { echo "FAIL: after the handover expected one master, not $M; status:"; show; exit 1; }
# NOT "the epochs differ": a replica ADOPTS its master's role epoch when it
# rejoins (the playground's 7001 logged "adopted the master's role epoch
# (0,82)"), so equal epochs are the converged state. The BUG-0207 shape was two
# nodes CLAIMING master at one epoch, which is the role check.
[ "$(role_of "$M")" = replica ] \
  || { echo "FAIL: the old master $M is not a replica after the handover"; show; exit 1; }
echo "  ok    one master ($NEW at $(epoch_of "$NEW")); the old one rejoined as its replica"
grep -q 'note: the control plane took no handover hold' "$D/failover1.log" \
  && { echo "FAIL: flintctl says the CP took no hold, against a CP that knows the verb"; exit 1; }

$CTL -f "$INV" verify >"$D/verify1.log" 2>&1 \
  || { echo "FAIL: fleet did not verify after the handover"; tail -15 "$D/verify1.log" | sed 's/^/  | /'; exit 1; }

# ---------------------------------------------------------------- arm 2
M=$(master)
echo "== arm 2: a flintctl that dies mid-handover of $M must not leave the pair unsupervised"
MARK=$(wc -l < "$CLOG" | tr -d ' ')
FLINT_HANDOVER_DRAIN_FLOOR_MS=60000 $CTL -f "$INV" failover "$M" >"$D/failover2.log" 2>&1 &
FPID=$!
# Long enough to be demoted and inside the drain, holding.
for _ in $(seq 1 40); do
  tail -n +"$((MARK + 1))" "$CLOG" | grep -q 'holding: a planned handover' && break
  sleep 0.25
done
tail -n +"$((MARK + 1))" "$CLOG" | grep -q 'holding: a planned handover' \
  || { kill -9 $FPID 2>/dev/null; echo "FAIL: the controller never held for the second handover"; show; exit 1; }
kill -9 $FPID 2>/dev/null; wait $FPID 2>/dev/null
T0=$(date +%s)
echo "  flintctl killed while the controller holds; the hold must lapse"
PROMOTED=""
for _ in $(seq 1 60); do
  [ "$(masters)" = 1 ] && { PROMOTED=1; break; }
  sleep 0.25
done
DT=$(( $(date +%s) - T0 ))
[ -n "$PROMOTED" ] \
  || { echo "FAIL: no master 15s after flintctl died mid-handover -- the hold outlived its caller"; show; exit 1; }
tail -n +"$((MARK + 1))" "$CLOG" | grep -q 'hold ended with no master' \
  || { echo "FAIL: a master appeared but not through the controller ending its hold"; show; exit 1; }
[ "$DT" -le 10 ] \
  || { echo "FAIL: the controller took ${DT}s to recover a pair whose handover died (hold is 5s)"; show; exit 1; }
echo "  ok    the hold lapsed and the controller promoted $(master) within ${DT}s"

$CTL -f "$INV" verify >"$D/verify2.log" 2>&1 \
  || { echo "FAIL: fleet did not verify after the dead handover"; tail -15 "$D/verify2.log" | sed 's/^/  | /'; exit 1; }

echo "PASS: a planned handover holds the controller off its gap, and a dead one does not hold it for long"
