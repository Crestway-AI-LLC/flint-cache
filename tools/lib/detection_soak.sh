#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The COST side of tightening detection: how often does the controller promote
# when nothing died?
#
# Bring up a real fleet at the caller's poll-ms/confirm, drive continuous
# writes through the proxy edge, kill NOTHING, and count PromoteIssued in the
# fleet journal. Every one of those is a healthy master being demoted for a
# probe that was merely late — which costs a replacement full sync and an
# epoch, and is strictly worse than the slower detection it bought.
#
# Prints ONE line: the promotion count, or a reason it could not be measured.
# Called by tools/detection_sweep.sh; env: FLINT_POLL_MS, FLINT_CONFIRM, SOAK.
set -u
cd "$(dirname "$0")/../.."
. "$(dirname "$0")/fleet.sh"

D=${1:-/tmp/flint-detsoak}
SOAK=${SOAK:-45}
POLL=${FLINT_POLL_MS:-150}
CONF=${FLINT_CONFIRM:-3}
CTL=./target/release/flintctl
INV="$D/cluster.flint"

# Its own port block, so a sweep can never be confused with the drill it runs
# beside — same rule assert_no_default_ports enforces across the suite.
fleet_init "$D" 7451 7452 7453 7454 7481 7482
fleet_guard
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; fleet_kill controller
sleep 0.3
cleanup() {
  fleet_kill server; fleet_kill proxy; fleet_kill controlplane; fleet_kill controller
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

cat > "$INV" <<EOF
disposable on
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:7481
pair 127.0.0.1:7451,127.0.0.1:7452
pair 127.0.0.1:7453,127.0.0.1:7454
proxy 127.0.0.1:7482
controller on
poll-ms $POLL
confirm $CONF
EOF

$CTL -f "$INV" bootstrap >"$D/bootstrap.log" 2>&1 || { echo "bootstrap failed"; exit 0; }
$CTL -f "$INV" tenant add soak tok-soak soak 1 >/dev/null 2>&1 \
  || { echo "tenant failed"; exit 0; }

JOURNAL="$D/state/cp.state.journal"
[ -f "$JOURNAL" ] || JOURNAL=$(find "$D/state" -name '*.journal' 2>/dev/null | head -1)
[ -n "$JOURNAL" ] && [ -f "$JOURNAL" ] || { echo "no journal found"; exit 0; }

# Baseline: bootstrap itself legitimately promotes, so only promotions AFTER
# this point count. Comparing against zero would score the fleet coming up as
# a false positive and make every setting look broken.
# grep -c prints its count AND exits 1 on zero matches, so `|| echo 0`
# yields "0\n0" and the subtraction below dies on it. `|| true` keeps
# grep's own zero, which is the number we actually want.
BEFORE=$(grep -c PromoteIssued "$JOURNAL" 2>/dev/null || true)

# Continuous load, no kills. Load matters: an idle controller probing an idle
# master is the easiest case there is, and the misses that `confirm` exists to
# absorb happen when the box is busy.
END=$(( $(date +%s) + SOAK ))
# POSITIVE CONTROL. A counter that reports "0 spurious promotions" is
# worthless until it has been shown to report anything else — this suite has
# already shipped one check that could only ever say zero (the -THROTTLED
# shed path, #121). FLINT_SOAK_KILL=1 kills a master mid-soak so the caller
# can confirm the instrument moves off zero for a REAL promotion.
KILL_AT=$(( END - SOAK / 2 ))
KILLED=0
while [ "$(date +%s)" -lt "$END" ]; do
  valkey-cli -p 7482 -a tok-soak --no-auth-warning SET "soak:$RANDOM" v >/dev/null 2>&1
  if [ "${FLINT_SOAK_KILL:-0}" = "1" ] && [ "$KILLED" = "0" ] \
     && [ "$(date +%s)" -ge "$KILL_AT" ]; then
    M=$($CTL -f "$INV" status 2>/dev/null | awk '/master/ {print $3; exit}')
    [ -n "$M" ] && $CTL -f "$INV" kill-node "$M" >/dev/null 2>&1
    KILLED=1
  fi
done

AFTER=$(grep -c PromoteIssued "$JOURNAL" 2>/dev/null || true)
echo $(( AFTER - BEFORE ))
