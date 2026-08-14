#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Repeated failover on ONE pair must keep promoting, every time (#171).
#
# THE OBSERVATION THIS CHASES. The 5 TB scale run's baseline chaos passed ten
# iterations — six master kills promoting in 340-1638ms — and then, on the
# eleventh, the controller did not promote the survivor within 30s:
#
#   thread 'main' panicked at crates/flint-chaos/src/cluster.rs:366
#   kill master: controller did not promote 172.31.70.189:7001 within 30s
#
# That was on HEAD with the #168 lease fixes deployed and asserted on all
# eleven hosts, so it is NOT the self-fence. It is intermittent, which is
# exactly why the #168 work did not catch it, and the fleet tore down before
# the controller log could be read — flint-chaos panics without dumping it.
#
# So this drill exists to make that failure reproducible for FREE on loopback,
# and to keep it reproducible. It replicates Attached::kill's shape exactly —
# kill the master, restart the dead seat immediately, wait for the OTHER member
# to report master — and repeats it on ONE pair, which is the condition the
# scale run's random walk only reached by luck. No writer: if this reproduces
# without load, load was never the trigger.
#
# WHAT MAKES A RUN MEANINGFUL. The failure is intermittent, so a single clean
# pass proves nothing; ITERS is deliberately high and the drill reports the
# slowest promotion it saw, because a promotion drifting toward the timeout is
# the same finding arriving early. On failure it dumps what run 9 could not:
# flintctl status (role, epoch, seq_lag and live_replicas for both seats) and
# the controller's own log.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-churn 7380 7381 7382 7383
fleet_guard
D=/tmp/flint-churn; INV=$D/cluster.flint
CTL=./target/release/flintctl
ITERS="${FLINT_CHURN_ITERS:-14}"
# The budget flint-chaos gives the controller. Kept identical on purpose: this
# drill is asking the same question the scale run asked, so a different budget
# would make a pass here compatible with a failure there.
PROMOTE_S="${FLINT_CHURN_PROMOTE_S:-30}"
# Gap between the promotion landing and the next kill. 0 means "as fast as the
# harness goes"; raising it is the knob that tells us whether the trigger is a
# race against re-convergence.
GAP_MS="${FLINT_CHURN_GAP_MS:-0}"

fleet_kill server; fleet_kill proxy; fleet_kill controlplane; fleet_kill controller
sleep 0.4
cleanup() {
  $CTL -f "$INV" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill proxy; fleet_kill controlplane; fleet_kill controller
  rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

# Same knobs the scale fleet runs (packaging/aws/chaos-cluster/run.sh's
# inventory): poll-ms 100, confirm 3, lease-ttl-ms 4000. Copying them matters —
# the detection window IS the thing under test, and a drill tuned differently
# would answer a different question.
cat > "$INV" <<EOF
disposable on
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:7383
pair 127.0.0.1:7380,127.0.0.1:7381
proxy 127.0.0.1:7382
controller on
poll-ms 100
confirm 3
lease-ttl-ms 4000
EOF

echo "== bootstrap (CP, 1 pair, proxy, controller; poll-ms 100 confirm 3 lease 4000)"
$CTL -f "$INV" bootstrap >"$D/bootstrap.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -20 "$D/bootstrap.log" | sed 's/^/  | /'; exit 1; }
$CTL -f "$INV" verify >"$D/verify.log" 2>&1 \
  || { echo "FAIL: fleet did not verify before the churn"; tail -15 "$D/verify.log" | sed 's/^/  | /'; exit 1; }

A=127.0.0.1:7380; B=127.0.0.1:7381
# `flintctl status` prints one row per node and is the ONE reader that already
# dials both seats over the mesh TLS:
#   pair 0    127.0.0.1:7380  master  epoch 1  build X  seq_lag 0  live_replicas 1
# Parsed positionally ($4 role, $10 seq_lag, $12 live_replicas), and a DOWN node
# prints a short row that simply matches none of the field tests below.
st()      { $CTL -f "$INV" status 2>/dev/null; }
role_of() { st | awk -v a="$1" '$1=="pair" && $3==a {print $4}'; }
other()   { [ "$1" = "$A" ] && echo "$B" || echo "$A"; }
master()  { st | awk '$1=="pair" && $4=="master"{print $3; exit}'; }

# The controller log is the artifact run 9 lost. Find it once, dump it on any
# failure — an intermittent bug that destroys its own evidence costs a whole
# run per attempt, which is what happened.
CLOG=
dump() {
  echo "  --- flintctl status ---"; $CTL -f "$INV" status 2>&1 | sed 's/^/  | /'
  # Re-resolve: the log only exists once the controller has been spawned, and
  # on a rolled seat the name can change.
  CLOG=$(ls -t "$D"/state/logs/*controller* 2>/dev/null | head -1)
  [ -n "${CLOG:-}" ] \
    && { echo "  --- controller log (tail 60): $CLOG ---"; tail -60 "$CLOG" | sed 's/^/  | /'; } \
    || echo "  (no controller log found under $D/state/logs)"
}

# HARNESS-SHAPED PRECONDITION. flint-chaos asserts wait_healthy before every
# master kill: a live replica AND seq_lag==0, which is the same predicate the
# controller uses for legit_converged. Asserting it here too is what makes a
# refusal to promote a genuine finding rather than "killed a single-copy pair",
# which would be correct behaviour and not a bug.
healthy() {
  st | awk '$1=="pair" && $4=="master" && $10=="0" && $12+0>=1 {ok=1} END{exit ok?0:1}'
}
wait_healthy() { local d=$((SECONDS+$1)); while [ $SECONDS -lt $d ]; do healthy && return 0; sleep 0.3; done; return 1; }

wait_healthy 30 || { echo "FAIL: pair never converged before the churn began"; dump; exit 1; }
echo "  baseline: master $(master), replica converged"

WORST=0; WORST_ITER=0
for i in $(seq 1 "$ITERS"); do
  wait_healthy 60 || { echo "FAIL: iter $i: pair never re-converged (live replica + seq_lag 0) within 60s"; dump; exit 1; }
  M=$(master); S=$(other "$M")
  [ -n "$M" ] || { echo "FAIL: iter $i: no master to kill"; dump; exit 1; }

  [ "$GAP_MS" -gt 0 ] && sleep "$(awk -v m="$GAP_MS" 'BEGIN{print m/1000}')"

  # ORDER MATTERS AND IS COPIED, NOT CHOSEN. Attached::kill waits for the
  # promotion INSIDE kill(), and flint-chaos only restarts the dead seat once
  # that returns. So the window under test has the killed seat still DOWN and
  # the survivor alone — which is precisely the state in which the survivor
  # reports live_replicas 0 and seq_lag none. Restarting first (my initial
  # mistake) is not merely different, it is rejected outright: restart-node
  # refuses to re-attach a node as a replica of itself while the pair has no
  # new master.
  $CTL -f "$INV" kill-node "$M" >/dev/null 2>&1 \
    || { echo "FAIL: iter $i: could not kill $M"; dump; exit 1; }

  T0=$SECONDS; OK=0
  while [ $((SECONDS-T0)) -lt "$PROMOTE_S" ]; do
    [ "$(role_of "$S")" = master ] && { OK=1; break; }
    sleep 0.2
  done
  TOOK=$((SECONDS-T0))
  [ "$OK" = 1 ] || {
    echo "FAIL: iter $i: controller did not promote $S within ${PROMOTE_S}s after killing $M"
    echo "      This is #171 — the same failure the 5 TB run hit at iteration 11."
    dump
    exit 1
  }
  # Now re-attach the dead seat as a replica of the new master, same as the
  # harness. Fresh data, same fixed address.
  $CTL -f "$INV" restart-node "$M" >/dev/null 2>&1 \
    || { echo "FAIL: iter $i: could not restart $M as a replica of $S"; dump; exit 1; }
  [ "$TOOK" -gt "$WORST" ] && { WORST=$TOOK; WORST_ITER=$i; }
  echo "  iter $i: killed $M, $S promoted in ${TOOK}s"
done

$CTL -f "$INV" verify >"$D/verify-after.log" 2>&1 \
  || { echo "FAIL: fleet does not verify after $ITERS failovers"; tail -15 "$D/verify-after.log" | sed 's/^/  | /'; dump; exit 1; }

echo "PASS: $ITERS consecutive failovers on one pair, every one promoted (slowest ${WORST}s at iter ${WORST_ITER}), fleet still verifies (#171)"
