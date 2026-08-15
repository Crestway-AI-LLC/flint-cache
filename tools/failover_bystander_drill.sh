#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A self-fenced master with NO replica must still be recoverable (#171).
# (Reworked for ADR-0018: the fence is now triggered by stalling the CP —
# masters renew their own lease there — instead of the controller, whose
# stall no longer fences anything. The gate under test is unchanged.)
#
# THE BUG THIS PINS, measured on the 5 TB scale run (run 11). The controller
# refuses to auto-promote a pair that has not converged recently
# (--max-stale-ms, default 5s), with exactly one escape: `insync_lineage_holder`
# wants a reachable node at the top epoch reporting
# `live_replicas >= 1 && seq_lag == Some(0)`.
#
# That predicate was written for #168's SELF-FENCE case, where the master is
# alive-but-read-only and therefore still has a replica attached. It cannot fire
# when the lineage holder has LOST its replica: FLINTINFO renders seq_lag as the
# string "none" whenever no live replica is attached, so such a node reports
# `live_replicas 0, seq_lag none` and satisfies nothing. The controller then
# pages every tick, forever:
#
#   [ctl][g3] no master and pair not converged within 5s — REFUSING
#             (degraded window; needs spare/S3). PAGE.
#
# Two pairs of four ended permanently write-dead that way, with every node
# alive and holding its data.
#
# WHY THE EXISTING DRILLS MISS IT, which is the interesting part. Both
# controller_stall_drill and failover_churn_drill CONVERGE EVERY PAIR before
# they do anything — faithfully copying what flint-chaos does — and a converged
# pair is precisely the condition that avoids this gate. failover_churn passes
# 16/16 at four pairs under load. No number of iterations could have found this,
# because the setup excluded it. So this drill deliberately builds the un-ideal
# precondition: a BYSTANDER pair that nobody is testing, left with no replica,
# so its `last_converged` freezes while the rest of the fleet carries on.
#
# THE SHAPE:
#   pair 0  — healthy, master + caught-up replica. The CONTROL: the in-sync
#             escape (#168) already covers it, so it must still recover,
#             proving the fence fired and that the fix did not simply
#             disable the gate.
#   pair 1  — the BYSTANDER: replica killed, so its master has live_replicas 0
#             and the pair stops converging.
#   then    — SIGSTOP the CP past the TTL so BOTH masters self-fence (under
#             ADR-0018 masters renew their own lease at the CP; a stalled
#             CONTROLLER no longer fences anything — see
#             controller_stall_drill), SIGCONT, and require BOTH pairs back.
#
# Pair 1's surviving node is the same process that was master, at the top epoch,
# holding all of the pair's data, with no competing lineage anywhere. Promoting
# it back cannot lose a write. Refusing it is the outage.
# NO PROXY in the inventory, like controller_drill and
# controller_multipair_drill: this drill reads roles from `flintctl status` and
# never writes through an edge, so a proxy would only add a seat to start and
# one more fixed 10s readiness window to miss on a busy machine — which it did,
# repeatedly, while this was being written.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-bystander 7490 7491 7492 7493 7495
fleet_guard
D=$FLINT_DRILL_ROOT/flint-bystander; INV=$D/cluster.flint
CTL=./target/release/flintctl
LEASE="${FLINT_LEASE_TTL_MS:-3000}"
STALL_S="${FLINT_STALL_S:-10}"
# The controller's --max-stale-ms default. NOT settable from the inventory
# (flintctl only plumbs poll-ms/confirm/lease-ttl-ms), so this mirrors the
# default rather than setting it — if that default moves, this must follow or
# the wait below stops arming the gate and the drill quietly tests nothing.
MAX_STALE_MS=5000

fleet_kill server; fleet_kill proxy; fleet_kill controlplane; fleet_kill controller
sleep 0.4
cleanup() {
  pkill -CONT -f 'flint-[c]ontrolplane' 2>/dev/null
  $CTL -f "$INV" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill proxy; fleet_kill controlplane; fleet_kill controller
  rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:7495
pair 127.0.0.1:7490,127.0.0.1:7491
pair 127.0.0.1:7492,127.0.0.1:7493
controller on
poll-ms 100
confirm 3
lease-ttl-ms $LEASE
EOF

echo "== bootstrap (CP, 2 pairs, controller; lease=${LEASE}ms max-stale=${MAX_STALE_MS}ms)"
$CTL -f "$INV" bootstrap >"$D/bootstrap.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -20 "$D/bootstrap.log" | sed 's/^/  | /'; exit 1; }
$CTL -f "$INV" verify >"$D/verify.log" 2>&1 \
  || { echo "FAIL: fleet did not verify before the test"; tail -15 "$D/verify.log" | sed 's/^/  | /'; exit 1; }

st()       { $CTL -f "$INV" status 2>/dev/null; }
# $2 pair index, $3 addr, $4 role, $10 seq_lag, $12 live_replicas
masters()  { st | awk '$1=="pair" && $4=="master"' | wc -l | tr -d ' '; }
master_of(){ st | awk -v p="$1" '$1=="pair" && $2==p && $4=="master"{print $3; exit}'; }
other_of() { st | awk -v p="$1" -v m="$2" '$1=="pair" && $2==p && $3!=m {print $3; exit}'; }
pairs()    { st | grep -E '^pair|^controller' | sed 's/^/    /'; }
clog()     { ls -t "$D"/state/logs/*ontroller* 2>/dev/null | head -1; }

for _ in $(seq 1 60); do [ "$(masters)" = 2 ] && break; sleep 0.5; done
[ "$(masters)" = 2 ] || { echo "FAIL: expected 2 masters before the test"; pairs; exit 1; }

M1=$(master_of 1); R1=$(other_of 1 "$M1")
[ -n "$M1" ] && [ -n "$R1" ] || { echo "FAIL: could not identify pair 1's members"; pairs; exit 1; }
echo "  baseline: 2/2 masters; pair 1 master $M1, replica $R1"

# MAKE PAIR 1 A BYSTANDER. Killing its replica leaves the master with
# live_replicas 0, so `legit_converged` goes false and `last_converged` STOPS
# ADVANCING for that pair — the drift that happens on a real fleet to a pair
# nobody is testing. Pair 0 is untouched and keeps converging.
echo "== pair 1 loses its replica (its last_converged now freezes)"
$CTL -f "$INV" kill-node "$R1" >/dev/null 2>&1 || { echo "FAIL: could not kill $R1"; exit 1; }
# Prove the premise rather than assume it: the master must actually report no
# live replica, or the pair is still converging and the test is vacuous.
WIDOWED=""
for _ in $(seq 1 40); do
  LR=$(st | awk -v m="$M1" '$1=="pair" && $3==m {print $12; exit}')
  [ "${LR:-1}" = 0 ] && { WIDOWED=1; break; }
  sleep 0.5
done
[ -n "$WIDOWED" ] || { echo "FAIL: pair 1's master still reports a live replica — it is not a bystander"; pairs; exit 1; }
echo "  pair 1 master reports live_replicas 0"

# Let its convergence go stale by MORE than max-stale, which is what arms the
# degraded-window gate for that pair.
SLEEP_S=$(( MAX_STALE_MS / 1000 + 3 ))
echo "== waiting ${SLEEP_S}s so pair 1's last_converged goes stale (> ${MAX_STALE_MS}ms)"
sleep "$SLEEP_S"

# The fence trigger (ADR-0018): masters renew their own lease at the CP, so
# stopping the CP past the TTL fences every serving master — while the
# CONTROLLER keeps running and keeps observing, which is truer to run 11
# than the old controller-stall trigger anyway (there the controller was
# alive and refusing, not absent). Bracketed pattern for the same
# self-match reason as controller_stall_drill.
CPPID=$(pgrep -f 'flint-[c]ontrolplane' | head -1)
[ -n "$CPPID" ] || { echo "FAIL: no control-plane process to stall"; exit 1; }
NCP=$(pgrep -f 'flint-[c]ontrolplane' | wc -l | tr -cd '0-9')
[ "${NCP:-0}" = 1 ] || { echo "FAIL: expected exactly 1 CP seat, found ${NCP:-0}"; pgrep -fl 'flint-[c]ontrolplane' | sed 's/^/    /'; exit 1; }

echo "== SIGSTOP the CP (pid $CPPID) for ${STALL_S}s — lease is ${LEASE}ms"
kill -STOP "$CPPID"
sleep 0.5
CSTAT=$(ps -o stat= -p "$CPPID" 2>/dev/null | tr -d ' ')
case "$CSTAT" in
  T*) echo "  CP is stopped (state $CSTAT)" ;;
  *)  echo "FAIL: CP pid $CPPID is in state '${CSTAT:-gone}', not stopped"; exit 1 ;;
esac

# POSITIVE CONTROL: both masters must actually self-fence, or the recovery
# assertion below would pass without ever testing recovery.
FENCED=0
for _ in $(seq 1 "$STALL_S"); do
  [ "$(masters)" = 0 ] && { FENCED=1; break; }
  sleep 1
done
[ "$FENCED" = 1 ] || {
  echo "FAIL: the fleet never self-fenced during a ${STALL_S}s CP stall (lease=${LEASE}ms) —"
  echo "      without the fence this drill asserts nothing."
  pairs; exit 1
}
echo "  fenced as designed: 0/2 masters while the CP is stalled"

echo "== SIGCONT — BOTH pairs must recover, including the replica-less one"
kill -CONT "$CPPID"
HEAL=""
for i in $(seq 1 45); do
  [ "$(masters)" = 2 ] && { HEAL=$i; break; }
  sleep 1
done
if [ -z "$HEAL" ]; then
  echo "FAIL: 45s after the CP resumed only $(masters)/2 pairs have a master."
  echo "      This is #171: the pair whose master self-fenced with NO replica cannot"
  echo "      satisfy insync_lineage_holder (live_replicas 0, seq_lag none), so the"
  echo "      degraded-window gate refuses it forever — while the node is alive and"
  echo "      holds the pair's whole dataset at the top epoch."
  pairs
  L=$(clog); [ -n "$L" ] && { echo "  --- controller log (tail 25) ---"; tail -25 "$L" | sed 's/^/  | /'; }
  exit 1
fi
echo "  both pairs recovered ${HEAL}s after the CP resumed"
pairs

# The healthy pair must be whole again; the bystander legitimately has one copy
# until its replica is restarted, so `verify` is checked only after re-attaching.
$CTL -f "$INV" restart-node "$R1" >/dev/null 2>&1 \
  || { echo "FAIL: could not restart $R1 as a replica"; pairs; exit 1; }
OK=""
for _ in $(seq 1 60); do $CTL -f "$INV" verify >/dev/null 2>&1 && { OK=1; break; }; sleep 1; done
[ -n "$OK" ] || { echo "FAIL: fleet does not verify after recovery"; $CTL -f "$INV" verify 2>&1 | tail -12 | sed 's/^/  /'; exit 1; }

echo "PASS: a self-fenced master with NO replica is recovered, not abandoned; the healthy pair recovers too, and the fleet verifies (#171)"
