#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# How long does the EDGE take to route around an UNPLANNED master kill?
#
# WHY THIS IS NOT promote_notice_drill.sh. That one measures the PLANNED
# handoff: the old master is demoted in place, stays up, and answers
# -READONLY, so the proxy has a live peer telling it to look again. This one
# kills the master outright. The proxy's next write hits a dead socket, and
# recovery depends on a different path entirely — the CP's promotion hint
# reaching it, or failing that its reactive re-probe. Nothing in the suite
# timed that path, which is how soak run 26 could spend 43850ms unroutable
# after a promotion that finished in 597ms and have every local drill stay
# green (#187).
#
# WHAT IT ASSERTS
#
#   1. After the kill, the edge serves writes again within the published RTO
#      budget (docs/slo.md, 10s). This is the gate.
#   2. The proxy SAYS how it got there. Since #187 the proxy logs the
#      promotion hint on arrival and logs any re-probe that actually moves a
#      pair's master, so a pass names its mechanism instead of leaving
#      "hint arrived and worked" and "hint never arrived, the reactive path
#      saved us slowly" indistinguishable — the ambiguity that made run 26's
#      44s unattributable from its evidence bundle.
#
# THE NEGATIVE CONTROL. Run B kills the master with the controller STOPPED,
# so nothing promotes and nothing can tell the proxy anything. The edge must
# then stay unwritable. Without this arm the drill would pass just as happily
# against a proxy that got fast for an unrelated reason, and would keep
# passing if the notice were deleted — #121's lesson, which this suite has
# now paid for twice.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-reroute 6398 6399 6400 6401
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-reroute; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy; fleet_kill controller; fleet_kill controlplane
sleep 0.3
cleanup() { fleet_kill server; fleet_kill proxy; fleet_kill controller; fleet_kill controlplane; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy -p flint-controller \
  -p flint-controlplane --features flint-server/rocks || { echo "FAIL: build"; exit 1; }
# The first exec of a freshly linked binary can spend 20s in the loader; pay
# it here rather than inside a timed window (see the warm-up in gates.sh).
fleet_warm ./target/release/flint-server ./target/release/flint-proxy ./target/release/flint-controlplane ./target/release/flint-controller

RTO_BUDGET_MS=10000

echo "== control plane + pair + proxy"
$B --port 6398 --engine rocks --data-dir "$D/a" >"$D/a.log" 2>&1 &
$B --port 6399 --engine rocks --data-dir "$D/b" --replica-of 127.0.0.1:6398 >"$D/b.log" 2>&1 &
fleet_wait_listen 6398 6399
sleep 0.8
./target/release/flint-controlplane --port 6400 --state "$D/cp" >"$D/cp.log" 2>&1 &
fleet_wait_listen 6400
sleep 0.5
fleet_cp 6400 CPADDPROXY 127.0.0.1:6401
fleet_cp 6400 CPADDPAIR 127.0.0.1:6398,127.0.0.1:6399
fleet_cp 6400 CPADDTENANT acme tok-acme acme 1
./target/release/flint-proxy --port 6401 --control-plane 127.0.0.1:6400 \
   --advertise 127.0.0.1:6401 >"$D/proxy.log" 2>&1 &
fleet_wait_listen 6401
sleep 1.5

CLI="-p 6401 -a tok-acme --no-auth-warning"
# Prove the edge writes BEFORE the kill. A drill that starts measuring
# without this can report a fast recovery from a fleet that never worked.
# shellcheck disable=SC2086
[ "$(valkey-cli $CLI SET reroute:pre ok 2>/dev/null)" = "OK" ] || {
  echo "FAIL: the edge could not write before any kill — nothing to measure"
  tail -5 "$D/proxy.log"; exit 1
}
echo "  edge writable through the proxy"

# How long until the edge takes a write again. Polled, not slept: the whole
# point is the duration.
time_to_writable() {
  local t0 now
  t0=$(python3 -c 'import time; print(int(time.time()*1000))')
  while :; do
    # shellcheck disable=SC2086
    if valkey-cli $CLI SET "$1" ok 2>/dev/null | grep -q OK; then
      now=$(python3 -c 'import time; print(int(time.time()*1000))')
      echo $(( now - t0 )); return 0
    fi
    now=$(python3 -c 'import time; print(int(time.time()*1000))')
    [ $(( now - t0 )) -gt "$2" ] && { echo $(( now - t0 )); return 1; }
    sleep 0.05
  done
}

echo "== arm B (negative control): kill the master with NO controller running"
MASTER=$(valkey-cli -p 6398 FLINTINFO 2>/dev/null | tr '\r' '\n' | grep -q '^role:master' && echo 6398 || echo 6399)
kill -9 "$(pgrep -f "flint-server --port $MASTER" | head -1)" 2>/dev/null
if MS=$(time_to_writable reroute:b 4000); then
  echo "FAIL: the edge became writable in ${MS}ms with no controller — nothing"
  echo "      promoted, so a write CANNOT have been served correctly. Either the"
  echo "      proxy routed to a replica, or a stale master answered."
  exit 1
fi
echo "  edge stayed unwritable for ${MS}ms with nothing to promote (as it must)"

echo "== restore the pair, start the controller, and kill the master again"
# Comes back as a REPLICA of whoever holds the lineage now: the ex-master
# wipe contract (#107) — a node that was killed while master must not
# re-enter claiming the role.
OTHER=$([ "$MASTER" = 6398 ] && echo 6399 || echo 6398)
$B --port "$MASTER" --engine rocks --data-dir "$D/$([ "$MASTER" = 6398 ] && echo a || echo b)" \
   --replica-of "127.0.0.1:$OTHER" >>"$D/restart.log" 2>&1 &
fleet_wait_listen "$MASTER"
sleep 1
./target/release/flint-controller --pairs "127.0.0.1:6398,127.0.0.1:6399" \
   --commit-cp 127.0.0.1:6400 --id R >"$D/ctl.log" 2>&1 &
sleep 3   # not readiness: let the controller observe convergence and arm

NOW_MASTER=6398
valkey-cli -p 6398 FLINTINFO 2>/dev/null | tr '\r' '\n' | grep -q '^role:master' || NOW_MASTER=6399
echo "  master is :$NOW_MASTER, controller armed"

echo "== arm A: kill the master, time the edge back to writable"
kill -9 "$(pgrep -f "flint-server --port $NOW_MASTER" | head -1)" 2>/dev/null
if MS=$(time_to_writable reroute:a $(( RTO_BUDGET_MS * 3 ))); then
  echo "  edge writable again after ${MS}ms (budget ${RTO_BUDGET_MS}ms)"
else
  echo "FAIL: the edge never took a write within $(( RTO_BUDGET_MS * 3 ))ms of the kill."
  echo "      Promotion and ROUTING are separate legs — check the controller log for"
  echo "      the promotion instant, then proxy.log for whether the hint arrived:"
  grep -E "promotion hint|master .* ->" "$D/proxy.log" | tail -5 | sed 's/^/        /'
  tail -3 "$D/ctl.log" | sed 's/^/        ctl: /'
  exit 1
fi

# The gate.
[ "$MS" -le "$RTO_BUDGET_MS" ] || {
  echo "FAIL: ${MS}ms exceeds the ${RTO_BUDGET_MS}ms budget (docs/slo.md)."
  echo "      This is the #187 shape: check whether promotion was fast and only"
  echo "      ROUTING was slow — the proxy lines below say which."
  grep -E "promotion hint|master .* ->" "$D/proxy.log" | tail -5 | sed 's/^/        /'
  exit 1
}

echo "== the proxy names the mechanism it recovered by"
HINT=$(grep -c "promotion hint" "$D/proxy.log" 2>/dev/null || echo 0)
MOVED=$(grep -cE "master .* -> " "$D/proxy.log" 2>/dev/null || echo 0)
echo "  promotion hints applied: $HINT; re-probes that moved a master: $MOVED"
[ "$MOVED" -ge 1 ] || {
  echo "FAIL: the edge recovered but the proxy never logged a master moving."
  echo "      Then it did not re-route — it is serving from somewhere unexpected,"
  echo "      and the ${MS}ms above is not measuring what this drill claims."
  exit 1
}
[ "$HINT" -ge 1 ] || {
  echo "NOTE: recovery happened WITHOUT a promotion hint, i.e. via the reactive"
  echo "      re-probe alone. That is the slow path; it passed the budget here on"
  echo "      loopback and is exactly what would not scale (#187)."
}

echo "PASS: edge re-route drill — after an unplanned master kill the edge took a write again in ${MS}ms (budget ${RTO_BUDGET_MS}ms), $HINT hint(s) applied, $MOVED master move(s) logged; with no controller the edge correctly stayed unwritable"
