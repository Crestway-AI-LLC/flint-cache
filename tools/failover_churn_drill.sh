#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Repeated failover must keep promoting, on a fleet shaped like the scale test (#171).
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
# exactly why the #168 work did not catch it.
#
# WHY THIS DRILL HAS MULTIPLE PAIRS AND WRITERS. The first version had one pair
# and no load, and it passed 14/14 sub-second — a NEGATIVE result that ruled out
# "repeated failover on one pair" as the trigger and left the differences from
# the scale fleet as the remaining suspects. Two of those are reproducible on a
# laptop: the controller's decision loop is SERIAL over every pair with an
# 800ms probe timeout per node, so more pairs mean more probes per sweep, and
# the scale run keeps a writer hammering every pair while only the pair under
# test is parked. If the sweep stretches, `confirm` rounds of it stretch with
# it, and the harness's fixed 30s budget stops being generous. The third
# difference — a real network under every probe — cannot be had here, and that
# is stated rather than papered over.
#
# MEASURED, and it is a second negative. At the full four-pair shape with four
# writers proven to be landing ~60k writes/s (691_753 keys live at the end),
# 16/16 promoted: worst 397ms, mean 331ms, against a 30_000ms budget. Two pairs:
# worst 332ms, mean 298ms. That is ~75x headroom with no drift across either
# shape, and ~330ms is simply confirm(3) x poll(100ms) plus change — exactly the
# designed detection time. Neither pair count nor write load reproduces #171 on
# loopback, which leaves the real network as the remaining difference and makes
# the instrumented AWS run (flint 438e116, flint-cache 483e20a) the next step
# rather than more local guessing.
#
# So this measures PROMOTION LATENCY IN MILLISECONDS across many failovers and
# reports the worst. A clean pass is not the only useful outcome: latency
# drifting toward the budget is the same finding arriving early, and a bare
# PASS/FAIL would hide it.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
# Ports are listed LITERALLY, not computed: tools/gates.sh's
# assert_no_port_overlap greps the fleet_init line out of this file, so a
# variable expansion here would silently exempt this drill from the one check
# that stops two drills fighting over a port.
fleet_init /tmp/flint-churn 7380 7381 7382 7383 7384 7385 7386 7387 7388 7389
fleet_guard
D=/tmp/flint-churn; INV=$D/cluster.flint
CTL=./target/release/flintctl
ITERS="${FLINT_CHURN_ITERS:-16}"
# The budget flint-chaos gives the controller. Kept identical on purpose: this
# drill asks the same question the scale run asked, so a different budget would
# make a pass here compatible with a failure there.
PROMOTE_S="${FLINT_CHURN_PROMOTE_S:-30}"
# Writers are the point of the multi-pair shape; the knob exists to isolate
# their contribution when a failure does show up, not to make passing easier.
WRITERS="${FLINT_CHURN_WRITERS:-4}"
# HOW MANY PAIRS, and why this is a knob rather than a constant.
#
# Four is the scale fleet's shape and the number this drill exists to imitate.
# It is also EIGHT RocksDB seats plus a proxy, a CP and a controller on one
# laptop, and bootstrap starts them serially: measured here, the proxy came up
# 18s after the CP, and flintctl allows a seat a fixed 10s to answer before it
# gives up. On a machine already busy compiling, that window is genuinely tight
# and bootstrap fails for reasons that have nothing to do with failover — a
# flaky gate that cries wolf is worse than a narrower one that does not.
#
# So the GATE runs two pairs, which still exercises a multi-pair sweep, and
# the four-pair scale shape is one env var away for the investigation itself.
NPAIRS="${FLINT_CHURN_PAIRS:-2}"
case "$NPAIRS" in 1|2|3|4) ;; *) echo "FAIL: FLINT_CHURN_PAIRS must be 1-4 (ports are declared for 4)"; exit 2 ;; esac
PROXY=7388

fleet_kill server; fleet_kill proxy; fleet_kill controlplane; fleet_kill controller
sleep 0.4
WPIDS=()
stop_writers() { local p; for p in ${WPIDS[@]+"${WPIDS[@]}"}; do kill -CONT "$p" 2>/dev/null; kill -TERM "$p" 2>/dev/null; done; WPIDS=(); }
cleanup() {
  stop_writers
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
# the detection window IS the thing under test.
{
  echo "disposable on"
  echo "statedir $D/state"
  echo "bins ./target/release"
  echo "tls on"
  echo "cp 127.0.0.1:7389"
  # Pairs are emitted, not hard-coded, so NPAIRS actually changes the fleet.
  # The ports themselves still appear literally in the fleet_init line above,
  # which is what assert_no_port_overlap reads.
  for ((p = 0; p < NPAIRS; p++)); do
    echo "pair 127.0.0.1:$((7380 + p * 2)),127.0.0.1:$((7381 + p * 2))"
  done
  echo "proxy 127.0.0.1:$PROXY"
  echo "controller on"
  echo "poll-ms 100"
  echo "confirm 3"
  echo "lease-ttl-ms 4000"
} > "$INV"

echo "== bootstrap (CP, $NPAIRS pair(s), proxy, controller; poll-ms 100 confirm 3 lease 4000)"
$CTL -f "$INV" bootstrap >"$D/bootstrap.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -20 "$D/bootstrap.log" | sed 's/^/  | /'; exit 1; }
$CTL -f "$INV" verify >"$D/verify.log" 2>&1 \
  || { echo "FAIL: fleet did not verify before the churn"; tail -15 "$D/verify.log" | sed 's/^/  | /'; exit 1; }
$CTL -f "$INV" tenant add churn tok-churn churn 1 >/dev/null 2>&1 \
  || { echo "FAIL: could not create the tenant"; exit 1; }

# `flintctl status` prints one row per node and already dials every seat over
# the mesh TLS:
#   pair 0    127.0.0.1:7380  master  epoch (0,1)  build X  seq_lag 0  live_replicas 1
# Parsed positionally: $2 pair index, $3 addr, $4 role, $10 seq_lag, $12 live_replicas.
# A DOWN node prints a short row that matches none of the field tests.
st()       { $CTL -f "$INV" status 2>/dev/null; }
role_of()  { st | awk -v a="$1" '$1=="pair" && $3==a {print $4}'; }
# Milliseconds, portably. macOS has no `date +%N` (it prints a literal N), which
# has silently invalidated a timing measurement in this project before.
now_ms()   { perl -MTime::HiRes=time -e 'printf "%.0f", time*1000'; }

dump() {
  echo "  --- flintctl status ---"; st 2>&1 | sed 's/^/  | /'
  local clog; clog=$(ls -t "$D"/state/logs/*ontroller* 2>/dev/null | head -1)
  [ -n "${clog:-}" ] \
    && { echo "  --- controller log (tail 60): $clog ---"; tail -60 "$clog" | sed 's/^/  | /'; } \
    || echo "  (no controller log under $D/state/logs)"
}

# WRITERS. Undirected random keys through the proxy edge, so every pair takes
# traffic; the drill parks ALL of them to establish convergence rather than
# tracking which pair each key hashes to. That is a deliberate simplification
# of what flint-chaos does (it parks only the pair under test), and it errs
# toward LESS load at the moment of the kill, not more — so a pass here is
# weaker evidence than a pass there, which is the safe direction for a drill
# whose job is to find a failure.
VAL=$(head -c 512 /dev/urandom | base64 | head -c 256)
start_writers() {
  local j
  for j in $(seq 1 "$WRITERS"); do
    valkey-benchmark -h 127.0.0.1 -p "$PROXY" -a tok-churn \
      -n 100000000 -r 500000 -c 8 -P 4 -q \
      SET "w$j:__rand_int__" "$VAL" >/dev/null 2>&1 &
    WPIDS+=($!)
    # Stop bash tracking it as a job, so tearing the writers down at the end
    # does not spray "Terminated: 15" over the drill's own result lines.
    disown $! 2>/dev/null || true
  done
}
# A writer that dies on a failover blip stops being load exactly when the load
# matters. Restart any that exited, the way the scale harness had to learn to.
respawn_dead_writers() {
  local i alive=()
  for i in ${WPIDS[@]+"${WPIDS[@]}"}; do kill -0 "$i" 2>/dev/null && alive+=("$i"); done
  WPIDS=(${alive[@]+"${alive[@]}"})
  local missing=$(( WRITERS - ${#WPIDS[@]} ))
  [ "$missing" -gt 0 ] || return 0
  local j
  for j in $(seq 1 "$missing"); do
    valkey-benchmark -h 127.0.0.1 -p "$PROXY" -a tok-churn \
      -n 100000000 -r 500000 -c 8 -P 4 -q \
      SET "wr$j:__rand_int__" "$VAL" >/dev/null 2>&1 &
    WPIDS+=($!)
    disown $! 2>/dev/null || true
  done
}
park()   { local p; for p in ${WPIDS[@]+"${WPIDS[@]}"}; do kill -STOP "$p" 2>/dev/null; done; }
resume() { local p; for p in ${WPIDS[@]+"${WPIDS[@]}"}; do kill -CONT "$p" 2>/dev/null; done; }

if [ "$WRITERS" -gt 0 ]; then
  if ! command -v valkey-benchmark >/dev/null 2>&1; then
    echo "  NOTE: valkey-benchmark not on PATH — running WITHOUT write load."
    echo "        That is the 1-pair-shaped question again, not the scale-shaped one."
    WRITERS=0
  else
    start_writers
    # POSITIVE CONTROL. "with N writers" is a claim about the fleet being under
    # load, and it is invisible if it is false: a wrong token, a rejected
    # custom command, or a proxy that will not take the connection all leave
    # valkey-benchmark exiting instantly and respawn_dead_writers cheerfully
    # restarting the same failure, while the drill still prints PASS and the
    # headline says the promotions happened under load. So prove the writes
    # LAND before trusting anything measured afterwards.
    KW0=$(valkey-cli -h 127.0.0.1 -p "$PROXY" -a tok-churn --no-auth-warning DBSIZE 2>/dev/null | tr -cd '0-9')
    sleep 2
    KW1=$(valkey-cli -h 127.0.0.1 -p "$PROXY" -a tok-churn --no-auth-warning DBSIZE 2>/dev/null | tr -cd '0-9')
    [ "${KW1:-0}" -gt "${KW0:-0}" ] || {
      echo "FAIL: the writers are not writing — DBSIZE went ${KW0:-?} -> ${KW1:-?} in 2s."
      echo "      Everything below would be measured on an IDLE fleet while claiming load."
      pgrep -fl 'valkey-[b]enchmark' | sed 's/^/  proc| /'
      dump; exit 1
    }
    echo "  $WRITERS writer(s) hammering the edge at 127.0.0.1:$PROXY (DBSIZE ${KW0:-0} -> ${KW1:-0} in 2s)"
  fi
fi

# ALL pairs converged: every pair must have a master with a live, caught-up
# replica. flint-chaos asserts this per-pair before each kill (wait_healthy,
# cluster.rs:161) using the same predicate the controller uses for
# legit_converged (flint-controller main.rs:672). Asserting it here is what
# makes a refusal to promote a genuine finding rather than "killed a
# single-copy pair", which would be correct behaviour and not a bug.
all_healthy() {
  st | awk -v n="$NPAIRS" '$1=="pair" && $4=="master" && $10=="0" && $12+0>=1 {c++} END{exit c==n?0:1}'
}
wait_all_healthy() {
  local d=$((SECONDS+$1))
  while [ $SECONDS -lt $d ]; do all_healthy && return 0; sleep 0.3; done
  return 1
}

# Converge with the writers PARKED — under a live hammer seq_lag never settles
# at 0, which is the same reason flint-chaos parks its writer to arm the
# controller (main.rs:261-270).
park
wait_all_healthy 60 || { echo "FAIL: the 4 pairs never all converged before the churn began"; dump; exit 1; }
resume
echo "  baseline: all $NPAIRS pairs converged"

WORST=0; WORST_ITER=0; TOTAL=0
for i in $(seq 1 "$ITERS"); do
  [ "$WRITERS" -gt 0 ] && respawn_dead_writers
  park
  wait_all_healthy 90 || { echo "FAIL: iter $i: the pairs never all re-converged within 90s (writers parked)"; dump; exit 1; }
  # Round-robin the pair under test, so every pair takes the same number of
  # kills. The scale run picks at random and only reached a fourth kill on one
  # pair by luck; a rotation makes the coverage a property of the drill.
  P=$(( (i - 1) % NPAIRS ))
  M=$(st | awk -v p="$P" '$1=="pair" && $2==p && $4=="master"{print $3; exit}')
  S=$(st | awk -v p="$P" -v m="$M" '$1=="pair" && $2==p && $3!=m {print $3; exit}')
  [ -n "$M" ] && [ -n "$S" ] || { echo "FAIL: iter $i: could not identify master/survivor for pair $P"; dump; exit 1; }
  # Resume BEFORE the kill and give the hammer a beat, so writes are genuinely
  # in flight when the master dies — the harness's ordering, and the condition
  # under which the controller has to make its decision on a busy fleet.
  resume
  sleep 0.3

  # ORDER MATTERS AND IS COPIED, NOT CHOSEN. Attached::kill waits for the
  # promotion INSIDE kill(), and flint-chaos only restarts the dead seat once
  # that returns — so the window under test has the killed seat still DOWN and
  # the survivor alone, reporting live_replicas 0 and seq_lag none. Restarting
  # first is not merely different: restart-node refuses to re-attach a node as
  # a replica of itself while its pair has no new master.
  $CTL -f "$INV" kill-node "$M" >/dev/null 2>&1 \
    || { echo "FAIL: iter $i: could not kill $M"; dump; exit 1; }

  T0=$(now_ms); OK=0
  while [ $(( ($(now_ms) - T0) / 1000 )) -lt "$PROMOTE_S" ]; do
    [ "$(role_of "$S")" = master ] && { OK=1; break; }
    sleep 0.1
  done
  TOOK=$(( $(now_ms) - T0 ))
  [ "$OK" = 1 ] || {
    echo "FAIL: iter $i: controller did not promote $S (pair $P) within ${PROMOTE_S}s after killing $M"
    echo "      This is #171 — the same failure the 5 TB run hit at iteration 11."
    dump
    exit 1
  }
  [ "$TOOK" -gt "$WORST" ] && { WORST=$TOOK; WORST_ITER=$i; }
  TOTAL=$(( TOTAL + TOOK ))
  echo "  iter $i: pair $P: killed $M, $S promoted in ${TOOK}ms"

  $CTL -f "$INV" restart-node "$M" >/dev/null 2>&1 \
    || { echo "FAIL: iter $i: could not restart $M as a replica of $S"; dump; exit 1; }
done

# How much was actually written across the whole churn — the other half of the
# positive control, and the number that says whether the load was a trickle or
# a hammer.
KWEND=$(valkey-cli -h 127.0.0.1 -p "$PROXY" -a tok-churn --no-auth-warning DBSIZE 2>/dev/null | tr -cd '0-9')
stop_writers
for _ in $(seq 1 30); do $CTL -f "$INV" verify >/dev/null 2>&1 && break; sleep 1; done
$CTL -f "$INV" verify >"$D/verify-after.log" 2>&1 \
  || { echo "FAIL: fleet does not verify after $ITERS failovers"; tail -20 "$D/verify-after.log" | sed 's/^/  | /'; dump; exit 1; }

# The budget is reported next to the worst case ON PURPOSE. The failure this
# drill hunts is a promotion that outruns a fixed 30s budget, so "how much
# headroom is left" is the number that says whether a pass is comfortable or
# lucky.
echo "PASS: $ITERS failovers across $NPAIRS pairs with $WRITERS writer(s), every one promoted"
echo "      worst ${WORST}ms (iter ${WORST_ITER}), mean $(( TOTAL / ITERS ))ms, budget $(( PROMOTE_S * 1000 ))ms"
echo "      ${KWEND:-0} keys live at the end — the load was real, not assumed (#171)"
