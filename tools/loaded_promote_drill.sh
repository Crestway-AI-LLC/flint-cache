#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Does auto-failover still work when the pair is BUSY?
#
# WHY THIS EXISTS, and why controller_drill did not catch it. Every existing
# controller drill kills an IDLE master. Idle is the one regime where
# `seq_lag == 0` is true, and the promotion gate was an equality against
# exactly that. Under a continuous writer the target never stops moving:
# measured on a 5-host fleet (2026-08-15) the master's seq_lag sampled 82-151
# across ten consecutive 1s samples and was never once 0. So the controller
# never recorded a convergence, `last_insync` froze at the moment load began,
# and a master killed five seconds in left the pair REFUSING to promote its
# healthy in-sync replica — 137 consecutive refusal ticks, write-dead until a
# human ran `flintctl start` (#191).
#
# The suites all dodged it because their workloads are batchy: a lag-0 tick
# falls between iterations and the gate latches on that. A customer under
# steady ingestion gets no such gap.
#
# WHY --pipe AND NOT A SHELL WRITE LOOP. controller_drill's header records a
# removed `--load` knob and the reason: spawning `valkey-cli` per write tops
# out near a hundred writes a second, "nowhere near enough to leave the
# replica a backlog", and the measured difference was invisible. That verdict
# is about per-write PROCESS SPAWNS, not about load. One `valkey-cli --pipe`
# holds a single connection and streams, which is a different instrument
# entirely — and step 1 below refuses to continue unless it demonstrably
# moved seq_lag off zero, so this drill cannot repeat that mistake quietly.
#
# WHAT IT PINS
#   1. THE LOAD IS REAL. seq_lag observed non-zero while the writer runs. A
#      drill that skipped this would pass on an idle pair and prove nothing —
#      which is precisely how the bug survived every existing drill.
#   2. THE PAIR STILL PROMOTES. Kill the master mid-write and the controller
#      promotes the survivor within budget, with no operator.
#   3. IT NEVER REFUSES. The controller log must not contain the
#      degraded-window refusal. Promoting late is a latency bug; refusing is
#      a permanent outage, and only the second one made this a P0.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
# 6956/6957, NOT the 6460/6461 this drill was written with: main gave that
# block to proxy_chain after this branch diverged. Two drills on one port
# collide as a startup failure in whichever loses, which reads as a product
# defect in a drill that was merely unlucky.
fleet_init "$FLINT_DRILL_ROOT/flint-loadpromote" 6956 6957
fleet_guard

MPORT=6956
RPORT=6957
MDIR="$FLINT_DRILL_ROOT/flint-loadpromote/m"
RDIR="$FLINT_DRILL_ROOT/flint-loadpromote/r"
CTLLOG="$FLINT_DRILL_ROOT/flint-loadpromote-ctl.log"
B=./target/release/flint-server
# How long the controller may take to promote. Generous on purpose: the
# assertion that matters is "it promotes AT ALL", not the millisecond count
# (controller_drill owns the RTO number, measured idle with a real clock).
PROMOTE_BUDGET_S="${LOADPROMOTE_BUDGET_S:-25}"

fail() { echo "FAIL: $*"; exit 1; }
cleanup() {
  [ -n "${FEEDER:-}" ] && kill "$FEEDER" 2>/dev/null
  fleet_kill server
  fleet_kill controller
  rm -rf "$MDIR" "$RDIR"
}
trap cleanup EXIT
rm -rf "$MDIR" "$RDIR"; mkdir -p "$MDIR" "$RDIR"

cargo build --release -q -p flint-server -p flint-controller --features flint-server/rocks \
  || fail "build"
fleet_warm "$B" ./target/release/flint-controller

"$B" --port "$MPORT" --engine rocks --data-dir "$MDIR" >"$MDIR/log" 2>&1 &
fleet_wait_listen "$MPORT"
"$B" --port "$RPORT" --engine rocks --data-dir "$RDIR" \
  --replica-of "127.0.0.1:$MPORT" >"$RDIR/log" 2>&1 &
fleet_wait_listen "$RPORT"

info() { valkey-cli -p "$1" FLINTINFO 2>/dev/null | tr -d '\r' | sed -n "s/^$2://p"; }

# Wait for a healthy, ATTACHED start, so anything later is attributable.
CAUGHT=0
for _ in $(seq 1 60); do
  [ "$(info "$MPORT" live_replicas)" = "1" ] && { CAUGHT=1; break; }
  sleep 0.25
done
[ "$CAUGHT" = 1 ] || fail "replica never attached before the drill began"
echo "== pair up, replica attached"

echo "== 1. start a CONTINUOUS writer and prove it moves seq_lag off zero"
# One connection, streaming forever, until cleanup kills it. Values are small:
# the goal is a steady stream of SEQUENCES, not bytes on disk.
( while :; do
    python3 -c '
import sys
out = sys.stdout.buffer
for i in range(20000):
    k = b"load:%08d" % i
    out.write(b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$8\r\nvalue123\r\n" % (len(k), k))
' 2>/dev/null || break
  done | valkey-cli -p "$MPORT" --pipe ) >/dev/null 2>&1 &
FEEDER=$!

# THE POSITIVE CONTROL. Sample until we SEE a non-zero seq_lag; without this
# the drill would happily pass against an idle pair and assert nothing.
SAW_LAG=""
MAXLAG=0
for _ in $(seq 1 40); do
  L=$(info "$MPORT" seq_lag)
  case "$L" in
    ''|none|0) ;;
    *) SAW_LAG=$L; [ "$L" -gt "$MAXLAG" ] && MAXLAG=$L ;;
  esac
  sleep 0.25
done
[ -n "$SAW_LAG" ] || fail "seq_lag never left 0 — the writer is not loading the pair, so this drill would test nothing (is valkey-cli --pipe keeping up?)"
echo "  seq_lag observed non-zero under load (max seen ${MAXLAG}) — the regime is real"

echo "== 2. start the controller INTO that load"
# It must record a convergence despite seq_lag never being 0. Before #191 it
# could not, and everything below fails as a consequence.
./target/release/flint-controller --nodes "127.0.0.1:$MPORT,127.0.0.1:$RPORT" --id ctl \
  --poll-ms 150 --confirm 3 2>"$CTLLOG" &
sleep 3

echo "== 3. KILL the master mid-write; the controller must promote, not refuse"
pkill -9 -f "flint-server --port $MPORT"
PROMOTED=0
for _ in $(seq 1 $((PROMOTE_BUDGET_S * 4))); do
  [ "$(info "$RPORT" role)" = "master" ] && { PROMOTED=1; break; }
  sleep 0.25
done

# Check the refusal FIRST: it is the specific #191 signature and names the
# cause, where "did not promote" alone would send the next reader hunting.
if grep -q "REFUSING (degraded window" "$CTLLOG" 2>/dev/null; then
  echo "  controller log said:"
  grep -m2 "REFUSING (degraded window" "$CTLLOG" | sed 's/^/    /'
  fail "the controller REFUSED to promote a healthy in-sync replica under load (#191).
      Under a continuous writer seq_lag never reaches 0, so a convergence gate
      written as an equality never latches, last_insync stays empty or holds
      only the now-dead master, and the pair is write-dead until an operator
      intervenes. This is a permanent outage, not a slow failover."
fi
[ "$PROMOTED" = 1 ] || fail "no promotion within ${PROMOTE_BUDGET_S}s and no refusal logged either — look at $CTLLOG"
echo "  replica promoted to master under sustained write load"

echo "== 4. the promoted master serves writes"
[ "$(valkey-cli -p "$RPORT" SET after-promote v 2>&1 | tr -d '\r')" = "OK" ] \
  || fail "the promoted master will not take a write"

echo "PASS: loaded promote drill — with a writer holding seq_lag off zero the"
echo "      controller still observes convergence, promotes the survivor on"
echo "      master death, and never falls into the degraded-window refusal"
