#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A master with no replica must not accept writes forever.
#
# THE HOLE THIS CLOSES. Every bound Flint publishes on failover loss runs
# through the lag cap, and the lag cap needs a replica to measure against:
# `ReplHub::lag_ms` returns None when none is live, and the write path's match
# falls straight through to "no backpressure". So the precise moment a master
# becomes the only copy of the data is the moment every bound switches off.
# Measured on a default pair before `--widowed-grace-ms` existed: freeze the
# replica and the master sheds 88 writes while the replica is still inside
# LIVENESS_WINDOW_MS, then accepts **539 more in ~4 s** with zero replicas and
# no throttling at all. Nothing in the shipping path set the one guard that
# would have stopped it (`min-replicas-to-write`, default 0, never emitted by
# render-inventory.sh because FLINT_MIN_REPLICAS is set nowhere).
#
# WHY A GRACE AND NOT min-replicas-to-write 1, which sheds at zero replicas
# and would also have closed it. That gate cannot tell "my peer died" from "I
# was promoted five milliseconds ago" — both are a master with no replica —
# so on a pair it freezes writes for the whole replacement full-sync on EVERY
# failover, trading the published RTO to buy the RPO. Verified directly: a
# master started with `--min-replicas-to-write 1` and no replica answers
# `-THROTTLED` to the very first write. The grace buys the same bound without
# that, because a normal promotion attaches a replacement far inside it.
#
# WHAT THIS DRILL ASSERTS, in the order the failure would matter:
#
#   1. inside the grace, a widowed master keeps serving   (RTO is not traded)
#   2. past the grace, it sheds                           (the bound is real)
#   3. a returning replica lifts the gate with no restart (self-healing)
#   4. a node with no peer configured is NEVER gated      (standalone is not
#      punished for redundancy it was never asked to have)
#
# (4) is the one most likely to regress silently: it is the difference between
# a safety gate and an outage on every single-node deployment.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
D=$FLINT_DRILL_ROOT/flint-widowed
MPORT=6465; RPORT=6466; SPORT=6467
fleet_init "$D" $MPORT $RPORT $SPORT
fleet_guard
fleet_kill server
sleep 0.3
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

BIN=./target/release/flint-server
cargo build --release -q -p flint-server --features rocks \
  || { echo "FAIL: build"; exit 1; }

# 2s grace: long enough to be visibly distinct from the 2s liveness window
# that precedes it, short enough that the drill stays quick.
GRACE=${FLINT_DRILL_GRACE_MS:-2000}

( $BIN --port $MPORT --engine rocks --data-dir "$D/m" --widowed-grace-ms $GRACE \
  >"$D/m.log" 2>&1 & )
fleet_wait_listen $MPORT || exit 1
( $BIN --port $RPORT --engine rocks --data-dir "$D/r" \
  --replica-of 127.0.0.1:$MPORT >"$D/r.log" 2>&1 & )
fleet_wait_listen $RPORT || exit 1

# Wait for the replica to actually ACK, not merely to listen. The grace is
# measured from the last ack, so starting the clock before one exists would
# test a different thing than intended.
for _ in $(seq 1 100); do
  [ "$(valkey-cli -p $MPORT FLINTINFO 2>/dev/null | sed -n 's/^live_replicas:\([0-9]*\).*/\1/p')" = "1" ] && break
  sleep 0.1
done
[ "$(valkey-cli -p $MPORT FLINTINFO 2>/dev/null | sed -n 's/^live_replicas:\([0-9]*\).*/\1/p')" = "1" ] \
  || { echo "FAIL: replica never acked, so the drill would prove nothing"; exit 1; }

echo "== healthy pair: writes flow"
[ "$(valkey-cli -p $MPORT SET w:healthy v 2>&1)" = "OK" ] \
  || { echo "FAIL: a healthy pair refused a write"; exit 1; }

echo "== freeze the replica (SIGSTOP): the master is now the only copy"
fleet_signal_port $RPORT -STOP || { echo "FAIL: could not freeze the replica"; exit 1; }

# (1) Inside the grace the master must still serve. Sampled right after the
# freeze, while the replica is still inside the liveness window.
R=$(valkey-cli -p $MPORT SET w:inside v 2>&1)
[ "$R" = "OK" ] || {
  echo "FAIL: widowed master refused a write INSIDE the grace: $R"
  echo "      that is the min-replicas-to-write behaviour this gate exists to avoid —"
  echo "      it would freeze every freshly promoted master until its replacement synced"
  exit 1
}
echo "  inside the grace: still serving"

# (2) Past it, the master must shed — and specifically with the widowed
# reason, not merely any -THROTTLED. A lag-cap shed here would mean the test
# passed for the wrong reason, since the lag cap cannot see a dead replica.
echo "== wait out the grace (${GRACE}ms) and keep writing"
deadline=$(( $(date +%s) + 15 ))
WIDOWED=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  case "$(valkey-cli -p $MPORT SET w:past v 2>&1)" in
    *widowed-grace*) WIDOWED=1; break ;;
  esac
  sleep 0.2
done
[ "$WIDOWED" = "1" ] || {
  echo "FAIL: the master never shed, so a widowed master still accepts writes"
  echo "      without limit — the bound docs/failover.md publishes is unenforced."
  valkey-cli -p $MPORT FLINTINFO 2>/dev/null | grep -E "^(live_replicas|widowed_grace_ms|widowed_shed):" | sed 's/^/      /'
  exit 1
}
echo "  past the grace: shed with -THROTTLED (widowed reason)"
[ "$(valkey-cli -p $MPORT FLINTINFO 2>/dev/null | sed -n 's/^widowed_shed:\([0-9]*\).*/\1/p')" = "1" ] \
  || { echo "FAIL: FLINTINFO does not report the gate as biting, so an operator cannot see why"; exit 1; }

# (3) Self-healing: the gate must lift on the replica's return, with no
# restart and no operator action. A gate that latches turns a transient stall
# into a permanent outage.
echo "== unfreeze the replica"
fleet_signal_port $RPORT -CONT || { echo "FAIL: could not resume the replica"; exit 1; }
LIFTED=0
deadline=$(( $(date +%s) + 15 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  if [ "$(valkey-cli -p $MPORT SET w:after v 2>&1)" = "OK" ]; then LIFTED=1; break; fi
  sleep 0.2
done
[ "$LIFTED" = "1" ] || { echo "FAIL: the gate latched — writes never resumed after the replica returned"; exit 1; }
echo "  gate lifted on its own, no restart"

# (4) A node with no peer must never be gated. flintctl only passes the flag
# to pair members, but the SERVER must also behave when it is absent: the
# default is 0 and 0 means off, forever.
echo "== standalone node (no --widowed-grace-ms): must never be gated"
( $BIN --port $SPORT --engine rocks --data-dir "$D/s" >"$D/s.log" 2>&1 & )
fleet_wait_listen $SPORT || exit 1
sleep $(( (GRACE / 1000) + 2 ))   # comfortably past what the grace would be
S=$(valkey-cli -p $SPORT SET solo v 2>&1)
[ "$S" = "OK" ] || {
  echo "FAIL: a standalone node was gated: $S"
  echo "      every single-node deployment would stop taking writes."
  exit 1
}
echo "  standalone still serving after $(( (GRACE / 1000) + 2 ))s alone"

echo "PASS: widowed grace drill — a master with no replica serves inside the grace, sheds past it, recovers on the replica's return, and a peerless node is never gated"
