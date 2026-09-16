#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# DOES A TENANT'S CONFIGURATION SURVIVE A CONTROL-PLANE RESTART? (BUG-0152)
#
# The single-node control plane persists through a hand-written line format in
# `state.rs`. Its tenant record carries ten fields; `Tenant` has twelve. The two
# it omits are `federated` and `async_writes`, and the loader hard-codes both to
# `false` with a comment saying the JSON state carries them -- which is true of
# the RAFT path's serde format and of nothing this path ever writes.
#
# So `CPTENANTASYNC <name> on` and `CPTENANTFEDERATE <name> on` hold until the
# control plane restarts and then silently revert, and the CP pushes the
# reverted configuration to the proxies. A tenant that asked for asynchronous
# writes gets synchronous ones back, with no error anywhere.
#
# THE SHAPE OF THE OMISSION IS WHY NOTHING CAUGHT IT. Every verb works, every
# read-back within one process agrees, and the drills that exercise these flags
# never restart the control plane. Persistence is not a property of a verb; it
# is a property of a verb AND a restart, and only the pair can be tested.
#
# CP-LEVEL, no servers: the flags live in the control plane's own record, and
# what is under test is whether writing it and reading it back agree.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-cprestart-state 6512
fleet_guard
fleet_kill controlplane; sleep 0.4
CP=./target/release/flint-controlplane
STATE=$FLINT_DRILL_ROOT/flint-cprestart-state
CPPORT=6512
cleanup() { fleet_kill controlplane; rm -rf "$STATE" "$STATE.tmp"; }
trap cleanup EXIT
rm -rf "$STATE"

start_cp() {
  $CP --port $CPPORT --state "$STATE" >>"$FLINT_DRILL_ROOT/flint-cprestart-cp.log" 2>&1 &
  fleet_wait_listen $CPPORT
  sleep 0.4
}
flag() {  # flag <name> -- read one field out of CPMYSTATUS
  valkey-cli -p $CPPORT CPMYSTATUS tok-acme 2>&1 | tr -d '\r' \
    | sed -n "s/^$1://p"
}

echo "== a tenant, with three flags deliberately ON and one deliberately OFF"
start_cp
fleet_cp $CPPORT CPADDTENANT acme tok-acme acme 1
fleet_cp $CPPORT CPTENANTASYNC acme on
fleet_cp $CPPORT CPTENANTFEDERATE acme on
fleet_cp $CPPORT CPTENANTREADS acme on
# local_cache is left OFF on purpose. Every assertion below is "this flag is
# still 1", and an instrument that reported 1 for everything would satisfy all
# of them -- so one flag must come back 0 or the drill proves nothing.

for f in async_writes federated replica_reads; do
  v=$(flag $f)
  [ "$v" = "1" ] || { echo "FAIL: $f did not take at all ($f=$v) -- the drill never"
                      echo "      reached the state it is about"; exit 1; }
done
[ "$(flag local_cache)" = "0" ] || {
  echo "FAIL: local_cache reads 1 and was never set -- CPMYSTATUS is reporting"
  echo "      flags rather than reading them, and every check below is vacuous"
  exit 1; }
echo "  async_writes=1 federated=1 replica_reads=1 local_cache=0"

echo "== restart the control plane on the same state file"
fleet_kill controlplane; sleep 0.6
start_cp

BAD=""
for f in async_writes federated replica_reads; do
  v=$(flag $f)
  [ "$v" = "1" ] || BAD="$BAD $f=$v"
done
LC=$(flag local_cache)

if [ -n "$BAD" ]; then
  echo "FAIL: a tenant's configuration did not survive a control-plane restart:$BAD"
  echo "      (each of these read 1 before the restart)"
  echo "      The persisted record is:"
  grep '^tenant ' "$STATE" | sed 's/^/        /'
  echo "      A field absent from that line is a field the loader defaults,"
  echo "      and the default is OFF. BUG-0152."
  exit 1
fi
[ "$LC" = "0" ] || {
  echo "FAIL: local_cache came back 1 having never been set ($LC) -- a restart"
  echo "      must not invent configuration either"; exit 1; }

echo "PASS: cp restart tenant flags — every flag a tenant set survives a control-plane restart, and one deliberately left off stays off"
