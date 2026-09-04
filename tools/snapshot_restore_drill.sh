#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Snapshot + spare-attach drill (the M3 disaster-recovery item).
#   - the controller takes scheduled durable snapshots of the managed master
#     (FLINTSNAPSHOT -> <root>/<pair>/<id> + LATEST, consistent checkpoint)
#   - kill BOTH nodes (whole-pair loss — the case failover cannot answer and
#     the controller previously could only page about)
#   - the controller confirms the pair dark, then spawns a spare seeded from
#     the LATEST snapshot; it asserts mastership in a BUMPED GENERATION
#     ((0,c) -> (1,1)), fencing the entire dead lineage; the replica respawns
#     and full-syncs from it — the pair is whole again, hands-free
#   - pre-snapshot data survives; the old lineage cannot out-rank the new one
#     (a counter from generation 0 loses to (1,1) no matter how high)
#   - the fleet journal records SnapshotTaken and SpareRestored
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-snap 6870 6871 7590
fleet_guard
B=./target/release/flint-server
CP=./target/release/flint-controlplane
CTL=./target/release/flint-controller
D=$FLINT_DRILL_ROOT/flint-snap; rm -rf "$D"; mkdir -p "$D"
fleet_kill controller; fleet_kill server
fleet_kill controlplane; sleep 0.4
cleanup() {
  fleet_kill controller; fleet_kill server
  fleet_kill controlplane; rm -rf "$D"
}
trap cleanup EXIT

echo "== CP (journal) + controller managing the pair with a snapshot schedule"
$CP --port 7590 --state "$D/cp-state" 2>"${FLEET_SCOPE}cp.log" &
fleet_wait_listen 7590
sleep 0.5
FLINT_SERVER_BIN=$B $CTL --manage-slots "6870:$D/n0,6871:$D/n1" \
  --server-bin "$B" --id ctl --poll-ms 200 --confirm 3 \
  --snapshot-root "$D/snaps" --snapshot-interval-ms 1500 \
  --journal 127.0.0.1:7590 2>"$D/ctl.log" &
# Bootstrap is confirm-gated now (3 x 200ms) + spawn + sync.
#
# READY IS PONG *AND* NOT LOADING. #176 makes a node bind and answer PING from
# inside its load, deliberately, so a client can tell "starting" from
# "absent" — which means PONG stopped being the same event as "serving". This
# loop waited on PONG alone and the first write below came back
# `-LOADING Flint is loading the dataset in memory`, failing an assertion
# about snapshots that had nothing to do with snapshots.
#
# Fourth spelling of this in the suite, after PONG-alone (disk_pressure), a
# bind (disk_selffill) and a non-empty field (cold_start_roles). The two later
# waits in this drill are already correct because they test for a DECIDED
# value — role:master, live_replicas:1 — rather than for an answer arriving.
ready_6870() {
  [ "$(valkey-cli -p 6870 PING 2>/dev/null)" = "PONG" ] &&
    ! valkey-cli -p 6870 FLINTINFO 2>/dev/null | tr -d '\r' | grep -qx 'loading:1'
}
UP=0
for i in $(seq 1 50); do
  ready_6870 && { UP=1; break; }
  sleep 0.3
done
[ "$UP" = "1" ] || {
  # Say WHICH of the two it was. "never bootstrapped" reads as "nothing
  # started", and a node that is up and still loading is a different problem
  # with a different fix — that ambiguity cost three investigations on
  # disk_pressure before its diagnosis block was split the same way.
  echo "FAIL: managed pair never became ready"
  echo "      PING    -> $(valkey-cli -p 6870 PING 2>/dev/null || echo '<no answer>')"
  echo "      loading -> $(valkey-cli -p 6870 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^loading://p')"
  tail -5 "$D/ctl.log"; exit 1; }
echo "  pair bootstrapped (confirm-gated)"

echo "== write data, then wait for a snapshot that INCLUDES it"
for i in $(seq 1 50); do cli_ok valkey-cli -p 6870 SET "sk:$i" "sv:$i"; done
cli_ok valkey-cli -p 6870 SET golden pre-disaster
SNAP=""
for i in $(seq 1 40); do
  SNAP=$(cat "$D/snaps/g0/LATEST" 2>/dev/null || true)
  [ -n "$SNAP" ] && break
  sleep 0.5
done
[ -n "$SNAP" ] || { echo "FAIL: no snapshot taken"; tail -5 "$D/ctl.log"; exit 1; }
# Wait for one MORE snapshot so the writes above are definitely covered.
for i in $(seq 1 40); do
  NOW=$(cat "$D/snaps/g0/LATEST" 2>/dev/null || true)
  [ -n "$NOW" ] && [ "$NOW" != "$SNAP" ] && { SNAP=$NOW; break; }
  sleep 0.5
done
echo "  snapshot covering the data: $SNAP"

echo "== WHOLE-PAIR LOSS: kill both nodes"
pkill -9 -f "flint-server --port 6870"; pkill -9 -f "flint-server --port 6871"

echo "== controller restores a spare from LATEST; pair heals hands-free"
REST=0
for i in $(seq 1 60); do
  R=$(valkey-cli -p 6870 FLINTINFO 2>/dev/null | tr '\r' '\n' | grep "^role:" | cut -d: -f2)
  [ "$R" = "master" ] && { REST=1; break; }
  sleep 0.5
done
[ "$REST" = "1" ] || { echo "FAIL: spare never restored"; tail -8 "$D/ctl.log"; exit 1; }
[ "$(valkey-cli -p 6870 GET golden)" = "pre-disaster" ] || { echo "FAIL: snapshot data lost"; exit 1; }
[ "$(valkey-cli -p 6870 GET sk:25)" = "sv:25" ] || { echo "FAIL: bulk data lost"; exit 1; }
echo "  restored master serves pre-disaster data"

echo "== generation bump: restored lineage is (1,1); the old lineage is fenced forever"
EPOCH=$(valkey-cli -p 6870 FLINTINFO | tr '\r' '\n' | grep "^role_epoch:" | cut -d: -f2)
echo "  role_epoch: $EPOCH"
echo "$EPOCH" | grep -q "(1," || { echo "FAIL: no generation bump (got $EPOCH)"; exit 1; }
# Any claim from the dead generation-0 lineage — however high its counter —
# must be fenced by the restored line.
F=$(valkey-cli -p 6870 FLINTPROMOTE 0 9999 2>&1)
echo "$F" | grep -q "FENCED" || { echo "FAIL: generation-0 claim not fenced: $F"; exit 1; }
echo "  FLINTPROMOTE 0 9999 -> FENCED (old generation cannot out-rank the restored line)"

echo "== redundancy repair completes: replica respawns and converges"
CONV=0
for i in $(seq 1 60); do
  L=$(valkey-cli -p 6870 FLINTINFO 2>/dev/null | tr '\r' '\n' | grep "^live_replicas:" | cut -d: -f2)
  [ "$L" = "1" ] && { CONV=1; break; }
  sleep 0.5
done
[ "$CONV" = "1" ] || { echo "FAIL: replica never re-attached"; exit 1; }
echo "  pair whole again (live_replicas:1)"

echo "== fleet journal: SnapshotTaken + SpareRestored recorded"
J=$(valkey-cli -p 7590 CPJOURNALREAD 100)
echo "$J" | grep -q '"kind":"SnapshotTaken"' || { echo "FAIL: no SnapshotTaken in journal"; exit 1; }
echo "$J" | grep -q '"kind":"SpareRestored"' || { echo "FAIL: no SpareRestored in journal"; exit 1; }
echo "$J" | grep '"kind":"SpareRestored"' | grep -q '"epoch":"(1,1)"' \
  || { echo "FAIL: SpareRestored missing the bumped epoch"; exit 1; }
echo "  journal: SnapshotTaken + SpareRestored (1,1)"

echo "PASS: scheduled snapshots + spare attach — whole-pair loss healed hands-free, generation bump fences the dead lineage, journal records the story"
