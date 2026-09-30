#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0194, second half: does a full re-seed work whatever the master's
# temp_dir is, and does a master that cannot checkpoint SAY so?
#
# WHY THIS EXISTS. On the 2026-09-30 rc.78 soak a seat sent to a full re-seed
# never got one. Its master logged `full sync starting (1/2 slots in use)` 38
# times and `full sync served` never; the replica logged `full sync not ready
# (peer closed connection without sending TLS close_notify)` 37 times. The
# master made the checkpoint in temp_dir(), which on the AMI (AL2023) is a
# tmpfs capped at half the RAM, 8 GiB on an i4i.large, while the pair held
# ~20 GB. Off the database's filesystem RocksDB COPIES every file instead of
# linking it, so each attempt copied into memory until the tmpfs filled. Then
# a `?` dropped the connection and nothing was logged on either side.
#
# ARM 1 (the placement): the master runs with TMPDIR pointing at a directory
# that does not exist, so any checkpoint made there fails. A fresh replica
# must still seed, and the checkpoint directory must be empty afterwards.
# ARM 2 (the report): the master's checkpoint directory is made unusable (a
# FILE where it expects a directory). The master must log the failure, the
# replica must retry naming it, and it must seed once the obstacle is gone.
# ARM 3 (the sweep): a checkpoint left by a crashed run is removed at boot.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-fullsync-ckpt 6494 6495
fleet_guard
S=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-fullsync-ckpt; rm -rf "$D"; mkdir -p "$D"
fleet_kill server
sleep 0.3
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $*"; exit 1; }

cargo build --release -q -p flint-server --features rocks || fail "build"
fleet_warm "$S"

info() {  # info <port> <field>
  valkey-cli -p "$1" FLINTINFO 2>/dev/null | tr '\r' '\n' | sed -n "s/^$2://p"
}
wait_seq() {  # wait until node $1's last_applied reaches $2 (budget: 20s)
  for _ in $(seq 1 200); do
    LA=$(info "$1" last_applied)
    [ -n "$LA" ] && [ "$LA" -ge "$2" ] && return 0
    sleep 0.1
  done
  return 1
}
seeded() {  # seeded <log> <what>: the replica completed a full sync and converged
  fleet_wait_log "$1" "full sync complete" 60 \
    || { sed 's/^/    replica: /' "$1"; tail -8 "$D/m.log" | sed 's/^/    master: /'; fail "$2: the replica never seeded"; }
  wait_seq 6495 "$(info 6494 latest_seq)" || fail "$2: the replica never caught up"
  [ "$(valkey-cli -p 6494 DBSIZE)" = "$(valkey-cli -p 6495 DBSIZE)" ] \
    || fail "$2: keyspaces differ after the seed"
}
replica() {  # replica <log>: a FRESH replica of 6494
  kill -9 "$(pgrep -f "flint-server --port 6495" | head -1)" 2>/dev/null
  sleep 0.3; rm -rf "$D/r"
  $S --port 6495 --engine rocks --data-dir "$D/r" --replica-of 127.0.0.1:6494 >"$1" 2>&1 &
}

echo "== arm 1: master's TMPDIR cannot hold a checkpoint; a fresh replica must still seed"
TMPDIR="$D/no-such-dir" $S --port 6494 --engine rocks --data-dir "$D/m" >"$D/m.log" 2>&1 &
fleet_wait_ready 6494
for i in $(seq 1 2000); do printf 'SET k:%d v%d\n' "$i" "$i"; done | valkey-cli -p 6494 >/dev/null
replica "$D/r1.log"
seeded "$D/r1.log" "arm 1"
grep -q "full sync served" "$D/m.log" || fail "arm 1: the master never logged 'full sync served'"
LEFT=$(find "$D/m/flint-fullsync" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l | tr -d ' ')
[ "$LEFT" = 0 ] || fail "arm 1: $LEFT checkpoint(s) left in $D/m/flint-fullsync after the sync"
echo "  seeded with TMPDIR unusable; no checkpoint left behind"

echo "== arm 2: a master that cannot checkpoint says so, and the replica retries until it can"
rm -rf "$D/m/flint-fullsync"; : > "$D/m/flint-fullsync"
replica "$D/r2.log"
fleet_wait_log "$D/m.log" "full sync FAILED: could not checkpoint" 30 \
  || { tail -8 "$D/m.log" | sed 's/^/    master: /'; fail "arm 2: the master did not log the failed checkpoint"; }
fleet_wait_log "$D/r2.log" "full sync not ready (.*could not checkpoint" 30 \
  || { sed 's/^/    replica: /' "$D/r2.log"; fail "arm 2: the replica's retry does not name the cause"; }
rm -f "$D/m/flint-fullsync"
seeded "$D/r2.log" "arm 2"
echo "  failure logged on the master, named in the replica's retry; seeded once it cleared"

echo "== arm 3: a checkpoint left by a crashed run is swept at boot"
kill -9 "$(pgrep -f "flint-server --port 6494" | head -1)" 2>/dev/null
sleep 0.3
mkdir -p "$D/m/flint-fullsync/1-0-0" && echo x > "$D/m/flint-fullsync/1-0-0/000001.sst"
$S --port 6494 --engine rocks --data-dir "$D/m" >"$D/m2.log" 2>&1 &
fleet_wait_ready 6494
grep -q "a full sync checkpoint left by an earlier run" "$D/m2.log" \
  || { sed 's/^/    master: /' "$D/m2.log"; fail "arm 3: the boot did not report the sweep"; }
[ ! -e "$D/m/flint-fullsync" ] || fail "arm 3: $D/m/flint-fullsync survived the boot"
echo "  swept at boot"

echo "PASS: fullsync ckpt dir -- the re-seed checkpoints on the database's own filesystem, reports a failure on both ends, and sweeps leftovers"
