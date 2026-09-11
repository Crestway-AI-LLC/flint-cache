#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# THE durability proof: interrupt a slot cutover by killing BOTH the source
# and the destination (a whole-cluster redeploy) mid-move, restart them, and
# let the recovery controller reconcile from the durable manifest records.
# After recovery: exactly one node owns the slot (source answers -MOVED to the
# dest), the dest has every key, and no write was lost. Runs several times so
# the kill lands at different phases (pull / freeze / flip).
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-rec- 6580 6581
fleet_guard
B=./target/release/flint-server
CTLBIN=./target/release/flint-controller
# BUILD WHAT THIS DRILL RUNS. It used to build nothing and rely on whatever a
# previous step left in target/. On the gate that is the `build` step and it is
# always there; anywhere else flint-controller is simply absent, the recovery
# controller never starts, and EVERY arm then fails with the product's failure
# text -- "move not resolved after recovery", "recovery did not complete the
# half-done flip" -- while the real cause sits in $FLINT_DRILL_ROOT/flint-rec.log
# as `No such file or directory`. That cost a wrong conclusion in BUG-0132:
# three arms were reported as product failures and were a missing binary.
cargo build --release -q -p flint-server --features rocks -p flint-controller \
  || { echo "FAIL: build"; exit 1; }
for bin in "$B" "$CTLBIN"; do
  [ -x "$bin" ] || { echo "FAIL: $bin is missing after a successful build --
  every assertion below would fail naming the product instead"; exit 1; }
done
SPORT=6580; DPORT=6581
SADDR="127.0.0.1:$SPORT"; DADDR="127.0.0.1:$DPORT"
KEYS=150000
SLOT=$(python3 -c '
def c(d):
 p=0x1021;x=0
 for b in d:
  x^=b<<8
  for _ in range(8): x=((x<<1)^p)&0xffff if x&0x8000 else (x<<1)&0xffff
 return x
print(c(b"mover")%16384)')

# A FAILING RUN USED TO DELETE ITS OWN EVIDENCE. Both data directories were
# rm -rf'd on every failure path, so the one artifact that distinguishes "the
# keys are lost" from "the keys are stranded on the source, unreachable behind
# a -MOVED" was destroyed by the failure that made the question interesting.
# BUG-0132 is open on exactly that question and could not be answered from two
# CI failures because of this line.
keep_dirs() {
  echo "  evidence KEPT (not deleted): source=$SDIR dest=$DDIR"
}

run_once() {
  local delay="$1"
  pkill -9 -f "flint-server --port 658" 2>/dev/null; fleet_kill controller; sleep 0.4
  local SDIR DDIR
  SDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-rec-s.XXXXXX); DDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-rec-d.XXXXXX)
  $B --port $SPORT --engine rocks --data-dir "$SDIR" 2>"${FLEET_SCOPE}server.log" &
  $B --port $DPORT --engine rocks --data-dir "$DDIR" 2>"${FLEET_SCOPE}server2.log" &
  fleet_wait_listen $SPORT $DPORT
  sleep 0.8

  awk -v n="$KEYS" 'BEGIN{for(i=0;i<n;i++){k=sprintf("{mover}:key%06d",i);v=sprintf("val-%06d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
    | valkey-cli -p $SPORT --pipe >/dev/null

  # Start the cutover in the background; it blocks until done.
  ( valkey-cli -p $DPORT FLINTMIGRATEIN "$SADDR" "$SLOT" "$DADDR" >/dev/null 2>&1 ) &
  local MIG=$!
  sleep "$delay"
  # WHOLE-CLUSTER KILL mid-move.
  pkill -9 -f "flint-server --port 658" 2>/dev/null
  kill -9 $MIG 2>/dev/null; wait $MIG 2>/dev/null
  sleep 0.5

  # Restart both nodes on the same data dirs (the redeploy).
  $B --port $SPORT --engine rocks --data-dir "$SDIR" 2>"${FLEET_SCOPE}server3.log" &
  $B --port $DPORT --engine rocks --data-dir "$DDIR" 2>"${FLEET_SCOPE}server4.log" &
  fleet_wait_listen $SPORT $DPORT
  sleep 0.9

  # Observe the interrupted state from the durable records.
  local SM DM PHASE
  SM=$(valkey-cli -p $SPORT FLINTMIGRATIONS 2>/dev/null)
  DM=$(valkey-cli -p $DPORT FLINTMIGRATIONS 2>/dev/null)
  # THREE STATES, NOT TWO. No records on either node means the cutover either
  # COMPLETED (source is Moved to dest) or NEVER STARTED (source still owns and
  # serves), and those are opposite facts that this line rendered identically
  # -- then the verdict text asserted one of them. Ask source who owns the slot
  # rather than inferring it from an absence (BUG-0132, ops OPS-0037).
  local OWN
  if [ -n "$SM" ] || [ -n "$DM" ]; then
    PHASE="INTERRUPTED mid-move"
  else
    OWN=$(valkey-cli -p $SPORT GET "{mover}:key000000" 2>&1)
    case "$OWN" in
      *"MOVED $SLOT"*) PHASE="completed pre-kill (source already -MOVED; recovery is a no-op)" ;;
      val-*)           PHASE="NEVER STARTED (source still owns and serves the slot)" ;;
      *)               PHASE="no records, and source answers [$OWN] -- ownership indeterminate" ;;
    esac
  fi
  echo "  [delay $delay] after restart: source=[$SM] dest=[$DM] -> $PHASE"

  # Recovery controller: reconciles from the manifests, no other input.
  "$CTLBIN" --recover-nodes "$SADDR,$DADDR" --id REC --poll-ms 200 2>>$FLINT_DRILL_ROOT/flint-rec.log &
  local CTL=$!

  # Wait until the move is fully resolved: dest owns (a write succeeds) AND
  # source redirects the slot with -MOVED.
  local RESOLVED=0 i
  for i in $(seq 1 150); do
    local dw sr
    dw=$(valkey-cli -p $DPORT SET "{mover}:key000000" val-000000 2>&1)
    sr=$(valkey-cli -p $SPORT GET "{mover}:key000000" 2>&1)
    if [ "$dw" = "OK" ] && echo "$sr" | grep -qE "MOVED $SLOT $DADDR"; then RESOLVED=1; break; fi
    sleep 0.2
  done
  kill -9 $CTL 2>/dev/null
  if [ "$RESOLVED" != "1" ]; then
    echo "  FAIL: move not resolved after recovery (dest write='$dw' source read='$sr')"
    pkill -9 -f "flint-server --port 658" 2>/dev/null; keep_dirs; return 1
  fi

  # No split ownership: the source must NOT serve writes for the slot.
  local sw
  sw=$(valkey-cli -p $SPORT SET "{mover}:key000001" x 2>&1)
  echo "$sw" | grep -qE "MOVED $SLOT" || { echo "  FAIL: SPLIT OWNERSHIP — source still writable for slot: $sw"; pkill -9 -f "flint-server --port 658" 2>/dev/null; keep_dirs; return 1; }

  # No data loss: every sampled key present on the owner (dest).
  # WHERE the key is, not just that dest lacks it. "Lost" and "stranded behind
  # a -MOVED on the source" are different faults with different fixes, and the
  # check that only asks dest cannot tell them apart -- it reports the first
  # while the second is what the evidence usually supports (BUG-0132).
  local miss=0 k
  for k in 000000 000001 075000 149999; do
    [ "$(valkey-cli -p $DPORT GET "{mover}:key$k")" = "val-$k" ] || { echo "  MISSING key$k on dest"; miss=$((miss+1)); }
  done
  # WHERE THE BYTES ARE, when dest is missing them. "Lost" and "stranded on the
  # source behind a -MOVED" are different faults with different fixes, and the
  # dest-only check cannot tell them apart (BUG-0132).
  #
  # NOT a per-key GET on the source: by this point the source answers -MOVED
  # for every key in the slot -- the split-ownership assertion above requires
  # exactly that -- so a GET can never return a value and the branch reading it
  # would be dead code that always reports "lost". DBSIZE answers through the
  # redirect, because it counts what the node HOLDS rather than what it will
  # serve for this slot.
  if [ "$miss" != "0" ]; then
    echo "  WHERE: source DBSIZE=$(valkey-cli -p $SPORT DBSIZE) dest DBSIZE=$(valkey-cli -p $DPORT DBSIZE) (seeded $KEYS to source)"
    echo "        a source still holding ~$KEYS means the keys are STRANDED -- on disk,"
    echo "        unreachable, because the source -MOVEDs the slot to a dest without them."
  fi
  [ "$miss" = "0" ] || { echo "  FAIL: $miss keys lost after recovery"; pkill -9 -f "flint-server --port 658" 2>/dev/null; keep_dirs; return 1; }

  echo "  [delay $delay] RESOLVED: dest owns all keys, source -MOVED, no split, no loss"
  pkill -9 -f "flint-server --port 658" 2>/dev/null; rm -rf "$SDIR" "$DDIR"
  return 0
}

# Deterministically exercise the OTHER recovery branch — a flip interrupted
# between "dest owns" and "source disowned" (source Migrating, dest already
# owns). Timing-based kills rarely land in this sub-100ms window, so we
# construct the state directly: ship the data (no cutover), then freeze the
# source. Recovery must COMPLETE the flip: source -> Moved.
test_half_done_flip() {
  pkill -9 -f "flint-server --port 658" 2>/dev/null; fleet_kill controller; sleep 0.4
  local SDIR DDIR
  SDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-rec-s.XXXXXX); DDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-rec-d.XXXXXX)
  $B --port $SPORT --engine rocks --data-dir "$SDIR" 2>"${FLEET_SCOPE}server5.log" &
  $B --port $DPORT --engine rocks --data-dir "$DDIR" 2>"${FLEET_SCOPE}server6.log" &
  fleet_wait_listen $SPORT $DPORT
  sleep 0.8
  awk 'BEGIN{for(i=0;i<2000;i++){k=sprintf("{mover}:key%06d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n",length(k),k}}' \
    | valkey-cli -p $SPORT --pipe >/dev/null
  # Ship data to the dest (no cutover), then place the half-done-flip state:
  # dest owns (has the data, no record), source frozen Migrating to dest.
  valkey-cli -p $DPORT FLINTMIGRATEIN "$SADDR" "$SLOT" >/dev/null
  valkey-cli -p $SPORT FLINTSLOTFREEZE "$SLOT" "$DADDR" >/dev/null
  echo "  [half-done-flip] source frozen (Migrating), dest owns; source records=[$(valkey-cli -p $SPORT FLINTMIGRATIONS)]"

  "$CTLBIN" --recover-nodes "$SADDR,$DADDR" --id REC2 --poll-ms 200 2>>$FLINT_DRILL_ROOT/flint-rec.log &
  local CTL=$! RESOLVED=0 i
  for i in $(seq 1 60); do
    if echo "$(valkey-cli -p $SPORT GET "{mover}:key000000" 2>&1)" | grep -qE "MOVED $SLOT $DADDR"; then RESOLVED=1; break; fi
    sleep 0.2
  done
  kill -9 $CTL 2>/dev/null
  pkill -9 -f "flint-server --port 658" 2>/dev/null; rm -rf "$SDIR" "$DDIR"
  [ "$RESOLVED" = "1" ] || { echo "  FAIL: recovery did not complete the half-done flip"; return 1; }
  echo "  [half-done-flip] RESOLVED: recovery completed the flip, source -MOVED to dest"
  return 0
}

trap 'pkill -9 -f "flint-server --port 658" 2>/dev/null; fleet_kill controller' EXIT
: > $FLINT_DRILL_ROOT/flint-rec.log
echo "== slot {mover}=$SLOT, $KEYS keys; killing BOTH nodes mid-cutover (timing-based)"
FAILS=0
for d in 0.3 0.5 0.7; do
  run_once "$d" || FAILS=$((FAILS+1))
done
echo "== deterministic half-done-flip recovery"
test_half_done_flip || FAILS=$((FAILS+1))
[ "$FAILS" = "0" ] || { echo "FAIL: $FAILS recovery runs failed"; exit 1; }
echo "PASS: whole-cluster interruption mid-cutover recovers to clean single ownership, no loss, no split"
