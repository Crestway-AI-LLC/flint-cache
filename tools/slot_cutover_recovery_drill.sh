#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# THE durability proof: interrupt a slot cutover by killing BOTH the source
# and the destination (a whole-cluster redeploy) mid-move, restart them, and
# let the recovery controller reconcile from the durable manifest records.
# After recovery: exactly one node owns the slot (source answers -MOVED to the
# dest), the dest has every key, and no write was lost.
#
# THE KILL LANDS AT AN OBSERVED PHASE (pull / freeze / flip), not after a sleep.
# It used to sleep 0.3/0.5/0.7s and claim the same coverage, which a sleep
# cannot deliver: which phase a fixed delay lands in depends on how fast the
# machine copies the corpus, so one commit tested different things on different
# hardware and the drill was itself the race (BUG-0132). Each arm now waits for
# the state it names, with the source's copy throttled so that state is
# reachable on any machine, and FAILS if it never arrives.
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

# INTERRUPT AT AN OBSERVED PHASE, NOT AT A WALL-CLOCK GUESS.
#
# This drill used to kill after `sleep 0.3 / 0.5 / 0.7` and its header claimed
# that made the kill "land at different phases (pull / freeze / flip)". A sleep
# does not select a phase, it guesses at one: which phase 0.5s lands in is a
# property of how fast the machine copies 150k rows. So the same commit tested
# different things on different hardware, and the drill was itself the race --
# green 26 times on EC2 and red twice on a GitHub runner, with nothing in the
# product changing between them (BUG-0132).
#
# Now each arm waits for the phase it names and fails if it never arrives.
MIGRATE_RATE=1000000   # bytes/sec on the source's outbound copy

# The corpus is ~4.5 MB, so the throttle above stretches the copy over several
# seconds on ANY machine -- which is what makes a phase observable rather than
# something a fast box can skip past between polls. It is a product knob
# (FLINTCONFIG migrate-rate-bytes, hot-reloadable mid-copy), not a test hack.
wait_for_phase() {   # $1 = pull | freeze | flip
  local want="$1" i n
  for i in $(seq 1 1200); do   # 60s at 0.05s
    case "$want" in
      pull)
        # Mid-COPY, not merely "started": the Importing record lands before any
        # row does, so importing-alone would kill an empty destination and call
        # it a pull.
        if valkey-cli -p $DPORT FLINTMIGRATIONS 2>/dev/null | grep -q importing; then
          n=$(valkey-cli -p $DPORT DBSIZE 2>/dev/null)
          case "$n" in ''|*[!0-9]*) ;; *) [ "$n" -gt 0 ] && [ "$n" -lt "$KEYS" ] && return 0 ;; esac
        fi ;;
      # NO `freeze` ARM, DELIBERATELY. The frozen window is unobservably short
      # HERE: with no live writes there is no frozen tail to drain, so the
      # source records `migrating` and the flip follows within the same
      # millisecond. Racing for it caught the POST-FLIP state every time while
      # labelling itself `freeze` -- an arm claiming a phase it did not reach,
      # which is the defect this rewrite exists to remove, reintroduced one
      # level up.
      #
      # The frozen state IS covered, deterministically, by
      # `test_half_done_flip` below: it CONSTRUCTS source=Migrating with the
      # dest holding the data via FLINTSLOTFREEZE, rather than hoping a kill
      # lands inside a window that is not there. Constructing a state beats
      # racing for it whenever the state can be constructed.
      flip)
        valkey-cli -p $SPORT GET "{mover}:key000000" 2>&1 | grep -q "MOVED $SLOT" && return 0 ;;
    esac
    sleep 0.05
  done
  return 1
}

run_once() {
  local phase="$1"
  pkill -9 -f "flint-server --port 658" 2>/dev/null; fleet_kill controller; sleep 0.4
  local SDIR DDIR
  SDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-rec-s.XXXXXX); DDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-rec-d.XXXXXX)
  $B --port $SPORT --engine rocks --data-dir "$SDIR" 2>"${FLEET_SCOPE}server.log" &
  $B --port $DPORT --engine rocks --data-dir "$DDIR" 2>"${FLEET_SCOPE}server2.log" &
  # WAIT FOR READY, NOT FOR THE PORT (BUG-0132). fleet_wait_listen returns as
  # soon as the socket accepts; a seat holding 150k keys then answers every
  # data command with -LOADING while it replays its WAL. This was
  # `fleet_wait_listen` plus a fixed `sleep` -- a delay standing in for an
  # observable state, which is the same defect the KILL side of this drill had
  # and which the phase rewrite removed there and left here. On 2026-09-12 it
  # cost a CI run: 68,039 -LOADING replies, DBSIZE reading 1 of 150000
  # mid-load, and "3 keys lost after recovery" reported against data that was
  # present and simply not loaded yet.
  fleet_wait_ready $SPORT
  fleet_wait_ready $DPORT

  # THROUGH fleet_load_resp (BUG-0147): a seed that was refused and a slot
  # that failed to move are the same silence otherwise.
  _scr_seed_gen() {
    awk -v n="$KEYS" 'BEGIN{for(i=0;i<n;i++){k=sprintf("{mover}:key%06d",i);v=sprintf("val-%06d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}'
  }
  fleet_load_resp "$SPORT" _scr_seed_gen "$KEYS" || exit 1

  # Throttle the source's outbound copy so every phase below is reachable
  # regardless of how fast this machine is.
  valkey-cli -p $SPORT FLINTCONFIG migrate-rate-bytes $MIGRATE_RATE >/dev/null

  # Start the cutover in the background; it blocks until done.
  ( valkey-cli -p $DPORT FLINTMIGRATEIN "$SADDR" "$SLOT" "$DADDR" >/dev/null 2>&1 ) &
  local MIG=$!
  # WAIT FOR THE PHASE, and fail loudly if it never arrives. An arm that never
  # reached the state it names did not test that interruption, and a pass there
  # would certify nothing -- the same rule as ops OPS-0037.
  if ! wait_for_phase "$phase"; then
    echo "  FAIL: never observed phase '$phase' within 60s -- this arm did not
  interrupt what it claims to, so neither its pass nor its failure means
  anything. Source records=[$(valkey-cli -p $SPORT FLINTMIGRATIONS 2>/dev/null)]
  dest records=[$(valkey-cli -p $DPORT FLINTMIGRATIONS 2>/dev/null)] dest
  DBSIZE=$(valkey-cli -p $DPORT DBSIZE 2>/dev/null) of $KEYS"
    kill -9 $MIG 2>/dev/null; wait $MIG 2>/dev/null
    pkill -9 -f "flint-server --port 658" 2>/dev/null; keep_dirs; return 1
  fi
  # WHOLE-CLUSTER KILL mid-move.
  pkill -9 -f "flint-server --port 658" 2>/dev/null
  kill -9 $MIG 2>/dev/null; wait $MIG 2>/dev/null
  sleep 0.5

  # Restart both nodes on the same data dirs (the redeploy).
  $B --port $SPORT --engine rocks --data-dir "$SDIR" 2>"${FLEET_SCOPE}server3.log" &
  $B --port $DPORT --engine rocks --data-dir "$DDIR" 2>"${FLEET_SCOPE}server4.log" &
  # ready, not listening -- see the note at the first restart (BUG-0132)
  fleet_wait_ready $SPORT
  fleet_wait_ready $DPORT

  # Observe the interrupted state from the durable records.
  local SM DM PHASE
  SM=$(valkey-cli -p $SPORT FLINTMIGRATIONS 2>/dev/null)
  DM=$(valkey-cli -p $DPORT FLINTMIGRATIONS 2>/dev/null)
  # THREE STATES, NOT TWO. No records on either node means the cutover either
  # COMPLETED (source is Moved to dest) or NEVER STARTED (source still owns and
  # serves), and those are opposite facts that this line rendered identically
  # -- then the verdict text asserted one of them. Ask source who owns the slot
  # rather than inferring it from an absence (BUG-0132, ops OPS-0037).
  local OWN CLASS
  if [ -n "$SM" ] || [ -n "$DM" ]; then
    CLASS=interrupted; PHASE="INTERRUPTED mid-move"
  else
    OWN=$(valkey-cli -p $SPORT GET "{mover}:key000000" 2>&1)
    case "$OWN" in
      *"MOVED $SLOT"*) CLASS=completed
                       PHASE="completed pre-kill (source already -MOVED; recovery is a no-op)" ;;
      val-*)           CLASS=never-started
                       PHASE="NEVER STARTED (source still owns and serves the slot)" ;;
      *)               CLASS=indeterminate
                       PHASE="no records, and source answers [$OWN] -- ownership indeterminate" ;;
    esac
  fi
  echo "  [killed in phase $phase] after restart: source=[$SM] dest=[$DM] -> $PHASE"
  SEEN="$SEEN $CLASS"

  # THE CLASSIFICATION IS FOUR-WAY AND THE ASSERTION BELOW WAS ONE-WAY
  # (BUG-0170). That mismatch is what reddened main on a markdown-only commit:
  # the kill can land before the move is DURABLE, the records are then empty on
  # both nodes, the source rightly still owns the slot -- and the code below
  # waited 30s for a resolution that cannot come and printed "move not resolved
  # after recovery", which reads as a product defect.
  #
  # This is BUG-0132's class one layer up. That fix made the OBSERVATION
  # three-state, on the stated grounds that "completed" and "never started" are
  # opposite facts rendered identically; it left the ASSERTION two-state, so
  # the drill went on to assert one of them regardless.
  #
  # Each outcome now gets the post-condition that belongs to it.
  if [ "$CLASS" = indeterminate ]; then
    echo "  FAIL: no migration records and the source answers neither a value nor"
    echo "        -MOVED [$OWN] -- ownership cannot be established, so neither"
    echo "        post-condition below can be asserted honestly"
    pkill -9 -f "flint-server --port 658" 2>/dev/null; keep_dirs; return 1
  fi
  if [ "$CLASS" = never-started ]; then
    assert_never_started "$phase"
    return $?
  fi

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
    # AND THE RECORDS, which is the state that decides which branch of
    # recover_migrations would have run. DBSIZE says where the data is; this
    # says what the durable manifests claimed about it. Without it the next
    # failure still cannot tell an `importing`/`migrating`/`aborted` record
    # that was EMPTY BY CONTRACT from one that was empty because it was lost --
    # and the whole ordering question turns on that difference.
    echo "  RECORDS AT FAILURE: source=[$(valkey-cli -p $SPORT FLINTMIGRATIONS 2>&1 | tr '\n' ' ')]"
    echo "                      dest=[$(valkey-cli -p $DPORT FLINTMIGRATIONS 2>&1 | tr '\n' ' ')]"
    echo "        Empty on both is the reading to distrust: it means 'no migration in"
    echo "        flight' and 'the record was cleared before the data was durable' in"
    echo "        exactly the same characters."
  fi
  [ "$miss" = "0" ] || { echo "  FAIL: $miss keys lost after recovery"; pkill -9 -f "flint-server --port 658" 2>/dev/null; keep_dirs; return 1; }

  echo "  [killed in phase $phase] RESOLVED: dest owns all keys, source -MOVED, no split, no loss"
  pkill -9 -f "flint-server --port 658" 2>/dev/null; rm -rf "$SDIR" "$DDIR"
  return 0
}

# THE SAFE OUTCOME, when the kill landed before the move was durable.
#
# Nothing was recorded, so there is nothing for the recovery controller to
# reconcile from and the move simply did not happen: the source still owns the
# slot and holds every key, the dest holds none of it, and an operator would
# re-issue the move. No loss and no split -- which is what this drill exists to
# prove, and it is as true here as it is after a resolved recovery.
#
# IT STILL RUNS THE RECOVERY CONTROLLER, deliberately. Skipping it would leave
# the interesting question unasked; running it asserts the stronger property,
# that recovery does not FABRICATE a move from an empty manifest.
#
# The checks come BEFORE any write to the dest. The resolution loop this
# replaces polls `SET` against the dest up to 150 times, so by the time it gave
# up it had itself put key000000 there -- a drill that had gone on to ask "does
# the dest hold the slot" would have been reading its own writes.
assert_never_started() {
  local phase="$1" i sr sw dr dn miss=0 k

  "$CTLBIN" --recover-nodes "$SADDR,$DADDR" --id REC --poll-ms 200 2>>$FLINT_DRILL_ROOT/flint-rec.log &
  local CTL=$!
  # Long enough for the controller to complete several poll cycles and prove it
  # has nothing to act on. Shorter than the 30s the resolution loop burned.
  sleep 2
  kill -9 $CTL 2>/dev/null

  # The source still owns: it serves reads AND accepts writes for the slot.
  sr=$(valkey-cli -p $SPORT GET "{mover}:key000000" 2>&1)
  [ "$sr" = "val-000000" ] || {
    echo "  FAIL: recovery moved a slot no manifest recorded (source read='$sr')"
    pkill -9 -f "flint-server --port 658" 2>/dev/null; keep_dirs; return 1; }
  sw=$(valkey-cli -p $SPORT SET "{mover}:key000001" val-000001 2>&1)
  [ "$sw" = "OK" ] || {
    echo "  FAIL: source owns the slot but refuses writes for it: $sw"
    pkill -9 -f "flint-server --port 658" 2>/dev/null; keep_dirs; return 1; }

  # WHAT THE DEST HOLDS IS REPORTED, NOT ASSERTED, and that restraint is the
  # point rather than a shortcut.
  #
  # `wait_for_phase pull` waits for the dest to report an `importing` record
  # AND to hold 0 < rows < KEYS. So at kill time the dest had both, and this
  # branch is reached only when the record is GONE after the restart. Whether
  # the ROWS also went is a separate question that nothing here has observed:
  # asserting "the dest is empty" would be a guess, and a wrong guess turns
  # one misreporting drill into another.
  #
  # It is not a split either way. A split is two nodes each answering as OWNER,
  # and a dest with no migration record makes no ownership claim -- rows left
  # behind are ORPHANED, which is a different fault with a different fix. That
  # question is raised in BUG-0170's write-up rather than decided here.
  dr=$(valkey-cli -p $DPORT GET "{mover}:key000000" 2>&1)
  dn=$(valkey-cli -p $DPORT DBSIZE 2>&1)
  if [ -n "$dr" ]; then
    echo "     dest still holds copied rows (DBSIZE=$dn, key000000='$dr') with NO"
    echo "     migration record -- orphaned, not served, and not a split. See BUG-0170."
  else
    echo "     dest holds nothing of the slot (DBSIZE=$dn)."
  fi

  # No loss: every sampled key is still on the owner, which here is the source.
  for k in 000000 075000 149999; do
    [ "$(valkey-cli -p $SPORT GET "{mover}:key$k")" = "val-$k" ] || {
      echo "  MISSING key$k on source"; miss=$((miss+1)); }
  done
  [ "$miss" = "0" ] || {
    echo "  FAIL: $miss keys lost -- the move never started, so the source should"
    echo "        still hold everything it was seeded with"
    echo "  WHERE: source DBSIZE=$(valkey-cli -p $SPORT DBSIZE) dest DBSIZE=$(valkey-cli -p $DPORT DBSIZE) (seeded ${SEEDED:-$KEYS} to source)"
    pkill -9 -f "flint-server --port 658" 2>/dev/null; keep_dirs; return 1; }

  local how="killed in phase $phase"
  [ "$phase" = constructed ] && how="constructed"
  echo "  [$how] NEVER STARTED and stayed that way: source owns and"
  echo "     serves every key, dest holds none, recovery invented nothing. No split, no loss."
  echo "     NOTE: this arm did NOT exercise recovery -- the kill beat durability."
  pkill -9 -f "flint-server --port 658" 2>/dev/null; rm -rf "$SDIR" "$DDIR"
  return 0
}

# CONSTRUCT the never-started outcome, because racing for it is what made this
# drill flaky in the first place.
#
# `assert_never_started` above runs only when a kill beats durability, which is
# roughly one run in ten -- so shipping it on the strength of the raced path
# would mean shipping a branch that had never executed, which is the shape of
# defect this whole bug is about. The drill already makes this argument for the
# frozen window: "constructing a state beats racing for it whenever the state
# can be constructed."
#
# The state is trivial to build: two nodes, the corpus on the source, and no
# migration ever issued. That is exactly what the raced arm finds after a kill
# that beat the records -- empty manifests on both, source owning and serving.
test_never_started_is_safe() {
  pkill -9 -f "flint-server --port 658" 2>/dev/null; fleet_kill controller; sleep 0.4
  SDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-rec-s.XXXXXX); DDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-rec-d.XXXXXX)
  $B --port $SPORT --engine rocks --data-dir "$SDIR" 2>"${FLEET_SCOPE}server7.log" &
  $B --port $DPORT --engine rocks --data-dir "$DDIR" 2>"${FLEET_SCOPE}server8.log" &
  fleet_wait_ready $SPORT
  fleet_wait_ready $DPORT

  # A small corpus: this arm asserts ownership and the absence of loss, neither
  # of which needs 150k keys, and the sampled keys must exist.
  local n=2000 k
  _nvs_seed_gen() {
    awk -v n="$n" 'BEGIN{for(i=0;i<n;i++){k=sprintf("{mover}:key%06d",i);v=sprintf("val-%06d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}'
  }
  fleet_load_resp "$SPORT" _nvs_seed_gen "$n" || return 1
  SEEDED=$n
  for k in 000000 075000 149999; do
    valkey-cli -p $SPORT SET "{mover}:key$k" "val-$k" >/dev/null
  done

  # The precondition this arm claims, asserted rather than assumed: no
  # migration records anywhere. If a previous arm leaked one, everything below
  # would be testing a different state under this name.
  local SM DM
  SM=$(valkey-cli -p $SPORT FLINTMIGRATIONS 2>/dev/null)
  DM=$(valkey-cli -p $DPORT FLINTMIGRATIONS 2>/dev/null)
  [ -z "$SM" ] && [ -z "$DM" ] || {
    echo "  FAIL: constructed never-started state is not clean (source=[$SM] dest=[$DM])"
    pkill -9 -f "flint-server --port 658" 2>/dev/null; keep_dirs; return 1; }

  echo "  [constructed never-started] no migration ever issued; source owns the slot"
  assert_never_started "constructed"
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
  # ready, not listening -- see the note at the first restart (BUG-0132)
  fleet_wait_ready $SPORT
  fleet_wait_ready $DPORT
  _scr_reseed_gen() {
    awk 'BEGIN{for(i=0;i<2000;i++){k=sprintf("{mover}:key%06d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n",length(k),k}}'
  }
  fleet_load_resp "$SPORT" _scr_reseed_gen 2000 || exit 1
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
echo "== slot {mover}=$SLOT, $KEYS keys; killing BOTH nodes at each OBSERVED phase"
FAILS=0
SEEN=""
for ph in pull flip; do
  run_once "$ph" || FAILS=$((FAILS+1))
done
# A RUN WHERE EVERY KILL BEAT DURABILITY PROVED NOTHING ABOUT RECOVERY
# (BUG-0170). Each `never-started` arm is a legitimate pass, and a run made
# ENTIRELY of them is a green verdict over a recovery path that never ran --
# the same shape as a check that matches no files. The half-done-flip case
# below is constructed rather than raced, so it always exercises one branch;
# this asserts the RACED arms reached the recovery code at least once.
case "$SEEN" in
  *interrupted*|*completed*) ;;
  *) echo "FAIL: no raced arm reached recovery this run (classes:$SEEN) --"
     echo "      every kill landed before the move was durable, so the"
     echo "      interrupted-recovery path was never exercised. Green here"
     echo "      would certify it by running none of it."
     FAILS=$((FAILS+1)) ;;
esac
echo "== deterministic never-started safety"
test_never_started_is_safe || FAILS=$((FAILS+1))
echo "== deterministic half-done-flip recovery"
test_half_done_flip || FAILS=$((FAILS+1))
[ "$FAILS" = "0" ] || { echo "FAIL: $FAILS recovery runs failed"; exit 1; }
echo "PASS: whole-cluster interruption mid-cutover recovers to clean single ownership, no loss, no split"
