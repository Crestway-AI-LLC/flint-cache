#!/usr/bin/env bash
# BUG-0145 — a single-seat control plane cannot be grown in place, and
# flintctl now refuses the transition instead of half-performing it.
#
# WHY. `cp_seat_args` gives a lone seat no --raft, no --node-id and no --peers;
# there is no `else` on that branch. So `cp-state` and `cp-state-n1` hold
# DIFFERENT FORMATS, and editing one `cp` line into three is not a topology
# change the code can carry out. Nothing refused it: the count assert passes
# (1 or 3, and 3 is what you now have), `cp_seat_name` looks for `cp-n1` where
# `cp` is running, finds nothing, and a duplicate is spawned on the live seat's
# port -- which BUG-0144 then reports as a successful start.
#
# The expensive branch is a reboot, the path `start` takes: three Raft seats
# come up with EMPTY state while the fleet's ownership truth sits orphaned in
# cp-state.
#
# WHAT THIS ASSERTS is the discrimination. One refusal is easy; the value is in
# the three cases that must NOT fire, because a wrong refusal here blocks every
# ordinary bring-up.
#
# AND IT REACHES THE CHECK. The first version of this test asserted on output
# that never got past the `disposable on` gate -- two arms "passed" against a
# refusal about build provenance, not about CP growth. The control below fails
# the drill if that gate is what answered.
set -uo pipefail
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
# THE ARMS THAT MUST NOT BE REFUSED DO REACH THE SPAWN, so this drill starts
# Flint processes and has to declare its ports like any other: the gate's
# port-overlap preflight cannot see an undeclared drill, and its cleanup would
# own nothing. Caught by assert_spawning_drills_declare_ports on the first gate.
#
# 6435-6439 rather than 6430-6434: next-free-ports.sh offered 6430 to this
# drill and to lease_after_repoint inside the same hour, because each claim was
# unpushed while the other was being written. The helper can only see what has
# LANDED, so a block it calls free is free as of the last fetch -- re-check
# after a rebase, which is where this collision surfaced.
fleet_init $FLINT_DRILL_ROOT/flint-cpgrowth 6435 6436 6437 6438 6439
fleet_guard
CTL=./target/release/flintctl
fail() { echo "CP GROWTH DRILL FAILED: $*"; exit 1; }

[ -x "$CTL" ] || fail "no flintctl at $CTL -- build it first"

D=$FLINT_DRILL_ROOT/flint-cpgrowth
# fleet_kill FIRST, and again on the way out: the allowed arms genuinely bring
# a fleet up, and a drill that leaves seats behind is the thing the port
# preflight exists to prevent.
cleanup() { fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane; rm -rf "$D"; }
trap cleanup EXIT
cleanup
mkdir -p "$D/state"

cat > "$D/inv1.flint" <<EOF
statedir $D/state
bins $PWD/target/release
disposable on
cp 127.0.0.1:6435
pair 127.0.0.1:6436,127.0.0.1:6437
EOF
sed 's|^cp 127.0.0.1:6435|cp 127.0.0.1:6435\ncp 127.0.0.1:6438\ncp 127.0.0.1:6439|' \
  "$D/inv1.flint" > "$D/inv3.flint"

run() { "$CTL" -f "$1" start 2>&1; }

# EVERY ARM STARTS FROM AN EMPTY STATEDIR, and this is not tidiness.
#
# The arms that must NOT be refused genuinely bring a fleet up, and a 3-seat
# start CREATES cp-state-n1. So arm 1 poisoned arm 3: by the time the grow case
# ran, both directories existed, the deliberate both-present allowance fired,
# and the drill reported "growing a single-seat CP was not refused" against a
# statedir the previous arm had made Raft.
#
# It passed on a laptop and failed on the box, because locally the fresh start
# did not get far enough to write the directory. An arm whose premise is built
# by the arm before it is not a test of anything.
reset() {
  fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
  rm -rf "$D/state"; mkdir -p "$D/state"
}

# THE CONTROL COMES FIRST, because every arm below reads output and an earlier
# gate answering instead would make all of them vacuous (OPS-0044).
reset
echo "== the command reaches launch, rather than being answered by another gate"
OUT=$(run "$D/inv3.flint")
case "$OUT" in
  *disposable*|*"not a release"*)
    fail "the disposable/provenance gate answered first, so nothing below tests CP growth: $(printf '%s' "$OUT" | head -1)" ;;
esac
echo "  reached"

echo "== a fresh 3-seat bring-up with no CP state is NOT refused"
case "$OUT" in
  *"cannot be grown"*|*"cannot be shrunk"*)
    fail "a fresh 3-seat bring-up was refused -- this blocks every new Raft fleet: $OUT" ;;
esac
echo "  allowed"

reset
echo "== single-seat state under a 3-seat inventory IS refused, and says why"
mkdir -p "$D/state/cp-state"
OUT=$(run "$D/inv3.flint")
case "$OUT" in
  *"cannot be grown in place"*) ;;
  *) fail "growing a single-seat CP was not refused: $(printf '%s' "$OUT" | head -2)" ;;
esac
case "$OUT" in
  *"not Raft state"*) ;;
  *) fail "the refusal does not say WHY -- a name mismatch leaves the reader to work it out (BUG-0145's correction): $OUT" ;;
esac
case "$OUT" in
  *cp-state*) ;;
  *) fail "the refusal does not name the directory it found" ;;
esac
echo "  refused, naming the state dir and the reason"

reset
echo "== and the reverse: Raft state under a 1-seat inventory"
mkdir -p "$D/state/cp-state-n1"
OUT=$(run "$D/inv1.flint")
case "$OUT" in
  *"cannot be shrunk in place"*) ;;
  *) fail "shrinking a Raft CP was not refused: $(printf '%s' "$OUT" | head -2)" ;;
esac
echo "  refused"

reset
mkdir -p "$D/state/cp-state-n1"
echo "== an ordinary 3-seat fleet on Raft state is NOT refused"
OUT=$(run "$D/inv3.flint")
case "$OUT" in
  *"cannot be grown"*|*"cannot be shrunk"*)
    fail "an ordinary Raft fleet restart was refused -- this is the reboot path: $OUT" ;;
esac
echo "  allowed"

# DELIBERATE, and asserted so it is a decision rather than an oversight: both
# directories present is a half-finished migration someone is in the middle of,
# and guessing which half is live would be a worse answer than proceeding.
reset
mkdir -p "$D/state/cp-state-n1"
echo "== both state dirs present is left alone, on purpose"
mkdir -p "$D/state/cp-state"
OUT=$(run "$D/inv3.flint")
case "$OUT" in
  *"cannot be"*) fail "a half-migrated statedir was refused; the file says this case is deliberately allowed: $OUT" ;;
esac
echo "  allowed"

echo "CP GROWTH DRILL PASSED"
