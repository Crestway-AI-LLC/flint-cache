#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The gate's own argument handling (docs/bugs/0009-unknown-stage-passes-the-gate.md).
#
# `tools/gates.sh` took its stage names off the command line without
# validating them, and `want()` asked whether a stage appeared in that string.
# An argument that was not a stage name therefore matched nothing: every stage
# was skipped, `FAILED` stayed empty, and the script printed "GATES PASSED"
# and exited 0. `gates.sh --help` and `gates.sh drill` (the stage is `drills`)
# were both a green run of nothing, in under a second.
#
# That is the failure this suite exists to prevent — a check that verifies
# nothing and reads as green — arriving through the gate's own front door,
# while the release checklist names that script as the authority for a tag.
#
# Asserts: --help is served deliberately, an unrecognised stage refuses
# non-zero, every documented stage still validates, refusing keeps the
# previous run's logs, and a run that executed no steps cannot report a pass.
# The last one carries a POSITIVE CONTROL, because "it cannot print GATES
# PASSED" is also true of a gates.sh that is simply broken.
#
# THIS DRILL RUNS THE GATE, AND THE GATE RUNS THIS DRILL. It may only invoke
# gates.sh in ways that exit during argument handling — --help, or a bad stage
# — or against a copy whose `step` is stubbed out. An invocation that reaches
# a real stage runs the whole suite from inside the suite.
#
# Requires: bash. No build: this is a property of the harness rather than of
# the product, which is why it costs a second and can sit early in CORE.
set -u
cd "$(dirname "$0")/.."

TMP=${FLINT_DRILL_ROOT:-/tmp}/flint-gates-drill
KEPT="$TMP/logs-kept"       # a log directory that must SURVIVE every refusal
# THIS DRILL MUST CONTROL FLINT_GATE_JOBS, NOT INHERIT IT.
#
# Same shape as fleet_guard_drill and FLINT_DRILL_FORCE. This drill forges a
# tree whose `step` is STUBBED -- it counts and prints instead of executing,
# which is what stops the suite running itself recursively -- and then asserts
# the pass line counts CORE + 1 steps. Both assumptions are about the
# SEQUENTIAL path: at FLINT_GATE_JOBS>1 the drills stage bypasses step() for
# the drills (it reports through step_report instead), so the stub is skipped
# and the forged run tries to execute drill scripts the forged tree does not
# have; and the parallel path adds a prebuild step, so the count is CORE + 2.
#
# Measured 2026-08-24: inheriting JOBS=4 from the outer gate failed this drill
# with "bash: tools/wal_headroom_drill.sh: No such file or directory" -- a
# forged tree being asked to run the real suite.
#
# What this drill tests -- argument handling, refusing an unknown stage, and
# the CORE-list-is-the-count contract -- is parallelism-independent, so pin it.
FLINT_GATE_JOBS_WAS="${FLINT_GATE_JOBS:-}"
unset FLINT_GATE_JOBS
[ -n "$FLINT_GATE_JOBS_WAS" ] && echo "   (FLINT_GATE_JOBS=$FLINT_GATE_JOBS_WAS cleared: this drill asserts the sequential path)"

SCRATCH="$TMP/logs-scratch" # one the copies below are free to clear
OUT="$TMP/out"; ERR="$TMP/err"
POISON=__not_a_stage__
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT
rm -rf "$TMP"; mkdir -p "$TMP" "$KEPT" "$SCRATCH"
echo "the previous run" > "$KEPT/previous-run.log"

# gate <want-exit> <args...> — the REAL gates.sh, with a log directory of our
# own.
#
# FLINT_GATE_LOGS is set on EVERY invocation deliberately. CI exports it, so a
# regression that cleared the logs before reading the arguments would delete
# the logs of the very run executing this drill, and the drill would be the
# thing that destroyed the evidence.
#
# Three assertions ride along on every case, because they are the bug: the
# exit status, that GATES PASSED is never printed by a run that started no
# stage, and that the log directory is still there afterwards.
gate() {
  local want=$1; shift
  FLINT_GATE_LOGS="$KEPT" bash tools/gates.sh "$@" >"$OUT" 2>"$ERR"
  local rc=$?
  O=$(cat "$OUT"); E=$(cat "$ERR")
  [ "$rc" = "$want" ] || {
    echo "FAIL: [gates.sh $*] exit $rc, want $want"
    sed 's/^/        /' "$OUT" "$ERR"; exit 1; }
  # ANCHORED. The verdict is a line of its own, and gates.sh's refusal text
  # is allowed to discuss the pass string without being mistaken for it —
  # the same reason its own summary greps `^FAIL` rather than /FAIL/.
  printf '%s\n%s\n' "$O" "$E" | grep -q '^GATES PASSED' && {
    echo "FAIL: [gates.sh $*] printed GATES PASSED without starting a stage."
    echo "      This is docs/bugs/0009: an argument that selects nothing is"
    echo "      being reported as a gate that verified everything."
    exit 1; }
  [ -f "$KEPT/previous-run.log" ] || {
    echo "FAIL: [gates.sh $*] cleared the log directory before reading its"
    echo "      arguments. Asking for help, or mistyping a stage, must keep the"
    echo "      last run's logs — they are what you go back to when the gate"
    echo "      says something surprising."
    exit 1; }
}

echo "== --help and -h are served: usage, exit 0, nothing run"
for h in --help -h; do
  gate 0 "$h"
  case "$O" in *"Usage: tools/gates.sh"*) ;;
    *) echo "FAIL: $h printed no usage block:"
       echo "$O" | sed 's/^/        /'; exit 1;; esac
  for s in check conformance drills chaos; do
    case "$O" in *"$s"*) ;;
      *) echo "FAIL: $h does not name the $s stage"; exit 1;; esac
  done
  echo "  $h: usage naming all four stages, exit 0"
done

echo "== an unrecognised stage refuses, and says which argument it was"
for bad in drill checks chao --strict; do
  gate 2 "$bad"
  case "$E" in *"unrecognised stage: $bad"*) ;;
    *) echo "FAIL: [$bad] refused without naming the argument:"
       echo "$E" | sed 's/^/        /'; exit 1;; esac
  echo "  $bad -> exit 2, named"
done
# The empty argument has no name to print and must still refuse. It is what a
# quoted shell variable expands to when it is unset, so it arrives by accident
# rather than by typo.
gate 2 ""
echo "  '' -> exit 2"

echo "== one bad stage among good ones refuses the WHOLE run"
gate 2 drills "$POISON"
case "$E" in *"unrecognised stage: $POISON"*) ;;
  *) echo "FAIL: a valid+invalid pair did not name the invalid one:"
     echo "$E" | sed 's/^/        /'; exit 1;; esac
echo "  drills $POISON -> exit 2 (no partial gate reported on as a whole one)"

echo "== every documented stage still validates"
# Each stage is passed WITH the poison, so validation refuses before any stage
# can start. That is the only way to exercise acceptance of a real stage name
# from inside the gate without running the gate. A stage dropped from the
# validated set, or renamed on one side only, would be named in the refusal
# instead of the poison.
for s in check conformance drills chaos; do
  gate 2 "$s" "$POISON"
  case "$E" in *"unrecognised stage: $POISON"*) ;;
    *) echo "FAIL: gates.sh no longer accepts the documented stage '$s':"
       echo "$E" | sed 's/^/        /'
       echo "      README.md and docs/release-checklist.md both tell people to"
       echo "      run stages by name; a rejected stage breaks every one of them."
       exit 1;; esac
  echo "  $s accepted"
done
gate 2 conformance drills chaos "$POISON"
case "$E" in *"unrecognised stage: $POISON"*) ;;
  *) echo "FAIL: the CI line 'conformance drills chaos' is no longer accepted:"
     echo "$E" | sed 's/^/        /'; exit 1;; esac
echo "  conformance drills chaos accepted (.github/workflows/gate.yml)"

echo "== the validated set and the dispatched set are the same set"
# RESOLVE THE VALUE, DO NOT READ THE TEXT.
#
# This was `sed -n 's/^ALL_STAGES="\(.*\)"$/\1/p'`, which worked only while
# that line held the stage names literally. `4bcba0c` split them into
# DEFAULT_STAGES + OPT_IN_STAGES and left `ALL_STAGES="$DEFAULT_STAGES
# $OPT_IN_STAGES"`, so the sed dutifully reported the two VARIABLE NAMES as the
# declared stages and this check failed on a correct refactor:
#
#     FAIL: ALL_STAGES and the want() calls disagree:
#       < $DEFAULT_STAGES
#       < $OPT_IN_STAGES
#       > chaos / check / conformance / drills …
#
# Evaluating the assignments instead follows any shape the declaration takes.
# It matches every simple `NAME="..."` above `want()` rather than naming the
# three that exist today: a list of component variables would be one more thing
# to keep in sync with the file it is reading, which is this bug again.
DECLARED=$( {
  eval "$(sed -n '1,/^want()/p' tools/gates.sh | grep -E '^[A-Z][A-Z0-9_]*="[^"]*"$')"
  printf '%s\n' $ALL_STAGES
} | sort -u )
# A CALL, NOT THE WORD. `want [a-z_]+` matches any occurrence, and gates.sh
# gained `local want scanned rest` in 4bcba0c -- a variable DECLARATION whose
# second name is `scanned`. This check duly reported `scanned` as a stage that
# is dispatched but not declared, which is a real defect when it is true and
# was not here.
#
# Every genuine call sits at a command position: `if want msrv; then`, or
# `if want conformance || want drills || want chaos; then`. Requiring that
# position excludes the declaration without naming it, which a `grep -v local`
# would not -- the next collision would be some other keyword.
DISPATCHED=$(grep -v '^[[:space:]]*#' tools/gates.sh \
  | grep -oE '(^|[;&|] *|if )want [a-z_]+' \
  | grep -oE 'want [a-z_]+' | awk '{print $2}' | sort -u)
# Both greps must find something. Two empty sets compare equal, which would
# make this check pass on a gates.sh it could not read at all.
[ -n "$DECLARED" ] || { echo "FAIL: no ALL_STAGES line in tools/gates.sh"; exit 1; }
[ -n "$DISPATCHED" ] || { echo "FAIL: no 'want <stage>' calls in tools/gates.sh"; exit 1; }
[ "$DECLARED" = "$DISPATCHED" ] || {
  echo "FAIL: ALL_STAGES and the want() calls disagree:"
  diff <(echo "$DECLARED") <(echo "$DISPATCHED") | sed 's/^/        /'
  echo "      A stage in one and not the other is either a stage nobody can"
  echo "      ask for, or one that is accepted and then runs nothing."
  exit 1; }
echo "  $(echo "$DECLARED" | tr '\n' ' ')"

# ---- states the command line can no longer reach ----
#
# The guards above stop an unrecognised argument from selecting nothing. The
# backstop is for what they do not cover: a refactor that selects nothing for
# some other reason. Reaching that needs a gates.sh whose dispatch is forced,
# so the last two cases run against a COPY.
#
# The copy lives at <dir>/tools/gates.sh so that the script's own
# `cd "$(dirname "$0")/.."` lands in the copy's directory rather than back in
# this repository.
forge() {  # forge <dir> <forced-STAGES> <stub-step: yes|no>
  local dir=$1 stages=$2 stub=$3
  rm -rf "$dir"; mkdir -p "$dir/tools"
  awk -v stages="$stages" -v stub="$stub" '
    # MATCH THE FORM, NOT ONE VARIABLE NAME. This compared against the exact
    # literal `STAGES="${*:-$ALL_STAGES}"`, and 4bcba0c changed the default to
    # $DEFAULT_STAGES so msrv could be opt-in. The equality then never held,
    # the copy was never forced, and the drill correctly refused to assert
    # against an untouched script -- a red gate for a correct refactor.
    $0 ~ /^STAGES="\$\{\*:-\$[A-Z_]+\}"$/ { print "STAGES=\"" stages "\""; forced=1; next }
    $0 == "if want check; then" && stub == "yes" && !stubbed {
      print "step() { RAN_STEPS=$((RAN_STEPS + 1)); echo \"STEP $1\"; }"
      stubbed = 1
    }
    { print }
    END { if (!forced) exit 3; if (stub == "yes" && !stubbed) exit 4 }
  ' tools/gates.sh > "$dir/tools/gates.sh"
  # An edit that silently did not apply would leave the cases below asserting
  # against an unmodified gates.sh — passing while testing nothing, which is
  # the defect this whole drill is about.
  case $? in
    0) ;;
    3) echo "FAIL: no 'STAGES=\"\${*:-\$<VAR>}\"' line in tools/gates.sh, so"
       echo "      the dispatch could not be forced and the backstop below would"
       echo "      have been asserted against an untouched script."; exit 1;;
    4) echo "FAIL: no 'if want check; then' line to stub step() ahead of"; exit 1;;
    *) echo "FAIL: could not build the gates.sh copy"; exit 1;;
  esac
}

echo "== a run that executes no steps refuses, and does not report a pass"
forge "$TMP/nothing" "$POISON" no
# FLINT_GATE_SKIP_DISK because the copy runs the host disk check too, and a
# tight disk is not what is being asserted here.
FLINT_GATE_LOGS="$SCRATCH" FLINT_GATE_SKIP_DISK=1 \
  bash "$TMP/nothing/tools/gates.sh" >"$OUT" 2>&1
rc=$?; O=$(cat "$OUT")
echo "$O" | grep -q '^GATES PASSED' && {
  echo "FAIL: a gates.sh that selected no stage still printed GATES PASSED:"
  echo "$O" | sed 's/^/        /'
  echo "      The RAN_STEPS backstop is gone. docs/bugs/0009 is reachable again"
  echo "      by whatever route this refactor opened."
  exit 1; }
case "$O" in *"GATES DID NOT RUN"*) ;;
  *) echo "FAIL: a run of nothing printed no GATES DID NOT RUN line:"
     echo "$O" | sed 's/^/        /'; exit 1;; esac
[ "$rc" = "2" ] || { echo "FAIL: a run of nothing exited $rc, want 2"; exit 1; }
echo "  GATES DID NOT RUN, exit 2"

echo "== POSITIVE CONTROL: the same harness still passes when steps do run"
# Without this, the case above is equally satisfied by a gates.sh that can
# never print GATES PASSED at all. Stubbing `step` to count and print rather
# than execute is also what keeps this from running the suite recursively.
forge "$TMP/something" drills yes
FLINT_GATE_LOGS="$SCRATCH" FLINT_GATE_SKIP_DISK=1 \
  bash "$TMP/something/tools/gates.sh" >"$OUT" 2>&1
rc=$?; O=$(cat "$OUT")
[ "$rc" = "0" ] || { echo "FAIL: a run with steps exited $rc, want 0"
  echo "$O" | tail -5 | sed 's/^/        /'; exit 1; }
N=$(echo "$O" | sed -n 's/.*GATES PASSED.*[^0-9]\([0-9][0-9]*\) steps.*/\1/p')
[ -n "$N" ] || { echo "FAIL: the pass line carries no step count:"
  echo "$O" | tail -3 | sed 's/^/        /'
  echo "      The count is how a gate that quietly shrinks becomes visible in"
  echo "      its own output rather than only in a diff of gates.sh."
  exit 1; }
RAN=$(echo "$O" | grep -c '^STEP ')
[ "$N" = "$RAN" ] || { echo "FAIL: the pass line counted $N steps, $RAN ran"; exit 1; }
# And the drills stage runs every name in CORE, +1 for the release build that
# the stage depends on. Derived from the list, so adding a drill to CORE does
# not have to be remembered here as well.
CORE_N=$(sed -n '/^CORE="/,/"$/p' tools/gates.sh | sed 's/^CORE="//; s/"$//' | wc -w | tr -d ' ')
[ "${CORE_N:-0}" -gt 0 ] || { echo "FAIL: could not read the CORE list"; exit 1; }
[ "$N" = "$((CORE_N + 1))" ] || {
  echo "FAIL: the drills stage ran $N steps for $CORE_N drills (+1 build)."
  echo "      Every name in CORE has to become a step: that list is the count,"
  echo "      and a loop that skips some of it is a smaller gate reporting as"
  echo "      the whole one."
  exit 1; }
echo "  GATES PASSED — $N steps for $CORE_N drills + the build, exit 0"

echo "PASS: the gate refuses an argument it does not recognise, serves --help deliberately, keeps the previous run's logs while doing both, and cannot report a pass after running nothing"
