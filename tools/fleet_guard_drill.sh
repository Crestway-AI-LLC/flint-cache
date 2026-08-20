#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# fleet_guard: does the box-is-busy check actually see everything that makes
# the box busy?
#
# WHY THIS EXISTS. The guard's detector was anchored
# `^flint-(server|proxy|...)$`, which never matched a sibling project's
# `flint-kv-server`. So with another project's chaos fleet running, the guard
# reported a clean box and every drill went ahead into CPU and disk
# contention. That does not fail loudly — it produces a slow, flaky drill,
# and sends you debugging the drill instead of the collision. A check that
# says "clear" when it simply cannot see is worse than no check.
#
# HOW THE PROCESSES ARE FAKED. `exec -a <name> sleep` sets argv[0], which is
# exactly how a real compiled binary presents in ps. Two earlier attempts got
# this wrong and are worth remembering: a shell script shows up as
# "/bin/sh /path/name" (so the detector correctly saw `sh`, and the test
# wrongly read that as a miss), and a COPY of /bin/sleep is SIGKILLed by
# macOS because copying breaks its code signature. Both produced a green-
# looking test of nothing.
#
# NOTHING HERE TOUCHES ANOTHER PROJECT. The fakes are this script's own
# `sleep` children, killed on exit. The guard is detection-only by design:
# flint-kv's fleets belong to flint-kv, and may be mid-measurement.
set -u
# Job-control notices ("Terminated: 15") for our own fakes are noise that
# reads like a failure in the middle of a passing drill.
set +m
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"

PIDS=""
spawn_as() { bash -c "exec -a $1 sleep 60" & PIDS="$PIDS $!"; }
cleanup() { [ -n "$PIDS" ] && kill $PIDS 2>/dev/null; wait 2>/dev/null; true; }
trap cleanup EXIT

# THIS DRILL MUST CONTROL FLINT_DRILL_FORCE, NOT INHERIT IT.
#
# CI exports FLINT_DRILL_FORCE=1 for the whole job, because a fresh runner
# is genuinely a clean box and the guard cannot tell that from someone's
# laptop. Every other drill wants that. This one is ABOUT the guard, so
# inheriting it made the refusal assertions unprovable: the guard returned 0
# by design and the drill read that as "did NOT refuse". It passed locally,
# where the variable is unset, and failed on the first CI run — the exact
# environment-difference class the multi-host work exists to catch.
#
# So: clear it here, and set it explicitly in the one step that tests it.
FORCE_WAS="${FLINT_DRILL_FORCE:-}"
unset FLINT_DRILL_FORCE
[ -n "$FORCE_WAS" ] && echo "   (FLINT_DRILL_FORCE=$FORCE_WAS cleared for this drill; step E sets it back)"

fleet_init $FLINT_DRILL_ROOT/flint-guard-drill 6999 6378 6379 6380 6381 6382 6383 6384 6385

echo "== A) a quiet box: the guard must let the drill run"
# THIS CASE NEEDS A QUIET BOX AND CANNOT CREATE ONE. Another project's build
# or another session's fleet is not ours to stop, and when one is up the
# guard is RIGHT to refuse — failing here would report a correct refusal as a
# defect, and the fix someone reaches for is to weaken the guard.
#
# So say NOT EXERCISED and name what blocked it. A case that cannot reach its
# own precondition must not report a verdict about the thing it never tested;
# a precondition that quietly stops holding is how a check passes forever.
PRE_SIB=$(_fleet_sibling); PRE_FOR=$(_fleet_foreign)
if [ -n "$PRE_SIB" ] || [ -n "$PRE_FOR" ]; then
  echo "  n/a — the box is NOT quiet, so this case did not run:"
  [ -n "$PRE_SIB" ] && printf '%s\n' "$PRE_SIB" | cut -c1-90 | sed 's/^/      sibling: /'
  [ -n "$PRE_FOR" ] && printf '%s\n' "$PRE_FOR" | cut -c1-90 | sed 's/^/      foreign: /'
  A_SKIPPED=1
else
  OUT=$(fleet_guard 2>&1); RC=$?
  [ "$RC" = 0 ] \
    || { echo "FAIL: guard refused on a quiet box (exit $RC)"; echo "$OUT"; exit 1; }
  echo "  no refusal, as expected"
fi

echo "== B) a SIBLING project's fleet is up: the guard must refuse"
spawn_as flint-kv-server
spawn_as flint-kv-chaos
sleep 1
# Prove the fakes are actually visible the way a real binary would be. If
# this is empty the rest of the run proves nothing.
ps -eo pid=,args= | grep -qE '^ *[0-9]+ flint-kv-server' \
  || { echo "FAIL: the fake sibling is not visible in ps — this drill would pass vacuously"; exit 1; }
OUT=$(fleet_guard 2>&1); RC=$?
[ "$RC" = 1 ] \
  || { echo "FAIL: guard did NOT refuse with a sibling fleet up (exit $RC)"; echo "$OUT"; exit 1; }
echo "$OUT" | grep -q "another Flint-family project" \
  || { echo "FAIL: refused, but not for the sibling reason:"; echo "$OUT"; exit 1; }
# The message must not tell an operator to go kill someone else's processes.
echo "$OUT" | grep -q "NOT ours and this suite will not touch them" \
  || { echo "FAIL: the refusal must say these are not ours to stop"; echo "$OUT"; exit 1; }
echo "  refused, named the sibling, and disclaimed ownership"
cleanup; PIDS=""; sleep 0.5

echo "== C) NEGATIVE CONTROL: our OWN binaries must not read as a sibling"
# The rule is structural — sibling projects use flint-<project>-<component>,
# everything this workspace builds is one segment. If that ever stops holding,
# the guard starts calling our own processes foreign and refuses EVERY drill,
# which is a worse failure than the one it was written to fix.
for n in flint-server flint-proxy flint-controlplane flint-controller \
         flint-chaos flint-bench flint-conformance flint-balance \
         flint-agent flint-console flint-ops flint-register flint-exporter flint-meter; do
  spawn_as "$n"
done
sleep 1
SIB=$(_fleet_sibling)
# SCOPED TO OUR OWN FAKES, not to the whole box.
#
# This asserted `_fleet_sibling` was EMPTY, which quietly assumed no real
# sibling project was running here. That held only while the detector could
# not see one. Once BUG-0036 taught it to match a sibling's cargo target
# path, a genuine flint-kv test binary on the box failed this case — and the
# message blamed OUR binaries and printed a line that was not one of them,
# asserting a cause it had never checked. The question this case asks is
# whether OUR fourteen are misread; answer exactly that.
MINE=$(for p in $PIDS; do printf '%s\n' "$SIB" | awk -v p="$p" '$1 == p'; done)
[ -z "$MINE" ] \
  || { echo "FAIL: our own binaries were classified as another project's:"; echo "$MINE" | sed 's/^/    /'; exit 1; }
echo "  all 14 of our own binaries classified correctly"

echo "== D) ...but they are still caught as FOREIGN when out of scope"
OUT=$(fleet_guard 2>&1); RC=$?
[ "$RC" = 1 ] \
  || { echo "FAIL: guard ignored out-of-scope processes of our own (exit $RC)"; exit 1; }
echo "$OUT" | grep -q "outside $FLINT_DRILL_ROOT/flint-guard-drill" \
  || { echo "FAIL: refused, but not for the out-of-scope reason:"; echo "$OUT"; exit 1; }
echo "  refused for the out-of-scope reason, not the sibling one"

echo "== E) FLINT_DRILL_FORCE still overrides both"
# Set explicitly, so this asserts the override rather than whatever the
# surrounding environment happened to be.
OUT=$(FLINT_DRILL_FORCE=1 fleet_guard 2>&1); RC=$?
[ "$RC" = 0 ] || { echo "FAIL: FORCE did not override (exit $RC)"; echo "$OUT"; exit 1; }
echo "  force proceeds"

echo "PASS: fleet_guard sees sibling projects' fleets, refuses without claiming ownership, does not misread our own binaries, and still honours FORCE"

[ "${A_SKIPPED:-0}" = "1" ] \
  && echo "NOTE: case A (quiet box) was NOT exercised on this run — see above"
