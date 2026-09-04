#!/usr/bin/env bash
# Copyright (c) 2026 Crestway AI LLC. All rights reserved.
#
# OPS-0122 — the induced-control ratchet must refuse a REMOVAL, tolerate an
# addition, and refuse to run on nothing.
#
# A ratchet nobody tests is a counter. The three behaviours below are the whole
# contract, and the third is the one that makes it honest: if the glob stops
# matching the corpus, the check has examined nothing, and "nothing < floor" is
# indistinguishable from "the drills moved" unless it says so.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d "${FLINT_DRILL_ROOT:-/tmp}/flint-ratchet-XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
fail() { echo "INDUCED RATCHET DRILL FAILED: $*"; exit 1; }

# The function under test, lifted from the gate it lives in.
FN="$WORK/fn.sh"
sed -n '/^assert_induced_controls_have_not_regressed()/,/^}/p' "$ROOT/tools/gates.sh" > "$FN"
[ -s "$FN" ] || fail "could not extract the function from gates.sh -- this drill examined nothing"
grep -q "induced-control-floor" "$FN" || fail "extracted the wrong block"

# <n_with_control> <n_plain> <floor>  -> prints the check's output, sets RC
run_case() {
  local w="$WORK/case"; rm -rf "$w"; mkdir -p "$w/tools"
  local i
  # NOT `seq 1 $n`: BSD seq counts DOWN when the end is below the start, so
  # `seq 1 0` yields "1 0" on macOS and nothing on Linux -- the zero case would
  # then build two files on the laptop and none on the gate box, and the
  # examines-nothing assertion would pass for the wrong reason on one of them.
  i=1; while [ "$i" -le "$1" ]; do printf '# mutation control\n' > "$w/tools/c${i}_drill.sh"; i=$((i+1)); done
  i=1; while [ "$i" -le "$2" ]; do printf '# nothing special\n' > "$w/tools/p${i}_drill.sh"; i=$((i+1)); done
  [ -n "$3" ] && echo "$3" > "$w/tools/induced-control-floor.txt"
  ( cd "$w" && FAILED=""; . "$FN"; assert_induced_controls_have_not_regressed; echo "FAILED=[$FAILED]" )
}

echo "== a removal must FAIL"
out="$(run_case 5 5 7)"
grep -q "induced-control-regressed" <<<"$out" || fail "a drop from 7 to 5 did not fail:
$out"

echo "== steady state must be silent"
out="$(run_case 7 3 7)"
grep -q "FAILED=\[\]" <<<"$out" || fail "n == floor should pass:
$out"
grep -q "NOTE" <<<"$out" && fail "n == floor should not print a NOTE:
$out"

echo "== an addition must NOTE, never fail"
out="$(run_case 9 2 7)"
grep -q "FAILED=\[\]" <<<"$out" || fail "n > floor must NOT fail -- adding a drill
        would redden someone else's gate:
$out"
grep -q "can be raised to 9" <<<"$out" || fail "n > floor should say the floor can rise:
$out"

echo "== examining nothing must FAIL, not pass"
out="$(run_case 0 0 7)"
grep -q "induced-control-examined-nothing" <<<"$out" \
  || fail "an empty corpus did not fail -- a matcher that finds nothing agreed
        with everything:
$out"

echo "== a missing floor must FAIL, not default to zero"
out="$(run_case 5 5 "")"
grep -q "induced-control-no-floor" <<<"$out" \
  || fail "a missing floor file did not fail:
$out"

echo "INDUCED RATCHET DRILL PASSED"
