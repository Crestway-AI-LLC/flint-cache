#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The ADR-state check, against fixtures (BUG-0262).
#
# `assert_adr_index_markers_agree_with_status` holds each index row's trailing
# state marker against its own write-up's Status line. Added 2026-10-15 after
# FOUR ADRs were found carrying a state that contradicted reality: 0020, 0021
# and 0027 said PROPOSED over code that described what each had removed, and
# 0014's row said `(proposed)` over "Accepted and implemented in full".
#
# This lifts the real function out of tools/gates.sh and runs it over fixture
# trees, so its vocabulary is what the fixtures exercise rather than whatever
# the live index happens to contain -- the gap BUG-0180 recorded for the bug
# checks, which had only ever run against the real tree.
#
# THE 0029 CASE IS THE ONE THAT MATTERS. The first version of the rule took
# the last state parenthetical anywhere in the row, and ADR-0029's cell says
# "**ACCEPTED 2026-09-06** (proposed the same day)" -- prose about an earlier
# state. It read that as the row's claim and reported a contradiction that was
# not there, on its first run. The fixture for it is a regression test for the
# check, not for the repo.
# INDUCED-FAILURE CONTROLS, which is most of what this drill is. Every
# contradiction below is constructed on purpose and the check must NAME it;
# every agreeing shape is constructed on purpose and the check must stay
# SILENT; and the index is deliberately removed to prove a check that cannot
# read its input fails rather than reporting nothing wrong. A check whose
# controls are only agreeing cases cannot tell "nothing is wrong" from
# "nothing was examined".
set -uo pipefail
cd "$(dirname "$0")/.."
fail() { echo "ADR STATE MARKERS DRILL FAILED: $*"; exit 1; }

FN=assert_adr_index_markers_agree_with_status
body=$(sed -n "/^$FN() {/,/^}/p" tools/gates.sh)
[ -n "$body" ] || fail "could not lift $FN out of tools/gates.sh -- an extraction that finds nothing checks nothing"
eval "$body"

W=$(mktemp -d "${FLINT_DRILL_ROOT:-/tmp}/adrstate.XXXXXX") || fail "mktemp gave nothing"
trap 'rm -rf "$W"' EXIT

tree() {  # tree <name> -> a fresh fixture tree with an index header
  mkdir -p "$W/$1/docs/adr"
  printf '# ADRs\n\n| ADR | what |\n|---|---|\n' > "$W/$1/docs/adr/README.md"
}
adr() {   # adr <tree> <nnnn> <status-line>
  printf '# ADR-%s: fixture\n\n%s\n\nBody.\n' "$2" "$3" > "$W/$1/docs/adr/$2-fixture.md"
}
row() {   # row <tree> <nnnn> <cell>
  printf '| [%s](%s-fixture.md) | %s |\n' "$2" "$2" "$3" >> "$W/$1/docs/adr/README.md"
}
run() {   # run <tree> -> the check's output, from inside that tree
  ( cd "$W/$1" && $FN 2>&1 )
}

named() {   # named <tree> <nnnn> <what>
  local out; out=$(run "$1")
  case "$out" in
    *DISAGREE*) ;;
    *) fail "$3: the check stayed SILENT. A contradiction it cannot name is one it does not check.
$out" ;;
  esac
  case "$out" in
    *"$2"*) ;;
    *) fail "$3: named a contradiction but not ADR-$2.
$out" ;;
  esac
  echo "   ok -- named: $3"
}
silent() {  # silent <tree> <what>
  local out; out=$(run "$1")
  case "$out" in
    *DISAGREE*) fail "$2: the check FIRED on an agreeing tree. A check that cries wolf is switched off within a week.
$out" ;;
  esac
  echo "   ok -- silent: $2"
}

echo "== contradictions, both directions, must be named"

tree a; adr a 0101 'Status: **ACCEPTED 2026-01-01.**'; row a 0101 'a settled ADR (proposed)'
named a 0101 "(proposed) over a Status of ACCEPTED"

tree b; adr b 0102 '**Status:** proposed'; row b 0102 'a proposed ADR (implemented)'
named b 0102 "(implemented) over a Status of proposed -- ADR-0027's shape"

tree c; adr c 0103 'Status: **Accepted and implemented in full — D1, D2, D3.**'; row c 0103 'as 0014 read (proposed)'
named c 0103 "(proposed) over 'Accepted and implemented in full' -- ADR-0014's shape"

echo "== agreement and legitimate prose must stay silent"

tree d; adr d 0104 'Status: PROPOSED — August 2026.'; row d 0104 'still a design (proposed)'
silent d "(proposed) over PROPOSED"

tree e; adr e 0105 'Status: **ACCEPTED 2026-10-12** (Jeff).'; row e 0105 'decided (accepted 2026-10-12, option A)'
silent e "(accepted ...) over ACCEPTED"

tree f; adr f 0106 '**Status:** **ACCEPTED 2026-09-06** (proposed the same day).'
row f 0106 'ACCEPTED on its gating measurement (proposed the same day), then amended'
silent f "a history parenthetical mid-cell, no trailing marker -- ADR-0029's shape"

tree g; adr g 0107 'Status: PROPOSED, 2026-08-26.'; row g 0107 'carries no state word (amended — the threshold now derives from observed bytes)'
silent g "a trailing parenthetical whose first word is not a state word"

tree h; adr h 0108 'Status: **ACCEPTED 2026-09-27**; built.'; row h 0108 'built then revisited (reopened 2026-10-08)'
silent h "(reopened ...) -- in neither vocabulary on purpose"

tree i; adr i 0109 'Status: PROPOSED.'; row i 0109 'no parenthetical at all'
silent i "an unmarked row -- 26 of 35 live rows are unmarked and that is honest"

echo "== a check that cannot read its input must say so, not pass"
mkdir -p "$W/j/docs/adr"
out=$( cd "$W/j" && $FN 2>&1 )
case "$out" in
  *"unreadable"*|*NOINDEX*) echo "   ok -- a missing index FAILS rather than reporting nothing wrong" ;;
  *) fail "a missing docs/adr/README.md did not fail: $out" ;;
esac

echo "== the live tree"
$FN || fail "the real docs/adr tree disagrees with itself"

echo "ADR STATE MARKERS DRILL PASSED"
