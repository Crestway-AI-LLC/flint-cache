#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The two bug-state checks, against fixtures (docs/bugs/0180-an-open-marker-with-punctuation-was-not-a-marker.md).
#
# `assert_bug_index_markers_agree_with_status` and
# `assert_bug_titles_agree_with_status` had only ever run against the real
# index, so their vocabulary was whatever the index happened to exercise. A row
# marker counted as OPEN only by equality, while closed words matched by
# prefix, so "(OPEN, ...)" was not a marker at all. The ops repo's copy also
# lacked DONE and BUILT and let a row say OPEN for a month over a deployed fix
# (ops OPS-0324).
#
# This lifts BOTH real functions out of gates.sh and runs them over a fixture
# docs/bugs tree. Every contradiction shape must be named, every agreeing shape
# must not, and a clean tree must pass. This index reads a marker in the TITLE
# cell before one elsewhere in the row, and the fixtures respect that.
set -uo pipefail
cd "$(dirname "$0")/.."
fail() { echo "BUG STATE MARKERS DRILL FAILED: $*"; exit 1; }

FNS=""
for fn in assert_bug_index_markers_agree_with_status assert_bug_titles_agree_with_status; do
  body=$(sed -n "/^$fn() {/,/^}/p" tools/gates.sh)
  [ -n "$body" ] || fail "could not lift $fn out of tools/gates.sh -- an extraction that finds nothing checks nothing"
  FNS="$FNS$body
"
done
eval "$FNS"

W=$(mktemp -d "${FLINT_DRILL_ROOT:-/tmp}/bugstate.XXXXXX") || fail "mktemp gave nothing"
trap 'rm -rf "$W"' EXIT

bug() {  # bug <tree> <nnnn> <h1-suffix> <status-line>
  printf '# BUG-%s: fixture%s\n\nStatus: %s\n\nBody.\n' "$2" "$3" "$4" \
    > "$W/$1/docs/bugs/$2-fixture.md"
}
row() {  # row <tree> <nnnn> <title> <evidence>
  printf '| BUG-%s | %s | %s |\n' "$2" "$3" "$4" >> "$W/$1/docs/bugs/README.md"
}
tree() {  # tree <name> -> a fresh fixture tree with an index header
  mkdir -p "$W/$1/docs/bugs"
  printf '# Bugs\n\n| id | title | evidence |\n|---|---|---|\n' > "$W/$1/docs/bugs/README.md"
}
check() {  # check <name> -> both checks' output, FAILED left in $W/<name>.failed
  ( cd "$W/$1" || exit 1; FAILED=""; assert_bug_index_markers_agree_with_status
    assert_bug_titles_agree_with_status; printf '%s' "$FAILED" > "$W/$1.failed" )
}

# --- contradictions: every one must be named ---------------------------------
tree bad
bug bad 0001 "" "BUILT 2026-01-01 and deployed."
row bad 0001 "a built fix" "evidence (OPEN — build after rc.1)"
bug bad 0002 "" "FIXED 2026-01-02. Both halves."
# No title marker, so the evidence decides, and its LAST marker is the one: an
# earlier FIXED must not hide a later OPEN-with-a-comma. Without a title
# marker and with only the OPEN, the unmarked-row rule would catch it anyway,
# which would make this fixture blind to the change it exists for.
row bad 0002 "an OPEN after a FIXED, no title marker" "evidence (FIXED 2026-01-02) and later (OPEN, mechanism confirmed)"
bug bad 0003 "" "DONE 2026-01-03."
row bad 0003 "a done fix with no marker" "evidence"
bug bad 0004 "" "OPEN, found 2026-01-04."
row bad 0004 "a row that closed too early (DONE 2026-01-04)" "evidence"
bug bad 0005 " (OPEN)" "DONE 2026-01-05."
bug bad 0006 " (BUILT 2026-01-06)" "OPEN, the build was reverted."

OUT=$(check bad) || true
FAILED_BAD=$(cat "$W/bad.failed")
for f in 0001 0002 0003 0004; do
  printf '%s\n' "$OUT" | grep -q "^        $f-fixture.md$" \
    || fail "the index check did not name $f:
$OUT"
done
for f in 0005 0006; do
  printf '%s\n' "$OUT" | grep -q "$f-fixture.md" \
    || fail "the title check did not name $f:
$OUT"
done
case "$FAILED_BAD" in *bug-index-markers-contradict-status*) ;; *) fail "the index check named rows but did not fail: FAILED='$FAILED_BAD'" ;; esac
case "$FAILED_BAD" in *bug-titles-contradict-status*) ;; *) fail "the title check named files but did not fail: FAILED='$FAILED_BAD'" ;; esac
echo "   ok — named: BUILT under (OPEN — …), a later (OPEN, …) over FIXED, DONE with no marker, (DONE) over OPEN, title (OPEN) over DONE, title (BUILT) over OPEN"

# --- agreement: none of these may be named ------------------------------------
tree good
bug good 0011 " (DONE 2026-02-01)" "DONE 2026-02-01."
row good 0011 "a done fix (DONE 2026-02-01)" "evidence"
bug good 0012 " (BUILT 2026-02-02, ships with the next release)" "BUILT 2026-02-02, not in the last release."
row good 0012 "a built fix (BUILT 2026-02-02, ships with the next release)" "evidence"
bug good 0013 "" "OPEN, found 2026-02-03."
row good 0013 "an open one (OPEN)" "evidence"
bug good 0014 "" "OPEN, found 2026-02-04."
row good 0014 "an open one with no marker" "evidence"
bug good 0015 "" "FIXED 2026-02-05."
row good 0015 "a fixed one (FIXED 2026-02-05)" "evidence that quotes its history (OPEN, mechanism confirmed)"

OUT=$(check good) || true
FAILED_GOOD=$(cat "$W/good.failed")
[ -z "$FAILED_GOOD" ] || fail "a tree where every row and title agrees failed: FAILED='$FAILED_GOOD'
$OUT"
printf '%s\n' "$OUT" | grep -q "every bug index row agrees" \
  || fail "the clean tree did not report its comparison -- the index check examined nothing:
$OUT"
echo "   ok — silent: DONE/DONE, BUILT/BUILT, OPEN/OPEN, unmarked over OPEN, a FIXED title over quoted OPEN history"

echo "BUG STATE MARKERS DRILL PASSED"
