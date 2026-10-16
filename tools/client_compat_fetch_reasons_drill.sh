#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Every dependency fetch in client_compat_drill.sh keeps the reason it failed.
#
# BUG-0263. gates.sh reds a gate when a client skips, so a skip is a verdict,
# and on 2026-10-16 one carried no evidence: "could not fetch the module;
# offline?" -- question mark included -- because the fetch ran under
# `>/dev/null 2>&1`. The cause had to be reconstructed from which of the other
# 18 clients survived.
#
# Two independent mechanisms were throwing the reason away, which is why this
# drill asserts two rules rather than one:
#
#   A. the redirect: a fetch under >/dev/null has nothing to capture.
#   B. the fetch's own quiet flag. MEASURED 2026-10-16: `npm install --silent`
#      and `gem install --silent` emit ZERO BYTES on a failed install. So
#      removing only the redirect would leave a skip that prints "(nothing)" --
#      a fix that looks applied and is not. pip's -q is safe (keeps its ERROR
#      line) and is deliberately NOT banned here.
#   C. and the skip itself must go through skip_with_reason, so that the next
#      client added cannot reintroduce a reasonless skip by copying a neighbour.
#
# INDUCED-FAILURE CONTROLS: each rule is run against a deliberately broken
# copy of the drill and must NAME the site it breaks -- a positive control,
# because a matcher that finds nothing agrees with everything. The helper
# itself is then exercised live, on a real failing command and on an empty
# capture, since "the reason is empty" is the exact shape that started this.
#
# Offline by design: no fetch runs here. It must pass on the gate box, which
# is the machine that could not fetch in the first place.
set -u
cd "$(dirname "$0")/.."
SRC=tools/client_compat_drill.sh
W=$(mktemp -d); trap 'rm -rf "$W"' EXIT
RC=0
fail() { echo "FAIL: $*"; RC=1; }

[ -f "$SRC" ] || { echo "FAIL: $SRC is missing"; exit 1; }

# --- the three matchers, each over one file, each printing the lines it finds --
# The binary is usually a variable -- "$GO" get, "$VENV/bin/pip" install,
# "$(dirname "$RUBY")/gem" install -- so a pattern written as `go get` or
# `pip install` matches NONE of the real sites. The positive controls below
# caught exactly that: the first draft of this rule was silent on the real
# file because it was looking for a spelling the file never uses.
FETCH_VERB='pip install|pip" install|npm install|npm init|gem install|gem" install|go get|" get |go mod |" mod tidy|" mod init|-m venv'
rule_a() {   # a fetch that discards its output
  grep -nE "$FETCH_VERB" "$1" | grep -v '^[0-9]*:[[:space:]]*#' | grep '>/dev/null'
}
rule_b() {   # an npm or gem fetch still carrying --silent
  grep -nE 'npm install|gem install' "$1" | grep -v '^[0-9]*:[[:space:]]*#' | grep -- '--silent'
}
rule_c() {   # a fetch-failure skip printed by a bare echo
  grep -n 'SKIP (could not' "$1" | grep -v '^[0-9]*:[[:space:]]*#' | grep 'echo'
}

# --- the pattern must still SEE the fetches -------------------------------
# An un-run check and a clean result look identical. If FETCH_VERB stops
# matching -- a rename, a new package manager, a refactor -- rule a goes quiet
# and reads as "no site discards its output". So count the stem bare first.
VERB_HITS=$(grep -cE "$FETCH_VERB" "$SRC")
MIN_SITES=14
[ "$VERB_HITS" -ge "$MIN_SITES" ] || fail "the fetch pattern matches only
     $VERB_HITS lines of $SRC, below the $MIN_SITES known fetch lines. Rule a
     is examining almost nothing, so its silence means nothing. Widen
     FETCH_VERB (or lower MIN_SITES, with the reason)."
echo "  ok  the fetch pattern sees $VERB_HITS lines of $SRC (>= $MIN_SITES)"

# --- negative control: the real file must be silent on all three -------------
for r in a b c; do
  out=$(rule_$r "$SRC") && {
    fail "rule $r fires on the real $SRC:"; printf '%s\n' "$out" | sed 's/^/     /'; }
done

# --- positive controls: each rule must NAME its induced break ----------------
probe() {  # probe <rule> <description> <line to append>
  cp "$SRC" "$W/broken.sh"
  printf '%s\n' "$3" >> "$W/broken.sh"
  out=$(rule_"$1" "$W/broken.sh")
  if [ -z "$out" ]; then
    fail "POSITIVE CONTROL rule $1 ($2): the induced break was NOT named.
     The rule agrees with everything, so its silence above proves nothing.
     Induced line: $3"
  else
    echo "  ok  rule $1 names an induced $2"
  fi
}
probe a "fetch under >/dev/null"        '    (cd "$D" && npm install leftpad --loglevel=error >/dev/null 2>&1)'
probe a "go get under >/dev/null"       '    "$GO" get example.com/x >/dev/null 2>&1'
probe a "pip under >/dev/null"          '    "$V/bin/pip" install -q thing >/dev/null 2>&1'
probe a "gem under >/dev/null"          '    "$(dirname "$RUBY")/gem" install --no-document thing >/dev/null 2>&1'
probe a "venv under >/dev/null"         '    "$BASE" -m venv "$VENV" >/dev/null 2>&1'
probe b "npm --silent"                  '    (cd "$D" && npm install leftpad --silent)'
probe b "gem --silent"                  '    gem install --no-document --silent thing'
probe c "reasonless skip"               '    echo "== newclient: SKIP (could not fetch the module; offline?)"'

# A fetch that captures correctly must NOT trip rule a -- the rules have to
# tell the fixed shape from the broken one, or they only measure file length.
cp "$SRC" "$W/ok.sh"
cat >> "$W/ok.sh" <<'FIXED'
    NEW_ERR=$( (cd "$D" && npm install leftpad --loglevel=error) 2>&1 )
    skip_with_reason newclient "could not install" "${NEW_ERR-}"
FIXED
for r in a b c; do
  out=$(rule_$r "$W/ok.sh") && {
    fail "rule $r fires on a CORRECTLY written new site -- it would red the
     gate for doing the right thing:"; printf '%s\n' "$out" | sed 's/^/     /'; }
done
echo "  ok  all three rules stay silent on a correctly written new site"

# --- the helper itself, exercised live --------------------------------------
# Lifted from the drill rather than copied, so this tests the shipped helper.
sed -n '/^FETCH_TAIL=/,/^}$/p' "$SRC" > "$W/helper.sh"
grep -q 'skip_with_reason()' "$W/helper.sh" || { echo "FAIL: could not lift skip_with_reason from $SRC"; exit 1; }
# shellcheck disable=SC1090
. "$W/helper.sh"

CAP=$( ls /nonexistent-flint-0263-path 2>&1 )
[ -n "$CAP" ] || { echo "FAIL: the control command produced no output, so the
     next assertion would pass for the wrong reason"; exit 1; }
out=$(skip_with_reason newclient "could not install" "$CAP")
grep -q 'nonexistent-flint-0263-path' <<<"$out" \
  || fail "the captured reason did not reach the skip message:
$(printf '%s\n' "$out" | sed 's/^/     /')"
grep -q '^== newclient: SKIP (could not install)\. The fetch said:$' <<<"$out" \
  || fail "the skip line is not in the shape gates.sh and a reader expect:
$(printf '%s\n' "$out" | sed 's/^/     /')"
grep -q '^     ' <<<"$out" \
  || fail "the reason is not indented, so it reads as drill output, not as a quote"
echo "  ok  a real failure's reason reaches the skip message, indented"

out=$(skip_with_reason newclient "could not install" "")
grep -q 'silenced, not quiet' <<<"$out" \
  || fail "an EMPTY capture did not say so -- this is the shape that cost the
     2026-10-16 verdict, and it must not render as a clean skip:
$(printf '%s\n' "$out" | sed 's/^/     /')"
echo "  ok  an empty capture says it was silenced rather than printing nothing"

# tail -N must actually bound the quote: a 200-line failure should not paste
# 200 lines into a gate log.
LONG=$(seq 1 200)
n=$(skip_with_reason x y "$LONG" | grep -c '^     ')
[ "$n" -le 7 ] || fail "the quote is not bounded: $n indented lines from a 200-line capture"
grep -q '     200$' <<<"$(skip_with_reason x y "$LONG")" \
  || fail "the quote kept the HEAD of the capture; the error is at the END"
echo "  ok  the quote is bounded ($n lines) and keeps the tail, where the error is"

# --- count what is covered, so the number is in the log ----------------------
SITES=$(grep -cE '_ERR=\$\(' "$SRC")
USES=$(grep -c 'skip_with_reason ' "$SRC")
echo
if [ "$RC" -eq 0 ]; then
  echo "PASS: every dependency fetch in $SRC keeps its reason"
  echo "      ($SITES capturing fetch sites, $((USES - 1)) skips quoting them;"
  echo "       3 rules, each proved able to fail)"
else
  echo "FAILED: client_compat fetch reasons"
fi
exit $RC
