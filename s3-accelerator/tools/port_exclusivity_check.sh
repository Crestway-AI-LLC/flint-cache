#!/usr/bin/env bash
# Do this harness's ports belong to it alone?
#
# Written after claiming an empty intersection with a sibling harness and being
# wrong. The claim came from `grep -E "(700[0-9]|7[45][0-9][0-9]|7379|9464)"` --
# a pattern listing the ports I already believed were in use, which therefore
# found exactly those and nothing else. A check whose pattern encodes the answer
# cannot fail. The real span was 6300-9999 across 454 declared ports, and two of
# them were mine.
#
# The collision mattered in both directions, and one of them acts rather than
# reports: this harness ends by shutting down whatever answers on the tier port,
# which is a clean RESP shutdown -- so stopping a neighbour's node would not
# look like a kill in their logs, it would look like their seat exiting for no
# reason.
#
# So: DECLARE the ports, then assert the declaration matches reality on both
# sides. Same shape as the sibling harness's own rule that every data dir must
# sit under a declared scope.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PASS=0; FAIL=0
ck() { if [ "$1" = 0 ]; then PASS=$((PASS+1)); printf "[ok] %s\n" "$2";
       else FAIL=$((FAIL+1)); printf "[FAIL] %s\n" "$2"; fi; }

# ---- the declaration -------------------------------------------------------
# Every port this harness binds. Adding one without adding it here is the
# failure the first check below exists to catch.
DECLARED="$(printf '%s\n' 9000 9301 9302 9303 9304 9305 9306 9307 9308 9309 \
                          9310 9311 9318 9319 9398 9399 9400 9401 9407 9530 \
                          9531 9810 | sort -un)"

# 6379 is Redis's standard port and the documented default for the CUSTOMER's
# own endpoint. This harness never binds it -- every suite is passed an explicit
# tier URI -- so it is exempt from the exclusivity assertion rather than
# absent from it. Stated here because an unexplained exemption is how a real
# collision gets waved through later.
EXEMPT="6379"

# ---- 1. the declaration covers what the code actually binds -----------------
# Only port CONTEXTS, so a four-digit constant that is not a port cannot
# masquerade as one.
USED="$(grep -rhoE -- '(--port|--listen|--upstream|-p) ?[0-9]{4}|:[0-9]{4}\b|redis://[^ "]*:[0-9]{4}' \
          "$ROOT/tools" "$ROOT/python" "$ROOT/jvm-spike/src" 2>/dev/null \
        | grep -oE '[0-9]{4}' | sort -un)"
UNDECLARED="$(comm -23 <(printf '%s\n' "$USED") \
                    <(printf '%s\n' "$DECLARED" "$EXEMPT" | sort -un) | tr '\n' ' ')"
[ -z "$(echo "$UNDECLARED" | tr -d ' ')" ]
ck $? "every port the code binds is declared${UNDECLARED:+ -- UNDECLARED: $UNDECLARED}"

# ---- 2. the declaration does not intersect the sibling harness --------------
# Parsed from fleet_init lines GENERICALLY -- every number on the line, not a
# list of numbers we expected to find. That distinction is the whole lesson.
SIB="$ROOT/../tools"
if ls "$SIB"/*_drill.sh >/dev/null 2>&1; then
  THEIRS="$(grep -rhoE 'fleet_init[^#]*' "$SIB"/*.sh 2>/dev/null \
            | grep -oE '\b[0-9]{4}\b' | sort -un)"
  N="$(printf '%s\n' "$THEIRS" | grep -c . || true)"
  [ "${N:-0}" -gt 0 ]
  ck $? "armed: parsed the sibling harness's declarations ($N ports) -- a check "\
"that found ZERO would pass check 3 for the wrong reason"
  CLASH="$(comm -12 <(printf '%s\n' "$DECLARED") <(printf '%s\n' "$THEIRS") | tr '\n' ' ')"
  [ -z "$(echo "$CLASH" | tr -d ' ')" ]
  ck $? "no port is claimed by both harnesses${CLASH:+ -- COLLISION: $CLASH}"
else
  echo "[--] sibling harness not present; exclusivity check skipped"
  echo "     (this directory is meant to work standalone, so its absence is"
  echo "      not a failure -- but in-tree it must run)"
fi

echo "--- $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
