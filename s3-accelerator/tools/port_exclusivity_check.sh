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
# 9312-9314 were bound by this harness and NOT declared here, and check 1 did
# not see them: it scans for host:port literals, and run_suite takes its port as
# a bare argument. 9314 surfaced only because the stage that uses it also passes
# a "http://127.0.0.1:9314" string. So check 1 covers the ports that happen to
# be written one way, which is worth knowing about a check whose job is coverage.
# 9498 is declared even though nothing BINDS it: it is the port the config-reach
# suite points a client at to prove a dead tier degrades. A deliberately-closed
# port is MORE dangerous undeclared than a bound one -- if the sibling harness
# ever bound it, the dead-tier probe would connect to something and pass for the
# wrong reason, which is a silent false green rather than a clash. Declaring it
# is what puts it through check 3.
DECLARED="$(printf '%s\n' 9000 9301 9302 9303 9304 9305 9306 9307 9308 9309 \
                          9310 9311 9312 9313 9314 9318 9319 9397 9398 9399 \
                          9315 9400 9401 9407 9498 9530 9531 9810 | sort -un)"

# 6379 is Redis's standard port and the documented default for the CUSTOMER's
# own endpoint. This harness never binds it -- every suite is passed an explicit
# tier URI -- so it is exempt from the exclusivity assertion rather than
# absent from it. Stated here because an unexplained exemption is how a real
# collision gets waved through later.
EXEMPT="6379"

# ---- 1. the declaration covers what the code actually binds -----------------
# Only port CONTEXTS, so a four-digit constant that is not a port cannot
# masquerade as one.
# This file is excluded from its own scan, and the reason is not convenience:
# it is a file ABOUT ports, so its prose necessarily contains port-shaped text.
# Writing the comment above -- which quotes `-p 6391:6379` while explaining that
# very pattern -- made this check report 6390 and 6391 as ports this harness
# binds. A detector that reads its own explanation as evidence is the same
# defect the comment is warning about, so the exclusion is the honest fix rather
# than rewording the prose until the regex stops noticing it.
# --exclude-dir is not cosmetic. `$ROOT/python` contains `.venv` once anyone
# has run the Python suites -- and CI creates it there too, before the gate --
# so this scanned every installed third-party file and read `:0706`, `:1024`,
# `:5432` out of their text as ports THIS harness binds. 45 phantom ports from
# the venv against 0 from our own Python. The check has been failing on it in
# CI since it was written; the numbers were so obviously not ports that the
# failure read as noise, which is how a red gate stays red.
USED="$(grep -rhoE --exclude=port_exclusivity_check.sh \
          --exclude-dir=.venv --exclude-dir=site-packages --exclude-dir=target -- \
          '(--port|--listen|--upstream|-p) ?[0-9]{4}|:[0-9]{4}\b|redis://[^ "]*:[0-9]{4}' \
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
  # CONSERVATIVE ON PURPOSE: every 4-digit number in 6000-9999 anywhere in the
  # sibling tree counts as theirs, not just the ones matching a port pattern.
  #
  # Three times in one thread a pattern here caught something by luck rather
  # than by design. The last: 6391 was found only because a docker-run COMMENT
  # happened to read `-p 6391:6379`, while 6390 -- the same idiom, same kind of
  # script, no such comment -- was missed entirely. Scripts declare ports as
  # `PORT=${PORT:-6390}`, as `ADDR=host:6390`, in comments, and in ways nobody
  # has thought of yet, and a detector that must anticipate the spelling will
  # keep being right by accident.
  #
  # The asymmetry decides it. A false positive costs one number I then do not
  # use, out of 700 free in my own range. A false negative costs a silent
  # cross-harness collision that presents as somebody else's durability bug.
  # Measured: this reserves 467 numbers, 11.7% of the range, and exactly one of
  # them falls in 9300-9999 where this harness lives.
  #
  # Precision stays where it is useful and harmless -- the failure message says
  # whether a clash is fleet_init-DECLARED, explicitly BOUND, or merely present
  # -- rather than in the detection, where being clever means being wrong.
  THEIRS="$(grep -rhoE '\b[0-9]{4}\b' "$SIB"/*.sh 2>/dev/null \
            | awk '$1 >= 6000 && $1 <= 9999' | sort -un)"
  THEIRS_DECLARED="$(grep -rhoE 'fleet_init[^#]*' "$SIB"/*.sh 2>/dev/null \
                     | grep -oE '\b[0-9]{4}\b' | sort -un)"
  THEIRS_BOUND="$(grep -rhoE -- '--port [0-9]{4}|--listen [0-9]{4}|--target [^ ]*:[0-9]{4}|-p [0-9]{4}' \
                    "$SIB"/*.sh 2>/dev/null | grep -oE '[0-9]{4}' | sort -un)"
  N="$(printf '%s\n' "$THEIRS" | grep -c . || true)"
  [ "${N:-0}" -gt 0 ]
  ck $? "armed: parsed the sibling harness's declarations ($N ports) -- a check "\
"that found ZERO would pass check 3 for the wrong reason"
  CLASH="$(comm -12 <(printf '%s\n' "$DECLARED") <(printf '%s\n' "$THEIRS") | tr '\n' ' ')"
  # Capture the verdict BEFORE printing anything. The diagnostic block below
  # was added after this line and silently disabled the check: an `if`
  # statement sets $?, so `ck $?` afterwards graded the if, not the test, and
  # a real collision printed its explanation and then reported [ok]. Caught
  # only by re-running the armed test after editing the checker -- which is
  # the rule this file now earns twice over: an assertion is only armed as of
  # the last time you watched it fail.
  [ -z "$(echo "$CLASH" | tr -d ' ')" ]; clash_rc=$?
  if [ -n "$(echo "$CLASH" | tr -d ' ')" ]; then
    for c in $CLASH; do
      kind="present in their tree"
      printf '%s\n' "$THEIRS_BOUND" | grep -qx "$c" && kind="BOUND by them"
      printf '%s\n' "$THEIRS_DECLARED" | grep -qx "$c" && kind="fleet_init DECLARED by them"
      echo "     $c -- $kind"
    done
  fi
  ck $clash_rc "no port is claimed by both harnesses${CLASH:+ -- COLLISION: $CLASH}"
else
  echo "[--] sibling harness not present; exclusivity check skipped"
  echo "     (this directory is meant to work standalone, so its absence is"
  echo "      not a failure -- but in-tree it must run)"
fi

echo "--- $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
