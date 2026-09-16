# SPDX-License-Identifier: Elastic-2.0
# Which ports each drill declares to fleet_init — ONE implementation.
#
# Two things need this answer and must never disagree: the gate's
# assert_no_duplicate_drill_ports, which refuses a collision, and
# tools/next-free-ports.sh, which suggests where to put a new drill. A helper
# that suggested a port the gate then rejected would be worse than no helper,
# so they read the same function rather than each carrying a copy.
#
# DEFINITIONS ONLY, deliberately. tools/lib/fleet.sh cannot serve this purpose:
# sourcing it exports FLINT_DRILL_ROOT, creates the directory and `exit 1`s if
# it is not writable, so pulling it into a static gate check would change WHEN
# the gate fails. This file does nothing when sourced.

# drill_declared_ports [dir]  ->  lines of "<port> <drill>"
#
# Continuations are joined FIRST. Several drills wrap their fleet_init argument
# list with a trailing backslash, and reading only the first physical line
# silently drops every port after the break — an under-count that reads exactly
# like "no collision".
# gates.sh IS IN THE LIST, and is not a drill. Its conformance stage starts
# three seats and so claims ports exactly as a drill does. Left out, the
# collision checks could not see the harness's own claims: conformance sat on
# 6397-6399, inside edge_reroute's 6398-6401 and on fullsync_rate's 6397, and
# no check could say so because nothing scanned the file the claim was in.
#
# ANCHORED, and that is not cosmetic. The unanchored match reported gates.sh as
# declaring 7001 — picked out of assert_no_default_ports, whose grep PATTERN
# contains the literal text `fleet_init .*(7001|7002|7379|7500)`. A scanner
# that reads its own sibling check's regex as a declaration would mark the
# default ports claimed and collide with every drill that legitimately uses
# them. Requiring the line to BEGIN with fleet_init (indented or not) is what
# separates a declaration from a mention of one.
# _fleet_init_lines_in <file>  ->  "<lineno>:<joined declaration>"
#
# THE ONE PREPROCESSING STEP every fleet_init question needs, and the reason it
# is a function rather than three greps: both details below are load-bearing,
# each was learned from a real miss, and a hand-written scan gets them wrong
# (BUG-0156 measured five consumers, three of which did).
#
#   - CONTINUATIONS ARE JOINED FIRST. Several drills wrap their argument list
#     with a trailing backslash, and reading only the first physical line drops
#     every port after the break -- an under-count that reads exactly like "no
#     collision".
#   - THE LINE MAY BE INDENTED, and it must BEGIN with fleet_init. Requiring
#     column 0 misses an indented declaration; not anchoring at all reads a
#     sibling check's own grep PATTERN as a declaration, which is how gates.sh
#     was once reported as declaring 7001.
_fleet_init_lines_in() {
  sed -e :a -e '/\\$/N; s/\\\n//; ta' "$1" 2>/dev/null \
    | grep -nE '^[[:space:]]*fleet_init '
}

# drill_fleet_init_lines [dir]  ->  "<file>:<lineno>:<joined declaration>"
#
# Same shape `grep -n` over the glob would print, so a check that wants to
# report WHERE can use it directly.
drill_fleet_init_lines() {
  local dir="${1:-tools}" f
  for f in "$dir"/*_drill.sh "$dir"/gates.sh; do
    [ -f "$f" ] || continue
    _fleet_init_lines_in "$f" | sed "s|^|$f:|"
  done
}

# drill_declared_scopes [dir]  ->  lines of "<scope> <drill>"
#
# The OTHER half of `_fleet_ours`'s ownership test: it takes a pid if the ps
# line carries the scope dir OR a declared port, so two drills sharing a scope
# select each other's seats even with disjoint port blocks.
#
# DRILLS ONLY, and that difference from `drill_declared_ports` is deliberate
# rather than an oversight: gates.sh declares `fleet_init "$CDIR"`, a variable,
# so its scope has no literal to compare against another drill's literal.
# `assert_declared_scopes_cover_data_dirs` resolves it and therefore reads the
# lines itself.
drill_declared_scopes() {
  local dir="${1:-tools}" f d
  for f in "$dir"/*_drill.sh; do
    [ -f "$f" ] || continue
    d=$(basename "$f" _drill.sh)
    _fleet_init_lines_in "$f" \
      | sed 's|^[0-9]*:||' \
      | awk '{print $2}' \
      | sort -u \
      | while read -r s; do printf '%s %s\n' "$s" "$d"; done
  done
}

drill_declared_ports() {
  local dir="${1:-tools}" f d
  for f in "$dir"/*_drill.sh "$dir"/gates.sh; do
    [ -f "$f" ] || continue
    case "$f" in
      *_drill.sh) d=$(basename "$f" _drill.sh) ;;
      *)          d=gates-conformance ;;
    esac
    _fleet_init_lines_in "$f" \
      | grep -oE 'fleet_init [^;&|]+' \
      | grep -oE '\b[0-9]{4,5}\b' \
      | sort -u \
      | while read -r p; do printf '%s %s\n' "$p" "$d"; done
  done
}

# Ports that must stay DEAD — nothing may ever bind them.
#
# Several drills point something at an address deliberately chosen to be
# unreachable: `FLINTSLOTFREEZE <slot> 127.0.0.1:6999` in backup and
# txn_failure, and an unreachable control plane in config_drift. The assertion
# under test is "this fails the way an absent peer fails".
#
# That only holds while nothing listens there. If a future drill were handed
# 6999 as a free port, those three would quietly stop testing unreachability
# and start testing something else entirely, while still passing. So the set is
# named here: the allocator never hands them out, and the gate does not report
# them as undeclared usage.
# 7788/7789 join the set for a different reason worth stating. They appear only
# in the argv of fleet_guard case G's FAKE peer controller; nothing binds them.
# DECLARING them to fleet_init was tried and reverted: the declaration is also
# the ownership key, so _fleet_ours then claimed the fake as OURS, and case G's
# negative control — "with the peer's ports removed, its path-less seat must
# read as foreign again" — could no longer fire. The check that caught it was
# the drill itself, on the next gate.
# Ports bound ELSEWHERE IN THE REPO by things that are not drills.
#
# For the ALLOCATOR ONLY, and the distinction is deliberate. The gate's
# assertions police tools/ — a drill's declaration is a contract with
# _fleet_ours and with the other drills. s3-accelerator/ is a separate product
# surface with its own gate, its own CI workflow and its own owners; making
# assert_no_duplicate_drill_ports scan it would be this suite passing judgement
# on a tree it does not own.
#
# But handing out a port that something in this repo is ALREADY BINDING is a
# different question, and the answer is simply no. That was not hypothetical:
# next-free-ports.sh gave out 6388-6390 for the conformance stage while the
# accelerator's gate took 6391 for its tier. Adjacent by luck. Ask for four
# ports instead of three that afternoon and the allocator would have handed
# over a port already in use, and the collision would have surfaced as one of
# the two gates failing for reasons neither could see.
#
# So the allocator RESERVES these and asserts nothing about them.
# drill_kill_prefixes [dir]  ->  lines of "<prefix> <drill>"
#
# The SECOND population the gate refuses on, and the one `next-free-ports.sh`
# did not know about (BUG-0154). A truncated port in a pkill pattern is a
# SUBSTRING match: `pkill -f "flint-server --port 644"` reaches 6440-6449, so a
# drill declaring 6442 is one parallel batch away from being SIGKILLed by a
# stranger. `assert_no_cross_drill_kill_patterns` refuses that, and the
# allocator has to avoid suggesting into it -- which is the same argument the
# top of this file makes about `drill_declared_ports`, so it gets the same
# treatment: one function, both callers.
#
# COMMENTS ARE NOT CALL SITES. `controlplane_drill` and `lease_drill` both quote
# the pattern they used to have, inside the comment explaining why it was wrong.
# Matching those would refuse the tree over the write-up of a fix rather than
# over a defect -- a check that cannot tell a cure from a disease.
#
# The pattern is the LAST quoted word on the line, and only `--port NNN` forms
# are returned: a pattern naming a path under the drill's own scratch root is
# already scoped and reaches nobody.
drill_kill_prefixes() {
  local dir="${1:-tools}" f d
  for f in "$dir"/*_drill.sh; do
    [ -f "$f" ] || continue
    d=$(basename "$f" _drill.sh)
    grep -v '^[[:space:]]*#' "$f" 2>/dev/null \
      | grep -hoE 'pkill[^|]*"[^"]*--port [0-9]{1,5}"' \
      | grep -oE -- '--port [0-9]{1,5}' \
      | awk '{print $2}' \
      | sort -u \
      | while read -r p; do printf '%s %s\n' "$p" "$d"; done
  done
}

repo_bound_ports() {
  local root="${1:-.}"
  # COMMENTS STRIPPED FIRST, and that is not hygiene. The first version of this
  # reserved 6391 on the strength of a COMMENT in the accelerator's own port
  # check -- prose quoting `-p 6391:6379` while explaining this exact trap. The
  # port was never bound by anything. Worse, I then wrote a control that
  # "proved" the allocator now avoids 6391 and reported it as a save; it was
  # demonstrating a false positive.
  #
  # AND NO SPELLING ANTICIPATION. The first version matched only flagged or
  # host-prefixed forms (--port N, 127.0.0.1:N), and missed seven live ports
  # spelled as bare positional arguments -- `run_suite "..." <class> 9301
  # "$CP"` and `PORT=9407`. Guessing the next spelling is the losing half of
  # this game, so this takes every integer on a non-comment line instead.
  #
  # OVER-RESERVING IS FREE, UNDER-RESERVING IS NOT: an allocator that skips a
  # port costs one slot out of thousands; one that hands out a bound port costs
  # a collision in two suites that cannot see each other. So the filter is a
  # RANGE rather than a pattern, bounded to what this allocator could ever
  # suggest anyway -- 6300..9999, matching next-free-ports.sh's own BASE..MAX.
  # A literal outside that window cannot be handed out, so it does not matter
  # whether it was a port at all.
  #
  # TWO ROOTS, AND THE SECOND WAS THE HOLE. This read only the accelerator
  # subtree, so a port bound by a NON-DRILL script in tools/ was invisible to
  # both halves of the allocator: `drill_declared_ports` reads `fleet_init`
  # declarations, and drills are the only things that declare. Nine ports were
  # in that gap -- among them 6391, which `redisbloom_compare.sh` binds as
  # `PORT=${PORT:-6391}` and then starts a server on.
  #
  # Found 2026-09-15 the expensive way: the allocator offered 6391 for a new
  # drill, and the only reason it was not taken is that a COMMENT in
  # `flintinfo_numeric_drill.sh` -- prose, in another file, written by someone
  # who had moved off that port -- said the 639x block was claimed. An
  # allocator that is correct only when a reader happens to remember a comment
  # elsewhere is not an allocator.
  #
  # `*_drill.sh` is excluded because `drill_declared_ports` already reads those
  # precisely, from the declaration that is their contract. Everything else in
  # tools/ gets the same blunt treatment as the accelerator, for the reason
  # given above: measured, it reserves 30 numbers of which 16 are new, against
  # ~3100 still free. Over-reserving stays free.
  {
    grep -rhv '^[[:space:]]*#' "$root/s3-accelerator" \
      --include='*.sh' --include='*.py' --include='*.java' --include='*.xml' 2>/dev/null
    find "$root/tools" -name '*_drill.sh' -prune -o \
      \( -name '*.sh' -o -name '*.py' \) -print 2>/dev/null \
      | xargs grep -hv '^[[:space:]]*#' 2>/dev/null
  } \
    | grep -oE '\b[0-9]{4,5}\b' \
    | awk '$1 >= 6300 && $1 <= 9999' \
    | sort -un
}

DRILL_DEAD_PORTS="6999 7999 7788 7789"

drill_is_dead_port() {
  case " $DRILL_DEAD_PORTS " in *" $1 "*) return 0 ;; *) return 1 ;; esac
}
