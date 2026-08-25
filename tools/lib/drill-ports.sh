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
drill_declared_ports() {
  local dir="${1:-tools}" f d
  for f in "$dir"/*_drill.sh "$dir"/gates.sh; do
    [ -f "$f" ] || continue
    case "$f" in
      *_drill.sh) d=$(basename "$f" _drill.sh) ;;
      *)          d=gates-conformance ;;
    esac
    sed -e :a -e '/\\$/N; s/\\\n//; ta' "$f" 2>/dev/null \
      | grep -E '^[[:space:]]*fleet_init ' \
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
repo_bound_ports() {
  local root="${1:-.}"
  grep -rhoE '(--port|--listen|-p)[[:space:]]+[0-9]{4,5}|127\.0\.0\.1:[0-9]{4,5}|localhost:[0-9]{4,5}' \
    "$root/s3-accelerator" 2>/dev/null \
    | grep -oE '[0-9]{4,5}$' | sort -un
}

DRILL_DEAD_PORTS="6999 7999 7788 7789"

drill_is_dead_port() {
  case " $DRILL_DEAD_PORTS " in *" $1 "*) return 0 ;; *) return 1 ;; esac
}
