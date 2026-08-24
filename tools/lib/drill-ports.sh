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
drill_declared_ports() {
  local dir="${1:-tools}" f d
  for f in "$dir"/*_drill.sh; do
    [ -f "$f" ] || continue
    d=$(basename "$f" _drill.sh)
    sed -e :a -e '/\\$/N; s/\\\n//; ta' "$f" 2>/dev/null \
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
DRILL_DEAD_PORTS="6999 7999"

drill_is_dead_port() {
  case " $DRILL_DEAD_PORTS " in *" $1 "*) return 0 ;; *) return 1 ;; esac
}
