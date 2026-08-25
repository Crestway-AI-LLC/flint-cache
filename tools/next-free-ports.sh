#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Print N consecutive ports no drill has claimed.
#
# WHY THIS EXISTS. Adding a drill means picking ports, and picking them meant
# reading other drills until you found a gap. Twice on 2026-08-24 a drill
# arrived from a branch carrying ports main had since given away —
# loaded_promote on 6460/61 and loading_visible on 6463/64, both inside
# proxy_chain's 6460-6467. Neither was caught by the suite; the first was
# caught by hand and the second only because the first had made someone look.
#
# The gate now refuses a collision (assert_no_duplicate_drill_ports). This is
# the other half: making the right port trivial to find, so the refusal is
# rare rather than routine. Scarcity is not the problem — 445 of the ~3800
# usable slots are claimed — discovery was.
#
# Usage:  tools/next-free-ports.sh [N] [--base B]
#         N     how many CONSECUTIVE ports (default 2; drills usually want a
#               master/replica pair)
#         B     where to start looking (default 6300, just under the block
#               the suite already uses)
#
# Deliberately 6000-9999 and never the ephemeral range (32768-60999 on Linux):
# a server bound there can lose the race to the drill's OWN client connections.
# No drill port falls in it today and none should.
set -u
cd "$(dirname "$0")/.." || exit 1
. tools/lib/drill-ports.sh

N=2; BASE=6300; MAX=9999
while [ $# -gt 0 ]; do
  case "$1" in
    --base) BASE="${2:-}"; shift 2 ;;
    -h|--help) sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) N="$1"; shift ;;
  esac
done
case "$N" in ''|*[!0-9]*) echo "N must be a positive integer, got '$N'" >&2; exit 2 ;; esac
[ "$N" -ge 1 ] || { echo "N must be >= 1" >&2; exit 2; }
case "$BASE" in ''|*[!0-9]*) echo "--base must be an integer, got '$BASE'" >&2; exit 2 ;; esac

# Drill declarations UNION anything else in the repo already binding a port.
# The gate asserts on the first set only; the allocator must avoid both, or it
# suggests a port that is free according to tools/ and taken in fact.
CLAIMED=$( { drill_declared_ports | awk '{print $1}'; repo_bound_ports "$(dirname "$0")/.."; } | sort -un)
is_claimed() { printf '%s\n' "$CLAIMED" | grep -qx "$1"; }

p="$BASE"
while [ "$p" -le "$((MAX - N + 1))" ]; do
  ok=1
  for off in $(seq 0 $((N - 1))); do
    is_claimed "$((p + off))" && { ok=0; break; }
    # never suggest a port some drill relies on being DEAD
    drill_is_dead_port "$((p + off))" && { ok=0; break; }
  done
  if [ "$ok" = 1 ]; then
    # SELF-CHECK: prove the answer against the same data the gate will use,
    # rather than trusting the loop above. A helper that can emit a port the
    # gate then rejects is worse than no helper.
    for off in $(seq 0 $((N - 1))); do
      if is_claimed "$((p + off))"; then
        echo "internal error: suggested $((p + off)) which IS claimed" >&2; exit 1
      fi
    done
    for off in $(seq 0 $((N - 1))); do printf '%s ' "$((p + off))"; done
    echo
    exit 0
  fi
  p=$((p + 1))
done
echo "no run of $N free ports between $BASE and $MAX" >&2
exit 1
