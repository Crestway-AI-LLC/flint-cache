#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# `tools/next-free-ports.sh`, which nothing exercised.
#
# Two defects in the allocator landed within ninety minutes of each other on
# 2026-09-15 -- BUG-0154, it offered ports the gate's kill-pattern check then
# refuses; and the cache session's `dcbb1a1`, `repo_bound_ports` read only the
# accelerator subtree so nine ports bound by NON-DRILL scripts in tools/ were
# invisible to both halves. Both rested on controls run by hand, and each of us
# demonstrated the other's defect while verifying our own: their "still
# allocates" control recorded 6442-6445, which is precisely the band the
# kill-pattern fix had just taught the allocator to avoid. Neither of us noticed
# until the two commits met.
#
# WHAT THIS ASSERTS, AND WHAT IT REFUSES TO. A port becomes unavailable through
# four independent channels, and the honest test of each is to INJECT A NEW
# CLAIM AND REQUIRE THE ALLOCATOR TO MOVE. Recomputing "claimed" here the way
# the allocator computes it, and asserting the two agree, would be a check that
# cannot fail: it would pass against an allocator returning the same wrong
# answer as its own helper. That is the shape this repo files most often and it
# is very easy to write by accident in a test for a pure function.
#
# A MINIMAL TREE, NOT THE REAL ONE. Against tools/ the answer is whatever ~540
# existing claims happen to allow, so the only assertion available would be "it
# returned something". Against a tree holding two files the answer is EXACT, and
# an exact answer is what separates "moved off the port I claimed" from "moved
# for some other reason". It also means a fixture that fails to take fails the
# arm, rather than quietly making it pass.
# THE FIXTURES ARE ASSEMBLED, NOT SPELLED, and that is not style. This file
# lives in tools/ and is therefore scanned by the very functions it tests: a
# literal `pkill -9 -f "flint-server --port 630"` sitting here would register a
# kill prefix owned by port_allocator, and `assert_no_cross_drill_kill_patterns`
# would then refuse the tree over the TEN drills that sit in 630x -- cert_rotate,
# collection_admission, controlplane_ha, federation_plumbing and lease. The first
# draft of this drill did exactly that, and its own control caught it. gates.sh
# assembles a backtick with chr(96) for the same reason; this is that rule
# applied to a fixture rather than to a regex.
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $*"; exit 1; }

# `${FLINT_DRILL_ROOT:-/tmp}` and not the bare variable, which is the same
# default `_fleet_live_peer_scopes` and gates.sh's conformance stage use. Every
# other drill gets it exported by `fleet_init`; this one starts no seats, so it
# never calls fleet_init and nothing sets it. On this laptop ~/.zshenv exports
# it and the bare form worked; on the gate box it is unset and `set -u` killed
# the drill at line 1 with "unbound variable". Sourcing tools/lib/fleet.sh for
# it would be worse: its own header warns that doing so exports the variable,
# CREATES the directory and exits 1 if it is not writable, which is a lot of
# behaviour to acquire for a default.
T="${FLINT_DRILL_ROOT:-/tmp}/flint-portalloc"
rm -rf "$T"; mkdir -p "$T/tools/lib"
trap 'rm -rf "$T"' EXIT
cp tools/next-free-ports.sh "$T/tools/"
cp tools/lib/drill-ports.sh "$T/tools/lib/"

# The allocator cds to its own parent, so running the copy runs it against the
# throwaway tree and nothing else.
ask() { bash "$T/tools/next-free-ports.sh" "$@" 2>&1; }

# The baseline is 6301 and NOT 6300, and the difference is worth knowing: the
# allocator's own source carries `BASE=6300` on a non-comment line, and the
# matcher takes every integer on one. That is the file's stated trade -- "over-
# reserving is free, under-reserving is not" -- costing exactly one port, and
# this drill would be asserting against a fiction if it pretended otherwise.
BASE=6300
echo "== control: a tree claiming almost nothing offers the first free port"
# FIRST, because every arm below reads a MOVE as evidence and a mover that never
# sits still proves nothing.
GOT=$(ask 1 --base "$BASE") || fail "the allocator exited non-zero on an empty tree: $GOT"
GOT=$(echo $GOT)
[ "$GOT" = 6301 ] || fail "on a tree whose only claim is the allocator's own BASE=6300,
  --base $BASE answered '$GOT', want 6301"
echo "   6301"

echo "== a port a DRILL declares is not offered"
printf 'fleet_init /x/flint-a %s\n' 6301 > "$T/tools/a_drill.sh"
GOT=$(echo $(ask 1 --base "$BASE"))
[ "$GOT" = 6302 ] || fail "with a drill declaring 6301, --base $BASE answered '$GOT', want 6302.
  This is the channel drill_declared_ports reads, and the one the allocator has
  always had."
echo "   6301 declared -> 6302"

echo "== a port a NON-DRILL script BINDS is not offered"
# The cache session's dcbb1a1. `drill_declared_ports` reads fleet_init, and
# drills are the only things that declare -- so a plain script starting a server
# was invisible to BOTH halves of the allocator. Nine ports were in that gap,
# and 6391 was saved from being handed out only because a COMMENT in an
# unrelated file happened to say the block was taken.
printf 'PORT=${PORT:-%s}\nexec flint-server --port "$PORT"\n' 6302 > "$T/tools/b_bind.sh"
GOT=$(echo $(ask 1 --base "$BASE"))
[ "$GOT" = 6303 ] || fail "with a non-drill script binding 6302, --base $BASE answered '$GOT',
  want 6303. A port a script BINDS is as taken as one a drill declares."
echo "   6302 bound by a non-drill script -> 6303"

echo "== a port another drill's TRUNCATED kill pattern reaches is not offered"
# BUG-0154. `pkill -f "flint-server --port 630"` is a SUBSTRING match, so it
# reaches every 630x. A drill placed there is one parallel batch away from being
# SIGKILLed by a stranger, and the gate refuses the tree for it -- which is how
# this was found, by the gate rejecting a port this helper had just suggested.
PFX=630
printf 'pkill -9 -f "flint-server --port %s"\n' "$PFX" > "$T/tools/c_drill.sh"
GOT=$(echo $(ask 1 --base "$BASE"))
[ "$GOT" = 6310 ] || fail "with a kill pattern reaching ${PFX}x, --base $BASE answered '$GOT',
  want 6310. The whole prefixed band has to go, not just the first port in it."
echo "   ${PFX}x reachable -> 6310"

echo "== a port on the DEAD list is not offered"
# DERIVED FROM THE LIST, not hard-coded: the assertion is that the allocator
# honours DRILL_DEAD_PORTS, not which ports are on it this month. A drill relies
# on each of these being dead, so handing one out breaks that drill and not this
# one.
. tools/lib/drill-ports.sh
DEAD=$(printf '%s\n' $DRILL_DEAD_PORTS | head -1)
[ -n "$DEAD" ] || fail "DRILL_DEAD_PORTS is empty, so this arm would pass without testing anything"
GOT=$(echo $(ask 1 --base "$DEAD"))
[ "$GOT" != "$DEAD" ] || fail "--base $DEAD answered $DEAD, which is on DRILL_DEAD_PORTS"
echo "   $DEAD is dead -> $GOT"

echo "== NEGATIVE CONTROL: a port nothing claims is still offered"
GOT=$(echo $(ask 1 --base 6310))
[ "$GOT" = 6310 ] || fail "--base 6310 answered '$GOT'. Nothing in this tree claims 6310, so an
  allocator that will not offer it is refusing everything -- and every arm above
  would pass against exactly that."
echo "   6310"

echo "== N consecutive means N consecutive"
GOT=$(echo $(ask 3 --base 6310))
[ "$GOT" = "6310 6311 6312" ] || fail "3 --base 6310 answered '$GOT', want '6310 6311 6312'.
  A drill asking for a pair gets a master and a replica; a run with a hole in it
  is two drills sharing a port a week later."
echo "   6310 6311 6312"

# MUTATION-VERIFIED, and one of the two says something extra.
#
#   - Drop the kill-prefix exclusion from the allocator's loop and this drill
#     fails, but not with a wrong port: the allocator's own SELF-CHECK fires
#     first -- "internal error: suggested 6303, which another drill's kill
#     pattern reaches". That check had nothing exercising it either, and the
#     mutation shows it is live rather than decorative.
#   - Drop the tools/ branch of `repo_bound_ports` and this drill fails at the
#     CONTROL, one arm before the binding arm it was aimed at, because that
#     branch is also what makes the allocator's own `BASE=6300` visible. Same
#     channel, one arm earlier; worth knowing so the failure is not read as
#     something else.
echo "PORT ALLOCATOR DRILL PASSED"
