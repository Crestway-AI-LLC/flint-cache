# SPDX-License-Identifier: Elastic-2.0
# Shared drill process control.
#
# Drills used to open with an UNSCOPED sweep:
#
#     pkill -9 -f flint-server
#
# which matches every Flint process on the machine, not just the drill's own.
# That carries an unstated assumption — that the drill owns the box — and
# enforces it by destroying whatever disagrees. Two consequences, both seen:
#
#   * Running two drill suites at once, one killed the other's freshly
#     spawned replica and produced `replacement replica up` — a failure
#     indistinguishable from a genuine replication bug, on a build that was
#     fine. It cost a debugging detour and nearly blocked a release.
#
#   * Anyone with a live Flint fleet on the same machine loses it. `pkill -9`
#     on a rocks node is a kill -9 mid-write.
#
# So: every drill declares a SCOPE — the unique /tmp directory it works in —
# and kills only processes whose argv carries it. Same discipline flintctl's
# sweep_orphans applies (it requires a match on THIS inventory's statedir
# before killing anything), for the same reason: a stray kill is worse than a
# stray process.
#
# TWO RULES LEARNED THE HARD WAY, both encoded below:
#
#   1. Match argv[0]'s BASENAME, never the whole command line. A whole-line
#      match flags anything that merely NAMES a binary — an editor with
#      crates/flint-controller/src open, a build, or the agent driving the
#      session, whose argv carries `--add-dir .../crates/flint-server/src`.
#      The first version did this and refused to run on a clean box.
#
#   2. Never match flintctl. It is not a fleet seat, and over ssh a
#      `flintctl host-*` argv carries both a binary name and a statedir, so
#      an unfiltered sweep kills the command doing the sweeping.

# fleet_init <scope-dir> [port ...]
#
# `scope` is the drill's own directory. PORTS are the drill's own port block,
# and they are not optional decoration: a proxy started `--port 6666 --pairs
# 127.0.0.1:6630,...` and a controller started `--pairs ... --id PX` carry NO
# path at all, so a directory-only match leaves both running. The first
# attempt at this did exactly that, and every drill after the leaker refused
# to start — which is the guard telling the truth about a real leak, but a
# leak the sweep should have cleaned.
#
# Ports are matched EXACTLY, never as prefixes: `--port 653` would also match
# 6531, and mistaking one seat for another is how #101 happened.
fleet_init() {
  FLEET_SCOPE="$1"; shift
  FLEET_PORTS="$(printf '%s' "$*" | tr ' ' '|')"
  [ -n "$FLEET_SCOPE" ] || { echo "fleet_init: empty scope"; exit 1; }
}

# Fleet processes on this box that are NOT ours, as "pid argv" lines.
_fleet_foreign() {
  ps -eo pid=,args= 2>/dev/null | awk -v scope="$FLEET_SCOPE" -v ports="$FLEET_PORTS" '
    {
      n = split($2, parts, "/")
      exe = parts[n]
      if (exe !~ /^flint-(server|proxy|controlplane|controller|agent)$/) next
      if (index($0, scope) > 0) next
      if (ports != "" && $0 ~ ("(^|[^0-9])(" ports ")([^0-9]|$)")) next
      print
    }'
}

# Our own fleet processes, as pids: a fleet binary whose argv carries either
# our directory or one of our ports. `want` optionally narrows to specific
# components (space separated, e.g. "controlplane proxy").
_fleet_ours() {
  local want="${1:-}"
  local re='^flint-(server|proxy|controlplane|controller|agent)$'
  if [ -n "$want" ]; then
    re="^flint-($(printf '%s' "$want" | tr ' ' '|'))$"
  fi
  ps -eo pid=,args= 2>/dev/null | awk -v scope="$FLEET_SCOPE" -v ports="$FLEET_PORTS" -v re="$re" '
    {
      n = split($2, parts, "/")
      exe = parts[n]
      if (exe !~ re) next
      if ($0 ~ /flintctl/) next
      mine = (index($0, scope) > 0)
      if (!mine && ports != "")
        mine = ($0 ~ ("(^|[^0-9])(" ports ")([^0-9]|$)"))
      if (mine) print $1
    }'
}

# fleet_guard — refuse to run on a box that already has a foreign fleet.
#
# The point is to FAIL rather than destroy. A drill that silently killed a
# developer's cluster, or a sibling suite, was doing the wrong thing quietly;
# stopping with an explanation is the right thing loudly.
# FLINT_DRILL_FORCE=1 proceeds anyway (a CI box that really is ours alone).
fleet_guard() {
  local foreign
  foreign="$(_fleet_foreign)"
  [ -z "$foreign" ] && return 0
  if [ "${FLINT_DRILL_FORCE:-0}" = "1" ]; then
    echo "  (FLINT_DRILL_FORCE=1: proceeding despite $(echo "$foreign" | wc -l | tr -d ' ') foreign flint process(es))"
    return 0
  fi
  echo "REFUSING TO RUN: this box already has Flint processes outside $FLEET_SCOPE"
  echo "$foreign" | cut -c1-120 | sed 's/^/    /'
  echo "  A drill that killed those would destroy a fleet it does not own —"
  echo "  a live cluster, or another suite's nodes. Stop them, or re-run with"
  echo "  FLINT_DRILL_FORCE=1 if this box really is yours alone."
  exit 1
}

# fleet_kill [component ...] — stop OUR processes only.
#
# With no argument, every seat we own. With components (server, proxy,
# controlplane, controller, agent) ONLY those.
#
# The component filter is not a convenience, it is the semantics. A drill
# that says `pkill -9 -f flint-controlplane` mid-run means "kill the control
# plane, leave the nodes and the proxy serving" — that IS the scenario under
# test. Collapsing those into a blanket sweep silently rewrites the drill:
# doing exactly that made token_rotation report its proxy unreachable and
# tenant_quota fail, because the sweep had killed seats the drill still
# needed. Opening and cleanup sweeps take no argument; mid-run kills name
# what they mean.
fleet_kill() {
  local want="$*"
  _fleet_ours "$want" | while read -r pid; do kill -9 "$pid" 2>/dev/null; done
  return 0
}
