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
# THREE RULES LEARNED THE HARD WAY, all encoded below:
#
#   1. Match argv[0]'s BASENAME, never the whole command line. A whole-line
#      match flags anything that merely NAMES a binary — an editor with
#      crates/flint-controller/src open, a build, or the agent driving the
#      session, whose argv carries `--add-dir .../crates/flint-server/src`.
#      The first version did this and refused to run on a clean box.
#
#   2. flintctl is never a seat — but do NOT exclude it by scanning the
#      command line. Rule 1 already handles it: argv[0] of `flintctl ...`
#      (or `sudo ... flintctl ...`) is not in the daemon set, so it cannot
#      match. A whole-line `/flintctl/` exclusion looks equivalent and is
#      not: `flint-ops` is started with `--flintctl <path>` as a legitimate
#      argument, so that exclusion made ops immune to its own cleanup and
#      it leaked out of every edge_tls and ops_portal run. The same
#      whole-line-versus-argv[0] confusion as rule 1, in the rule written
#      to prevent it.
#
#   3. The binary set is every long-running Flint DAEMON across both repos —
#      console, ops, register, exporter and meter exist only in the fleet
#      repo — so the two copies of this file stay byte-identical and a
#      public checkout simply never has those processes. Deliberately
#      excluded: flintctl (see 2) and the one-shot tools (bench, chaos,
#      conformance), which are not seats and exit on their own.

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
  # ONE drill per scope at a time. Two drills declaring the same scope and
  # port block pass each other's fleet_guard — shared scope and ports read as
  # "ours" by construction — and the second one's opening fleet_kill sweep
  # then SIGKILLs the first's freshly spawned seats. Seen for real: two
  # chaos_drill.sh runs on one box, and the loser died at "master up" with an
  # EMPTY node log, which reads as a boot bug rather than as the collision it
  # is (docs/bugs/0003 is the same collision through a different door).
  #
  # mkdir is the atomic take. Release is NOT a trap: 76 drills already set
  # their own `trap cleanup EXIT`, and the last trap installed wins, so a
  # trap here would be silently clobbered by most callers. Reclaiming a dead
  # owner's lock is therefore the path that has to work, not the fallback.
  #
  # Liveness is pid PLUS process START TIME, never the pid alone. These
  # drills spawn hundreds of short-lived processes, so a recorded pid can
  # exit and be REUSED — the same window fleet_kill re-verifies against
  # before signalling. A bare `kill -0` landing on an unrelated live process
  # would refuse every drill on this scope forever, turning one crashed run
  # into a permanently wedged box. The start time settles it exactly: a
  # reused pid always has a different one. (Matching on argv instead was
  # tried and is too loose — any process whose command line merely MENTIONS
  # a tools script matches, which a test caught immediately.)
  FLEET_LOCK="${FLEET_SCOPE}.lock"
  local owner owner_started now_started
  while ! mkdir "$FLEET_LOCK" 2>/dev/null; do
    owner="$(cat "$FLEET_LOCK/pid" 2>/dev/null || true)"
    owner_started="$(cat "$FLEET_LOCK/started" 2>/dev/null || true)"
    now_started=""
    if [ -n "$owner" ]; then
      now_started="$(ps -o lstart= -p "$owner" 2>/dev/null || true)"
    fi
    if [ -n "$now_started" ] && [ "$now_started" = "$owner_started" ]; then
      echo "REFUSING TO RUN: drill scope $FLEET_SCOPE is held by a live run (pid $owner)"
      echo "  Two drills on one scope kill each other's seats mid-boot."
      echo "  Wait for that run, or stop it from its own session."
      exit 1
    fi
    rm -rf "$FLEET_LOCK"
  done
  printf '%s\n' "$$" > "$FLEET_LOCK/pid"
  ps -o lstart= -p "$$" > "$FLEET_LOCK/started" 2>/dev/null || true
}

# Fleet processes belonging to ANOTHER Flint-family project, as "pid argv"
# lines. Today that means flint-kv (flint-kv-server, flint-kv-chaos, ...).
#
# WHY THIS IS SEPARATE FROM _fleet_foreign. That one is anchored
# `^flint-(server|proxy|...)$`, so `flint-kv-server` never matched it and the
# guard reported a clean box while another project's chaos fleet was running.
# Both suites then competed for CPU, disk and the machine's patience, and the
# result presented as a flaky drill rather than as a collision — which is the
# expensive kind of wrong, because it sends you debugging the drill.
#
# DETECTION ONLY, AND DELIBERATELY SO. Nothing here feeds a kill path. This
# repo must never signal another project's processes: those fleets are owned,
# and possibly being watched, by someone else's run. The guard's job is to
# refuse and name what it saw, not to tidy up.
#
# The rule is structural rather than a list of names: sibling projects
# namespace their binaries `flint-<project>-<component>`, while everything
# THIS workspace builds is a single segment (flint-server, flint-chaos,
# flint-controlplane) or flintctl. If this repo ever ships a two-segment
# binary, exclude it here — otherwise the guard will start calling our own
# processes a sibling project's and refuse to run at all.
_fleet_sibling() {
  ps -eo pid=,args= 2>/dev/null | awk '
    {
      n = split($2, parts, "/")
      exe = parts[n]
      if (exe ~ /^flint-(server|proxy|controlplane|controller|agent|console|ops|register|exporter|meter|chaos|bench|conformance|balance)$/) next
      if (exe !~ /^flint-[a-z0-9]+-[a-z0-9-]+$/) next
      print
    }'
}

# Fleet processes on this box that are NOT ours, as "pid argv" lines.
_fleet_foreign() {
  ps -eo pid=,args= 2>/dev/null | awk -v scope="$FLEET_SCOPE" -v ports="$FLEET_PORTS" '
    {
      n = split($2, parts, "/")
      exe = parts[n]
      if (exe !~ /^flint-(server|proxy|controlplane|controller|agent|console|ops|register|exporter|meter)$/) next
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
  local re='^flint-(server|proxy|controlplane|controller|agent|console|ops|register|exporter|meter|backup)$'
  if [ -n "$want" ]; then
    re="^flint-($(printf '%s' "$want" | tr ' ' '|'))$"
  fi
  ps -eo pid=,args= 2>/dev/null | awk -v scope="$FLEET_SCOPE" -v ports="$FLEET_PORTS" -v re="$re" '
    {
      n = split($2, parts, "/")
      exe = parts[n]
      # The COMPONENT filter. Dropping this makes every `fleet_kill <part>`
      # a blanket sweep, which silently rewrites the drill that called it —
      # `fleet_kill controlplane` would take the nodes with it. It went
      # missing once already, deleted by a careless two-line replacement
      # while removing an adjacent check, and cost a long bisect: the
      # symptom was a NODE vanishing during a step that only kills the
      # control plane, which reads like a product bug.
      if (exe !~ re) next
      mine = (index($0, scope) > 0)
      if (!mine && ports != "")
        mine = ($0 ~ ("(^|[^0-9])(" ports ")([^0-9]|$)"))
      if (mine) print $1
    }'
}

# fleet_wait_listen <port> [port ...] — block until each port accepts.
#
# Takes SEVERAL ports on purpose. Drills routinely start a pair with one
# sleep covering both, and waiting on only the last one would quietly narrow
# the check to half of what the sleep covered — the seat that starts first is
# usually ready first, so the narrowing would hold right up until the day it
# did not.
#
# Drills used to start a seat and then `sleep 0.5`, which is not a wait, it is
# a BET that the machine can start a just-linked binary in half a second. The
# first run after a build loses it — a first exec pays for signature
# validation and a cold page cache — and the drill then talks to a socket
# nobody is listening on. One such run failed five unrelated drills that all
# passed on every rerun, which reads exactly like a product bug and is not
# one; that cost an afternoon.
#
# A TCP accept is the right signal because it is the one every component
# shares: PING needs RESP and would need the right TLS config, while "are you
# listening" is answerable for a node, a proxy and a control plane alike. It
# is also STRICTLY better than a sleep for a fresh replica, whose listener
# binds only after its checkpoint download finishes.
#
# Waits that are not readiness — "let the controller observe convergence" —
# are a different thing and must stay sleeps.
fleet_wait_listen() {
  local port deadline
  for port in "$@"; do
    deadline=$(( $(date +%s) + 30 ))
    while :; do
      if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then break; fi
      if [ "$(date +%s)" -ge "$deadline" ]; then
        echo "FAIL: nothing listening on 127.0.0.1:$port after 30s"
        return 1
      fi
      sleep 0.05
    done
  done
  return 0
}

# fleet_wait_ping <port> [valkey-cli opts ...] — block until the seat answers
# PONG, or FAIL the drill on the spot.
#
# fleet_wait_listen proves a socket accepts; a control plane additionally has
# to speak RESP before the bootstrap commands after it mean anything. Drills
# used to hand-roll this as `for i in $(seq 1 30); do ... PING ... done` — a
# loop whose expiry falls through SILENTLY, so on a loaded box the CPADD*
# lines after it failed one by one and the drill reported WRONGPASS on a
# token that was never registered: a product-shaped failure with a harness
# cause (see the warm-up note in gates.sh). This helper exists so a CP that
# never comes up says so, here, and nothing later runs against it.
#
# Extra arguments ride through to valkey-cli (a TLS control plane needs
# --tls --cacert ... to answer at all).
fleet_wait_ping() {
  local port="$1"; shift
  local deadline=$(( $(date +%s) + 30 ))
  while :; do
    [ "$(valkey-cli -p "$port" "$@" PING 2>/dev/null)" = "PONG" ] && return 0
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "FAIL: no PONG from 127.0.0.1:$port after 30s"
      exit 1
    fi
    sleep 0.2
  done
}

# fleet_cp <port> [opts ...] <command ...> — a control-plane bootstrap command
# that MUST succeed, or the drill dies here.
#
# valkey-cli exits 0 whether the CP replied OK or ERR, so `>/dev/null` on a
# CPADDTENANT was discarding the only evidence that the tenant exists. Every
# assertion after a silently failed bootstrap tests a cluster that was never
# built. Success is a reply starting with OK (CPADDTENANT says "OK tenant
# ..."); anything else — ERR, an empty reply, connection refused — is fatal.
fleet_cp() {
  local port="$1"; shift
  local r
  r=$(valkey-cli -p "$port" "$@" 2>&1)
  case "$r" in
    OK*) return 0 ;;
  esac
  echo "FAIL: CP bootstrap on 127.0.0.1:$port: $* -> ${r:-<no reply>}"
  exit 1
}

# fleet_guard — refuse to run on a box that already has a foreign fleet.
#
# The point is to FAIL rather than destroy. A drill that silently killed a
# developer's cluster, or a sibling suite, was doing the wrong thing quietly;
# stopping with an explanation is the right thing loudly.
# FLINT_DRILL_FORCE=1 proceeds anyway (a CI box that really is ours alone).
fleet_guard() {
  local foreign sibling
  foreign="$(_fleet_foreign)"
  sibling="$(_fleet_sibling)"
  [ -z "$foreign" ] && [ -z "$sibling" ] && return 0
  if [ "${FLINT_DRILL_FORCE:-0}" = "1" ]; then
    local n=0
    [ -n "$foreign" ] && n=$(( n + $(echo "$foreign" | wc -l | tr -d ' ') ))
    [ -n "$sibling" ] && n=$(( n + $(echo "$sibling" | wc -l | tr -d ' ') ))
    echo "  (FLINT_DRILL_FORCE=1: proceeding despite $n other flint process(es))"
    return 0
  fi
  if [ -n "$foreign" ]; then
    echo "REFUSING TO RUN: this box already has Flint processes outside $FLEET_SCOPE"
    echo "$foreign" | cut -c1-120 | sed 's/^/    /'
    echo "  A drill that killed those would destroy a fleet it does not own —"
    echo "  a live cluster, or another suite's nodes."
  fi
  if [ -n "$sibling" ]; then
    # A DIFFERENT reason, said differently. These are not ours to stop, and a
    # message telling you to kill them would be wrong: another project's run
    # may be mid-measurement. The contention is the problem, not the process.
    echo "REFUSING TO RUN: another Flint-family project has a fleet up on this box"
    echo "$sibling" | cut -c1-120 | sed 's/^/    /'
    echo "  These are NOT ours and this suite will not touch them. Two fleets"
    echo "  sharing a box contend for CPU and disk, and the result shows up as"
    echo "  a flaky drill rather than as the collision it is. Wait for that run"
    echo "  to finish, or stop it from ITS project."
  fi
  echo "  Re-run with FLINT_DRILL_FORCE=1 if this box really is yours alone."
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
# fleet_signal <signal> <component ...> — send a NON-fatal signal to our own
# seats, with the same ownership check fleet_kill applies.
#
# Exists so a drill can SIGSTOP a node to simulate a stalled or unreachable
# peer without reaching for `pkill -STOP -f flint-server`, which matches every
# Flint process on the box — the exact unscoped pattern this file was written
# to remove. A stray -STOP is quieter than a stray -9 and therefore worse: the
# process is still there, still listening, and simply never answers, which
# reads as a replication bug in whatever else is running.
#
# Returns nonzero when it signalled nothing, so a caller can tell "frozen" from
# "I matched no process and carried on testing the wrong thing".
fleet_signal() {
  local sig="$1"; shift
  local want="$*"
  local pid args hit=1
  for pid in $(_fleet_ours "$want"); do
    args="$(ps -o args= -p "$pid" 2>/dev/null)"
    case "$args" in
      *flint-*) ;;
      *) continue ;;
    esac
    kill "$sig" "$pid" 2>/dev/null && hit=0
  done
  return $hit
}

# fleet_signal_port <port> <signal> — the same, narrowed to one seat.
#
# _fleet_ours already treats a declared port as ownership, so this is the
# component filter plus a literal `--port N` match. Kept separate because
# freezing ONE member of a pair is the whole point: freezing both proves
# nothing about a widowed master.
fleet_signal_port() {
  local port="$1" sig="$2"
  local pid args hit=1
  for pid in $(_fleet_ours server); do
    args="$(ps -o args= -p "$pid" 2>/dev/null)"
    case "$args" in
      *"--port $port"*) ;;
      *) continue ;;
    esac
    kill "$sig" "$pid" 2>/dev/null && hit=0
  done
  return $hit
}

fleet_kill() {
  local want="$*"
  local pid args
  for pid in $(_fleet_ours "$want"); do
    # RE-VERIFY before signalling. `pkill` matches and signals as one act;
    # this is a snapshot followed by a kill, and between the two the pid can
    # exit and be REUSED by something else. These drills spawn hundreds of
    # short-lived processes, so that window is not theoretical: a stale pid
    # from the snapshot landed on a live flint-server and killed the node the
    # drill was still using, which presented as "data path disturbed" — a
    # product failure that was really a harness race.
    args="$(ps -o args= -p "$pid" 2>/dev/null)"
    case "$args" in
      *flint-*) ;;                 # still one of ours
      *) continue ;;               # exited, or the pid now belongs elsewhere
    esac
    kill -9 "$pid" 2>/dev/null
  done
  return 0
}

# --- reply assertions -------------------------------------------------------
#
# valkey-cli prints RESP ERRORS TO STDOUT AND EXITS 0. Measured 2026-08-10
# against a local valkey-server:
#
#     $ out=$(valkey-cli -p P SET 2>/dev/null); echo "rc=$? '$out'"
#     rc=0 'ERR wrong number of arguments for 'set' command'
#     $ valkey-cli -p P GET <a-list-key> 2>&1 >/dev/null      # stderr
#     (empty)
#
# So `cmd >/dev/null 2>&1 || die` detects nothing, and neither does discarding
# stdout: a refused write — `-QUOTA` from the disk guard, `-NOAUTH`,
# `-READONLY` on a replica, a tenant over its cap — is indistinguishable from
# a successful one. The reply is the ONLY signal, and an error carries no
# marker in this output: no leading '-', no "(error)", just the code.
#
# This is not hypothetical. A drill's discarded seed write turned a `-QUOTA`
# into a false data-loss alarm 60 lines later, and the same defect was still
# in tools/quickstart.sh — the first command a new user runs — on 2026-08-10.
#
# Use these for any write whose success is a PRECONDITION of what follows.
# Do NOT use them where a failure is expected or tolerated: a writer racing a
# deliberate kill, or a probe that is measuring whether a write is refused.
#
#   cli_ok  valkey-cli -p 6379 SET k v          # expects +OK
#   cli_int valkey-cli -p 6379 HSET h f v       # expects an integer reply
#
cli_ok() {
  local r
  r=$("$@" 2>&1 | tr -d '\r')
  [ "$r" = "OK" ] && return 0
  echo "FAIL: expected OK from: $*"
  echo "      server said: ${r:-(no reply — is it still listening?)}"
  exit 1
}

cli_int() {
  local r
  r=$("$@" 2>&1 | tr -d '\r')
  case "$r" in
    ''|*[!0-9]*)
      echo "FAIL: expected an integer reply from: $*"
      echo "      server said: ${r:-(no reply — is it still listening?)}"
      exit 1 ;;
  esac
  return 0
}
