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

# WHERE DRILL SCRATCH LIVES. Every drill's state directory, seat log and
# fleet SCOPE hang off this root; it defaults to /tmp so nothing changes for
# CI or a plain checkout. Point it elsewhere to put drill I/O on another
# volume:
#
#     FLINT_DRILL_ROOT=/Volumes/FlintDev/drillscratch tools/gates.sh
#
# EXPORTED on purpose: several drills embed Python in a QUOTED heredoc
# (<<'PY'), where the shell cannot expand a variable, so those snippets read
# os.environ["FLINT_DRILL_ROOT"] instead. If this were a plain shell variable
# the shell half and the Python half would disagree about where the state
# lives — the drill would create certs in one place and open them from
# another, and fail in a way that looks like a TLS bug.
export FLINT_DRILL_ROOT="${FLINT_DRILL_ROOT:-/tmp}"
if ! mkdir -p "$FLINT_DRILL_ROOT" 2>/dev/null || [ ! -w "$FLINT_DRILL_ROOT" ]; then
  echo "fleet: FLINT_DRILL_ROOT=$FLINT_DRILL_ROOT is not writable" >&2
  echo "       (an unmounted external volume looks exactly like this)" >&2
  exit 1
fi

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
  # The declared ports, recorded for peer detection. A scope path alone cannot
  # identify a peer's seats: a proxy started `--port 6666 --pairs ...` and a
  # controller started `--pairs ... --id ctl` carry no path at all, and a drill
  # keeps auxiliary state in sibling directories (<scope>-state) that the
  # boundary rule in _fleet_ours deliberately does NOT treat as owned. Ports
  # are what those seats always have. Same two keys ownership already uses.
  printf '%s\n' "$FLEET_PORTS" > "$FLEET_LOCK/ports" 2>/dev/null || true

  # WARM THE BINARIES HERE, NOT IN EACH DRILL (BUG-0011).
  #
  # On macOS the FIRST exec of a freshly built binary pays kernel code-signature
  # validation — Rust ad-hoc/linker-signs, and this one carries 2971 page hashes
  # — and it is paid in the loader, before `main`. Measured on this box against
  # six fresh copies of flint-server, timing `--build-version`, which exits
  # before any server work:
  #
  #     first exec of a new inode : 23403 ms, 43472 ms, then ~360 ms
  #     immediate repeat exec     : ~25 ms, every time
  #
  # The seat-startup budget is 10 s. A 23-43 s stall blows it outright and
  # produces exactly the recorded signature: the process is ALIVE, it never
  # answers PING, and `sample` shows 2935 of 2935 frames in `_dyld_start` with
  # `main` never entered — because no code of ours has run. It is absent on
  # Linux (gate.yml green 29 of its last 30) because there is no dyld and no
  # AMFI, which is why 20 local runs at one commit were clean in only 6.
  #
  # `fleet_warm` was built for exactly this and says so in its own header. It
  # was called by 8 drills of 111, and only 5 of those warmed the control-plane
  # binary — which is the seat that fails. A mitigation that every drill must
  # remember to invoke is one most drills will not have.
  #
  # So it moves to the one function all 113 callers already run, before any
  # seat is spawned. After the first drill it costs ~25 ms per binary, and the
  # first drill pays the real stall OUTSIDE any startup budget, which is the
  # whole point.
  fleet_warm ./target/release/flint-server ./target/release/flint-proxy \
             ./target/release/flint-controlplane ./target/release/flint-controller
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
# TWO KINDS OF SIBLING, and they do not deserve the same answer.
#
#   NAMED (flint-kv-server, flint-kv-chaos): a sibling FLEET. Those are
#   servers -- idle this second and saturating the box the next, because that
#   is what a fleet under a drill does. Presence is the signal; refuse.
#
#   BUILD/TEST (.../flint-kv/debug/deps/ttl-ccba…): cargo artifacts. A long
#   -running one at 0.04 cores is background noise, and refusing on its
#   presence blocks the gate indefinitely for nothing.
#
# Getting this wrong in the permissive direction is what fleet_guard_drill
# caught: measuring contention for BOTH collapsed the fleet contract, which
# that drill has asserted since it was written, into "is it busy right now".
# A sleeping fake fleet is not busy right now and never was the question.
_fleet_sibling_named() {
  ps -eo pid=,args= 2>/dev/null | awk '
    {
      n = split($2, parts, "/")
      exe = parts[n]
      if (exe ~ /^flint-(server|proxy|controlplane|controller|agent|console|ops|register|exporter|meter|chaos|bench|conformance|balance)$/) next
      if (exe !~ /^flint-[a-z0-9]+-[a-z0-9-]+$/) next
      print
    }'
}

_fleet_sibling() {
  ps -eo pid=,args= 2>/dev/null | awk '
    {
      n = split($2, parts, "/")
      exe = parts[n]
      if (exe ~ /^flint-(server|proxy|controlplane|controller|agent|console|ops|register|exporter|meter|chaos|bench|conformance|balance)$/) next
      sib = 0
      # By NAME: sibling components are flint-<project>-<component>.
      if (exe ~ /^flint-[a-z0-9]+-[a-z0-9-]+$/) sib = 1
      # By PATH, because the name rule only holds for a sibling projects
      # SHIPPED binaries. Its tests and helpers are named whatever cargo
      # calls them -- cold-modify, ttl-ccbacfe2d0cd3f35 -- and those are
      # exactly the processes a test run puts on the box. What they DO
      # carry is the project cargo target dir.
      #   .../flint-kv/release/cold-modify
      #   .../flint-kv/debug/deps/ttl-ccbacfe2d0cd3f35
      # Ours never match: this workspace builds to .../target/release/,
      # and "target" is not flint-<project>. Nor is a bare "flint", so a
      # sibling checkout of THIS project is left to _fleet_foreign.
      if (n >= 3 && (parts[n-1] == "release" || parts[n-1] == "debug") \
          && parts[n-2] ~ /^flint-[a-z0-9]+$/) sib = 1
      if (n >= 4 && parts[n-1] == "deps" \
          && (parts[n-2] == "release" || parts[n-2] == "debug") \
          && parts[n-3] ~ /^flint-[a-z0-9]+$/) sib = 1
      if (!sib) next
      print
    }'
}

# Fleet processes on this box that are NOT ours, as "pid argv" lines.
# Emits "pid ppid argv". The PPID is the discriminator between a fleet
# somebody is driving and a corpse nobody is: an orphan has been reparented
# to init, so ppid 1 means whoever started it is gone.
#
# That distinction cost the ops session FIVE HOURS. They saw /tmp/flint-m3-67*
# on the box, read it as "a peer is running their gate", and waited politely
# three times. The processes were orphans from a leak in m3_exit and nobody
# was driving them. One `ps -o ppid=` would have said so, and neither session
# ran it in four separate inspections — so it belongs in the tool rather than
# in anyone's head.
_fleet_foreign() {
  ps -eo pid=,ppid=,args= 2>/dev/null | awk -v scope="$FLEET_SCOPE" -v ports="$FLEET_PORTS" '

    # OWNERSHIP IS A PATH-BOUNDARY TEST, NOT A SUBSTRING TEST.
    #
    # This was `index(args, scope) > 0`, so any scope that is a PREFIX of
    # another scope adopts that other drills seats. Four such pairs exist in
    # tools/ today -- flint-cpha vs flint-cpha-ctl and flint-cpharoll-state,
    # flint-coproc vs flint-coproc-family, flint-fam vs flint-famcp -- so
    # fleet_kill from the shorter-scoped drill would SIGKILL the longer-scoped
    # one, and _fleet_foreign would report a false CLEAN for a genuinely
    # foreign seat, which is the direction that hurts. Serially neither can
    # happen (one fleet at a time), so it stayed invisible; it goes live the
    # moment two drills share a box.
    #
    # A scope owns a process only when the path continues at a boundary: end
    # of string, a slash, or a space. Same family as every other unanchored
    # match in docs/field-notes.md.
    function owns(hay, sc,   p, nxt, rest) {
      rest = hay
      while ((p = index(rest, sc)) > 0) {
        nxt = substr(rest, p + length(sc), 1)
        if (nxt == "" || nxt == "/" || nxt == " ") return 1
        rest = substr(rest, p + length(sc))
      }
      return 0
    }
    {
      # Rebuild argv from field 3 on, and match scope/ports against THAT
      # rather than the whole line. $0 now carries the ppid, and a ppid that
      # happens to equal one of our declared ports would exclude a genuinely
      # foreign process as "ours" — a false clean, which is the direction
      # that hurts.
      args = ""
      for (k = 3; k <= NF; k++) args = args (k > 3 ? " " : "") $k
      n = split($3, parts, "/")
      exe = parts[n]
      if (exe !~ /^flint-(server|proxy|controlplane|controller|agent|console|ops|register|exporter|meter)$/) next
      if (owns(args, scope)) next
      if (ports != "" && args ~ ("(^|[^0-9])(" ports ")([^0-9]|$)")) next
      print $1, $2, args
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

    # OWNERSHIP IS A PATH-BOUNDARY TEST, NOT A SUBSTRING TEST.
    #
    # This was `index(args, scope) > 0`, so any scope that is a PREFIX of
    # another scope adopts that other drills seats. Four such pairs exist in
    # tools/ today -- flint-cpha vs flint-cpha-ctl and flint-cpharoll-state,
    # flint-coproc vs flint-coproc-family, flint-fam vs flint-famcp -- so
    # fleet_kill from the shorter-scoped drill would SIGKILL the longer-scoped
    # one, and _fleet_foreign would report a false CLEAN for a genuinely
    # foreign seat, which is the direction that hurts. Serially neither can
    # happen (one fleet at a time), so it stayed invisible; it goes live the
    # moment two drills share a box.
    #
    # A scope owns a process only when the path continues at a boundary: end
    # of string, a slash, or a space. Same family as every other unanchored
    # match in docs/field-notes.md.
    function owns(hay, sc,   p, nxt, rest) {
      rest = hay
      while ((p = index(rest, sc)) > 0) {
        nxt = substr(rest, p + length(sc), 1)
        if (nxt == "" || nxt == "/" || nxt == " ") return 1
        rest = substr(rest, p + length(sc))
      }
      return 0
    }
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
      mine = owns($0, scope)
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

# fleet_wait_ready <port> [valkey-cli opts ...] — block until a DATA node is
# SERVING, or FAIL the drill on the spot.
#
# Since #176 a node binds and answers PING from INSIDE its initial full sync,
# so PONG no longer means ready — it means alive, which is exactly the
# distinction #176 exists to draw. Waiting on PONG where you meant ready
# returns the instant a re-seed STARTS, and everything after it runs against a
# node that refuses data commands with -LOADING.
#
# `loading:1` on FLINTINFO is the seat's own answer, and only an explicit 1
# counts as not-ready: a seat from a build before #176 has no such field and
# reads ready as soon as it answers, exactly as it always did.
fleet_wait_ready() {
  local port="$1"; shift
  local deadline=$(( $(date +%s) + 120 ))
  while :; do
    if [ "$(valkey-cli -p "$port" "$@" PING 2>/dev/null)" = "PONG" ] &&
       ! valkey-cli -p "$port" "$@" FLINTINFO 2>/dev/null | tr -d '\r' |
         grep -qx 'loading:1'; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "FAIL: 127.0.0.1:$port never finished loading (120s)"
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
# fleet_warm <bin ...> — pay each binary's first-exec loader stall OUTSIDE
# any timed window. macOS can spend 20-30s in the dynamic loader the first
# time a freshly linked binary runs; a drill whose cargo build re-linked a
# binary mid-gate then eats that stall inside its own 30s ping budget and
# fails with "no PONG" against a perfectly healthy build (gate run 3 lost
# five drills to exactly this). --build-version prints and exits, so this
# can never bind a port or leave a seat.
fleet_warm() {
  local bin
  for bin in "$@"; do
    [ -x "$bin" ] && "$bin" --build-version >/dev/null 2>&1
  done
  return 0
}

# What the box looked like AT THIS MOMENT, in the drill's own log.
#
# BUG-0035 cost an evening reconstructing whether another project was building
# during a gate that shed 20328 writes -- from ps snapshots taken at unrelated
# times and from what anyone remembered. A flaky run and a clean run must
# differ by a RECORD, not by a recollection. This is the cheap half of that:
# one line, at the moments that matter, in the log that is kept.
#
# Reads args, never comm. comm truncates at 15 characters, which silently
# turns flint-controlplane into flint-controlpl and drops seats from a census
# -- that cost a peer session a wrong seat count during a release verification
# on the day this was written.
#
# AND NEVER FILTER FOREIGN LOAD BY %CPU. A threshold on a SHARE under-reports
# a cohort exactly when the cohort is large, because share is not conserved:
# twelve spinners on this box sat at ~57% each, so `awk $1>40` found them,
# but killing eight left the survivors at 92% -- and a snapshot taken after a
# partial cleanup would have looked clean while four were still running. That
# happened: a `>40%` census reported 8 of 12 orphaned burners, honestly, and
# acting on it would have produced a second corrupt gate behind a teardown
# that looked verified. Filter on the INVARIANT -- ppid 1 plus a name you
# recognise -- which does not move when the cohort does.
fleet_env_note() {
  local where="${1:-}" sib load
  load="$(uptime | sed 's/.*averages*: //')"
  sib="$(_fleet_sibling | cut -c1-90 | tr '\n' ';' | sed 's/;$//')"
  if [ -n "$sib" ]; then
    echo "  env${where:+ [$where]}: load $load | SIBLING BUILD/TEST ON THIS BOX: $sib"
  else
    echo "  env${where:+ [$where]}: load $load | no sibling processes"
  fi
}

# IS A SIBLING ACTUALLY CONTENDING, or merely present?
#
# The refusal below used to fire on PRESENCE, which blocks indefinitely on a
# long-lived background process. That matters because the sibling branch is
# not protecting anything: fleet_kill only ever signals pids from
# _fleet_ours, anchored to our single-segment binary names, so a sibling can
# never be killed by this suite. The only harm a sibling does is CONTENTION,
# and contention is a matter of degree.
#
# WHY NOT ZERO-VS-NONZERO CPU, which would need no magic number: measured on
# the box that prompted this, a flint-kv background process used 0.13s of CPU
# across 3s -- about 4% of one core out of eight. Non-zero, and nowhere near
# enough to disturb a drill. A categorical test would keep blocking forever,
# which is the behaviour being fixed.
#
# So two signals, and the cheap one carries most of the weight:
#
#   * THE PROCESS SET CHANGING. A `cargo test` sweep spawns and reaps a
#     rotating cast -- engine-…, stress-…, d5_sst_shape-…, load-… -- so its
#     signature is churn, not any particular process. This needs no threshold
#     at all and is what actually broke gates 25, 27 and 28.
#   * AGGREGATE CPU, for the case churn cannot see: one long-running heavy
#     test binary. Only here is a magnitude used, and it is expressed in
#     CORES so it means the same thing on a different machine.
#
# BUG-0030's lesson is that a threshold calibrated on one machine is a bug in
# waiting, so the measurement is PRINTED every time it decides. If this ever
# proceeds when it should not, the drill log says what it measured and which
# signal it read, which is the difference between a wrong call and a mystery.

# pid and cumulative CPU centiseconds for each sibling process.
_fleet_sibling_sample() {
  local p t
  _fleet_sibling | awk '{print $1}' | while read -r p; do
    t="$(ps -p "$p" -o time= 2>/dev/null | tr -d ' ')"
    [ -n "$t" ] || continue
    # M:SS.cc, or H:MM:SS.cc once a process has been up an hour.
    printf '%s %s\n' "$p" "$(printf '%s' "$t" | awk -F: '{
        secs = 0; mult = 1
        for (i = NF; i >= 1; i--) { secs += $i * mult; mult *= 60 }
        printf "%d", secs * 100
      }')"
  done
}

# Prints "<set-changed:0|1> <cores>" across a short window.
_fleet_sibling_activity() {
  local win="${FLINT_SIBLING_WINDOW:-3}" s1 s2
  s1="$(_fleet_sibling_sample)"
  [ -n "$s1" ] || { echo "0 0.00"; return 0; }
  sleep "$win"
  s2="$(_fleet_sibling_sample)"
  printf '%s\n---\n%s\n' "$s1" "$s2" | awk -v win="$win" '
    /^---$/ { second = 1; next }
    !second { t1[$1] = $2; next }
             { t2[$1] = $2 }
    END {
      changed = 0
      for (p in t1) if (!(p in t2)) changed = 1
      for (p in t2) if (!(p in t1)) changed = 1
      d = 0
      for (p in t2) if (p in t1) { x = t2[p] - t1[p]; if (x > 0) d += x }
      printf "%d %.2f\n", changed, d / (win * 100)
    }'
}

# 0 = safe to proceed, 1 = still contending. Waits up to FLINT_DRILL_WAIT
# seconds, which turns an indefinite block into a QUEUE: a sweep that ends
# lets the drill run instead of failing it. Waiting never proceeds under
# contention, so this weakens nothing -- it only stops throwing away a run
# that would have been fine five minutes later.
_fleet_sibling_settle() {
  local budget="${FLINT_DRILL_WAIT:-0}" waited=0 act changed cores
  local floor="${FLINT_SIBLING_CORES:-0.5}"
  while :; do
    act="$(_fleet_sibling_activity)"
    changed="${act%% *}"
    cores="${act##* }"
    if [ -z "$(_fleet_sibling)" ]; then
      [ "$waited" -gt 0 ] && echo "  sibling work finished after ${waited}s — proceeding"
      return 0
    fi
    if [ "$changed" = "0" ] && awk "BEGIN{exit !($cores < $floor)}"; then
      echo "  sibling present but NOT contending: ${cores} core(s) over ${FLINT_SIBLING_WINDOW:-3}s,"
      echo "  process set unchanged (floor ${floor}). Proceeding; the env line above names it."
      return 0
    fi
    if [ "$waited" -ge "$budget" ]; then
      if [ "$changed" = "1" ]; then
        echo "  sibling is CONTENDING: process set changing — a build or test sweep"
        echo "  (${cores} core(s) over ${FLINT_SIBLING_WINDOW:-3}s)"
      else
        echo "  sibling is CONTENDING: ${cores} core(s) over ${FLINT_SIBLING_WINDOW:-3}s,"
        echo "  at or above the ${floor}-core floor"
      fi
      [ "$budget" -gt 0 ] && echo "  waited ${waited}s of the ${budget}s FLINT_DRILL_WAIT budget"
      return 1
    fi
    echo "  waiting for sibling work to finish (${cores} core(s), ${waited}s/${budget}s)"
    sleep 10
    waited=$(( waited + 10 + ${FLINT_SIBLING_WINDOW:-3} ))
  done
}

# _fleet_live_peer_scopes -- scope dirs of drills from THIS suite running right
# now, each proven live by its own fleet_init lock.
#
# This is the distinction fleet_guard could not previously make. _fleet_foreign
# reports "one of OUR binaries, outside MY scope", which is true of a genuinely
# foreign fleet AND of the drill running beside this one in a parallel gate --
# and those two need opposite answers. Refusing a peer makes a parallel drills
# stage impossible; tolerating a foreign fleet is the harm tools/lib/fleet.sh
# was written to prevent.
#
# The lock settles it. fleet_init takes ${scope}.lock atomically and records
# pid PLUS process start time; a peer drill therefore leaves a live lock, and
# nothing else on the box does. Liveness is pid AND start time, never the pid
# alone -- a reused pid would launder a dead run into a live peer, which is the
# direction that hurts.
_fleet_live_peer_scopes() {
  local root="${FLINT_DRILL_ROOT:-/tmp}" lock owner started now scope
  for lock in "$root"/*.lock; do
    [ -d "$lock" ] || continue
    owner="$(cat "$lock/pid" 2>/dev/null || true)"
    [ -n "$owner" ] || continue
    started="$(cat "$lock/started" 2>/dev/null || true)"
    now="$(ps -o lstart= -p "$owner" 2>/dev/null || true)"
    [ -n "$now" ] && [ "$now" = "$started" ] || continue
    scope="${lock%.lock}"
    [ "$scope" = "${FLEET_SCOPE:-}" ] && continue
    # scope|ports -- neither key alone identifies every seat a peer owns.
    printf '%s|%s\n' "$scope" "$(cat "$lock/ports" 2>/dev/null || true)"
  done
}

# Drop the lines owned by a live peer scope. Boundary match, not substring:
# flint-cpha must not adopt flint-cpha-ctl.
_fleet_drop_peer_lines() {  # <peer entries: scope|ports>   (stdin: seat lines)
  local peers="$1" line entry scope ports keep
  while IFS= read -r line; do
    keep=1
    for entry in $peers; do
      scope="${entry%%|*}"; ports="${entry#*|}"
      [ "$ports" = "$entry" ] && ports=""
      case "$line" in
        *"$scope"/*|*"$scope"|*"$scope "*) keep=0; break ;;
      esac
      if [ -n "$ports" ] && printf '%s\n' "$line" | grep -qE "(^|[^0-9])($ports)([^0-9]|\$)"; then
        keep=0; break
      fi
    done
    [ "$keep" = 1 ] && printf '%s\n' "$line"
  done
  return 0
}

fleet_guard() {
  local foreign sibling
  # Before the decision, not after it: a refused run is exactly the run whose
  # environment someone will want to read later.
  fleet_env_note guard
  foreign="$(_fleet_foreign)"
  sibling="$(_fleet_sibling)"
  # A PEER DRILL FROM THIS SUITE IS NOT A FOREIGN FLEET. Only under
  # FLINT_DRILL_PARALLEL=1, which the gate sets when it runs the drills stage
  # with more than one job -- outside that, a second fleet on the box is still
  # refused exactly as before, because outside a parallel gate nobody should be
  # starting one. A sibling PROJECT (flint-kv) is never tolerated here: that is
  # contention, and it is not ours to reason about.
  if [ -n "$foreign" ] && [ "${FLINT_DRILL_PARALLEL:-0}" = "1" ]; then
    local peers peers_n before_n after_n
    peers="$(_fleet_live_peer_scopes)"
    if [ -n "$peers" ]; then
      # `grep -c . || echo 0` prints TWO numbers when there is no match:
      # grep emits its count of 0 AND exits 1, so the fallback fires too and
      # the arithmetic sees "0\n0". Let grep report the count and swallow only
      # its exit status.
      before_n=$(printf '%s\n' "$foreign" | grep -c . || true)
      foreign="$(printf '%s\n' "$foreign" | _fleet_drop_peer_lines "$peers")"
      after_n=$(printf '%s\n' "$foreign" | grep -c . || true)
      peers_n=$(( ${before_n:-0} - ${after_n:-0} ))
      [ "$peers_n" -gt 0 ] && echo "  ($peers_n seat(s) belong to $(printf '%s\n' "$peers" | grep -c .) live peer drill(s) in this suite -- not foreign)"
    fi
  fi
  [ -z "$foreign" ] && [ -z "$sibling" ] && return 0
  if [ "${FLINT_DRILL_FORCE:-0}" = "1" ]; then
    local n=0
    [ -n "$foreign" ] && n=$(( n + $(echo "$foreign" | wc -l | tr -d ' ') ))
    [ -n "$sibling" ] && n=$(( n + $(echo "$sibling" | wc -l | tr -d ' ') ))
    echo "  (FLINT_DRILL_FORCE=1: proceeding despite $n other flint process(es))"
    return 0
  fi
  # A sibling project's FLEET refuses on sight, as it always has. Only its
  # build/test artifacts are resolved by measurement and waiting, because
  # presence of a cargo binary is not contention. `foreign` is different
  # again -- those are OUR binaries, which fleet_kill would signal.
  if [ -n "$sibling" ] && [ -z "$foreign" ] && [ -z "$(_fleet_sibling_named)" ]; then
    _fleet_sibling_settle && return 0
  fi
  # A SEAT ON ITS WAY OUT IS NOT A FLEET. The gate runs drills back to back,
  # and the previous drill's seats can still be exiting when the next one's
  # guard samples: gate 31 refused backup_schedule over two flint-lagreach
  # servers that were gone seconds later. Refusing there fails a run over a
  # teardown race.
  #
  # So give it a few seconds. A real foreign fleet -- someone's live cluster,
  # another suite mid-run -- is still there afterwards and still refuses; only
  # the dying case clears. The budget is deliberately small and separate from
  # FLINT_DRILL_WAIT, which is about a sibling's BUILD finishing (minutes).
  # This is about a process exiting (seconds).
  if [ -n "$foreign" ]; then
    local _fw=0
    while [ -n "$foreign" ] && [ "$_fw" -lt "${FLINT_FOREIGN_SETTLE:-15}" ]; do
      sleep 1
      _fw=$(( _fw + 1 ))
      foreign="$(_fleet_foreign)"
    done
    if [ -z "$foreign" ]; then
      echo "  out-of-scope seats exited during teardown (${_fw}s) — proceeding"
      [ -z "$sibling" ] && return 0
    fi
  fi
  if [ -n "$foreign" ]; then
    local _orph _live
    _orph=$(printf '%s\n' "$foreign" | awk '$2 == 1' | wc -l | tr -d ' ')
    _live=$(printf '%s\n' "$foreign" | awk '$2 != 1' | wc -l | tr -d ' ')
    echo "REFUSING TO RUN: this box already has Flint processes outside $FLEET_SCOPE"
    printf '%s\n' "$foreign" | awk '{ printf "    pid %-7s ppid %-7s %s\n", $1, $2, substr($0, index($0,$3), 100) }'
    echo "  A drill that killed those would destroy a fleet it does not own —"
    echo "  a live cluster, or another suite's nodes."
    # Say whether WAITING can possibly help. An orphan has no parent left to
    # finish and clear it, so "wait for the other session" is wrong advice.
    if [ "$_live" = "0" ]; then
      echo "  ALL $_orph ARE ORPHANS (ppid 1): nobody is driving them, so"
      echo "  waiting will not clear this. Someone has to remove them."
    elif [ "$_orph" != "0" ]; then
      echo "  $_orph of $((_orph + _live)) are ORPHANS (ppid 1) — those will"
      echo "  never clear on their own; the other $_live have a live parent."
    fi
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

# fleet_pids <component ...> -- the pids of THIS fleet's seats, scoped by the
# scope dir and ports the drill declared to fleet_init.
#
# Use this instead of `pgrep -f '/target/release/flint-<x> '`. pgrep answers
# "how many are on this BOX", which is a different question with a different
# answer the moment a second drill runs beside you: controller_stall and
# failover_bystander both asserted "expected exactly 1" against a box-wide
# count and failed at P=4 over a sibling's seat (2026-08-23 A/B run). Their
# asserts were right to fire -- the census was measuring the wrong set.
fleet_pids() { _fleet_ours "$@"; }

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
    # ANCHORED, exactly as _fleet_ours selected it. A substring test for
    # "flint-" was looser than the selection that fed this loop, so a reused
    # pid now owned by a SIBLING project (.../cargo-target/flint-kv/...
    # contains "flint-") would pass a check whose whole job is to confirm the
    # pid is still one of OURS -- and this repo must never signal another
    # project's processes. A re-verification weaker than the selection is not
    # a re-verification.
    args="$(ps -o args= -p "$pid" 2>/dev/null)"
    case "$(basename "${args%% *}")" in
      flint-server|flint-proxy|flint-controlplane|flint-controller|flint-agent) ;;
      flint-console|flint-ops|flint-register|flint-exporter|flint-meter|flint-backup) ;;
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

# -THROTTLED IS BACKPRESSURE, NOT LOSS, AND A DRILL MUST NOT CONFLATE THEM.
#
# The master sheds writes with -THROTTLED once replica lag passes
# --lag-hard-ms. A shed write was never acked, so a key missing because of it
# is CORRECTLY absent. Two drills got this wrong in opposite directions
# (BUG-0035):
#
#   * controller_drill piped 20000 keys, shed 356, then asserted
#     key:0019999 was on the promoted replica. It printed "FAIL: tail lost" —
#     acked-write loss across a failover, the most serious claim this product
#     makes — for a write the master had openly REFUSED.
#   * repl_drill piped under `set -euo pipefail`, where --pipe's non-zero exit
#     on any error aborted the run at the load step. It printed "FAIL repl"
#     for seven assertions it never reached.
#
# THE FIX THAT DID NOT WORK, recorded because it is the tempting one: replay
# the whole stream until nothing sheds. Measured against a 5 ms cap, attempts
# shed 19388, 19469, 19667, 19413, 19337 of 20000 — it never converges,
# because the replay is itself a firehose and recreates the lag that caused
# the shed. A retry whose load profile equals the load that failed is not a
# retry.
#
# So the load is allowed to shed, the count is reported, and the specific keys
# a drill ASSERTS on are repaired afterwards one at a time, which lets the
# replica drain between writes and therefore converges.

# Pipe a RESP stream in and REPORT what was shed. Never fails the drill on
# shed alone; does fail if the load could not be delivered at all, because
# "nothing was written" and "everything was refused" must not look alike.
# $1 = port, $2 = name of a function writing the RESP stream to stdout.
fleet_load_resp() {
  local port="$1" gen="$2" errs replies out
  # The load phase is where the shed happens, and it can be minutes after
  # fleet_guard ran. Record here too, so a -THROTTLED count and the box that
  # produced it sit on adjacent lines.
  fleet_env_note load
  # `|| true`: --pipe exits non-zero when it counts errors, and under the
  # caller's `set -e`/`pipefail` that alone would abort the drill here.
  out=$( { $gen | valkey-cli -p "$port" --pipe 2>&1 | tail -1; } || true )
  errs=$(printf '%s' "$out" | sed -n 's/.*errors: \([0-9][0-9]*\).*/\1/p')
  replies=$(printf '%s' "$out" | sed -n 's/.*replies: \([0-9][0-9]*\).*/\1/p')
  [ -n "$errs" ] || errs=0
  [ -n "$replies" ] || replies=0
  echo "  load: $out"
  if [ "$replies" = "0" ]; then
    echo "FAIL: the load delivered nothing at all — the node did not answer."
    echo "      This is NOT shedding; it is a dead or unreachable seat."
    return 1
  fi
  if [ "$errs" != "0" ]; then
    echo "  note: $errs of $replies writes shed -THROTTLED (replica lag past"
    echo "        the cap). Shed writes were NEVER ACKED, so nothing is lost."
    echo "        Keys this drill asserts on are repaired below; see BUG-0035"
    echo "        for why the cap is reachable here at all."
  fi
  return 0
}

# Retry ONE write past -THROTTLED. The primitive both repairs are built on.
# Converges where a bulk replay cannot: a single write at a time lets the
# replica drain between them, which a firehose replay never does.
# $1 = port, then the command and its arguments.
fleet_retry_write() {
  local port="$1"; shift
  local tries=0 out
  while :; do
    out=$(valkey-cli -p "$port" "$@" 2>&1 || true)
    case "$out" in
      OK|[0-9]*) return 0 ;;   # +OK, or an integer reply (HSET gives 0 or 1)
      *THROTTLED*)
        tries=$((tries + 1))
        if [ "$tries" -ge 60 ]; then
          echo "FAIL: '$*' still shed -THROTTLED after $tries single-write"
          echo "      retries over $((tries / 4))s. The replica is not draining."
          return 1
        fi
        sleep 0.25 ;;
      *)
        echo "FAIL: '$*' returned '$out'"
        return 1 ;;
    esac
  done
}

# Make sure specific keys exist. $1 = port, then k=v pairs.
fleet_ensure_keys() {
  local port="$1"; shift
  local kv k v
  for kv in "$@"; do
    k="${kv%%=*}"; v="${kv#*=}"
    fleet_retry_write "$port" SET "$k" "$v" || return 1
  done
  return 0
}
