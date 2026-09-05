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
  # LAST RUN'S LOGS ARE NOT THIS RUN'S EVIDENCE. The bring-up spawns write
  # ${FLEET_SCOPE}<name>.log and nothing else removes them, so a re-run would
  # find the PREVIOUS failure's output sitting there and fleet_why_not_up
  # would print it as the reason for today's -- a stale cause read as a fresh
  # one, which is worse than having none. Cleared here rather than in a trap,
  # because the drills install their own EXIT traps and the last one wins.
  rm -f "${FLEET_SCOPE}"*.log 2>/dev/null || true
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
  #
  # THE LIST IS `FLEET_BINARIES`, NOT A SUBSET OF IT. This warmed four of the
  # seven binaries flintctl can spawn, which was not a decision -- it was the
  # four that existed when the warm was written. `fleet_warm` skips anything
  # not present (`[ -x ]`), so naming all seven costs nothing on a workspace
  # that does not build them all and cannot go stale the next time a seat type
  # is added. `assert_warm_covers_fleet_binaries` in tools/gates.sh fails the
  # build if this drifts from the const again.
  fleet_warm ./target/release/flint-server ./target/release/flint-proxy \
             ./target/release/flint-controlplane ./target/release/flint-controller \
             ./target/release/flint-agent ./target/release/flint-backup \
             ./target/release/flint-vec
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
  # Emits "pid ppid argv", matching _fleet_foreign. The PPID is here for the
  # same reason it is there: it separates a fleet somebody is DRIVING from a
  # corpse nobody is. See _fleet_sibling_named_live.
  ps -eo pid=,ppid=,args= 2>/dev/null | awk '
    {
      n = split($3, parts, "/")
      exe = parts[n]
      if (exe ~ /^flint-(server|proxy|controlplane|controller|agent|console|ops|register|exporter|meter|chaos|bench|conformance|balance)$/) next
      if (exe !~ /^flint-[a-z0-9]+-[a-z0-9-]+$/) next
      print
    }'
}

# Named sibling FLEETS that somebody is actually driving (ppid != 1).
#
# BUG-0063. "A sibling project's fleet refuses on sight" is the contract, and
# it is the right one -- a sleeping fake fleet is still a fleet, which is what
# fleet_guard_drill asserts and what measuring contention for named binaries
# would have destroyed. But the contract rests on a presumption: that a named
# binary is a fleet SOMEONE IS RUNNING. An orphan is the case where that
# presumption is false, and it is not rare -- the flint-kv suite left one
# behind twice in one hour on 2026-08-27, each time blocking every drill on
# the box until a human killed it by hand.
#
# So orphanhood, NOT idleness, is the discriminator. A named sibling with a
# live parent still refuses on sight, unmeasured, exactly as before. One at
# ppid 1 falls through to the same activity check its own test binaries get,
# which proceeds only if it is genuinely not contending and still refuses if
# it is. The guard exists to prevent CONTENTION; a corpse at 0.0% CPU
# contends for nothing, and waiting for it is waiting for nobody.
_fleet_sibling_named_live() {
  _fleet_sibling_named | awk '$2 != 1'
}

_fleet_sibling() {
  # Emits "pid ppid argv" -- see _fleet_sibling_named. Consumers take the pid
  # from $1, which is unchanged.
  ps -eo pid=,ppid=,args= 2>/dev/null | awk '
    {
      n = split($3, parts, "/")
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
      # AN EMPTY SCOPE OWNS NOTHING, said here rather than assumed upstream.
      # With sc == "", index() returns 1 every iteration and rest never
      # shortens, so this loop either spins FOREVER or -- because
      # `ps -eo pid=,args=` right-aligns the pid, so every line begins with a
      # space -- takes the nxt == " " branch and returns 1 for EVERY flint
      # process on the box. That would turn `fleet_kill server` into a blanket
      # sweep across every drill sharing the machine.
      #
      # NOT reachable today: fleet_init refuses an empty scope, and all 131
      # drills plus the 5 benches that source this call it before their first
      # kill or guard (checked). A guard on the invariant, not a fix for a live
      # fault -- the cost of being wrong here is seats belonging to others.
      if (sc == "") return 0
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
  # Refuse loudly rather than quietly matching nothing. owns() returns 0 for an
  # empty scope now, which is the safe direction, but a fleet_kill that
  # silently kills nothing is its own wrong answer: the caller believes the
  # seats are gone. fleet_init makes this state impossible; if it ever becomes
  # possible, say so rather than proceed.
  if [ -z "${FLEET_SCOPE:-}" ] && [ -z "${FLEET_PORTS:-}" ]; then
    echo "_fleet_ours: no FLEET_SCOPE and no FLEET_PORTS -- call fleet_init first." >&2
    return 0
  fi
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
      # AN EMPTY SCOPE OWNS NOTHING, said here rather than assumed upstream.
      # With sc == "", index() returns 1 every iteration and rest never
      # shortens, so this loop either spins FOREVER or -- because
      # `ps -eo pid=,args=` right-aligns the pid, so every line begins with a
      # space -- takes the nxt == " " branch and returns 1 for EVERY flint
      # process on the box. That would turn `fleet_kill server` into a blanket
      # sweep across every drill sharing the machine.
      #
      # NOT reachable today: fleet_init refuses an empty scope, and all 131
      # drills plus the 5 benches that source this call it before their first
      # kill or guard (checked). A guard on the invariant, not a fix for a live
      # fault -- the cost of being wrong here is seats belonging to others.
      if (sc == "") return 0
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
# WHY DID IT NOT COME UP?
#
# "nothing listening on 127.0.0.1:6440 after 30s" is a symptom. The process
# said why -- a rejected flag, a port already held, a missing engine feature,
# an unwritable data dir -- and 77 spawns across 31 drills here send that to
# /dev/null. This prints whatever the scope directory does hold, and SAYS SO
# when it holds nothing, so the reader learns the reason was discarded rather
# than that there was none.
#
# The ops repo hit this as OPS-0117: a standing note there read "local
# flint-server lacks rocks -- drills fail at bring-up looking like a product
# bug", which was this exact defect written down as a fact about the
# environment instead of a bug in the harness.
fleet_why_not_up() {
  local d="${FLEET_SCOPE:-}" f n=0
  # THREE PLACES, because two sessions each found a different half of this.
  #
  #   "$d"*.log       the scope is a PREFIX, not a directory. `fleet_init` is
  #                   called with `$FLINT_DRILL_ROOT/flint-bloom-` and the
  #                   lock beside it is `${FLEET_SCOPE}.lock`, a sibling FILE,
  #                   so a `[ -d "$d" ]` guard is false and finds nothing.
  #   "$d"/*.log      the scope IS a directory for some drills.
  #   "$d"/logs/*.log `flintctl` writes seat logs to `<statedir>/logs/<name>.log`
  #                   (crates/flint-ctl/src/main.rs:1438), one level below the
  #                   top-level glob, for the 11 drills whose scope is the
  #                   inventory statedir.
  #
  # Each version alone reported "nothing found" for the cases the other
  # covered -- "cannot look" rendered as "absent", inside the function written
  # to stop exactly that. No `[ -d ]` gate: an unmatched glob is skipped by
  # the `-f` test below, and gating on it is what hid the prefix case.
  if [ -n "$d" ]; then
    for f in "$d"*.log "$d"/*.log "$d"/logs/*.log; do
      [ -f "$f" ] && [ -s "$f" ] || continue
      n=$((n + 1))
      echo "  --- $f (last 15 lines) ---"
      tail -n 15 "$f" | sed 's/^/  /'
    done
  fi
  # AND DO NOT ASSERT THE CAUSE. This said the spawn "sent stderr to
  # /dev/null", which is one explanation among several -- the scope may not be
  # a directory, the seat may have died before writing, or the logs may live
  # somewhere this does not know about. Name where it looked and let the
  # reader draw the conclusion.
  [ "$n" = 0 ] && echo "  (nothing readable at ${d:-<no FLEET_SCOPE>}*.log, in it, or in its logs/ -- the reason may have been discarded at the spawn, or written somewhere this did not look)"
  return 0
}


fleet_wait_listen() {
  local port deadline
  for port in "$@"; do
    deadline=$(( $(date +%s) + 30 ))
    while :; do
      if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then break; fi
      if [ "$(date +%s)" -ge "$deadline" ]; then
        echo "FAIL: nothing listening on 127.0.0.1:$port after 30s"
        fleet_why_not_up
        # EXIT, like fleet_wait_ping does. This RETURNED, and the drills run
        # under `set -u` rather than `set -e`, so 131 of the bring-up waits
        # here ignore the result and run on into their assertions -- reporting
        # whichever one trips first, a claim about the PRODUCT, for what was
        # really a seat that never started. Two functions in one file, one
        # returning and one exiting, reads as two contracts rather than one
        # oversight. Same defect as the ops repo's OPS-0117.
        exit 1
      fi
      sleep 0.05
    done
  done
  return 0
}

# fleet_wait_log <logfile> <pattern> [budget-s] — block until PATTERN appears
# in LOGFILE. Returns 0 if it appeared, 1 if the budget ran out.
#
# WHY THIS EXISTS. Eight sites across six drills spelled "wait until the seat
# has logged what it decided" as a fixed sleep — 0.6s, 0.8s, 1.0s, 1.2s — and
# then grepped the log on the very next line. The sleep is not the assertion;
# the grep is. So when the sleep is too short the drill fails with its
# CONFIGURATION message ("node not in mTLS mode", "proxy did not enable TLS")
# for a purely TIMING reason, and sends the reader to the wrong half of the
# problem. That misdirection dead-ended three investigations on disk_pressure
# before anyone doubted the failure text.
#
# Those numbers were all chosen on an idle laptop where a seat logs its mode in
# milliseconds. CI now runs drills four at a time, so they are being asked to
# hold under contention they were never measured against.
#
# The CALLER KEEPS ITS OWN grep as the verdict, deliberately: this only removes
# the race, so every drill's failure text stays exactly what its author wrote.
fleet_wait_log() {
  local _f="$1" _pat="$2" _budget="${3:-15}" _deadline
  _deadline=$(( $(date +%s) + _budget ))
  while :; do
    grep -q -- "$_pat" "$_f" 2>/dev/null && return 0
    [ "$(date +%s)" -ge "$_deadline" ] && return 1
    sleep 0.05
  done
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
# fleet_wait_alive <port> [opts] — block until the seat ANSWERS, loading or not.
#
# This is what fleet_wait_ping used to be, and it is now the rarer of the two.
# Use it only when the loading window is the thing under test: loading_visible
# has to catch a replica WHILE it reports loading:1, and a helper that waits
# for that flag to clear would delete the very state the drill exists to
# observe — a pass it did not earn.
#
# Everywhere else, wanting "alive" and meaning "ready" is the bug #176
# introduced. Reach for fleet_wait_ping unless you can say why not.
fleet_wait_alive() {
  local port="$1"; shift
  local deadline=$(( $(date +%s) + 30 ))
  while :; do
    [ "$(valkey-cli -p "$port" "$@" PING 2>/dev/null)" = "PONG" ] && return 0
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "FAIL: no PONG from 127.0.0.1:$port after 30s"
      fleet_why_not_up
      exit 1
    fi
    sleep 0.2
  done
}

# READY, AS A PREDICATE. Returns 0/1 and prints nothing, so a caller can keep
# its own loop, its own budget and its own failure message — which is why the
# drills that needed this were each spelling readiness by hand instead.
#
# PONG stopped meaning ready at #176: a node binds and answers PING from inside
# its load, deliberately, so a client can tell "starting" from "absent". Four
# drills reddened main one at a time on four different spellings of the same
# mistake — PONG alone, a bind, a non-empty field, a fixed sleep — and each was
# fixed where it was seen rather than where it lives.
#
# SAFE AGAINST A NON-FLINT SERVER, which is what makes one predicate enough.
# FLINTINFO to valkey, or to anything that does not implement it, errors and
# prints nothing; nothing contains no `loading:1`, so the answer is "ready" —
# correct, because a server with no loading state is ready as soon as it
# answers. Callers therefore do not have to know whether the port belongs to a
# node, a control plane, a proxy or the conformance oracle.
# LIVENESS ONLY. NEVER INVERT THIS FOR A DEATH CHECK.
#
# `fleet_ready` answers "is it up and serving". Its negation does NOT answer
# "is it gone": a failed PING is also what you get from a hung server, a
# saturated backlog, or a dropped packet, and none of those mean the process
# exited. Death is a SOCKET question — connect-refused — and asking it at the
# protocol level makes a slow server look like a dead one.
#
# So the discriminator for converting a wait is two-dimensional, not one:
# WHAT is on the other end (a node has a loading state, a proxy does not) and
# WHICH DIRECTION is being asked (liveness needs the protocol, death needs the
# socket). The same line can be correct for one direction and wrong for the
# other, so a sweep keyed on component alone flags the right lines with the
# wrong verdict half the time. Credit to the accelerator session, which hit
# this converting a fixture wait and correctly did NOT convert its
# corresponding kill check.
#
# In this tree the live example is cold_start_roles_drill.sh's
# `! valkey-cli -p "$p" PING ... || "still serving after stop"`. That is a
# death check and must stay socket-shaped. It is deliberately not converted.
fleet_ready() {
  local port="$1"; shift
  [ "$(valkey-cli -p "$port" "$@" PING 2>/dev/null)" = "PONG" ] &&
    ! valkey-cli -p "$port" "$@" FLINTINFO 2>/dev/null | tr -d '\r' | grep -qx 'loading:1'
}

fleet_wait_ping() {
  # PONG ALONE STOPPED MEANING READY, SO THIS CHECKS BOTH.
  #
  # Before #176 a node did not bind its listener until its initial full sync
  # finished, so PONG and "serving" were the same event and every caller here
  # spelled "ready" as PONG because that was the only spelling available.
  # #176 makes a node answer PING from INSIDE the sync — deliberately, so it is
  # not mistaken for dead — which silently converted 50 callers of this
  # function from "wait until ready" to "wait until alive", and everything
  # after them began racing a node that answers data commands with -LOADING.
  #
  # Measured: promote_notice ran in exactly 10s across 18 consecutive gates,
  # then 43s / 8s / 42s once #176 landed, failing on the third. That is what a
  # race looks like from the outside.
  #
  # 775911c converted four drills to fleet_wait_ready and left the rest. The
  # fix belongs here instead: restore what the callers already meant, at the
  # one place they all go through. `loading:1` is the seat's own answer and
  # only an explicit 1 counts, so a build without the field behaves exactly as
  # it always did.
  local port="$1"; shift
  local deadline=$(( $(date +%s) + 30 ))
  while :; do
    # ONE DEFINITION OF READY, shared with fleet_ready above. Two copies of a
    # predicate is how the drills drifted apart in the first place.
    if fleet_ready "$port" "$@"; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      # Say WHICH of the two conditions was not met. "no PONG" against a node
      # that is answering fine but still loading sends the reader to the wrong
      # half of the problem.
      if [ "$(valkey-cli -p "$port" "$@" PING 2>/dev/null)" = "PONG" ]; then
        echo "FAIL: 127.0.0.1:$port answers PING but still reports loading:1 after 30s"
      else
        echo "FAIL: no PONG from 127.0.0.1:$port after 30s"
      fi
      # Useful in BOTH branches: a node stuck in `loading` has a log saying
      # how far its sync got, which is the next question either way.
      fleet_why_not_up
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
# fleet_wait_replicated <master_port> [valkey-cli opts ...] — block until every
# live replica has ACKED everything the master has, or FAIL the drill.
#
# A write is acked when it is durable ON THE MASTER; the replica catches up
# afterwards. So "I wrote it, then I killed the master and promoted the
# replica" does NOT imply the promoted replica has it — that gap is the
# failover RPO, and it is a property of the system, not a bug in it.
#
# A drill that kills a master straight after writing is therefore racing
# replication, and the race is invisible while writes are slow. BUG-0078's
# TCP_NODELAY fix made a pipelined corpus land ~60x faster, which turned that
# latent race into a ~60% failure: measured on one box, same binary,
# backup_drill went 8/8 green with the stall left in (FLINT_NAGLE_TEST=1) and
# 3/8 with it removed. Nothing about the product changed; the drill had been
# relying on the master being slow.
#
# Waits on the master's own `seq_lag`, which is latest_seq minus the slowest
# live replica's ack, and requires a replica to actually BE there: with no
# live replica there is nothing to catch up and a lag of 0 would mean the
# opposite of what the caller is asking.
fleet_wait_replicated() {
  local port="$1"; shift
  local deadline=$(( $(date +%s) + 30 ))
  local lag live
  while :; do
    local info; info=$(valkey-cli -p "$port" "$@" FLINTINFO 2>/dev/null | tr -d '\r')
    live=$(printf '%s\n' "$info" | grep '^live_replicas:' | cut -d: -f2)
    lag=$(printf '%s\n' "$info" | grep '^seq_lag:' | cut -d: -f2)
    if [ "${live:-0}" -ge 1 ] 2>/dev/null && [ "${lag:-1}" = "0" ]; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "FAIL: 127.0.0.1:$port still has seq_lag=${lag:-?} across ${live:-0}"
      echo "      live replica(s) after 30s. Whatever this drill does next"
      echo "      assumes the replica holds what the master was just sent."
      exit 1
    fi
    sleep 0.2
  done
}

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
# AVAILABLE MEMORY, because load average cannot see the failure that matters.
#
# A drill seat SIGKILLed in the middle of a run, with peer drills live, is what
# an OOM kill looks like -- and OOM is a memory event, so the load figure beside
# it can be entirely unremarkable while it happens. That is not hypothetical
# here: load was measured across five failing and four passing gate runs and
# does not separate them at all (docs/bugs/0064), so the axis worth recording is
# the one nothing records.
#
# BUG-0060 is the reason to suspect it: every resource limit in this product is
# per-unit and nothing bounds the aggregate. A gate running FLINT_GATE_JOBS
# drills at once, each with several RocksDB seats, is precisely where an
# unbounded aggregate would be reached first.
#
# UNREADABLE is a third state, not zero. macOS has no /proc/meminfo and this
# says so rather than inventing a number from vm_stat -- the gate runs on Linux,
# which is where the question lives, and a fabricated local figure would be
# compared against real ones later.
fleet_mem_note() {
  if [ -r /proc/meminfo ]; then
    awk '/^MemAvailable:/ { a = $2 } /^MemTotal:/ { t = $2 }
         END { if (a && t) printf "mem %.1f of %.1f GiB free", a/1048576, t/1048576
               else printf "mem UNREADABLE (meminfo lacked the fields)" }' /proc/meminfo
  else
    printf 'mem UNREADABLE (no /proc/meminfo)'
  fi
}

fleet_env_note() {
  local where="${1:-}" sib load mem
  load="$(uptime | sed 's/.*averages*: //')"
  mem="$(fleet_mem_note)"
  sib="$(_fleet_sibling | cut -c1-90 | tr '\n' ';' | sed 's/;$//')"
  if [ -n "$sib" ]; then
    echo "  env${where:+ [$where]}: load $load | $mem | SIBLING BUILD/TEST ON THIS BOX: $sib"
  else
    echo "  env${where:+ [$where]}: load $load | $mem | no sibling processes"
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
  if [ -n "$sibling" ] && [ -z "$foreign" ] && [ -z "$(_fleet_sibling_named_live)" ]; then
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
    echo "  a flaky drill rather than as the collision it is."
    # BUG-0063: say whether WAITING can help, the same question the foreign
    # branch answers. Advising "wait for that run to finish" when there is no
    # run and no parent is advice for something that cannot happen, and it
    # cost two hand-kills in one hour before this line existed.
    local _sorph _slive
    _sorph=$(printf '%s\n' "$sibling" | awk '$2 == 1' | wc -l | tr -d ' ')
    _slive=$(printf '%s\n' "$sibling" | awk '$2 != 1 && NF' | wc -l | tr -d ' ')
    if [ "$_slive" = "0" ]; then
      echo "  ALL $_sorph ARE ORPHANS (ppid 1) and they are CONTENDING -- no run"
      echo "  is driving them, so waiting cannot clear this. They have to be"
      echo "  removed from THAT project. (An idle orphan would not have stopped"
      echo "  this run; these are burning CPU.)"
    elif [ "$_sorph" != "0" ]; then
      echo "  $_sorph of $((_sorph + _slive)) are ORPHANS (ppid 1) and will never clear"
      echo "  on their own; only the other $_slive can finish. Wait for those, then"
      echo "  remove the rest from their project."
    else
      echo "  Wait for that run to finish, or stop it from ITS project."
    fi
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
    # NO _killed_pids APPEND HERE (BUG-0051). This function SIGSTOPs and
    # SIGCONTs; it kills nothing and has no wait-for-death block to feed. The
    # append lived here and not in fleet_kill, which is the whole defect: it
    # also recorded pids it then `continue`d past without signalling, and it
    # was not declared local, so it leaked into whatever called this.
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
  local pid args _killed_pids=""
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
    # RECORD IT (BUG-0051). The wait below iterates this list, and nothing
    # ever appended to it: the sole append in the tree was in fleet_signal,
    # a different function. So the whole wait was unreachable and every
    # caller was back on the fixed `sleep` the comment below says stopped
    # being safe once #176 made a node bind within milliseconds of exec.
    # Appended AFTER the allowlist accepts the pid, so the list holds
    # exactly the seats this call signalled.
    _killed_pids="$_killed_pids $pid"
  done
  # AND DO NOT RETURN UNTIL THE PORTS ARE ACTUALLY FREE.
  #
  # Every caller follows fleet_kill with a fixed `sleep`, which is a guess at
  # exactly this postcondition. The guess held for as long as a restarted node
  # did not bind until its initial full sync finished — seconds of slack. #176
  # binds within milliseconds of exec, so a 0.4s sleep became the entire margin
  # against the previous process releasing the socket, and 21 drills follow
  # this call with a respawn on the same ports.
  #
  # Seen as promote_notice's "nothing listening on 127.0.0.1:6911 after 30s":
  # the replacement lost the race, got EADDRINUSE and exited, and the drill
  # then measured a promotion into a node that was not there (5064ms against a
  # 19ms steady state). Intermittent, and invisible until #176 tightened the
  # window.
  #
  # kill -9 is immediate; the socket release is not. Wait for the fact instead
  # of sleeping past it. Bounded, and silent on timeout — a port still held
  # after 5s is a different problem and the caller's own wait will report it
  # with better context than this function has.
  # WAIT FOR THE PROCESSES TO BE GONE, NOT FOR A PORT TO LOOK FREE.
  #
  # kill -9 is delivery, not death: the socket is released when the process
  # actually exits. Every caller papers over that with a fixed `sleep`, which
  # held only while a restarted node did not bind until after its full sync.
  # #176 binds in milliseconds, so promote_notice's 0.4s became the whole
  # margin and its replacement took EADDRINUSE and exited — "nothing listening
  # on 127.0.0.1:6911 after 30s", then a promotion measured into a node that
  # was not there.
  #
  # Two earlier attempts polled PORTS and both were wrong. Waiting on every
  # declared port waits for seats this call deliberately left alone (the proxy
  # is still up; its port never frees) and cost +10s per drill, 27 percent
  # across the suite. Waiting on only the killed seats ports does nothing when
  # the seats were already dead, which is exactly when the previous process is
  # still exiting. The pid is the precise signal: it is gone or it is not.
  local _pid _t
  if [ -n "${_killed_pids# }" ]; then
    _t=$(( $(date +%s) + 5 ))
    for _pid in $_killed_pids; do
      while kill -0 "$_pid" 2>/dev/null; do
        [ "$(date +%s)" -ge "$_t" ] && break
        sleep 0.02
      done
    done
  fi
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
  # fleet_load_resp <port> <gen> [expected_replies] [max_shed]
  #
  # ONE implementation of "a -THROTTLED is a refusal, not a loss". Both the
  # lag cap (BUG-0035) and the write deadline (BUG-0096) shed with the same
  # error, and `valkey-cli --pipe` exits non-zero when it counts ANY error, so
  # under the caller's `set -e`/`pipefail` a single shed aborts the drill.
  #
  # `expected_replies` and `max_shed` are OPTIONAL and default to unchecked,
  # because the two kinds of caller want opposite things. repl_drill drives the
  # lag cap deliberately and has seen 20,328 sheds of 50,500 -- 40% is the
  # condition under test there, not a fault. restart_drill and failover_drill
  # are durability drills where a shed is weather, and a rate that stops being
  # small is a finding; they pass a ceiling.
  local port="$1" gen="$2" want_replies="${3:-}" max_shed="${4:-}"
  local errs replies out summary nonshed
  # The load phase is where the shed happens, and it can be minutes after
  # fleet_guard ran. Record here too, so a -THROTTLED count and the box that
  # produced it sit on adjacent lines.
  fleet_env_note load
  out="$(mktemp "${FLINT_DRILL_ROOT:-/tmp}/flint-load.XXXXXX")"
  # `|| true`: --pipe exits non-zero when it counts errors, and under the
  # caller's `set -e`/`pipefail` that alone would abort the drill here.
  { $gen | valkey-cli -p "$port" --pipe > "$out" 2>&1; } || true
  summary=$(tail -1 "$out")
  errs=$(sed -n 's/.*errors: \([0-9][0-9]*\).*/\1/p' "$out" | tail -1)
  replies=$(sed -n 's/.*replies: \([0-9][0-9]*\).*/\1/p' "$out" | tail -1)
  [ -n "$errs" ] || errs=0
  [ -n "$replies" ] || replies=0
  echo "  load: $summary"
  if [ "$replies" = "0" ]; then
    echo "FAIL: the load delivered nothing at all — the node did not answer."
    echo "      This is NOT shedding; it is a dead or unreachable seat."
    rm -f "$out"; return 1
  fi
  if [ -n "$want_replies" ] && [ "$replies" != "$want_replies" ]; then
    echo "FAIL: the load replied $replies times, expected $want_replies —"
    echo "      a short load is not a clean one."
    rm -f "$out"; return 1
  fi
  if [ "$errs" != "0" ]; then
    # EVERY error line must be a shed, as a POSITIVE rule: subtract the
    # loader's own notices and the sheds, and require nothing to be left.
    # Enumerating error forms instead cannot know every error the server can
    # emit -- and case-insensitively `^-?ERR` matches the loader's own summary
    # `errors: 222, replies: 20000`, so that version reported the count as an
    # error in its own right (BUG-0096, caught by a control).
    nonshed=$(grep -vE '^(errors:|Last reply received|All data transferred)' "$out" \
              | grep -vi THROTTLED | grep -c . || true)
    if [ "${nonshed:-0}" -ne 0 ]; then
      echo "FAIL: the load produced $nonshed line(s) that are neither a THROTTLED"
      echo "      shed nor a loader notice — a shed is retryable and those may not be:"
      cat "$out"
      rm -f "$out"; return 1
    fi
    if [ -n "$max_shed" ] && [ "$errs" -gt "$max_shed" ]; then
      echo "FAIL: $errs writes shed of $replies, past the $max_shed this drill"
      echo "      tolerates. At that rate the shed is not reacting to a slow box;"
      echo "      look at the cap that fired and at write_service_us, not at"
      echo "      durability."
      cat "$out"
      rm -f "$out"; return 1
    fi
    echo "  note: $errs of $replies writes shed -THROTTLED. Shed writes were"
    echo "        NEVER ACKED, so nothing is lost. Keys a drill asserts on must"
    echo "        be proven written before they are relied on; see BUG-0035 and"
    echo "        BUG-0096 for the two caps that reach this."
  fi
  rm -f "$out"
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
