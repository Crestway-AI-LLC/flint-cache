#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# fleet_guard: does the box-is-busy check actually see everything that makes
# the box busy?
#
# WHY THIS EXISTS. The guard's detector was anchored
# `^flint-(server|proxy|...)$`, which never matched a sibling project's
# `flint-kv-server`. So with another project's chaos fleet running, the guard
# reported a clean box and every drill went ahead into CPU and disk
# contention. That does not fail loudly — it produces a slow, flaky drill,
# and sends you debugging the drill instead of the collision. A check that
# says "clear" when it simply cannot see is worse than no check.
#
# HOW THE PROCESSES ARE FAKED. `exec -a <name> sleep` sets argv[0], which is
# exactly how a real compiled binary presents in ps. Two earlier attempts got
# this wrong and are worth remembering: a shell script shows up as
# "/bin/sh /path/name" (so the detector correctly saw `sh`, and the test
# wrongly read that as a miss), and a COPY of /bin/sleep is SIGKILLed by
# macOS because copying breaks its code signature. Both produced a green-
# looking test of nothing.
#
# NOTHING HERE TOUCHES ANOTHER PROJECT. The fakes are this script's own
# `sleep` children, killed on exit. The guard is detection-only by design:
# flint-kv's fleets belong to flint-kv, and may be mid-measurement.
set -u
# Job-control notices ("Terminated: 15") for our own fakes are noise that
# reads like a failure in the middle of a passing drill.
set +m
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"

PIDS=""
spawn_as() { bash -c "exec -a $1 sleep 60" & PIDS="$PIDS $!"; }
cleanup() { [ -n "$PIDS" ] && kill $PIDS 2>/dev/null; wait 2>/dev/null; true; }
trap cleanup EXIT

# THIS DRILL MUST CONTROL FLINT_DRILL_FORCE, NOT INHERIT IT.
#
# CI exports FLINT_DRILL_FORCE=1 for the whole job, because a fresh runner
# is genuinely a clean box and the guard cannot tell that from someone's
# laptop. Every other drill wants that. This one is ABOUT the guard, so
# inheriting it made the refusal assertions unprovable: the guard returned 0
# by design and the drill read that as "did NOT refuse". It passed locally,
# where the variable is unset, and failed on the first CI run — the exact
# environment-difference class the multi-host work exists to catch.
#
# So: clear it here, and set it explicitly in the one step that tests it.
FORCE_WAS="${FLINT_DRILL_FORCE:-}"
unset FLINT_DRILL_FORCE
# AND FLINT_DRILL_PARALLEL, for the identical reason. The gate exports it for
# the whole drills stage when FLINT_GATE_JOBS>1, and case G asserts what the
# guard does WITHOUT it -- an assertion that inherits the flag tests nothing
# and reports exit 0 as "did not refuse". Caught by its own negative control
# on 2026-08-24, in the file that already documents this exact trap one
# variable earlier.
PARALLEL_WAS="${FLINT_DRILL_PARALLEL:-}"
unset FLINT_DRILL_PARALLEL
[ -n "$PARALLEL_WAS" ] && echo "   (FLINT_DRILL_PARALLEL=$PARALLEL_WAS cleared for this drill; case G sets it explicitly)"
[ -n "$FORCE_WAS" ] && echo "   (FLINT_DRILL_FORCE=$FORCE_WAS cleared for this drill; step E sets it back)"

fleet_init $FLINT_DRILL_ROOT/flint-guard-drill 6999 6378 6379 6380 6381 6382 6383 6384 6385

echo "== A) a quiet box: the guard must let the drill run"
# THIS CASE NEEDS A QUIET BOX AND CANNOT CREATE ONE. Another project's build
# or another session's fleet is not ours to stop, and when one is up the
# guard is RIGHT to refuse — failing here would report a correct refusal as a
# defect, and the fix someone reaches for is to weaken the guard.
#
# So say NOT EXERCISED and name what blocked it. A case that cannot reach its
# own precondition must not report a verdict about the thing it never tested;
# a precondition that quietly stops holding is how a check passes forever.
PRE_SIB=$(_fleet_sibling); PRE_FOR=$(_fleet_foreign)
if [ -n "$PRE_SIB" ] || [ -n "$PRE_FOR" ]; then
  echo "  n/a — the box is NOT quiet, so this case did not run:"
  [ -n "$PRE_SIB" ] && printf '%s\n' "$PRE_SIB" | cut -c1-90 | sed 's/^/      sibling: /'
  [ -n "$PRE_FOR" ] && printf '%s\n' "$PRE_FOR" | cut -c1-90 | sed 's/^/      foreign: /'
  A_SKIPPED=1
else
  OUT=$(fleet_guard 2>&1); RC=$?
  [ "$RC" = 0 ] \
    || { echo "FAIL: guard refused on a quiet box (exit $RC)"; echo "$OUT"; exit 1; }
  echo "  no refusal, as expected"
fi

echo "== B) a SIBLING project's fleet is up: the guard must refuse"
spawn_as flint-kv-server
spawn_as flint-kv-chaos
sleep 1
# Prove the fakes are actually visible the way a real binary would be. If
# this is empty the rest of the run proves nothing.
ps -eo pid=,args= | grep -qE '^ *[0-9]+ flint-kv-server' \
  || { echo "FAIL: the fake sibling is not visible in ps — this drill would pass vacuously"; exit 1; }
OUT=$(fleet_guard 2>&1); RC=$?
[ "$RC" = 1 ] \
  || { echo "FAIL: guard did NOT refuse with a sibling fleet up (exit $RC)"; echo "$OUT"; exit 1; }
echo "$OUT" | grep -q "another Flint-family project" \
  || { echo "FAIL: refused, but not for the sibling reason:"; echo "$OUT"; exit 1; }
# The message must not tell an operator to go kill someone else's processes.
echo "$OUT" | grep -q "NOT ours and this suite will not touch them" \
  || { echo "FAIL: the refusal must say these are not ours to stop"; echo "$OUT"; exit 1; }
echo "  refused, named the sibling, and disclaimed ownership"
cleanup; PIDS=""; sleep 0.5

echo "== C) NEGATIVE CONTROL: our OWN binaries must not read as a sibling"
# The rule is structural — sibling projects use flint-<project>-<component>,
# everything this workspace builds is one segment. If that ever stops holding,
# the guard starts calling our own processes foreign and refuses EVERY drill,
# which is a worse failure than the one it was written to fix.
for n in flint-server flint-proxy flint-controlplane flint-controller \
         flint-chaos flint-bench flint-conformance flint-balance \
         flint-agent flint-console flint-ops flint-register flint-exporter flint-meter; do
  spawn_as "$n"
done
sleep 1
SIB=$(_fleet_sibling)
# SCOPED TO OUR OWN FAKES, not to the whole box.
#
# This asserted `_fleet_sibling` was EMPTY, which quietly assumed no real
# sibling project was running here. That held only while the detector could
# not see one. Once BUG-0036 taught it to match a sibling's cargo target
# path, a genuine flint-kv test binary on the box failed this case — and the
# message blamed OUR binaries and printed a line that was not one of them,
# asserting a cause it had never checked. The question this case asks is
# whether OUR fourteen are misread; answer exactly that.
MINE=$(for p in $PIDS; do printf '%s\n' "$SIB" | awk -v p="$p" '$1 == p'; done)
[ -z "$MINE" ] \
  || { echo "FAIL: our own binaries were classified as another project's:"; echo "$MINE" | sed 's/^/    /'; exit 1; }
echo "  all 14 of our own binaries classified correctly"

echo "== D) ...but they are still caught as FOREIGN when out of scope"
OUT=$(fleet_guard 2>&1); RC=$?
[ "$RC" = 1 ] \
  || { echo "FAIL: guard ignored out-of-scope processes of our own (exit $RC)"; exit 1; }
echo "$OUT" | grep -q "outside $FLINT_DRILL_ROOT/flint-guard-drill" \
  || { echo "FAIL: refused, but not for the out-of-scope reason:"; echo "$OUT"; exit 1; }
echo "  refused for the out-of-scope reason, not the sibling one"

echo "== E) FLINT_DRILL_FORCE still overrides both"
# Set explicitly, so this asserts the override rather than whatever the
# surrounding environment happened to be.
OUT=$(FLINT_DRILL_FORCE=1 fleet_guard 2>&1); RC=$?
[ "$RC" = 0 ] || { echo "FAIL: FORCE did not override (exit $RC)"; echo "$OUT"; exit 1; }
echo "  force proceeds"

cleanup; PIDS=""; sleep 0.5

# A process whose ARGV carries a path, which `spawn_as` cannot produce --
# exec -a sets argv[0], so the whole string becomes the command line as ps
# renders it. Scope matching reads that line, so the fake has to have one.
# IS A PROCESS WITH THIS ARGV RUNNING? A SNAPSHOT, NOT A PIPELINE.
#
# `ps -eo args= | grep -q -- "$pat"` MATCHES ITS OWN GREP: the grep is running
# while ps samples, and its argv contains the pattern. So the pipeline form
# always finds at least one hit and can never report "gone" -- a check that
# cannot fail, sitting in four places whose whole job is to prove a fixture
# exists before something is asserted about it. Measured: a pattern matching
# nothing real returns 1 from the pipeline and 0 from the snapshot.
#
# Taking the snapshot FIRST closes it, because the command substitution
# completes before the grep is forked.
# NO PIPE EITHER. `printf ... | grep -q` makes grep exit at the first hit and
# leaves printf writing to a closed pipe, which prints "write error: Broken
# pipe" on every call -- noise in a drill's output that reads like a fault.
# A quoted case pattern is a LITERAL substring test, so it needs no subprocess
# and cannot be confused by a bracket in a path.
seat_in_ps() {   # seat_in_ps <argv substring>
  local snapshot
  snapshot=$(ps -eo args=)
  case "$snapshot" in *"$1"*) return 0 ;; *) return 1 ;; esac
}

spawn_argv() { bash -c "exec -a \"$1\" sleep 60" & PIDS="$PIDS $!"; }
# The same fake seat with a life span, for the one case that needs a seat to GO
# on its own: fleet_guard's settle loop exists for seats on their way out, and
# BUG-0153 is about what that loop samples while it waits.
spawn_argv_for() { bash -c "exec -a \"$1\" sleep $2" & PIDS="$PIDS $!"; }

echo "== F) a scope must not own a scope it is merely a PREFIX of"
# fleet.sh decided ownership with `index(args, scope) > 0`, so flint-cpha
# owned flint-cpha-ctl. Four such pairs exist in tools/ today. Serially only
# one fleet is up so it never fires; at P=4 the shorter-scoped drill SIGKILLs
# the longer-scoped one and the victim reports a product failure.
PFX="$FLINT_DRILL_ROOT/flint-guardpfx"
spawn_argv "flint-server --data-dir $PFX-ctl/d"
sleep 1
seat_in_ps "--data-dir $PFX-ctl/d" \
  || { echo "FAIL: the fake prefix seat is not visible in ps — this case would pass vacuously"; exit 1; }
SAVED_SCOPE="$FLEET_SCOPE"; SAVED_PORTS="$FLEET_PORTS"
FLEET_SCOPE="$PFX"; FLEET_PORTS=""
ADOPTED=$(_fleet_ours server)
[ -z "$ADOPTED" ] \
  || { echo "FAIL: scope $PFX adopted a seat living under $PFX-ctl:"; ps -o pid=,args= -p $ADOPTED | sed 's/^/    /'; FLEET_SCOPE="$SAVED_SCOPE"; FLEET_PORTS="$SAVED_PORTS"; exit 1; }
# POSITIVE CONTROL: the exact scope must still own it, or this case would
# pass just as well against a matcher that owns nothing at all.
FLEET_SCOPE="$PFX-ctl"
OWNED=$(_fleet_ours server)
FLEET_SCOPE="$SAVED_SCOPE"; FLEET_PORTS="$SAVED_PORTS"
[ -n "$OWNED" ] \
  || { echo "FAIL: scope $PFX-ctl did not own its OWN seat — the matcher owns nothing, so the negative above proved nothing"; exit 1; }
echo "  prefix disowned, exact scope still owns it"
cleanup; PIDS=""; sleep 0.5

echo "== G) a live PEER drill of this suite is not a foreign fleet (parallel only)"
PEER="$FLINT_DRILL_ROOT/flint-guardpeer"
rm -rf "$PEER.lock"; mkdir -p "$PEER.lock"

# THE FIXTURE IS A CLOCK, AND EVERY ARM IN THIS CASE DEPENDS ON IT.
#
# The peer is a `sleep`, and this case makes FIVE fleet_guard calls, each of
# which can spend FLINT_FOREIGN_SETTLE (15s by default) waiting for a teardown
# to clear. On a loaded 4-wide box that is over a minute, and the seats used to
# be spawned with the shared `spawn_argv`, which gives them 60 seconds.
#
# When the fixture expires mid-case, every remaining arm reads an EMPTY BOX as a
# verdict about the guard. The first reports "a peer seat did not refuse ... the
# flag would be untested"; the last reports "tolerated a foreign seat anyway --
# that is an amnesty, not a distinction". Both are descriptions of a sleep that
# ended. The last one failed a real gate on 2026-09-16, and killing the fixture
# immediately before it reproduces that message verbatim, exit 0, with nothing
# foreign on the box at all.
#
# TWO FIXES, because either alone leaves half the defect. The lifetime is
# derived from the budget it has to survive rather than being a round number,
# and `require_peer_seat` asks before each arm so an expiry is reported AS an
# expiry instead of as a guard that misbehaved.
PEER_LIFE_S=$(( 20 * ${FLINT_FOREIGN_SETTLE:-15} ))
require_peer_seat() {   # require_peer_seat <what the next assertion is about>
  seat_in_ps "--data-dir $PEER/d" && return 0
  echo "FAIL: the peer fixture's seat is GONE before '$1' could be asserted, so"
  echo "      that arm would have judged fleet_guard against an empty box."
  echo "      This is a WALL-CLOCK EXPIRY and not a defect in the guard: the"
  echo "      fixture is a sleep with ${PEER_LIFE_S}s, and this case can spend"
  echo "      ${FLINT_FOREIGN_SETTLE:-15}s per guard call waiting for a teardown."
  echo "      Raise PEER_LIFE_S; do not read this as an amnesty."
  rm -rf "$PEER.lock"; exit 1
}

# The lock's pid and the seats expire TOGETHER. They did not before -- the lock
# outlived its own seats by four minutes, which is the state that produces a
# false verdict rather than a loud one.
sleep "$PEER_LIFE_S" & PEERPID=$!; PIDS="$PIDS $PEERPID"
printf '%s\n' "$PEERPID" > "$PEER.lock/pid"
ps -o lstart= -p "$PEERPID" > "$PEER.lock/started" 2>/dev/null
printf '%s\n' "7788|7789" > "$PEER.lock/ports"
spawn_argv_for "flint-server --data-dir $PEER/d" "$PEER_LIFE_S"
# A PEER SEAT THAT CARRIES NO PATH. A proxy or controller is started with
# ports and no directory, and a drill keeps auxiliary state in a sibling
# <scope>-state dir that the boundary rule deliberately does not call owned.
# Matching peers on the scope path alone therefore misses most of a real
# peer fleet: measured 2026-08-24, 9 of 24 seats matched and the gate refused
# over the other 15. Ports are the second key, exactly as in _fleet_ours.
spawn_argv_for "flint-controller --pairs 127.0.0.1:7788,127.0.0.1:7789 --id peerctl" "$PEER_LIFE_S"
sleep 1
# Without the flag it is foreign, exactly as before. If this does not refuse,
# the case below proves nothing about the flag.
# Subshell, not `env -u`: fleet_guard is a shell FUNCTION, and env can only
# exec a binary -- it returned 127, which this assert read as "did not
# refuse". The unset above already clears the ambient value; this makes
# the arm say so at the point it matters.
require_peer_seat "a peer seat refuses WITHOUT the flag"
OUT=$( unset FLINT_DRILL_PARALLEL; fleet_guard 2>&1 ); RC=$?
[ "$RC" = 1 ] \
  || { echo "FAIL: a peer seat did not refuse WITHOUT FLINT_DRILL_PARALLEL (exit $RC) — the flag would be untested"; rm -rf "$PEER.lock"; exit 1; }
require_peer_seat "the flag tolerates a live peer"
OUT=$(FLINT_DRILL_PARALLEL=1 fleet_guard 2>&1); RC=$?
[ "$RC" = 0 ] \
  || { echo "FAIL: FLINT_DRILL_PARALLEL=1 still refused a live peer drill (exit $RC):"; echo "$OUT" | sed 's/^/    /'; rm -rf "$PEER.lock"; exit 1; }
# BUG-0153: THE SECOND SAMPLE MUST MEAN WHAT THE FIRST ONE DID.
#
# fleet_guard samples the box twice -- here, and once a second while it waits
# for a teardown to settle -- and only the first applied the peer filter. So one
# seat that survived the first pass put the peers back into the population for
# the whole of the settle loop, where they never clear because they are live.
# That changed the VERDICT and not only the message: a live peer plus a seat
# that was merely DYING refused a run that should have proceeded. It is also
# why case G failed 2 runs in 4 on 2026-09-15 -- another drill's orphan was
# enough to make this drill's own peer read as a foreign fleet.
#
# Reproduced with a seat under a scope no lock covers, so the first pass keeps
# it, and a 3s life span so it is gone well inside the 15s settle budget.
#
# ASSERTED LOCALLY, NOT AS A GLOBAL VERDICT. `RC = 0` here would be moved by any
# orphan anywhere on the box, which is the unscoped-census mistake the arm below
# already warns about. The property is about WHO gets named: after the dying
# seat goes, the peers must not take its place in the foreign list, whatever
# else the box makes the guard decide.
DYING="$FLINT_DRILL_ROOT/flint-guarddying"
spawn_argv_for "flint-server --data-dir $DYING/d" 3
sleep 1
seat_in_ps "--data-dir $DYING/d" \
  || { echo "FAIL: the dying seat is not visible in ps -- this case would pass vacuously"; rm -rf "$PEER.lock"; exit 1; }
seat_in_ps "--data-dir $PEER/d" \
  || { echo "FAIL: the PEER seat is not visible in ps -- an assertion that the peer is not named would pass because there is no peer"; rm -rf "$PEER.lock"; exit 1; }
OUT=$(FLINT_DRILL_PARALLEL=1 fleet_guard 2>&1); RC=$?
if printf '%s\n' "$OUT" | grep -q -- "--data-dir $PEER/d"; then
  echo "FAIL: after a dying seat cleared, the guard listed the LIVE PEER's seat as out of scope (exit $RC). The settle loop re-derived the foreign set without the peer filter, so the peers came back and cannot clear -- BUG-0153:"
  echo "$OUT" | sed 's/^/    /'; rm -rf "$PEER.lock"; exit 1
fi
echo "  a seat clearing during the settle does not make the live peers foreign"
# ISOLATE THE PORTS KEY, DO NOT COUNT THE BOX.
#
# This asserted the message read "2 seat(s) belong to", which is only true on
# a box where nothing else is running. Under the parallel gate that message
# reported 6 seats across 4 live peers and the case failed on a guard that had
# worked perfectly -- an exact global count, which is the same unscoped-census
# mistake this whole change set exists to remove, committed inside the test
# for it. Ask the question locally instead: drop the ports the peer declared,
# and the seat that had no path must become foreign again. Nothing else about
# the box can move that answer.
require_peer_seat "the ports key is what matched the path-less seat"
rm -f "$PEER.lock/ports"
OUT2=$(FLINT_DRILL_PARALLEL=1 fleet_guard 2>&1); RC2=$?
[ "$RC2" = 1 ] \
  || { echo "FAIL: with the peer's declared ports removed, the path-less peer seat was still tolerated (exit $RC2) -- so it was never matched BY port, and the ports key is untested:"; echo "$OUT2" | sed 's/^/    /'; rm -rf "$PEER.lock"; exit 1; }
printf '%s\n' "7788|7789" > "$PEER.lock/ports"
# NEGATIVE CONTROL: no live lock behind the seat and it is foreign again,
# flag or not. Otherwise the flag would be a blanket amnesty, which is what
# FLINT_DRILL_FORCE already is.
# THE ARM THAT FAILED A GATE, and the one this case could least afford to get
# wrong: "the guard tolerated a foreign seat" and "there was no foreign seat
# left to refuse" are the same exit 0 and the same message without this line.
require_peer_seat "a seat with no live lock is foreign again"
rm -rf "$PEER.lock"
OUT=$(FLINT_DRILL_PARALLEL=1 fleet_guard 2>&1); RC=$?
[ "$RC" = 1 ] \
  || { echo "FAIL: with no live peer lock, FLINT_DRILL_PARALLEL=1 tolerated a foreign seat anyway (exit $RC) — that is an amnesty, not a distinction"; exit 1; }
echo "  peer tolerated only while its lock is live; foreign otherwise"
cleanup; PIDS=""; sleep 0.5

# BUG-0063. The contract arm B asserts -- a sibling FLEET refuses on sight,
# busy or not -- rests on a presumption: that a named binary is a fleet SOMEONE
# IS RUNNING. An orphan is where that is false, and it is not hypothetical: the
# flint-kv suite left one behind twice in one hour on 2026-08-27, each time
# blocking every drill on this box until a human killed it by hand.
#
# So ORPHANHOOD is the discriminator, not idleness. Arms H and I pin both
# halves, because a fix that only widened the permissive side would silently
# undo arm B -- which is exactly what the comment above _fleet_sibling_named
# warns measuring contention for named binaries would do.
orphan_as() {  # a named process reparented to init: the intermediate shell exits
  bash -c "( exec -a $1 sleep 60 & )"
}
reap_orphans() { pkill -f '^flint-kvorphan-' 2>/dev/null; true; }

echo "== H) an ORPHANED sibling fleet at rest must NOT block the box"
reap_orphans
orphan_as flint-kvorphan-server
sleep 1
OPID=$(pgrep -f '^flint-kvorphan-server' | head -1)
[ -n "$OPID" ] \
  || { echo "FAIL: could not spawn the fake orphan — this arm would pass vacuously"; exit 1; }
OPPID=$(ps -o ppid= -p "$OPID" | tr -d ' ')
[ "$OPPID" = "1" ] \
  || { echo "FAIL: the fake orphan has parent $OPPID, not 1 — it is not an orphan,"; \
       echo "      so this arm is not testing what it claims"; reap_orphans; exit 1; }
# It must still be SEEN as a sibling; the fix is about the verdict, not the sight.
_fleet_sibling | grep -q 'flint-kvorphan-server' \
  || { echo "FAIL: the orphan is not visible to _fleet_sibling at all"; reap_orphans; exit 1; }
_fleet_sibling_named_live | grep -q 'flint-kvorphan-server' \
  && { echo "FAIL: an orphan counted as a LIVE named sibling — ppid is not being read"; reap_orphans; exit 1; }
# THIS ARM NEEDS THE ORPHAN TO BE THE ONLY THING IN THE WAY, and cannot make
# that true. Inside a full gate run the previous drills' seats are still
# exiting, so `foreign` is legitimately non-empty, the settle path is skipped
# for THAT reason, and fleet_guard refuses correctly for something this arm is
# not about. Worse, the sibling refusal PRINTS anyway whenever a sibling
# exists, so the output cannot tell "the orphan blocked me" from "foreign
# blocked me and the sibling message printed too".
#
# Asserting exit 0 assumed a pristine box: true standalone, false in the suite.
# That is how this arm passed locally and failed on its first CI run -- the
# non-hermetic-drill hazard, introduced by the same session that had just
# named it. So: same idiom as case A -- skip, and SAY the precondition did not
# hold, rather than report a verdict about something never tested.
# WAIT FIRST, because skipping is cheap and a case that always skips is a case
# that never runs. The foreign seats here are the previous drills' own, on
# their way out; the gate runs back to back, so a few seconds is usually the
# whole difference between exercising this and reporting n/a forever.
H_FOREIGN=$(_fleet_foreign)
if [ -n "$H_FOREIGN" ]; then
  for _ in $(seq 1 20); do
    sleep 1
    H_FOREIGN=$(_fleet_foreign)
    [ -z "$H_FOREIGN" ] && break
  done
  [ -z "$H_FOREIGN" ] && echo "  (waited for the previous drills' seats to exit)"
fi
if [ -n "$H_FOREIGN" ]; then
  echo "  n/a — foreign seats are on the box, so an orphan is not the only"
  echo "        blocker and this case did not run:"
  printf '%s\n' "$H_FOREIGN" | head -4 | cut -c1-90 | sed 's/^/      foreign: /'
  H_SKIPPED=1
else
  OUT=$(FLINT_SIBLING_WINDOW=1 fleet_guard 2>&1); RC=$?
  [ "$RC" = 0 ] \
    || { echo "FAIL: the guard REFUSED over an idle orphan on an otherwise clear box"; \
         echo "      (exit $RC). That is the bug:"; \
         echo "$OUT" | sed 's/^/    /'; reap_orphans; exit 1; }
  echo "  proceeded past an idle orphan without a human killing it"
fi
reap_orphans; sleep 0.5

echo "== I) NEGATIVE CONTROL: a LIVE sibling fleet must still refuse on sight"
# If D were implemented as "idle siblings are fine", this arm fails: a sleeping
# fake fleet is idle too, and was never the question. Arm B covers the busy
# case; this one pins that D did not weaken it for a QUIET live fleet.
spawn_as flint-kv-server
sleep 1
OUT=$(FLINT_SIBLING_WINDOW=1 fleet_guard 2>&1); RC=$?
# Exit 1 alone proves nothing here either: foreign leftovers produce it too, so
# this arm would pass with the sibling logic deleted. It must see the SIBLING
# refusal naming this fleet.
[ "$RC" = 1 ] \
  || { echo "FAIL: a live (parented) sibling fleet no longer refuses — H was"; \
       echo "      implemented as 'idle is fine' and collapsed the contract (exit $RC)"; \
       echo "$OUT" | sed 's/^/    /'; exit 1; }
echo "$OUT" | grep -q "another Flint-family project" \
  || { echo "FAIL: refused, but not for the sibling reason — this arm would pass"; \
       echo "      with the sibling logic removed, so it is asserting nothing:"; \
       echo "$OUT" | sed 's/^/    /'; exit 1; }
echo "$OUT" | grep -q "flint-kv-server" \
  || { echo "FAIL: the sibling refusal did not name the fleet it refused over"; \
       echo "$OUT" | sed 's/^/    /'; exit 1; }
echo "  a quiet but PARENTED sibling fleet still refuses, by name — orphanhood, not idleness"
cleanup; PIDS=""; sleep 0.5

echo "PASS: fleet_guard sees sibling projects' fleets, refuses without claiming ownership, proceeds past an ORPHANED one while still refusing a parented one, does not misread our own binaries, honours FORCE, disowns prefix scopes, and tells a live peer drill from a foreign fleet"

# `[ test ] && echo` as the LAST command makes the script's exit status the
# TEST's: when case A did run, the test is false, the echo is skipped, and the
# drill exited 1 having printed PASS. The gate read that as a failure and it
# was right to. Same family as appending an echo to a command whose exit code
# you meant to capture — the last statement owns the status, so say so.
if [ "${A_SKIPPED:-0}" = "1" ]; then
  echo "NOTE: case A (quiet box) was NOT exercised on this run — see above"
fi
if [ "${H_SKIPPED:-0}" = "1" ]; then
  echo "NOTE: case H (idle orphan) was NOT exercised on this run — see above"
fi
exit 0
