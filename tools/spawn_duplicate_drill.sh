#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0144 and OPS-0250, which are one defect seen from two sides.
#
# `host-spawn` was unconditional BY DESIGN -- its callers are contracted to have
# stopped the seat first -- and had no defence when that contract was false. It
# wrote the pidfile BEFORE anything knew the child would live, so a spawn over a
# seat that is already up records the pid of a process about to die on bind, and
# every stop through that pidfile then aims at a corpse while the real seat keeps
# serving. OPS-0250 is the road that makes it false: the box's own
# flint-supervise timer and a remote agent's Tier-2 restart repaired one seat 1.4
# seconds apart on 2026-09-14, and nothing interlocked them.
#
# TWO PROPERTIES, and they need each other. The refusal alone still loses if both
# actors check before either writes; the lock alone still lets the second actor
# overwrite a live seat's pidfile once it gets its turn. So this drill asserts
# both, and its controls are built around the two ways each could be fake:
#
#   - a refusal that refuses EVERYTHING would pass an arm that only looks for a
#     non-zero exit, so three arms require a spawn to be ALLOWED -- a clean
#     statedir, a pidfile naming a dead pid, and a pidfile naming a live process
#     that is not this seat.
#   - the identity is `argv[0] == {bins}/{bin}`: the same binary out of the same
#     install, under this seat's pidfile. NOT the whole argv, because the two
#     actors that collided on 2026-09-14 composed DIFFERENT argv for one seat --
#     two inventories that disagree (BUG-0138) -- and a whole-argv check would
#     have called that a different seat and waved it through. One arm requires
#     exactly that case to be refused.
#   - argv[0] is at the FRONT, so a truncating argv reader cannot disable the
#     refusal; what it could still do is make the message claim a divergence
#     that is not there. One arm gives a seat a >300-character argv and requires
#     an identical duplicate to be refused WITHOUT that claim.
#   - a lock arm that merely runs two spawns and finds one winner proves nothing
#     when they happen not to overlap, so the lock is tested DETERMINISTICALLY:
#     an external holder takes the seat's lock, and flintctl must be observed
#     waiting rather than proceeding. What that arm cannot rule out is a spawn
#     that was merely slow for two seconds for some other reason; what it does
#     rule out is the whole failure it is here for, a spawn that never consults
#     the lock at all, because such a spawn finishes in milliseconds.
set -euo pipefail
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"

# 6900 AND NOT 6442, WHICH next-free-ports.sh OFFERED. The allocator checks a
# port against declared ports and against what the repo binds; it does not check
# it against the TRUNCATED kill patterns other drills carry, and
# `controller_drill.sh` pkills `flint-server --port 644` -- a prefix of 6442, so
# that drill would SIGKILL this one's seat whenever the two ran in the same
# batch. The gate refuses that (assert_no_cross_drill_kill_patterns) and refused
# this, which is the check working; the allocator not knowing about it is a gap
# in the allocator, filed separately rather than fixed in a drill about spawning.
fleet_init $FLINT_DRILL_ROOT/flint-spawndup 6900
PORT=6900
fleet_guard
fleet_kill server
sleep 0.3

fail() { echo "FAIL: $*"; exit 1; }

S="$FLINT_DRILL_ROOT/flint-spawndup-state"
BINS="$(pwd)/target/release"
CTL="$BINS/flintctl"
SEAT=node-a
PIDFILE="$S/pids/$SEAT.pid"
LOCKFILE="$S/locks/$SEAT.lock"

# --features flint-server/rocks even though the seat runs --engine mem: the
# build output is SHARED, and omitting it downgrades ./target/release/flint-server
# to a mem-only binary that breaks every later drill wanting rocks. gates.sh lints
# for it. Nothing here is about storage -- the subject is a pidfile, an argv and a
# lock -- so the seat itself has no reason to pay for an engine.
cargo build --release -q -p flint-ctl -p flint-server --features flint-server/rocks || fail "build"

# EVERYTHING THIS DRILL STARTS, KILLED ON ANY EXIT. An arm that leaves a seat
# behind orphans it to init, and an orphan with no ancestor in this suite is what
# BUG-0152 has `fleet_guard` refusing intermittently in somebody else's drill.
STRAYS=""
cleanup() {
  for p in $STRAYS; do kill -9 "$p" 2>/dev/null || true; done
  if [ -f "$PIDFILE" ]; then kill -9 "$(cat "$PIDFILE")" 2>/dev/null || true; fi
  fleet_kill server 2>/dev/null || true
}
trap cleanup EXIT

reset() {   # every arm starts from a statedir with no seat, no pidfile, no lock
  if [ -f "$PIDFILE" ]; then kill -9 "$(cat "$PIDFILE")" 2>/dev/null || true; fi
  fleet_kill server
  rm -rf "$S"
  mkdir -p "$S"
  # WAIT FOR THE PORT, do not sleep at it. An arm that starts while the previous
  # arm's seat still holds the port fails at its setup spawn, which reads as a
  # defect in the guard and is a defect in this drill -- the mistake
  # cp_growth_drill made when arm 1 left state that poisoned arm 3.
  "$CTL" host-port-free "$PORT" 10000 >/dev/null 2>&1 \
    || fail "port $PORT did not come free between arms"
}

spawn() {   # spawn <data-dir> -- stdout+stderr captured in $OUT, rc in $RC
  set +e
  OUT=$("$CTL" host-spawn "$S" "$BINS" "$SEAT" flint-server \
        -- --port "$PORT" --engine mem --data-dir "$1" 2>&1)
  RC=$?
  set -e
}

# ---------------------------------------------------------------------------
echo "== control: a clean host-spawn still starts the seat"
# FIRST, and it is not a formality. Every arm below reads a refusal as evidence,
# and a guard that refused unconditionally would make all of them pass while
# breaking every roll in the fleet.
reset
spawn "$S/$SEAT"
[ "$RC" -eq 0 ] || fail "host-spawn on a clean statedir exited $RC -- the guard is
  refusing a seat nothing was running:
$OUT"
[ -s "$PIDFILE" ] || fail "host-spawn wrote no pidfile at $PIDFILE"
FIRST=$(cat "$PIDFILE")
fleet_wait_listen "$PORT"
kill -0 "$FIRST" 2>/dev/null || fail "pidfile names pid $FIRST, which is not alive"
echo "   pid $FIRST, listening on $PORT"

# ---------------------------------------------------------------------------
echo "== the refusal: an identical host-spawn over the live seat is refused"
spawn "$S/$SEAT"
[ "$RC" -ne 0 ] || fail "host-spawn exited 0 over a seat that is ALREADY RUNNING.
  It has taken the pidfile for a process that will die on bind, and every stop
  through it now aims at a corpse while pid $FIRST keeps serving. That is
  BUG-0144 exactly."
printf '%s\n' "$OUT" | grep -q "ALREADY running" \
  || fail "the refusal does not say the seat is already running. Its output was:
$OUT"
printf '%s\n' "$OUT" | grep -q "BUG-0144" \
  || fail "the refusal does not cite the bug, so whoever hits it in a roll has
  nowhere to read what it means. Its output was:
$OUT"
NOW=$(cat "$PIDFILE")
[ "$NOW" = "$FIRST" ] || fail "the pidfile changed from $FIRST to $NOW during a
  REFUSED spawn. Refusing and then clobbering the record anyway is the same
  corrupted state by a shorter road."
kill -0 "$FIRST" 2>/dev/null || fail "the live seat died during the refused spawn"
echo "   refused, pidfile still names $FIRST, seat still serving"

# ---------------------------------------------------------------------------
echo "== the refusal covers a duplicate whose ARGUMENTS differ"
# THE SHAPE THAT ACTUALLY HAPPENED. On 2026-09-14 the two actors repairing
# node-7002 did not agree on its argv -- `--journal 0.0.0.0:7500` against
# `--journal 172.31.64.94:7500` -- because they read two inventories that
# disagree (BUG-0138). An identity keyed on the WHOLE argv would call that a
# different seat and allow the second spawn, which is the entire bug. So the
# identity is argv[0], and the arguments are diagnosis rather than permission.
spawn "$S/$SEAT-other"
[ "$RC" -ne 0 ] || fail "host-spawn exited 0 over the live seat because the data
  dir differed. The guard is keyed on the whole argv, so two actors with two
  inventories -- which is exactly the 2026-09-14 collision -- read as two
  different seats and both spawn."
printf '%s\n' "$OUT" | grep -q "ARGUMENT LISTS DIFFER" \
  || fail "the refusal did not say that the two argument lists differ. That
  divergence is the DIAGNOSIS -- it is how BUG-0138 was found -- and a refusal
  that hides it sends the reader looking for a crash loop. Its output was:
$OUT"
[ "$(cat "$PIDFILE")" = "$FIRST" ] || fail "the pidfile moved during the refusal"
echo "   refused, and the message names the divergence"

# ---------------------------------------------------------------------------
echo "== the refusal survives a >300-character argv"
# THE TRUNCATION QUESTION, MADE FALSIFIABLE. `ps -o args=` is a formatted column
# and may be cut; `/proc/<pid>/cmdline` is not, which is why the reader tries it
# first. Truncation cannot disable the REFUSAL, because argv[0] is at the front
# -- but it would make a truncated `running` compare unequal to `wanted` and the
# message would then announce an argument divergence that does not exist, which
# sends the reader to BUG-0138 for a fault that is not there. A long data dir is
# the cheapest way to make a real seat's argv long enough to catch that.
reset
LONGDIR="$S/$SEAT"
for seg in aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
           bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
           cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc \
           dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd; do
  LONGDIR="$LONGDIR/$seg"
done
mkdir -p "$LONGDIR"
LEN=${#LONGDIR}
[ "$LEN" -gt 250 ] || fail "the long-argv fixture is only $LEN characters, which
  with the ~70 characters of binary path and flags around it is not long enough
  to catch a truncating reader -- the arm would pass for the wrong reason"
spawn "$LONGDIR"
[ "$RC" -eq 0 ] || fail "the seat with a long data dir would not start at all:
$OUT"
fleet_wait_listen "$PORT"
LONGPID=$(cat "$PIDFILE")
spawn "$LONGDIR"
[ "$RC" -ne 0 ] || fail "the duplicate spawn was ALLOWED when the argv is $LEN+
  characters long, while the identical test above refused a short one."
if printf '%s\n' "$OUT" | grep -q "ARGUMENT LISTS DIFFER"; then
  fail "the refusal claims the two argument lists differ, and they are IDENTICAL
  -- both spawns were composed from the same $LEN-character data dir. The argv
  reader is truncating, so every long-argv refusal will point the reader at
  BUG-0138 for a divergence that is not there. Its output was:
$OUT"
fi
[ "$(cat "$PIDFILE")" = "$LONGPID" ] || fail "the pidfile moved during the refusal"
echo "   ${LEN}-character data dir, refused with no false divergence claim"

# ---------------------------------------------------------------------------
echo "== control: a pidfile naming a DEAD pid does not refuse"
# The common legitimate case: a fleet restarted out of band leaves pids that no
# longer exist. A guard keyed on the pidfile's PRESENCE rather than on what it
# names would strand every one of those seats.
reset
spawn "$S/$SEAT"
[ "$RC" -eq 0 ] || fail "setup spawn failed: $OUT"
fleet_wait_listen "$PORT"
DEAD=$(cat "$PIDFILE")
kill -9 "$DEAD" 2>/dev/null || true
for _ in $(seq 1 50); do kill -0 "$DEAD" 2>/dev/null || break; sleep 0.1; done
kill -0 "$DEAD" 2>/dev/null && fail "could not kill the seat to make a stale pid"
printf '%s' "$DEAD" > "$PIDFILE"     # the state a crash leaves: file kept, pid gone
spawn "$S/$SEAT"
[ "$RC" -eq 0 ] || fail "host-spawn REFUSED over a pidfile naming dead pid $DEAD.
  That is the state every out-of-band restart leaves behind, and refusing it
  would make this guard fail more rolls than the race it prevents:
$OUT"
[ "$(cat "$PIDFILE")" != "$DEAD" ] || fail "the spawn reported success and left the
  DEAD pid in the pidfile"
echo "   stale pid $DEAD replaced, seat started"

# ---------------------------------------------------------------------------
echo "== control: a pidfile naming a LIVE but unrelated process does not refuse"
# PID REUSE, which is the whole reason the identity is argv[0] and not the
# number. A pidfile outlives what it named, and the kernel hands that number to
# something else; a liveness-only guard would then refuse a seat that is not
# running, on the evidence of a process that has nothing to do with it. This is
# the arm that says the two refusals above are about WHAT is running and not
# merely THAT something is.
reset
/bin/sleep 900 &
IMPOSTOR=$!
STRAYS="$STRAYS $IMPOSTOR"
mkdir -p "$S/pids"
printf '%s' "$IMPOSTOR" > "$PIDFILE"
spawn "$S/$SEAT"
[ "$RC" -eq 0 ] || fail "host-spawn REFUSED because pid $IMPOSTOR is alive, without
  checking WHAT it is running. It is a /bin/sleep, not this seat. A guard that
  cannot tell those apart refuses a legitimate start every time the kernel
  recycles a pid:
$OUT"
fleet_wait_listen "$PORT"
kill -0 "$IMPOSTOR" 2>/dev/null || fail "the drill's own impostor process died, so
  the arm proved nothing"
kill -9 "$IMPOSTOR" 2>/dev/null || true
echo "   live pid $IMPOSTOR running something else, spawn correctly allowed"

# ---------------------------------------------------------------------------
echo "== OPS-0250: host-spawn WAITS for a seat lock another actor holds"
reset
mkdir -p "$S/locks"
HELD="$S/locks/.held"
rm -f "$HELD"
python3 - "$LOCKFILE" "$HELD" <<'PY' &
import fcntl, os, sys, time
lock, held = sys.argv[1], sys.argv[2]
os.makedirs(os.path.dirname(lock), exist_ok=True)
f = open(lock, "a+")
fcntl.flock(f, fcntl.LOCK_EX)
open(held, "w").write("held\n")
time.sleep(120)
PY
HOLDER=$!
STRAYS="$STRAYS $HOLDER"
for _ in $(seq 1 100); do [ -f "$HELD" ] && break; sleep 0.1; done
[ -f "$HELD" ] || fail "the drill's own lock holder never acquired the lock, so
  this arm would have tested nothing"

"$CTL" host-spawn "$S" "$BINS" "$SEAT" flint-server \
  -- --port "$PORT" --engine mem --data-dir "$S/$SEAT" > "$S/spawn.out" 2>&1 &
SPAWNER=$!
sleep 2
if [ -f "$PIDFILE" ]; then
  fail "host-spawn wrote a pidfile while another process held $LOCKFILE. The
  check-spawn-write sequence is not under the lock, so two actors repairing one
  seat can both pass the duplicate check before either records a pid -- which is
  the OPS-0250 race with the BUG-0144 guard already in place."
fi
kill -0 "$SPAWNER" 2>/dev/null || fail "host-spawn exited within 2s while the lock
  was held. It must WAIT for the holder; exiting early either means it never
  took the lock, or it gave up long before the budget. Its output was:
$(cat "$S/spawn.out" 2>/dev/null)"
echo "   2s in: no pidfile, still waiting"

kill -9 "$HOLDER" 2>/dev/null || true
wait "$HOLDER" 2>/dev/null || true
set +e; wait "$SPAWNER"; SRC=$?; set -e
[ "$SRC" -eq 0 ] || fail "host-spawn failed after the lock was released (exit $SRC).
  A released lock must let the waiter through:
$(cat "$S/spawn.out" 2>/dev/null)"
[ -s "$PIDFILE" ] || fail "host-spawn returned 0 after the lock was released but
  wrote no pidfile"
fleet_wait_listen "$PORT"
LOCKED_PID=$(cat "$PIDFILE")
echo "   lock released, spawn completed as pid $LOCKED_PID"

# ---------------------------------------------------------------------------
echo "== OPS-0250: host-stop-seat waits for the same lock"
# THE OTHER HALF, and it is not symmetry for its own sake: a stop that runs
# inside someone else's spawn kills the seat that spawn is about to record, which
# leaves the pidfile naming a process the stop has already killed -- the same
# corrupted state, reached from the other direction.
rm -f "$HELD"
python3 - "$LOCKFILE" "$HELD" <<'PY' &
import fcntl, os, sys, time
lock, held = sys.argv[1], sys.argv[2]
f = open(lock, "a+")
fcntl.flock(f, fcntl.LOCK_EX)
open(held, "w").write("held\n")
time.sleep(120)
PY
HOLDER=$!
STRAYS="$STRAYS $HOLDER"
for _ in $(seq 1 100); do [ -f "$HELD" ] && break; sleep 0.1; done
[ -f "$HELD" ] || fail "the lock holder for the stop arm never acquired the lock"

"$CTL" host-stop-seat "$S" "$SEAT" flint-server "$S/$SEAT" "$PORT" > "$S/stop.out" 2>&1 &
STOPPER=$!
sleep 2
kill -0 "$LOCKED_PID" 2>/dev/null || fail "host-stop-seat killed the seat while
  another process held $LOCKFILE -- the stop is not under the lock, so it can
  land inside somebody else's repair."
kill -0 "$STOPPER" 2>/dev/null || fail "host-stop-seat exited within 2s while the
  lock was held, instead of waiting for it:
$(cat "$S/stop.out" 2>/dev/null)"
echo "   2s in: seat $LOCKED_PID still alive, stop still waiting"

kill -9 "$HOLDER" 2>/dev/null || true
wait "$HOLDER" 2>/dev/null || true
set +e; wait "$STOPPER"; TRC=$?; set -e
[ "$TRC" -eq 0 ] || fail "host-stop-seat failed after the lock was released
  (exit $TRC):
$(cat "$S/stop.out" 2>/dev/null)"
kill -0 "$LOCKED_PID" 2>/dev/null && fail "host-stop-seat returned 0 with pid
  $LOCKED_PID still alive"
echo "   lock released, seat stopped"

echo "SPAWN DUPLICATE DRILL PASSED"
