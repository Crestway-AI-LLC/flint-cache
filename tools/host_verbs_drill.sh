#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The remote runner's CALLEE half, exercised without ssh.
#
# `flintctl`'s `host-*` family is what a remote runner actually executes ON the
# target host; the ssh transport only carries the argv. Nothing exercised any
# of the nine verbs -- `grep -rl host-spawn tools/` found nothing -- while the
# roadmap's own entry on the multi-machine runner says: "Those are written and
# untested here, which is a different claim from working."
#
# So run them directly. That covers the half whose correctness is HOST-LOCAL --
# a pidfile written where the caller will look for it, a port actually bound on
# the machine that owns it -- and leaves only the transport untested. It is the
# reason `host-*` exists at all, stated the other way round: ONE implementation
# and two transports, so the local path and the remote path cannot drift. rc.15
# is what the other arrangement costs, and its own docstring says so:
# `wait_port_free` bound both 0.0.0.0 and 127.0.0.1, which macOS permits and
# Linux refuses, so every local drill went green while the first real roll
# refused to restart a seat.
#
# THE SAFETY VERBS ARE TESTED FOR THEIR REFUSAL, not their success.
# `host-wipe-node` and `host-mark-reseed` take a name from an argv a remote
# caller composed, and both guard it -- `node-` prefix, no `/`. A verb that
# deletes a directory named by someone else is exactly where a traversal
# belongs in a test.
set -euo pipefail
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"

fleet_init $FLINT_DRILL_ROOT/flint-hostverbs 6409
PORT=6409
fleet_guard
fleet_kill server
sleep 0.3

fail() { echo "FAIL: $*"; exit 1; }

S="$FLINT_DRILL_ROOT/flint-hostverbs-state"
BINS="$(pwd)/target/release"
CTL="$BINS/flintctl"

cargo build --release -q -p flint-ctl -p flint-server --features flint-server/rocks || fail "build"
rm -rf "$S"; mkdir -p "$S"

echo "== host-spawn: a seat, its pidfile, and a port that answers"
"$CTL" host-spawn "$S" "$BINS" node-a flint-server \
  -- --port "$PORT" --engine rocks --data-dir "$S/node-a" >/dev/null \
  || fail "host-spawn exited non-zero"
PIDFILE="$S/pids/node-a.pid"
[ -s "$PIDFILE" ] || fail "host-spawn wrote no pidfile at $PIDFILE -- the caller
  reads exactly this path to stop the seat later, so an absent one is a seat
  nothing can stop remotely"
PID=$(cat "$PIDFILE")
fleet_wait_listen "$PORT"
kill -0 "$PID" 2>/dev/null || fail "pidfile names pid $PID, which is not alive"
[ -s "$S/logs/node-a.log" ] || fail "host-spawn created no log for node-a"
echo "   pid $PID, listening on $PORT, log present"

echo "== host-port-free: the check whose correctness is 'on the host that binds'"
# A BOUND port must be reported busy. Short budget: this is the failing
# direction and a 15s default would only slow the drill down.
if "$CTL" host-port-free "$PORT" 800 >/dev/null 2>&1; then
  fail "host-port-free said $PORT is free while a seat is listening on it.
  That is rc.15 exactly: a port check that cannot fail passes a roll through a
  seat that never restarted."
fi
echo "   busy port correctly refused"

echo "== host-stop-seat: the seat goes, and the port comes back"
"$CTL" host-stop-seat "$S" node-a flint-server "--port $PORT" "$PORT" >/dev/null \
  || fail "host-stop-seat exited non-zero"
kill -0 "$PID" 2>/dev/null && fail "pid $PID still alive after host-stop-seat"
# POSITIVE CONTROL for the check above. Same port, same command, opposite
# answer -- without this, "busy port refused" is also what a host-port-free
# that refuses everything would print.
"$CTL" host-port-free "$PORT" 5000 >/dev/null 2>&1 \
  || fail "host-port-free still says $PORT is busy after the seat stopped, so
  the refusal above proves nothing about the port and everything about the check"
echo "   seat stopped, and the SAME check now passes on the SAME port"

echo "== host-mark-reseed / host-wipe-node: the guards, not the happy path"
# NAMED, not "some file appeared". The first draft globbed the directory and
# matched `OPTIONS-000007`, a RocksDB file the spawned seat had already left
# there -- so it would have passed against a host-mark-reseed that wrote
# nothing at all. The consumer is flint-server, which looks for this exact
# name on the next start-as-replica, so the exact name is the assertion.
mkdir -p "$S/node-a"
[ ! -e "$S/node-a/NEEDS_RESEED" ] || fail "NEEDS_RESEED already present before the verb ran"
"$CTL" host-mark-reseed "$S" node-a "drill" >/dev/null || fail "host-mark-reseed exited non-zero"
[ -s "$S/node-a/NEEDS_RESEED" ] \
  || fail "host-mark-reseed left no NEEDS_RESEED in $S/node-a. That is the exact
  name flint-server looks for on the next start-as-replica; anything else is a
  file nobody reads."
grep -q "drill" "$S/node-a/NEEDS_RESEED" \
  || fail "the marker does not carry the reason it was given"
echo "   NEEDS_RESEED written, carrying its reason"

# A NAME FROM SOMEONE ELSE'S ARGV. Both verbs guard on a `node-` prefix and a
# name with no slash; a remote caller composes that argument, so the refusal is
# the property worth asserting.
for bad in "../../etc" "node-a/../.." "notanode"; do
  if "$CTL" host-wipe-node "$S" "$bad" >/dev/null 2>&1; then
    fail "host-wipe-node ACCEPTED the name '$bad' -- this verb calls
  remove_dir_all on a path a remote caller named"
  fi
  if "$CTL" host-mark-reseed "$S" "$bad" why >/dev/null 2>&1; then
    fail "host-mark-reseed accepted the name '$bad'"
  fi
done
echo "   three malformed names refused by both verbs"

# CONTROL: the guard is not simply refusing everything.
"$CTL" host-wipe-node "$S" node-a >/dev/null || fail "host-wipe-node refused a WELL-FORMED name"
[ ! -d "$S/node-a" ] || fail "host-wipe-node returned 0 and left $S/node-a in place"
echo "   and a well-formed name is still wiped"

echo "== host-sweep and host-stop-all run and report"
"$CTL" host-sweep "$S" >/dev/null || fail "host-sweep exited non-zero"
"$CTL" host-stop-all "$S" >/dev/null || fail "host-stop-all exited non-zero"
echo "   both clean on an empty statedir"

echo "== an unknown host verb is refused, not silently ignored"
if "$CTL" host-nonesuch "$S" >/dev/null 2>&1; then
  fail "host-nonesuch exited 0. A typo'd verb arriving over ssh would then look
  exactly like a completed step -- BUG-0034's class, on the remote path."
fi
echo "   host-nonesuch -> non-zero"

rm -rf "$S"
echo "PASSED"
