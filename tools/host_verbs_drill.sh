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

# ---------------------------------------------------------------------------
# THE CALLER HALF, for the one thing it can be asked without a second machine:
# what does `stop` say about a host it could not reach?
#
# `sweep_orphans` had no error arm at all. `if let Ok(out) = r.output(&argv)`,
# an unparseable count `unwrap_or(0)`, and no status check -- and `output()`
# returns Ok for an ssh that FAILED, since only a missing ssh binary is Err.
# So an unreachable host contributed 0 swept and printed nothing, and `stop`'s
# only summary is `if swept > 0`: the command printed exactly what a clean
# fleet prints. `stop`'s own remote arm, three lines up, does report its
# failures -- the same call in the same loop, one arm reporting and one silent.
#
# `nosuchhost.invalid` is RFC 2606's reserved TLD: guaranteed never to resolve,
# so this needs no fixture host and cannot accidentally reach one. Every seat is
# placed there, so nothing local is stopped either.
#
# WAS nosuchhost.invalid (TEST-NET-1), which is equally safe and cost TWENTY SECONDS a
# run -- two ssh calls each paying the 10 s ConnectTimeout. A name that does not
# resolve fails in milliseconds, and the code under test cannot tell the two
# apart: both are ssh exit 255 with a line on stderr, which is exactly what the
# arms below read. Trading a connect timeout for a resolve failure buys the
# whole drill back.
echo "== stop names a host it could not sweep, instead of counting it as clean"
R=$S/remote; mkdir -p "$R/bin"
cat > "$R/inv" <<INV
disposable on
statedir $R
bins $R/bin
tls off
ssh-user nobody
ssh-key $R/no-such-key
cp nosuchhost.invalid:7500
pair nosuchhost.invalid:7001,nosuchhost.invalid:7002
INV
OUT=$("$CTL" -f "$R/inv" stop 2>&1 || true)
printf '%s\n' "$OUT" | grep -q "nosuchhost.invalid" || fail "stop said nothing about the
  unreachable host. Its output was:
$OUT"
printf '%s\n' "$OUT" | grep -qi "UNKNOWN, not zero" || fail "stop mentioned the
  host but not that its orphan count is unknown. 'swept 0' and 'could not ask'
  must not read the same. Its output was:
$OUT"
# BOTH REMOTE ARMS, not just the sweep. `stop` runs host-stop-all first and
# host-sweep second, and each had the same defect for the same reason -- Err
# means "no ssh binary", so an ssh that failed took the Ok arm and printed its
# (empty) stdout. Asserting only the sweep would leave the arm whose silence
# means "a seat may still be serving" uncovered, which is the more expensive
# half: this function's own opening comment says reading only the local
# directory "would leave remote seats running while reporting success".
printf '%s\n' "$OUT" | grep -q "host-stop-all exited" || fail "stop did not
  report that host-stop-all failed on the unreachable host. Its output was:
$OUT"
printf '%s\n' "$OUT" | grep -q "STILL BE RUNNING" || fail "stop reported the
  failed host-stop-all without saying what it means for the seats there.
  Its output was:
$OUT"
printf '%s\n' "$OUT" | grep "nosuchhost.invalid" | sed 's/^ */   /'

# ---------------------------------------------------------------------------
# THE SAME DEFECT, ONE FUNCTION OVER (BUG-0102). `start` prepares each host's
# statedir with a remote `mkdir -p` and checked only the Err -- which, as
# above, means "the ssh binary is missing" and never fires. A mkdir refused for
# permissions, or an ssh that never landed, was accepted, and the run went on
# to fail further down at a spawn that could not write its pidfile: a message
# naming the wrong step, on a host the operator had no reason to suspect.
echo "== start refuses when it cannot prepare a host's statedir"
OUT=$("$CTL" -f "$R/inv" start 2>&1 || true)
printf '%s\n' "$OUT" | grep -q "preparing statedir" || fail "start did not name the
  statedir step. It must fail THERE rather than at whatever breaks next.
  Its output was:
$OUT"
printf '%s\n' "$OUT" | grep -q "nosuchhost.invalid" || fail "start did not name the
  host whose statedir it could not prepare. Its output was:
$OUT"
printf '%s\n' "$OUT" | grep -q "exit 255" || fail "start reported the failure
  without the ssh exit status, which is what says the connection failed rather
  than the command. Its output was:
$OUT"
echo "   $(printf '%s\n' "$OUT" | grep -m1 "preparing statedir" | sed 's/^ *//')"

rm -rf "$S"
echo "PASSED"
