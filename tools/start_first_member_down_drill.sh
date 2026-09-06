#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0032, the FOURTH construction: `flintctl start` must tolerate a first
# pair member that will not answer, and must still refuse when NO member does.
#
# The fix landed 2026-08-19 and has been behaviourally unverified since, with
# three attempts recorded in the write-up and each failing differently:
#
#   1. Stop pair[0] and run `start` -- PASSED against the UNFIXED binary, so it
#      tested nothing. `start` SPAWNS every seat before it checks any of them,
#      so a merely stopped seat is restarted and then answers.
#   2. Hold pair[0]'s port with $(hold_ports ...) -- the holder ran in a
#      SUBSHELL and did not outlive it. The precondition was set up and never
#      checked, which is the bug under test one level up.
#   3. Hold the port correctly -- `start` refuses earlier with its own,
#      correct, unrelated guard about a port still bound after the process
#      was gone.
#
# The production shape (docs/bugs/0031) was a seat that STARTED AND EXITED,
# leaving its port FREE: it never answered because it kept dying, not because
# something else held its address. This reproduces that with a data directory
# the engine cannot open, so the seat `start` spawns dies at open and the port
# is never bound.
#
# TWO ARMS, AND THE SECOND IS THE POSITIVE CONTROL THAT MAKES THE FIRST MEAN
# ANYTHING. Exit 0 with one member down is also what a `start` that checks
# nothing would do. So the same command is run again with BOTH members unable
# to answer and must fail.
#
# AND IT WAS CHECKED AGAINST THE PRE-FIX SHAPE, which is the whole difference
# from construction 1. Narrowing the member scan back to `pair.iter().take(1)`
# -- "only pair[0] counts", the defect -- turns ARM 1 red:
#
#     FAIL: start exited 1 with one member down
#
# So this construction discriminates, where construction 1 passed against the
# unfixed binary and therefore tested nothing.
set -uo pipefail
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"

A=127.0.0.1:6424
B=127.0.0.1:6425
PROXY=127.0.0.1:6426
CP=127.0.0.1:6427
STATE="$FLINT_DRILL_ROOT/flint-startdown-state"
INV="$FLINT_DRILL_ROOT/flint-startdown.flint"

fleet_init "$STATE" 6424 6425 6426 6427
fleet_guard

fail() { echo "FAIL: $*"; exit 1; }
cleanup() { ./target/release/flintctl -f "$INV" stop >/dev/null 2>&1; }
trap cleanup EXIT

cargo build --release -q -p flint-ctl -p flint-server -p flint-proxy \
  -p flint-controlplane --features flint-server/rocks || fail "build"

rm -rf "$STATE" "$INV"; mkdir -p "$STATE"
cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
cp $CP
pair $A,$B
proxy $PROXY
EOF
CTL="./target/release/flintctl -f $INV"

echo "== bootstrap"
$CTL bootstrap >"$STATE-boot.log" 2>&1 || { echo "FAIL: bootstrap"; tail -20 "$STATE-boot.log"; exit 1; }
echo "   pair up"

# The seat directory `start` will hand the engine. Discovered rather than
# assumed: a name spelled from the port would silently stop matching the day
# flintctl renames a seat, and the sabotage would then be a no-op that this
# drill reports as a pass.
NODEDIR=$(ls -d "$STATE"/node-* 2>/dev/null | head -1)
[ -n "$NODEDIR" ] || fail "no node-* directory under $STATE after bootstrap --
  the sabotage below has nothing to aim at, so this drill would test nothing"
echo "   seat dir: $(basename "$NODEDIR")"

echo "== sabotage pair[0]: a data dir the engine cannot open"
$CTL stop >/dev/null 2>&1
sleep 1
rm -rf "$NODEDIR"
# A regular FILE where a directory belongs: RocksDB fails at open, the process
# exits, and the port is never bound -- the production shape, not a held port.
printf 'not a rocksdb directory\n' > "$NODEDIR"

# ASSERT THE PRECONDITION, which is construction 2's whole lesson. A drill that
# sets up a condition and never checks it is the bug under test.
if valkey-cli -p 6424 PING >/dev/null 2>&1; then
  fail "pair[0] still answers before the test even ran"
fi
python3 - <<'PY' || fail "port 6424 is NOT free -- this is construction 3's condition
  (a held port), which start refuses for a different and correct reason, so the
  arm below would be measuring that guard instead of this fix."
import socket, sys
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    s.bind(("127.0.0.1", 6424))
except OSError:
    sys.exit(1)
s.close()
PY
echo "   pair[0] silent, and its port is FREE (bind-probed)"

echo "== ARM 1: start must tolerate it, because the other member serves"
OUT=$($CTL start 2>&1); RC=$?
[ "$RC" = "0" ] || { echo "FAIL: start exited $RC with one member down"; echo "$OUT" | tail -20; exit 1; }
case "$OUT" in
  *"did not, which is normal after a failover"*) : ;;
  *) echo "FAIL: start exited 0 but never reported that pair[0] did not answer."
     echo "      Exit 0 alone is also what a start that checks nothing returns;"
     echo "      this message is the fix's signature -- it says start asked the"
     echo "      OTHER member and accepted its answer."
     echo "$OUT" | tail -20; exit 1 ;;
esac
valkey-cli -p 6425 PING 2>/dev/null | grep -q PONG \
  || fail "start exited 0 but pair[1] is not serving either"
echo "   exit 0, pair[0] reported down, pair[1] serving"

echo "== ARM 2 (the control): with NEITHER member answering, start must refuse"
$CTL stop >/dev/null 2>&1
sleep 1
NODEDIR2=$(ls -d "$STATE"/node-* 2>/dev/null | grep -v "^$NODEDIR$" | head -1)
[ -n "$NODEDIR2" ] || fail "cannot find pair[1]'s seat dir to sabotage"
rm -rf "$NODEDIR2"; printf 'not a rocksdb directory\n' > "$NODEDIR2"
OUT2=$($CTL start 2>&1); RC2=$?
[ "$RC2" != "0" ] || { echo "FAIL: start exited 0 with NO member answering."
  echo "      Then ARM 1 proves nothing: a start that tolerates everything"
  echo "      passes it too."
  echo "$OUT2" | tail -20; exit 1; }
case "$OUT2" in
  *"has no reachable member"*) : ;;
  *) echo "FAIL: start failed but not for the reason under test:"; echo "$OUT2" | tail -20; exit 1 ;;
esac
echo "   exit $RC2, naming the pair with no reachable member"

echo "PASSED"
