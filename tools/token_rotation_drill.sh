#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Token rotation drill: zero-downtime dual-version token rotation with
# per-version usage metrics.
#   - a tenant rotates its token: OLD and NEW both authenticate (no downtime)
#   - the proxy counts AUTHs per token — the operator watches the OLD token's
#     count go flat (clients migrated) before retiring it
#   - CPDROPPREV retires the old token; it then gets WRONGPASS, NEW still works
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-rot-state 6750 7550 6323
fleet_guard
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; sleep 0.4
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
STATE=$FLINT_DRILL_ROOT/flint-rot-state
cleanup() {
  pkill -9 -f "flint-server --port 675" 2>/dev/null
  fleet_kill proxy
  fleet_kill controlplane
  rm -rf $FLINT_DRILL_ROOT/flint-rot-* "$STATE" "$STATE.tmp"
}
trap cleanup EXIT
rm -f "$STATE"

$B --port 6750 --engine rocks --data-dir $FLINT_DRILL_ROOT/flint-rot-data 2>"${FLEET_SCOPE}server.log" &
fleet_wait_listen 6750
sleep 0.6
$CP --port 7550 --state "$STATE" 2>$FLINT_DRILL_ROOT/flint-rot-cp.log &
fleet_wait_listen 7550
sleep 0.4
fleet_cp 7550 CPADDPROXY 127.0.0.1:6323
fleet_cp 7550 CPADDPAIR 127.0.0.1:6750
fleet_cp 7550 CPADDTENANT acme tok-v1 acme 1
$PX --port 6323 --control-plane 127.0.0.1:7550 --advertise 127.0.0.1:6323 2>$FLINT_DRILL_ROOT/flint-rot-px.log &
fleet_wait_listen 6323
sleep 1.2

a() { valkey-cli -p 6323 -a "$1" --no-auth-warning "${@:2}"; }
echo "== tenant on token v1: write + read work"
[ "$(a tok-v1 SET k hello)" = "OK" ] || { echo "FAIL: v1 auth pre-rotation"; exit 1; }
[ "$(a tok-v1 GET k)" = "hello" ] || { echo "FAIL: v1 read"; exit 1; }
echo "  v1 serves"

echo "== hold a connection OPEN across the rotation (M3 exit: zero DROPPED connections)"
# THE CLAUSE'S LETTER, which everything below this file already covered in
# substance and not in form. M3's exit says "token rotation completes with
# zero dropped connections". Every other assertion here calls `a()`, which is
# a fresh `valkey-cli` process and therefore a fresh connection -- so they
# prove AUTHENTICATION stays continuous across the rotation, which is the
# valuable half, and say nothing about a connection that was already open.
# Rewording the criterion to match the tooling was the alternative and was
# refused: M3 is a CLOSED milestone, and editing a met exit to fit the drill
# is how an exit stops meaning anything (ADR-0043 makes the same argument).
#
# A RAW SOCKET, not valkey-cli, and that is the whole point. A client that
# reconnects on error would paper over exactly the failure being tested and
# report a pass; this one holds one fd, never retries, and a server-side FIN
# arrives as an empty read that its reader raises on.
HELD=$FLINT_DRILL_ROOT/flint-rot-held
rm -f "$HELD-ready" "$HELD-go" "$HELD-log"
python3 - "$HELD" >"$HELD-log" 2>&1 <<'HELDPY' &
import os, socket, sys, time
root = sys.argv[1]
def resp(*a):
    return f"*{len(a)}\r\n".encode() + b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
def rd(s):
    b = b""
    while b"\r\n" not in b:
        c = s.recv(4096)
        if not c:
            raise OSError("peer closed the connection")
        b += c
    return b
s = socket.create_connection(("127.0.0.1", 6323), timeout=10)
s.settimeout(10)
s.sendall(resp("AUTH", "tok-v1")); rd(s)
s.sendall(resp("GET", "k"))
if b"hello" not in rd(s):
    print("HELD-PRE-FAIL: the held connection did not serve BEFORE the rotation")
    sys.exit(1)
open(root + "-ready", "w").close()
t0 = time.time()
while not os.path.exists(root + "-go"):
    if time.time() - t0 > 30:
        print("HELD-TIMEOUT: the rotation never signalled")
        sys.exit(1)
    time.sleep(0.1)
# THE ASSERTION. Same socket, no re-AUTH, after the rotation. A server-side
# drop arrives here as an empty read, which `rd` raises on rather than
# returning as a short reply.
try:
    s.sendall(resp("GET", "k")); r = rd(s)
except OSError as e:
    print(f"HELD-DROPPED: {e}")
    sys.exit(1)
if b"hello" not in r:
    print(f"HELD-BAD-REPLY: {r[:60]!r}")
    sys.exit(1)
print("HELD-OK")
# POSITIVE CONTROL. Everything above is satisfied by a checker that cannot
# tell a live socket from a dead one, so kill this one and require the SAME
# code path to report it. This proves the detector, not the product.
s.close()
try:
    s.sendall(resp("GET", "k")); rd(s)
    print("CONTROL-FAILED-TO-FAIL: a closed socket read as serving")
    sys.exit(1)
except OSError:
    print("CONTROL-OK")
HELDPY
HELD_PID=$!
for _ in $(seq 1 100); do [ -f "$HELD-ready" ] && break; sleep 0.1; done
[ -f "$HELD-ready" ] || {
  echo "FAIL: the held connection never authenticated BEFORE the rotation, so"
  echo "      this arm would have tested nothing. Its output:"
  sed 's/^/  | /' "$HELD-log" 2>/dev/null; kill "$HELD_PID" 2>/dev/null; exit 1; }
echo "  one connection open and serving on tok-v1, held across what follows"

echo "== rotate to v2: BOTH tokens authenticate (zero downtime)"
R=$(valkey-cli -p 7550 CPROTATETOKEN acme tok-v2)
echo "  $R"
echo "$R" | grep -q "rotated" || { echo "FAIL: rotate rejected: $R"; exit 1; }
sleep 1.2   # let the snapshot push carry the new token set
[ "$(a tok-v2 GET k)" = "hello" ] || { echo "FAIL: NEW token v2 not accepted after rotate"; exit 1; }
[ "$(a tok-v1 GET k)" = "hello" ] || { echo "FAIL: OLD token v1 stopped working (downtime!)"; exit 1; }
echo "  v1 AND v2 both serve — no downtime"

# THE HELD CONNECTION, now that the rotation has completed.
touch "$HELD-go"
wait "$HELD_PID" 2>/dev/null; HELD_RC=$?
if [ "$HELD_RC" != 0 ] || ! grep -q "HELD-OK" "$HELD-log"; then
  echo "FAIL: a connection established BEFORE the rotation did not survive it."
  echo "      M3's exit says rotation completes with ZERO DROPPED CONNECTIONS;"
  echo "      re-authenticating clients passing is not that claim."
  sed 's/^/  | /' "$HELD-log" 2>/dev/null; exit 1
fi
grep -q "CONTROL-OK" "$HELD-log" || {
  echo "FAIL: the held-connection check could not fail. Its own control -- close"
  echo "      the socket and require the same read path to report it -- did not"
  echo "      fire, so the PASS above describes the checker, not the product."
  sed 's/^/  | /' "$HELD-log" 2>/dev/null; exit 1; }
echo "  the pre-rotation connection served afterwards on the SAME socket, no re-AUTH"
echo "  and its control confirms a dead socket would have been caught"

echo "== per-version usage: proxy counts AUTHs per token"
# Drive some traffic on each token, then read the counters.
for i in $(seq 1 5); do a tok-v1 PING >/dev/null; done
for i in $(seq 1 8); do a tok-v2 PING >/dev/null; done
C1=$(valkey-cli -p 6323 PROXYAUTHCOUNT tok-v1)
C2=$(valkey-cli -p 6323 PROXYAUTHCOUNT tok-v2)
echo "  auth counts: v1=$C1 v2=$C2"
[ "$C1" -ge 5 ] && [ "$C2" -ge 8 ] || { echo "FAIL: counters wrong (v1=$C1 v2=$C2)"; exit 1; }

echo "== drain check: old token's count goes FLAT as clients migrate"
BEFORE=$(valkey-cli -p 6323 PROXYAUTHCOUNT tok-v1)
# Simulate: clients now only use v2.
for i in $(seq 1 10); do a tok-v2 PING >/dev/null; done
AFTER=$(valkey-cli -p 6323 PROXYAUTHCOUNT tok-v1)
[ "$AFTER" = "$BEFORE" ] || { echo "FAIL: v1 count still climbing ($BEFORE -> $AFTER)"; exit 1; }
echo "  v1 count flat at $AFTER while v2 traffic continued — safe to retire"

echo "== retire the old token (CPDROPPREV): v1 -> WRONGPASS, v2 still serves"
valkey-cli -p 7550 CPDROPPREV acme >/dev/null
sleep 1.2
X=$(a tok-v1 GET k 2>&1)
echo "$X" | grep -q "WRONGPASS" || { echo "FAIL: retired token v1 still accepted: $X"; exit 1; }
[ "$(a tok-v2 GET k)" = "hello" ] || { echo "FAIL: v2 broke after dropping v1"; exit 1; }
echo "  v1 rejected, v2 serves"

echo "== rotation state is durable (CP restart preserves current token)"
fleet_kill controlplane; sleep 0.4
$CP --port 7550 --state "$STATE" 2>>$FLINT_DRILL_ROOT/flint-rot-cp.log &
fleet_wait_listen 7550
sleep 1.5
[ "$(a tok-v2 GET k)" = "hello" ] || { echo "FAIL: v2 lost across CP restart"; exit 1; }
echo "  current token survived CP restart"

echo "PASS: dual-version token rotation — zero downtime, per-token usage metric, durable"
