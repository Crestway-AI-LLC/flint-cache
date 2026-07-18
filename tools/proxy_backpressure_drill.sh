#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Proxy admission-control drill (backpressure at the front door).
#   A) the proxy caps concurrent client connections (--max-conns): at the cap
#      a new connection is SHED with -THROTTLED (a clean signal the client
#      already backs off on), not an unbounded thread spawn; freeing a slot
#      lets a new connection in again
#   B) the data plane's own -THROTTLED (widowed master below
#      min-replicas-to-write) passes THROUGH the proxy to the client unchanged
#      — the proxy relays durability backpressure, it does not swallow it
set -u
cd "$(dirname "$0")/.."
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=/tmp/flint-bp; rm -rf "$D"; mkdir -p "$D"
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null; sleep 0.4
cleanup() { pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null; rm -rf "$D"; }
trap cleanup EXIT

echo "== A) admission cap: --max-conns 4, shed the 5th with -THROTTLED"
$B --port 6810 --engine rocks --data-dir "$D/d" 2>/dev/null &
sleep 0.6
$PX --port 7810 --pairs "127.0.0.1:6810" --max-conns 4 2>"$D/px.log" &
sleep 1.0
grep -q "max-conns 4" "$D/px.log" || { echo "FAIL: proxy did not report the cap"; cat "$D/px.log"; exit 1; }

python3 - <<'PY'
import socket, sys, time
# Hold 4 idle connections open (each occupies a worker thread blocked on read).
held = []
for i in range(4):
    s = socket.create_connection(("127.0.0.1", 7810), timeout=3)
    held.append(s)
time.sleep(0.5)  # let all 4 workers be spawned and counted

# The 5th connection must be shed with -THROTTLED (no 5th worker).
s5 = socket.create_connection(("127.0.0.1", 7810), timeout=3)
s5.settimeout(3)
try:
    reply = s5.recv(256)
except socket.timeout:
    reply = b"<timeout>"
if b"THROTTLED" not in reply:
    print(f"FAIL: 5th connection not shed with THROTTLED (got: {reply!r})"); sys.exit(1)
print(f"  5th connection shed: {reply.decode('latin1').strip()}")
s5.close()

# Free one slot; a fresh connection must now be admitted and serve a PING.
held.pop().close()
time.sleep(0.6)  # worker exit -> counter decrement
ok = False
for _ in range(20):
    try:
        s6 = socket.create_connection(("127.0.0.1", 7810), timeout=3)
        s6.sendall(b"*1\r\n$4\r\nPING\r\n")
        s6.settimeout(3)
        if b"PONG" in s6.recv(64):
            ok = True; s6.close(); break
        s6.close()
    except OSError:
        pass
    time.sleep(0.2)
if not ok:
    print("FAIL: freed slot did not admit a new connection"); sys.exit(1)
print("  freed a slot -> new connection admitted and served PONG")
for s in held: s.close()
PY
[ $? -eq 0 ] || exit 1
echo "  admission cap enforced and recovers"

echo "== B) data-plane -THROTTLED passes through the proxy unchanged"
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null; sleep 0.4
# Widowed master: requires 1 live replica to write, but has none -> writes shed.
$B --port 6811 --engine rocks --data-dir "$D/d2" --min-replicas-to-write 1 2>/dev/null &
sleep 0.6
$PX --port 7811 --pairs "127.0.0.1:6811" 2>"$D/px2.log" &
sleep 1.0
R=$(valkey-cli -p 7811 SET k v 2>&1)
echo "$R" | grep -qi "THROTTLED" || { echo "FAIL: proxy did not relay -THROTTLED (got: $R)"; exit 1; }
echo "  SET through proxy -> $R"
# And a read still works (only writes are shed by the durability gate).
[ "$(valkey-cli -p 7811 GET missing)" = "" ] || { echo "FAIL: read path disturbed"; exit 1; }
echo "  reads unaffected; durability backpressure relayed end to end"

echo "PASS: proxy admission control (connection cap + shed/recover) and data-plane -THROTTLED passthrough"
