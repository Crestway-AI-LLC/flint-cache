#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Async-writes as a TENANT FLAG ('a', ADR-0005 D4 made a CP fact) drill.
# One rocks node with NO --async-writes CLI scope (but a tiny queue cap),
# one CP, one proxy fed by CPWATCH. Tenant 'hot' is flagged with
# CPTENANTASYNC; tenant 'cold' is not. The proof is differential:
#   - CPSNAPSHOT carries the 'a' flag for hot only
#   - an identical 24-connection INCR storm on ONE key per tenant:
#     hot sheds -THROTTLED (the cap-2 queue is in its path) and its
#     counter is EXACT after retries; cold sheds ZERO (inline path) and
#     its counter is exact too
#   - CPTENANTASYNC off propagates: a rerun storm on hot sheds zero
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-af-state 6760 7520 7611
fleet_guard
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; sleep 0.4
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
STATE=/tmp/flint-af-state
D=/tmp/flint-af-node
cleanup() {
  pkill -9 -f "flint-server --port 6760" 2>/dev/null
  fleet_kill proxy
  fleet_kill controlplane
  rm -rf "$D" "$STATE" "$STATE.tmp" /tmp/flint-af-*.log
}
trap cleanup EXIT
rm -rf "$D" "$STATE"

echo "== node (rocks, queue cap 2, NO --async-writes scope), CP, proxy"
$B --port 6760 --engine rocks --data-dir "$D" --async-queue-cap 2 2>/tmp/flint-af-node.log &
$CP --port 7520 --state "$STATE" 2>/tmp/flint-af-cp.log &
for i in $(seq 1 40); do [ "$(valkey-cli -p 7520 PING 2>/dev/null)" = "PONG" ] && break; sleep 0.2; done
for i in $(seq 1 40); do [ "$(valkey-cli -p 6760 PING 2>/dev/null)" = "PONG" ] && break; sleep 0.2; done
valkey-cli -p 7520 CPADDPROXY 127.0.0.1:7611 >/dev/null
valkey-cli -p 7520 CPADDPAIR 127.0.0.1:6760 >/dev/null
valkey-cli -p 7520 CPADDTENANT hot tok-hot hot 1 >/dev/null
valkey-cli -p 7520 CPADDTENANT cold tok-cold cold 1 >/dev/null
R=$(valkey-cli -p 7520 CPTENANTASYNC hot on)
[ "$R" = "OK" ] || { echo "FAIL: CPTENANTASYNC: $R"; exit 1; }
$PX --port 7611 --control-plane 127.0.0.1:7520 --advertise 127.0.0.1:7611 2>/tmp/flint-af-proxy.log &
fleet_wait_listen 7611
sleep 1.5

echo "== snapshot carries the 'a' flag for hot only"
S=$(valkey-cli -p 7520 CPSNAPSHOT 127.0.0.1:7611)
echo "$S" | grep -q "=hot#a" || { echo "FAIL: no 'a' flag on hot in snapshot: $S"; exit 1; }
echo "$S" | grep -q "=cold#" && { echo "FAIL: unexpected flags on cold: $S"; exit 1; }
echo "  hot#a present, cold unflagged"

# A storm: N_CONN authed connections x N_EACH INCRs on one shared key,
# honoring the -THROTTLED retry contract. Prints "<throttled> <counter>".
storm() { # $1 = token, $2 = key
  python3 - "$1" "$2" <<'PY'
import socket, sys, threading
tok, key = sys.argv[1], sys.argv[2]
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
N_CONN, N_EACH = 24, 200
throttled = [0]
lock = threading.Lock()
def worker():
    s = socket.create_connection(("127.0.0.1", 7611), timeout=15); s.settimeout(15)
    s.sendall(resp(["AUTH", tok])); s.recv(64)
    local = 0
    for _ in range(N_EACH):
        while True:
            s.sendall(resp(["INCR", key]))
            b = b""
            while not b.endswith(b"\r\n"): b += s.recv(64)
            if b"THROTTLED" not in b:
                break
            local += 1
    with lock: throttled[0] += local
    s.close()
ts = [threading.Thread(target=worker) for _ in range(N_CONN)]
[t.start() for t in ts]; [t.join() for t in ts]
s = socket.create_connection(("127.0.0.1", 7611), timeout=15); s.settimeout(15)
s.sendall(resp(["AUTH", tok])); s.recv(64)
s.sendall(resp(["GET", key]))
b = b""
while not b.endswith(b"\r\n"): b += s.recv(64)
print(throttled[0], int(b.split(b"\r\n")[1]))
PY
}

echo "== storm on FLAGGED tenant: queue in the path (cap 2 -> sheds), counter exact"
read -r T1 C1 <<<"$(storm tok-hot af:counter)"
[ "$C1" = "4800" ] || { echo "FAIL: hot counter $C1 (want 4800)"; exit 1; }
[ "$T1" -ge 1 ] || { echo "FAIL: flagged tenant never touched the queue (0 THROTTLED at cap 2)"; exit 1; }
echo "  hot: $T1 -THROTTLED sheds absorbed, counter exactly 4800"

echo "== identical storm on UNFLAGGED tenant: inline path, zero sheds, counter exact"
read -r T2 C2 <<<"$(storm tok-cold af:counter)"
[ "$C2" = "4800" ] || { echo "FAIL: cold counter $C2 (want 4800)"; exit 1; }
[ "$T2" = "0" ] || { echo "FAIL: unflagged tenant hit the queue ($T2 THROTTLED)"; exit 1; }
echo "  cold: 0 -THROTTLED, counter exactly 4800"

echo "== CPTENANTASYNC off propagates to NEW connections"
valkey-cli -p 7520 CPTENANTASYNC hot off >/dev/null
sleep 1.5   # snapshot push
read -r T3 C3 <<<"$(storm tok-hot af:counter2)"
[ "$C3" = "4800" ] || { echo "FAIL: post-off counter $C3 (want 4800)"; exit 1; }
[ "$T3" = "0" ] || { echo "FAIL: flag off but still queued ($T3 THROTTLED)"; exit 1; }
echo "  flag off: 0 -THROTTLED, counter exactly 4800"

echo "== tenant SELF-SERVICE path: CPMYCONFIG <token> async-writes on (the portal's call)"
R=$(valkey-cli -p 7520 CPMYCONFIG tok-cold async-writes on)
[ "$R" = "OK" ] || { echo "FAIL: CPMYCONFIG async-writes: $R"; exit 1; }
sleep 1.5   # snapshot push
read -r T4 C4 <<<"$(storm tok-cold af:counter3)"
[ "$C4" = "4800" ] || { echo "FAIL: self-service counter $C4 (want 4800)"; exit 1; }
[ "$T4" -ge 1 ] || { echo "FAIL: self-service opt-in never reached the queue"; exit 1; }
echo "  cold self-opted in: $T4 -THROTTLED sheds, counter exactly 4800"

echo "PASS: 'a' is a live CP tenant flag — snapshot-pushed, proxy-pinned, queue-routed, exact counters, off propagates, self-service works"
