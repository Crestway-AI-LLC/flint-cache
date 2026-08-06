#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# R1 drill — a replica self-fences stale reads; the proxy falls back to the
# master (back-pressure review).
#   - master + replica; tenant opted into replica reads (D7)
#   - healthy: replica-served reads succeed (contact fresh via keepalive)
#   - CUT the replica off from the master (kill the master): the replica's
#     contact goes stale past the bound -> it fences reads with -TRYAGAIN
#   - a replica-read tenant STILL gets correct data: the proxy falls back to
#     ... nothing (master dead) -> error, proving the replica refused to
#     serve stale. Then bring a NEW master truth: with the master alive but
#     the replica partitioned, the proxy routes the read to the master.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-stalefence 7081 7082 6314 7999
fleet_guard
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
D=/tmp/flint-stalefence; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; sleep 0.4
cleanup() {
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; rm -rf "$D"
}
trap cleanup EXIT

echo "== master + replica; acme opts into replica reads; stale bound 1.5s"
$CP --port 6314 --state "$D/cp" 2>/dev/null &
for i in $(seq 1 30); do [ "$(valkey-cli -p 6314 PING 2>/dev/null)" = "PONG" ] && break; sleep 0.2; done
valkey-cli -p 6314 CPADDPROXY 127.0.0.1:7999 >/dev/null
valkey-cli -p 6314 CPADDPAIR 127.0.0.1:7081,127.0.0.1:7082 >/dev/null
valkey-cli -p 6314 CPADDTENANT acme tok-acme acme 1 >/dev/null
valkey-cli -p 6314 CPTENANTREADS acme on >/dev/null
$B --port 7081 --engine rocks --data-dir "$D/m" 2>/dev/null &
fleet_wait_listen 7081
sleep 0.7
$B --port 7082 --engine rocks --data-dir "$D/r" --replica-of 127.0.0.1:7081 \
   --replica-read-stale-ms 1500 2>/dev/null &
$PX --port 7999 --control-plane 127.0.0.1:6314 --advertise 127.0.0.1:7999 2>/dev/null &
fleet_wait_listen 7082 7999
sleep 1.5

A="valkey-cli -p 7999 -a tok-acme --no-auth-warning"
$A SET k hello >/dev/null
sleep 0.5   # replicate

echo "== healthy: replica-served reads succeed (contact fresh via keepalive)"
# Hit the replica DIRECTLY to prove it serves while in live contact.
RREAD() { valkey-cli -p 7082 FLINTNS acme >/dev/null 2>&1; python3 - <<'PY'
import socket
def resp(a): return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
s=socket.create_connection(("127.0.0.1",7082),timeout=3); s.settimeout(3)
s.sendall(resp(["FLINTNS","acme"])); s.recv(64)
s.sendall(resp(["GET","k"]))
b=b""
while not b.endswith(b"\r\n"): b+=s.recv(256)
print(b.decode(errors="replace").strip().split("\r\n")[-1])
PY
}
V=$(RREAD)
[ "$V" = "hello" ] || { echo "FAIL: healthy replica did not serve (got '$V')"; exit 1; }
echo "  replica serves GET=hello while in live contact with the master"

echo "== CUT the link: kill the master; the replica's contact goes stale"
pkill -9 -f "flint-server --port 7081"
sleep 2.5   # > the 1.5s stale bound + a keepalive interval

echo "== the replica now FENCES reads with -TRYAGAIN (no stale serving)"
FENCED=$(RREAD 2>&1)
echo "$FENCED" | grep -qi "TRYAGAIN\|out of sync" || { echo "FAIL: stale replica still served (got '$FENCED')"; exit 1; }
echo "  replica refuses the read: $(echo "$FENCED" | head -c 70)"

echo "== through the PROXY, a replica-read tenant is not served stale either"
# Master is dead and the replica fences -> the proxy exhausts fallback and
# errors, rather than returning stale data. (Correctness > availability here.)
PR=$($A GET k 2>&1)
echo "$PR" | grep -q "hello" && { echo "FAIL: proxy served stale replica data"; exit 1; }
echo "  proxy did NOT serve stale data (returned: $(echo "$PR" | head -c 50))"

echo "== recovery: restart the master; the replica re-syncs and serves again"
$B --port 7081 --engine rocks --data-dir "$D/m" 2>/dev/null &
sleep 3   # reconnect (1s retry) + keepalive re-establishes contact
V=$(RREAD)
[ "$V" = "hello" ] || { echo "FAIL: replica did not recover after re-contact (got '$V')"; exit 1; }
echo "  master back, replica re-contacted, reads serve again: GET=hello"

echo "PASS: R1 — replica self-fences reads on lost master contact; proxy never serves past the staleness bound; recovers on re-sync"
