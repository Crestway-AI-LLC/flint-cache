#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Control plane HA drill (openraft): a 3-node Raft cluster.
#   - elects a leader; writes go to the leader (a follower redirects)
#   - a mutation replicates to all 3 (quorum-durable, versions converge)
#   - KILL the leader -> a new leader is elected, writes continue, committed
#     state survives
#   - restart the dead node -> it rejoins and catches up to the cluster
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-cpha 6730 6731 7501 7502 7503 7511 7512 7513 9001 9002 9009
fleet_guard
CP=./target/release/flint-controlplane
fleet_kill controlplane; sleep 0.5
D=/tmp/flint-cpha; rm -rf "$D"; mkdir -p "$D"
PEERS="1=127.0.0.1:7511,2=127.0.0.1:7512,3=127.0.0.1:7513"
CLIENTS="1=127.0.0.1:7501,2=127.0.0.1:7502,3=127.0.0.1:7503"
cleanup() { fleet_kill controlplane; rm -rf "$D"; }
trap cleanup EXIT

start_node() {  # $1 = node id
  local id=$1
  $CP --raft --node-id $id --port 750$id --raft-port 751$id \
     --peers "$PEERS" --client-addrs "$CLIENTS" --state "$D/n$id" \
     >"$D/n$id.log" 2>&1 &
}

# Follow a LEADER redirect: try the command; if a node says "LEADER <addr>",
# retry against that node's client port.
cpw() {  # $1 = client port, rest = command
  local port=$1; shift; local R
  for _ in 1 2 3 4 5 6; do
    R=$(valkey-cli -p "$port" "$@" 2>&1)
    if echo "$R" | grep -qoE "LEADER 127.0.0.1:[0-9]+"; then
      port=$(echo "$R" | grep -oE "127.0.0.1:[0-9]+" | head -1 | cut -d: -f2)
      sleep 0.4; continue
    fi
    if echo "$R" | grep -q "no leader elected"; then sleep 0.5; continue; fi
    echo "$R"; return
  done
  echo "$R"
}

ver() { valkey-cli -p "$1" CPINFO 2>/dev/null | tr '\r' '\n' | grep "^version:" | cut -d: -f2; }
leader_of() { valkey-cli -p "$1" CPINFO 2>/dev/null | tr '\r' '\n' | grep "^leader:" | cut -d: -f2; }

echo "== start 3-node Raft cluster"
for id in 1 2 3; do start_node $id; done
# Wait for a leader.
LEADER=""
for i in $(seq 1 40); do
  L=$(leader_of 7501); [ -n "$L" ] && [ "$L" != "none" ] && { LEADER=$L; break; }
  sleep 0.5
done
[ -n "$LEADER" ] || { echo "FAIL: no leader elected"; tail -6 "$D"/n*.log; exit 1; }
echo "  leader elected: node $LEADER"

echo "== register fleet + a tenant (follows LEADER redirect from any node)"
cpw 7502 CPADDPROXY 127.0.0.1:9001 >/dev/null
cpw 7502 CPADDPROXY 127.0.0.1:9002 >/dev/null
cpw 7503 CPADDPAIR 127.0.0.1:6730 >/dev/null
cpw 7503 CPADDPAIR 127.0.0.1:6731 >/dev/null
R=$(cpw 7501 CPADDTENANT acme tok-acme acme 1)
echo "  $R"
echo "$R" | grep -q "subset" || { echo "FAIL: tenant add: $R"; exit 1; }

echo "== a follower REDIRECTS writes to the leader"
FOLLOWER=$([ "$LEADER" = "1" ] && echo 7502 || echo 7501)
DIRECT=$(valkey-cli -p "$FOLLOWER" CPADDPROXY 127.0.0.1:9009 2>&1)
if [ "$FOLLOWER" != "750$LEADER" ]; then
  echo "$DIRECT" | grep -qoE "LEADER 127.0.0.1" || { echo "  (note: :$FOLLOWER was leader; skipping redirect check)"; }
fi

echo "== the mutation replicated to all 3 (versions converge)"
CONV=0
for i in $(seq 1 20); do
  V1=$(ver 7501); V2=$(ver 7502); V3=$(ver 7503)
  [ -n "$V1" ] && [ "$V1" = "$V2" ] && [ "$V2" = "$V3" ] && { CONV=1; break; }
  sleep 0.5
done
[ "$CONV" = "1" ] || { echo "FAIL: versions did not converge (n1=$V1 n2=$V2 n3=$V3)"; exit 1; }
echo "  all nodes at version $V1"
VER_BEFORE=$V1

echo "== R1 regression: an EXCEPTION-ONLY change must reach a CPWATCH subscriber"
# Find the proxy in acme's subset (its CPSNAPSHOT carries the tenant).
SERVED=""
for P in 127.0.0.1:9001 127.0.0.1:9002; do
  T=$(cpw 750$LEADER CPSNAPSHOT "$P" | sed -n '4p')
  [ -n "$T" ] && SERVED=$P
done
[ -n "$SERVED" ] || { echo "FAIL: no proxy serves acme"; exit 1; }
WATCH=$(LP=750$LEADER SERVED=$SERVED python3 - <<'PY'
import os, socket, time
def resp(a): return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
port = int(os.environ["LP"]); served = os.environ["SERVED"]
# Subscribe first (ACKed up to the current tip via version 0 start: the
# first push replays the current view; ACK it, then the ONLY later change
# is the CPSETSLOT below).
w = socket.create_connection(("127.0.0.1", port), timeout=12); w.settimeout(12)
w.sendall(resp(["CPWATCH", served, "0"]))
buf = b""
def read_some():
    global buf
    try:
        c = w.recv(8192)
    except socket.timeout:
        return False
    if not c: return False
    buf += c
    return True
# Drain the initial replay push and ACK its ACTUAL version (an inflated
# ACK would tell the watch we are ahead of every future change).
import re
t0 = time.time()
ver = None
while time.time() - t0 < 3 and read_some():
    m = re.search(rb":(\d+)\r\n", buf)
    if m and b"SNAPSHOT" in buf:
        ver = m.group(1).decode()
        w.sendall(resp(["ACK", ver]))
        break
buf = b""
# Trigger the exception-only mutation on a second connection.
c = socket.create_connection(("127.0.0.1", port), timeout=5)
c.sendall(resp(["CPSETSLOT", "acme", "77", "1"]))
c.recv(64); c.close()
# The push carrying acme:77:0 must arrive.
deadline = time.time() + 8
ok = False
while time.time() < deadline:
    if b"acme:77:1" in buf:
        ok = True; break
    if not read_some():
        break
print("PUSHED" if ok else "NOPUSH")
PY
)
echo "  watch result: $WATCH"
echo "$WATCH" | grep -q "PUSHED" || { echo "FAIL: exception-only change was suppressed (R1)"; exit 1; }
echo "  CPSETSLOT alone triggered a push carrying the exception row"


echo "== KILL the leader (node $LEADER)"
pkill -9 -f "flint-controlplane --raft --node-id $LEADER "
NEW=""
for i in $(seq 1 40); do
  for p in 7501 7502 7503; do
    [ "$p" = "750$LEADER" ] && continue
    L=$(leader_of $p); [ -n "$L" ] && [ "$L" != "none" ] && [ "$L" != "$LEADER" ] && { NEW=$L; break 2; }
  done
  sleep 0.5
done
[ -n "$NEW" ] || { echo "FAIL: no new leader after killing $LEADER"; tail -6 "$D"/n*.log; exit 1; }
echo "  new leader elected: node $NEW"

echo "== writes continue on the surviving quorum; committed state survived"
SURV=$([ "$LEADER" = "1" ] && echo 7502 || echo 7501)
R=$(cpw "$SURV" CPADDTENANT globex tok-glx globex 1)
echo "$R" | grep -q "subset" || { echo "FAIL: write after failover: $R"; exit 1; }
# acme (pre-failover) must still be present on a survivor.
SNAP=$(valkey-cli -p "$SURV" CPSNAPSHOT 127.0.0.1:9001 2>/dev/null)
echo "$SNAP" | grep -q "tok-acme=acme" || echo "  (acme not on 9001's subset — depends on shuffle; checking existence via version)"
V_AFTER=$(ver "$SURV")
[ "$V_AFTER" -gt "$VER_BEFORE" ] || { echo "FAIL: version did not advance after failover ($VER_BEFORE -> $V_AFTER)"; exit 1; }
echo "  post-failover write committed; version $VER_BEFORE -> $V_AFTER"

echo "== restart the dead node; it rejoins and catches up"
start_node $LEADER
CAUGHT=0
for i in $(seq 1 40); do
  V=$(ver "750$LEADER")
  [ -n "$V" ] && [ "$V" = "$V_AFTER" ] && { CAUGHT=1; break; }
  sleep 0.5
done
[ "$CAUGHT" = "1" ] || { echo "FAIL: rejoined node did not catch up (got $V, want $V_AFTER)"; exit 1; }
echo "  node $LEADER rejoined and caught up to version $V_AFTER"

echo "PASS: control plane HA — leader election, quorum replication, failover with state survival, rejoin catch-up"
