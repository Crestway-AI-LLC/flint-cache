#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Control-plane mTLS drill (mTLS block, final increment: every remaining hop).
#   - a 3-node Raft control plane runs its inter-node RPC over mutual TLS:
#     leader election, replication, and failover all encrypted
#   - the CP client port is mutual TLS too: admin commands and the proxy's
#     CPWATCH subscription present the mesh cert
#   - the proxy joins the CP over mTLS, discovers the (mTLS) backend pair,
#     and serves a tenant end to end — every internal hop encrypted
#   - kill the CP leader -> a new leader is elected over mTLS RPC; admin
#     writes continue; the data path never notices
#   - the controller speaks mTLS to the nodes: kill the backend master and
#     it promotes the replica through encrypted probes/commands
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-ctls 6795 6796 7541 7542 7543 7551 7552 7553 7790
fleet_guard
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
CTL=./target/release/flint-controller
D=$FLINT_DRILL_ROOT/flint-ctls; rm -rf "$D"; mkdir -p "$D"
fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane; sleep 0.4
cleanup() {
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane
  rm -rf "$D"
}
trap cleanup EXIT

echo "== mint internal CA + cert"
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$D/ca.key" -out "$D/ca.crt" \
  -days 1 -subj "/CN=flint-internal-ca" -addext "basicConstraints=critical,CA:TRUE" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$D/int.key" -out "$D/int.csr" \
  -subj "/CN=flint-internal" >/dev/null 2>&1
openssl x509 -req -in "$D/int.csr" -CA "$D/ca.crt" -CAkey "$D/ca.key" -CAcreateserial \
  -out "$D/int.crt" -days 1 \
  -extfile <(printf "subjectAltName=DNS:flint-internal\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE") \
  >/dev/null 2>&1
[ -s "$D/int.crt" ] || { echo "FAIL: cert generation"; exit 1; }
INT="--internal-ca $D/ca.crt --internal-cert $D/int.crt --internal-key $D/int.key"
echo "  minted"

# Mutual-TLS RESP helper with LEADER-redirect: send one command to a CP/node
# port; follow "-ERR LEADER 127.0.0.1:<port>" redirects to the leader.
resp() {  # $1=port $2=timeout-s, rest = command words
  python3 - "$@" <<'PY'
import re, socket, ssl, sys, os
port, tmo, words = int(sys.argv[1]), float(sys.argv[2]), sys.argv[3:]
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
ctx.load_cert_chain(os.environ.get("FLINT_DRILL_ROOT","/tmp")+"/flint-ctls/int.crt", os.environ.get("FLINT_DRILL_ROOT","/tmp")+"/flint-ctls/int.key")
frame = f"*{len(words)}\r\n".encode() + b"".join(
    f"${len(w)}\r\n{w}\r\n".encode() for w in words)
for _ in range(8):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=3) as raw:
            with ctx.wrap_socket(raw, server_hostname="flint-internal") as s:
                s.sendall(frame)
                s.settimeout(tmo)
                data = s.recv(65536)
    except (OSError, ssl.SSLError):
        import time; time.sleep(0.4); continue
    m = re.search(rb"LEADER 127\.0\.0\.1:(\d+)", data)
    if m:
        port = int(m.group(1)); import time; time.sleep(0.3); continue
    sys.stdout.buffer.write(data)
    break
PY
}

echo "== 3-node Raft control plane, inter-node RPC over mutual TLS"
PEERS="1=127.0.0.1:7551,2=127.0.0.1:7552,3=127.0.0.1:7553"
CLIENTS="1=127.0.0.1:7541,2=127.0.0.1:7542,3=127.0.0.1:7543"
for id in 1 2 3; do
  $CP --raft --node-id $id --port 754$id --raft-port 755$id \
      --peers "$PEERS" --client-addrs "$CLIENTS" --state "$D/n$id" $INT \
      >"$D/n$id.log" 2>&1 &
done
LEADER=""
for i in $(seq 1 40); do
  L=$(resp 7541 2 CPINFO | tr '\r' '\n' | grep "^leader:" | cut -d: -f2)
  [ -n "$L" ] && [ "$L" != "none" ] && { LEADER=$L; break; }
  sleep 0.5
done
[ -n "$LEADER" ] || { echo "FAIL: no leader over mTLS RPC"; tail -4 "$D"/n*.log; exit 1; }
echo "  leader elected over encrypted Raft RPC: node $LEADER"

echo "== admin over mTLS: register fleet + tenant"
resp 7541 3 CPADDPROXY 127.0.0.1:7790 >/dev/null
resp 7541 3 CPADDPAIR 127.0.0.1:6795,127.0.0.1:6796 >/dev/null
R=$(resp 7541 3 CPADDTENANT acme tok-acme acme 1)
echo "$R" | grep -q "subset" || { echo "FAIL: tenant add over mTLS: $R"; exit 1; }
echo "  fleet + tenant registered through the encrypted client port"

echo "== mTLS data pair + proxy (CPWATCH over mTLS, backends over mTLS)"
$B --port 6795 --engine rocks --data-dir "$D/m" $INT 2>"$D/m.log" &
fleet_wait_listen 6795
sleep 0.8
$B --port 6796 --engine rocks --data-dir "$D/r" --replica-of 127.0.0.1:6795 $INT 2>"$D/r.log" &
# Point the proxy at a NON-leader CP node so the leader-kill below never
# touches the proxy's watch connection.
WATCH_NODE=$(( LEADER % 3 + 1 ))
$PX --port 7790 --control-plane 127.0.0.1:754$WATCH_NODE --advertise 127.0.0.1:7790 $INT 2>"$D/px.log" &
fleet_wait_listen 7790
sleep 2
a() { valkey-cli -p 7790 -a tok-acme --no-auth-warning "$@"; }
[ "$(a SET ek hello)" = "OK" ] || { echo "FAIL: tenant write through full mesh"; tail -3 "$D/px.log"; exit 1; }
[ "$(a GET ek)" = "hello" ] || { echo "FAIL: tenant read"; exit 1; }
echo "  tenant served: client -> proxy -> (mTLS) backend, topology via (mTLS) CPWATCH"

echo "== kill the CP LEADER: new leader elected over mTLS RPC, writes continue"
pkill -9 -f "flint-controlplane --raft --node-id $LEADER "
NEW=""
for i in $(seq 1 40); do
  for p in 7541 7542 7543; do
    [ "$p" = "754$LEADER" ] && continue
    L=$(resp $p 2 CPINFO | tr '\r' '\n' | grep "^leader:" | cut -d: -f2)
    [ -n "$L" ] && [ "$L" != "none" ] && [ "$L" != "$LEADER" ] && { NEW=$L; break 2; }
  done
  sleep 0.5
done
[ -n "$NEW" ] || { echo "FAIL: no new leader after killing $LEADER"; exit 1; }
SURV=$([ "$LEADER" = "1" ] && echo 7542 || echo 7541)
R=$(resp $SURV 3 CPADDTENANT globex tok-glx globex 1)
echo "$R" | grep -q "subset" || { echo "FAIL: post-failover admin write: $R"; exit 1; }
[ "$(a GET ek)" = "hello" ] || { echo "FAIL: data path disturbed by CP failover"; exit 1; }
echo "  node $NEW leads; admin writes continue; data path untouched"

echo "== controller over mTLS: kill the backend master -> replica promoted"
$CTL --nodes 127.0.0.1:6795,127.0.0.1:6796 --id ctl --poll-ms 150 --confirm 3 $INT 2>"$D/ctl.log" &
sleep 1.5
pkill -9 -f "flint-server --port 6795"
PROM=0
for i in $(seq 1 40); do
  resp 6796 2 FLINTINFO | grep -q "role:master" && { PROM=1; break; }
  sleep 0.5
done
[ "$PROM" = "1" ] || { echo "FAIL: controller did not promote over mTLS"; tail -5 "$D/ctl.log"; exit 1; }
G=""
for i in $(seq 1 30); do
  G=$(a GET ek); [ "$G" = "hello" ] && break; sleep 0.5
done
[ "$G" = "hello" ] || { echo "FAIL: proxy did not chase the promoted master (got: $G)"; exit 1; }
echo "  replica promoted via encrypted controller probes; proxy chased; data intact"

echo "PASS: full mesh mTLS — Raft RPC, CP client port, CPWATCH, controller probes, backends, replication: every internal hop encrypted"
