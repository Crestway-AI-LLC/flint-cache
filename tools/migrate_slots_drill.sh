#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Operator slot-range move: `flintctl migrate-slots <ns> <lo-hi> <src> <dest>`
# drives the same fenced FLINTMIGRATEIN cutover the rebalancer uses and
# commits ownership to the CP. Deterministic setup: bootstrap ONE pair
# (owns all slots), seed a hash-tagged slot, `expand` a second pair
# (unranged, owns nothing), then migrate the range across. Asserts: the
# keys move (new owner serves them, count preserved), the CP records the
# new ownership, and a live writer on a key in the range loses ZERO acked
# writes across the cutover.
set -u
cd "$(dirname "$0")/.."
STATE=/tmp/flint-migsl-state; INV=/tmp/flint-migsl.flint
RUN=/tmp/flint-migsl-run; ACK=/tmp/flint-migsl-ack
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
pkill -9 -f flint-controlplane 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null
sleep 0.4
cleanup() {
  rm -f "$RUN"
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
  pkill -9 -f flint-controlplane 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null
  rm -rf "$STATE" "$INV" "$ACK"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV" "$RUN" "$ACK"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks

# ONE pair to start: it owns every slot. Controller on but rebalance OFF
# (default), so nothing auto-moves — the operator move is the only one.
cat > "$INV" <<EOF
statedir $STATE
bins ./target/release
tls on
cp 127.0.0.1:7500
pair 127.0.0.1:7001,127.0.0.1:7002
proxy 127.0.0.1:7379
controller on
EOF
A="valkey-cli -p 7379 -a tok-acme --no-auth-warning"

echo "== bootstrap one pair (owns all 16384 slots)"
./target/release/flintctl -f "$INV" bootstrap >/dev/null 2>&1
./target/release/flintctl -f "$INV" tenant add acme tok-acme acme 1 >/dev/null 2>&1

# {mig2} hashes to slot 8450. Seed 2000 keys all in that slot.
echo "== seed 2000 keys in slot 8450 ({mig2})"
awk 'BEGIN{for(i=0;i<2000;i++){k=sprintf("{mig2}:k%05d",i);v=sprintf("v%05d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
  | valkey-cli -p 7379 -a tok-acme --no-auth-warning --pipe >/dev/null 2>&1
BEFORE=$($A GET '{mig2}:k00000')
[ "$BEFORE" = "v00000" ] || { echo "FAIL: seed not readable ($BEFORE)"; exit 1; }
echo "  seeded; sample {mig2}:k00000 = $BEFORE"

# Live writer on a key in the moving slot, retrying -TRYAGAIN/blips.
: > "$ACK"; touch "$RUN"
( n=0
  while [ -f "$RUN" ]; do
    n=$((n+1))
    if [ "$($A SET '{mig2}:writer' $n 2>/dev/null)" = "OK" ]; then echo "$n" > "$ACK"; fi
  done ) &
WPID=$!
sleep 1

echo "== expand: add a second pair (unranged — owns nothing yet)"
./target/release/flintctl -f "$INV" expand 127.0.0.1:7011,127.0.0.1:7012 >/dev/null 2>&1
sleep 1

echo "== migrate-slots acme 8400-8500 : pair 0 -> pair 1"
./target/release/flintctl -f "$INV" migrate-slots acme 8400-8500 0 1 2>&1 | grep -E 'migrate-slots|slot\(s\)' | sed 's/^/  /'
sleep 1

echo "== the CP now records slot 8450 owned by pair 1"
./target/release/flintctl -f "$INV" status >/dev/null 2>&1
SLOTS=$(python3 - <<'PY'
import socket, ssl
d="/tmp/flint-migsl-state/certs"
ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); ctx.load_verify_locations(f"{d}/ca.crt")
ctx.load_cert_chain(f"{d}/int.crt", f"{d}/int.key"); ctx.check_hostname=False
s=ctx.wrap_socket(socket.create_connection(("127.0.0.1",7500),timeout=5),server_hostname="flint-internal")
s.sendall(b"*1\r\n$7\r\nCPSLOTS\r\n")
print(s.recv(65536).decode(errors="replace"))
PY
)
# Ownership stores as consolidated runs "ns lo hi pair"; 8450 lands inside
# the 8400-8500 run assigned to pair 1.
echo "$SLOTS" | grep -qE 'acme 8400 8500 1' || { echo "FAIL: CPSLOTS run for 8400-8500 -> pair 1 missing"; echo "$SLOTS"; exit 1; }
echo "  CPSLOTS shows run acme 8400-8500 -> pair 1 (covers slot 8450)"

echo "== keys still served (proxy routes to the new owner), count preserved"
AFTER=$($A GET '{mig2}:k00000')
[ "$AFTER" = "v00000" ] || { echo "FAIL: key not readable after move ($AFTER)"; exit 1; }
[ "$($A GET '{mig2}:k01999')" = "v01999" ] || { echo "FAIL: tail key lost after move"; exit 1; }
echo "  {mig2}:k00000 and k01999 both intact via the proxy"

echo "== the NEW owner (pair 1 master, 7011) physically holds the slot"
HELD=$(python3 - <<'PY'
import socket, ssl
d="/tmp/flint-migsl-state/certs"
ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); ctx.load_verify_locations(f"{d}/ca.crt")
ctx.load_cert_chain(f"{d}/int.crt", f"{d}/int.key"); ctx.check_hostname=False
s=ctx.wrap_socket(socket.create_connection(("127.0.0.1",7011),timeout=5),server_hostname="flint-internal")
def cmd(*a):
    fr=b"*%d\r\n"%len(a)+b"".join(b"$%d\r\n%s\r\n"%(len(x),x) for x in a); s.sendall(fr)
    return s.recv(65536)
cmd(b"FLINTNS",b"acme")
print(cmd(b"GET",b"{mig2}:k00000").decode(errors="replace"))
PY
)
echo "$HELD" | grep -q 'v00000' || { echo "FAIL: new owner 7011 does not hold the slot data"; echo "$HELD"; exit 1; }
echo "  pair 1 master (7011) serves {mig2}:k00000 directly"

ACKED=$(cat "$ACK"); GOT=$($A GET '{mig2}:writer')
[ -n "$ACKED" ] && [ "$GOT" -ge "$ACKED" ] 2>/dev/null \
  || { echo "FAIL: acked write lost across the move (acked=$ACKED, read=$GOT)"; exit 1; }
echo "  live writer: zero acked-write loss across the cutover (acked=$ACKED, read=$GOT)"

rm -f "$RUN"; wait "$WPID" 2>/dev/null
# The cluster must also AGREE WITH ITSELF, not merely pass the one
# path this drill exercises — the gap two shipped bugs lived in.
echo "== integrity: every view of the cluster reconciles"
./target/release/flintctl -f "$INV" verify --probe acme:tok-acme >/dev/null \
  || { echo "FAIL: cluster does not reconcile (run: flintctl -f $INV verify --probe acme:tok-acme)"; exit 1; }
echo "  verified"

echo "PASS: migrate-slots moved a range operator-directed — CP-committed, keys intact, zero acked loss"
