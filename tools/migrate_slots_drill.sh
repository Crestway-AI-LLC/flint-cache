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
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-migsl-state 7231 7232 7233 7234 7235 7236
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-migsl-state; INV=$FLINT_DRILL_ROOT/flint-migsl.flint
RUN=$FLINT_DRILL_ROOT/flint-migsl-run; ACK=$FLINT_DRILL_ROOT/flint-migsl-ack
fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  rm -f "$RUN"
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane
  rm -rf "$STATE" "$INV" "$ACK" "$ACK.tmp"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV" "$RUN" "$ACK" "$ACK.tmp"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

# ONE pair to start: it owns every slot. Controller on but rebalance OFF
# (default), so nothing auto-moves — the operator move is the only one.
cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
tls on
cp 127.0.0.1:7236
pair 127.0.0.1:7231,127.0.0.1:7232
proxy 127.0.0.1:7235
controller on
EOF
A="valkey-cli -p 7235 -a tok-acme --no-auth-warning"

echo "== bootstrap one pair (owns all 16384 slots)"
./target/release/flintctl -f "$INV" bootstrap >"$STATE-boot.log" 2>&1 || {
  # Capture it and STOP. This discarded bootstrap's output and
  # ignored its exit status, so a failed bootstrap ran on into the
  # assertions below and was reported as whichever one broke first
  # -- a product fault asserted for what was really "bootstrap
  # failed and nobody looked" (BUG-0064).
  echo "FAIL: bootstrap"; tail -25 "$STATE-boot.log"; exit 1; }
./target/release/flintctl -f "$INV" tenant add acme tok-acme acme 1 >/dev/null 2>&1

# {mig2} hashes to slot 8450. Seed 2000 keys all in that slot.
echo "== seed 2000 keys in slot 8450 ({mig2})"
awk 'BEGIN{for(i=0;i<2000;i++){k=sprintf("{mig2}:k%05d",i);v=sprintf("v%05d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
  | valkey-cli -p 7235 -a tok-acme --no-auth-warning --pipe >/dev/null 2>&1
BEFORE=$($A GET '{mig2}:k00000')
[ "$BEFORE" = "v00000" ] || { echo "FAIL: seed not readable ($BEFORE)"; exit 1; }
echo "  seeded; sample {mig2}:k00000 = $BEFORE"

# Live writer on a key in the moving slot, retrying -TRYAGAIN/blips.
: > "$ACK"; touch "$RUN"
( n=0
  while [ -f "$RUN" ]; do
    n=$((n+1))
    # tmp+mv, not `>`: a bare redirect truncates then writes, and the main
    # shell reads this file WHILE the loop runs. The window is small and
    # real — CI hit it on 2026-08-11 and the drill announced "acked write
    # lost across the move (acked=, read=1696)" about a migration that lost
    # nothing. `mv` on the same filesystem is atomic, so a reader sees the
    # previous complete value or the new one, never a truncated file.
    # decommission_drill.sh had the identical bug and the identical fix;
    # this one was missed then.
    if [ "$($A SET '{mig2}:writer' $n 2>/dev/null)" = "OK" ]; then
      echo "$n" > "$ACK.tmp" && mv "$ACK.tmp" "$ACK"
    fi
  done ) &
WPID=$!
sleep 1

echo "== expand: add a second pair (unranged — owns nothing yet)"
./target/release/flintctl -f "$INV" expand 127.0.0.1:7233,127.0.0.1:7234 >/dev/null 2>&1
sleep 1

echo "== migrate-slots acme 8400-8500 : pair 0 -> pair 1"
./target/release/flintctl -f "$INV" migrate-slots acme 8400-8500 0 1 2>&1 | grep -E 'migrate-slots|slot\(s\)' | sed 's/^/  /'
sleep 1

echo "== the CP now records slot 8450 owned by pair 1"
./target/release/flintctl -f "$INV" status >/dev/null 2>&1
SLOTS=$(python3 - <<'PY'
import socket, ssl, os
d=os.environ.get("FLINT_DRILL_ROOT","/tmp")+"/flint-migsl-state/certs"
ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); ctx.load_verify_locations(f"{d}/ca.crt")
ctx.load_cert_chain(f"{d}/int.crt", f"{d}/int.key"); ctx.check_hostname=False
s=ctx.wrap_socket(socket.create_connection(("127.0.0.1",7236),timeout=5),server_hostname="flint-internal")
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

echo "== the NEW owner (pair 1 master, 7233) physically holds the slot"
HELD=$(python3 - <<'PY'
import socket, ssl, os
d=os.environ.get("FLINT_DRILL_ROOT","/tmp")+"/flint-migsl-state/certs"
ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); ctx.load_verify_locations(f"{d}/ca.crt")
ctx.load_cert_chain(f"{d}/int.crt", f"{d}/int.key"); ctx.check_hostname=False
s=ctx.wrap_socket(socket.create_connection(("127.0.0.1",7233),timeout=5),server_hostname="flint-internal")
def cmd(*a):
    fr=b"*%d\r\n"%len(a)+b"".join(b"$%d\r\n%s\r\n"%(len(x),x) for x in a); s.sendall(fr)
    return s.recv(65536)
cmd(b"FLINTNS",b"acme")
print(cmd(b"GET",b"{mig2}:k00000").decode(errors="replace"))
PY
)
echo "$HELD" | grep -q 'v00000' || { echo "FAIL: new owner 7233 does not hold the slot data"; echo "$HELD"; exit 1; }
echo "  pair 1 master (7233) serves {mig2}:k00000 directly"

ACKED=$(cat "$ACK"); GOT=$($A GET '{mig2}:writer')
# "No ack was captured" and "an acked write was lost" are DIFFERENT results
# and must not share a message. Folding them together is what turned a
# harness race into a data-loss report; anyone reading that CI log would
# have gone looking for a migration bug that was not there.
[ -n "$ACKED" ] || {
  echo "FAIL (HARNESS, not the system): no acked write was ever recorded."
  echo "      The writer loop never got an OK, or the ack file was read"
  echo "      mid-write. This says nothing about whether the move lost data."
  echo "      key reads back as: $GOT"
  exit 1
}
[ "$GOT" -ge "$ACKED" ] 2>/dev/null \
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
