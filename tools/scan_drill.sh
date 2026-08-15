#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Keyspace SCAN end to end, the way evaluation tools use it: valkey-cli
# --scan (a real client-driven SCAN cursor loop) through the PROXY against
# a TWO-pair cluster, so the proxy's one-cursor-stream-over-all-masters
# session logic is exercised, not just the node command. Asserts:
#   - full enumeration: every seeded key exactly once, across both masters;
#   - MATCH filtering via --pattern;
#   - tenant isolation: tenant B's scan sees none of tenant A's keys;
#   - a garbage cursor gets an honest error.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-scan-state 7301 7302 7303 7304 7679 7720
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-scan-state; INV=$FLINT_DRILL_ROOT/flint-scan.flint
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; fleet_kill controller
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; fleet_kill controller
  rm -rf "$STATE" "$INV"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks

cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
tls on
cp 127.0.0.1:7720
pair 127.0.0.1:7301,127.0.0.1:7302
pair 127.0.0.1:7303,127.0.0.1:7304
proxy 127.0.0.1:7679
EOF
A="valkey-cli -p 7679 -a tok-acme --no-auth-warning"
B="valkey-cli -p 7679 -a tok-beta --no-auth-warning"

echo "== bootstrap 2 pairs (slot space split) + two tenants + 500 keys"
./target/release/flintctl -f "$INV" bootstrap >/dev/null 2>&1
./target/release/flintctl -f "$INV" tenant add acme tok-acme acme 1 >/dev/null 2>&1
./target/release/flintctl -f "$INV" tenant add beta tok-beta beta 1 >/dev/null 2>&1
awk 'BEGIN{for(i=0;i<500;i++){k=sprintf("key:%04d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$2\r\nvv\r\n",length(k),k}}' \
  | valkey-cli -p 7679 -a tok-acme --no-auth-warning --pipe >/dev/null 2>&1
$B SET beta-only 1 >/dev/null
[ "$($A DBSIZE)" = "500" ] || { echo "FAIL: seed ($($A DBSIZE))"; exit 1; }

echo "== both masters hold a share (keys really span pairs)"
C1=$($A GET key:0000 >/dev/null; echo ok)  # proxy routing sanity
# distribution check via CPSLOTS-backed routing is implicit: DBSIZE is a
# fan-out sum (500) while each master's own count is < 500.
M1=$(python3 - <<'PY'
import socket, ssl, os
d=os.environ.get("FLINT_DRILL_ROOT","/tmp")+"/flint-scan-state/certs"
ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); ctx.load_verify_locations(f"{d}/ca.crt")
ctx.load_cert_chain(f"{d}/int.crt", f"{d}/int.key"); ctx.check_hostname=False
s=ctx.wrap_socket(socket.create_connection(("127.0.0.1",7301),timeout=5),server_hostname="flint-internal")
def cmd(*a):
    s.sendall(b"*%d\r\n"%len(a)+b"".join(b"$%d\r\n%s\r\n"%(len(x),x) for x in a)); return s.recv(65536)
cmd(b"FLINTNS",b"acme")
print(cmd(b"DBSIZE").decode(errors="replace").strip().lstrip(":"))
PY
)
[ "$M1" -gt 0 ] 2>/dev/null && [ "$M1" -lt 500 ] || { echo "FAIL: pair 0 share is $M1 (want 0<n<500)"; exit 1; }
echo "  pair-0 master holds $M1 of 500 — the rest live on pair 1"

echo "== valkey-cli --scan through the proxy enumerates ALL 500 exactly once"
SCANNED=$($A --scan | sort)
COUNT=$(echo "$SCANNED" | grep -c 'key:')
DUPES=$(echo "$SCANNED" | uniq -d | wc -l | tr -d ' ')
[ "$COUNT" = "500" ] || { echo "FAIL: scanned $COUNT of 500"; exit 1; }
[ "$DUPES" = "0" ] || { echo "FAIL: $DUPES duplicated keys"; exit 1; }
echo "$SCANNED" | grep -q '^key:0000$' && echo "$SCANNED" | grep -q '^key:0499$' \
  || { echo "FAIL: boundary keys missing"; exit 1; }
echo "  500/500 keys, zero duplicates"

echo "== MATCH filtering (--pattern) pages correctly"
N=$($A --scan --pattern 'key:04*' | wc -l | tr -d ' ')
[ "$N" = "100" ] || { echo "FAIL: pattern matched $N (want 100)"; exit 1; }
echo "  key:04* -> 100 keys"

echo "== tenant isolation: beta sees only its own key"
BS=$($B --scan | sort | tr '\n' ' ')
[ "$BS" = "beta-only " ] || { echo "FAIL: beta scan leaked ($BS)"; exit 1; }
echo "  beta scan = [beta-only]"

echo "== a never-issued cursor is an honest error"
E=$($A SCAN 424242424242 2>&1)
echo "$E" | grep -qi 'invalid cursor' || { echo "FAIL: bad cursor answered: $E"; exit 1; }
echo "  ERR invalid cursor"

# The cluster must also AGREE WITH ITSELF, not merely pass the one
# path this drill exercises — the gap two shipped bugs lived in.
echo "== integrity: every view of the cluster reconciles"
./target/release/flintctl -f "$INV" verify --probe acme:tok-acme >/dev/null \
  || { echo "FAIL: cluster does not reconcile (run: flintctl -f $INV verify --probe acme:tok-acme)"; exit 1; }
echo "  verified"

echo "PASS: SCAN through the proxy — full exactly-once enumeration across 2 pairs, MATCH, tenant isolation, honest cursors"
