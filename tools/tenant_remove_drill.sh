#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Tenant removal lifecycle: `flintctl tenant remove <name>` revokes the
# tenant at the CP (next snapshot push drops the grant — new auths fail),
# retires its ns's slot-exception rows, and wipes the namespace's data on
# every pair master. Re-adding the same name afterward (the "re-apply"
# path) yields a working, EMPTY account. Another tenant is untouched.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-tdel-state 7201 7202 7579 7710
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-tdel-state; INV=$FLINT_DRILL_ROOT/flint-tdel.flint
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
cp 127.0.0.1:7710
pair 127.0.0.1:7201,127.0.0.1:7202
proxy 127.0.0.1:7579
EOF
A="valkey-cli -p 7579 -a tok-acme --no-auth-warning"
B="valkey-cli -p 7579 -a tok-beta --no-auth-warning"

echo "== bootstrap + two tenants + data in both"
./target/release/flintctl -f "$INV" bootstrap >/dev/null 2>&1
./target/release/flintctl -f "$INV" tenant add acme tok-acme acme 1 >/dev/null 2>&1
./target/release/flintctl -f "$INV" tenant add beta tok-beta beta 1 >/dev/null 2>&1
for i in $(seq 1 50); do $A SET a:$i v$i >/dev/null; $B SET b:$i v$i >/dev/null; done
[ "$($A DBSIZE)" = "50" ] || { echo "FAIL: acme seed"; exit 1; }
[ "$($B DBSIZE)" = "50" ] || { echo "FAIL: beta seed"; exit 1; }
echo "  acme=50 keys, beta=50 keys"

echo "== remove acme (revoke + wipe)"
./target/release/flintctl -f "$INV" tenant remove acme 2>&1 | sed 's/^/  /'
sleep 1

echo "== acme auth is dead (revocation pushed), beta untouched"
R=$($A PING 2>&1)
echo "$R" | grep -qi 'auth\|denied\|invalid' || { echo "FAIL: removed tenant still auths ($R)"; exit 1; }
[ "$($B DBSIZE)" = "50" ] || { echo "FAIL: beta data disturbed by acme removal"; exit 1; }
[ "$($B GET b:7)" = "v7" ] || { echo "FAIL: beta value wrong"; exit 1; }
echo "  acme rejected; beta still serves 50 keys"

echo "== the namespace's data is physically gone on the master"
GONE=$(python3 - <<'PY'
import socket, ssl, os
d=os.environ.get("FLINT_DRILL_ROOT","/tmp")+"/flint-tdel-state/certs"
ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); ctx.load_verify_locations(f"{d}/ca.crt")
ctx.load_cert_chain(f"{d}/int.crt", f"{d}/int.key"); ctx.check_hostname=False
s=ctx.wrap_socket(socket.create_connection(("127.0.0.1",7201),timeout=5),server_hostname="flint-internal")
def cmd(*a):
    s.sendall(b"*%d\r\n"%len(a)+b"".join(b"$%d\r\n%s\r\n"%(len(x),x) for x in a)); return s.recv(65536)
cmd(b"FLINTNS",b"acme")
print(cmd(b"DBSIZE").decode(errors="replace").strip())
PY
)
echo "$GONE" | grep -q ':0' || { echo "FAIL: acme rows remain on master ($GONE)"; exit 1; }
echo "  master DBSIZE(acme) = 0"

echo "== re-apply: same name gets a fresh, EMPTY, working account"
./target/release/flintctl -f "$INV" tenant add acme tok-acme2 acme 1 >/dev/null 2>&1
sleep 1
A2="valkey-cli -p 7579 -a tok-acme2 --no-auth-warning"
[ "$($A2 DBSIZE)" = "0" ] || { echo "FAIL: re-applied account not empty"; exit 1; }
[ "$($A2 SET fresh 1)" = "OK" ] || { echo "FAIL: re-applied account cannot write"; exit 1; }
[ "$($A2 GET fresh)" = "1" ] || { echo "FAIL: re-applied account cannot read"; exit 1; }
R=$($A PING 2>&1)  # the OLD token stays dead
echo "$R" | grep -qi 'auth\|denied\|invalid' || { echo "FAIL: old token re-animated ($R)"; exit 1; }
echo "  re-applied acme: empty, writable; old token stays dead"

echo "PASS: tenant remove — auth revoked, ns wiped, neighbors untouched, re-apply clean"
