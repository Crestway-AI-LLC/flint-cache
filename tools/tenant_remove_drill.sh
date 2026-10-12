#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Tenant removal lifecycle: `flintctl tenant remove <name>` revokes the
# tenant at the CP (next snapshot push drops the grant — new auths fail),
# retires its ns's slot-exception rows, and wipes the namespace's data on
# every pair master. Re-adding the same name afterward (the "re-apply"
# path) yields a working, EMPTY account. Another tenant is untouched.
#
# BUG-0260: a removal whose wipe fails leaves the keys behind with no tenant
# record naming them. `tenant add` must refuse that namespace until
# `tenant wipe <ns>` has finished the job, and a namespace two tenants share
# survives the removal of one.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-tdel-state 7201 7202 7579 7710 7203 7204
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-tdel-state; INV=$FLINT_DRILL_ROOT/flint-tdel.flint
# The same fleet through an inventory whose one pair is at 7203/7204, where
# nothing listens: the control plane answers, the pair does not. Declared to
# fleet_init so no other drill takes them.
DEAD=$FLINT_DRILL_ROOT/flint-tdel-dead.flint
fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane
  rm -rf "$STATE" "$INV" "$DEAD"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV" "$DEAD"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks || { echo "FAIL: build"; exit 1; }
fleet_warm ./target/release/flint-server ./target/release/flint-proxy ./target/release/flint-controlplane ./target/release/flint-controller

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
./target/release/flintctl -f "$INV" bootstrap >"$STATE-boot.log" 2>&1 || {
  # Capture it and STOP. This discarded bootstrap's output and
  # ignored its exit status, so a failed bootstrap ran on into the
  # assertions below and was reported as whichever one broke first
  # -- a product fault asserted for what was really "bootstrap
  # failed and nobody looked" (BUG-0064).
  echo "FAIL: bootstrap"; tail -25 "$STATE-boot.log"; exit 1; }
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
# DBSIZE of one namespace, asked of the pair master over the mesh.
master_dbsize() {
  NS=$1 python3 - <<'PY2'
import socket, ssl, os
d=os.environ.get("FLINT_DRILL_ROOT","/tmp")+"/flint-tdel-state/certs"
ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); ctx.load_verify_locations(f"{d}/ca.crt")
ctx.load_cert_chain(f"{d}/int.crt", f"{d}/int.key"); ctx.check_hostname=False
s=ctx.wrap_socket(socket.create_connection(("127.0.0.1",7201),timeout=5),server_hostname="flint-internal")
def cmd(*a):
    s.sendall(b"*%d\r\n"%len(a)+b"".join(b"$%d\r\n%s\r\n"%(len(x),x) for x in a)); return s.recv(65536)
cmd(b"FLINTNS",os.environ["NS"].encode())
print(cmd(b"DBSIZE").decode(errors="replace").strip())
PY2
}
GONE=$(master_dbsize acme)
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

CTL=./target/release/flintctl
echo "== BUG-0260: a removal whose wipe cannot reach the pair"
$CTL -f "$INV" tenant add gamma tok-gamma gamma 1 >/dev/null 2>&1
G="valkey-cli -p 7579 -a tok-gamma --no-auth-warning"
sleep 1
for i in $(seq 1 20); do $G SET g:$i v$i >/dev/null; done
[ "$($G DBSIZE)" = "20" ] || { echo "FAIL: gamma seed"; exit 1; }
sed 's/^pair .*/pair 127.0.0.1:7203,127.0.0.1:7204/' "$INV" > "$DEAD"
R=$($CTL -f "$DEAD" tenant remove gamma 2>&1); RC=$?
echo "$R" | sed 's/^/  /'
[ $RC = 0 ] || { echo "FAIL: a removal whose wipe failed exited $RC: the tenant IS removed"; exit 1; }
echo "$R" | grep -q 'WIPE FAILED' || { echo "FAIL: no WIPE FAILED line"; exit 1; }
echo "$R" | grep -q 'tenant wipe gamma' || { echo "FAIL: the remedy is not named"; exit 1; }
sleep 1
R=$($G PING 2>&1)
echo "$R" | grep -qi 'auth\|denied\|invalid' || { echo "FAIL: removed gamma still auths ($R)"; exit 1; }
GONE=$(master_dbsize gamma)
echo "$GONE" | grep -q ':20' || { echo "FAIL: expected gamma's 20 keys left behind ($GONE)"; exit 1; }
echo "  removed, auth dead, and the 20 keys are still on the master"

echo "== re-running the removal cannot finish it, and says what can"
R=$($CTL -f "$INV" tenant remove gamma 2>&1) && { echo "FAIL: re-run succeeded"; exit 1; }
echo "$R" | grep -q 'no such tenant' && echo "$R" | grep -q 'tenant wipe' \
  || { echo "FAIL: re-run did not name tenant wipe ($R)"; exit 1; }
echo "  $(echo "$R" | head -1 | cut -c1-110)..."

echo "== tenant add refuses the namespace while it holds keys, or cannot say"
R=$($CTL -f "$INV" tenant add gamma tok-gamma2 gamma 1 2>&1) && { echo "FAIL: add over 20 stale keys succeeded"; exit 1; }
echo "$R" | grep -q 'still holds 20 key(s)' || { echo "FAIL: refusal does not name the keys ($R)"; exit 1; }
R=$($CTL -f "$DEAD" tenant add gamma tok-gamma2 gamma 1 2>&1) && { echo "FAIL: add with the pair unreachable succeeded"; exit 1; }
echo "$R" | grep -q 'has no master to say' || { echo "FAIL: unreachable-pair refusal ($R)"; exit 1; }
sleep 1
R=$(valkey-cli -p 7579 -a tok-gamma2 --no-auth-warning GET g:1 2>&1)
[ "$R" != "v1" ] || { echo "FAIL: a refused add still let the new token read the old keys"; exit 1; }
echo "  refused both ways; tok-gamma2 reads nothing"

echo "== tenant wipe: refuses a namespace a tenant holds, finishes the removal"
R=$($CTL -f "$INV" tenant wipe beta 2>&1) && { echo "FAIL: wiped a live tenant's namespace"; exit 1; }
echo "$R" | grep -q "tenant beta's" || { echo "FAIL: live-namespace refusal ($R)"; exit 1; }
[ "$($B DBSIZE)" = "50" ] || { echo "FAIL: beta disturbed by a refused wipe"; exit 1; }
$CTL -f "$DEAD" tenant wipe gamma >/dev/null 2>&1 && { echo "FAIL: wipe with the pair unreachable exited 0"; exit 1; }
$CTL -f "$INV" tenant wipe gamma 2>&1 | grep -q '== ns gamma wiped' || { echo "FAIL: tenant wipe gamma"; exit 1; }
GONE=$(master_dbsize gamma)
echo "$GONE" | grep -q ':0' || { echo "FAIL: gamma rows remain after tenant wipe ($GONE)"; exit 1; }
$CTL -f "$INV" tenant add gamma tok-gamma2 gamma 1 >/dev/null 2>&1 || { echo "FAIL: add after the wipe"; exit 1; }
sleep 1
G2="valkey-cli -p 7579 -a tok-gamma2 --no-auth-warning"
[ "$($G2 DBSIZE)" = "0" ] || { echo "FAIL: gamma re-added but not empty"; exit 1; }
echo "  beta refused and untouched; gamma wiped, then re-added empty"

echo "== a namespace two tenants share outlives the removal of one"
$CTL -f "$INV" tenant add delta tok-delta beta 1 >/dev/null 2>&1 || { echo "FAIL: add delta on beta's namespace"; exit 1; }
sleep 1
[ "$(valkey-cli -p 7579 -a tok-delta --no-auth-warning DBSIZE)" = "50" ] || { echo "FAIL: delta does not share beta's keys"; exit 1; }
R=$($CTL -f "$INV" tenant remove delta 2>&1) || { echo "FAIL: remove delta ($R)"; exit 1; }
echo "$R" | grep -q 'ns beta kept: tenant beta shares it' || { echo "FAIL: shared ns not kept ($R)"; exit 1; }
[ "$($B DBSIZE)" = "50" ] || { echo "FAIL: removing delta wiped beta's keys"; exit 1; }
echo "  delta removed; beta keeps its 50 keys"

echo "PASS: tenant remove — auth revoked, ns wiped, neighbors untouched, re-apply clean; a failed wipe is refused by tenant add and finished by tenant wipe"
