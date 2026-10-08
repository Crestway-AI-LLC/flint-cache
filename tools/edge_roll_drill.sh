#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A full `flintctl upgrade` on a fleet whose EDGE speaks TLS to clients.
#
# WHY THIS EXISTS. Every roll drill in this suite puts `tls on` in the
# inventory — mesh TLS — and leaves the client edge in plaintext. So the
# client-TLS branch of the roll path had never once been executed, and a
# defect sat in it that made `flintctl upgrade` UNABLE TO COMPLETE on the
# playground or on any production deployment:
#
#     fn proxystats_field(inv, i, field) -> Option<String> {
#         if inv.client_tls { return None; }        // gives up without asking
#
# `roll_edge` treats "the proxy would not report a build" as fatal, so the
# rc.47 roll on 2026-08-09 rolled all six seats and then aborted with exit 3
# — while the proxy was serving and answering PROXYSTATS over its edge with
# the new build. The roll had worked; only the report of it had not.
#
# That is the same shape as #102 (verify --probe could not probe a
# client-TLS edge) and as rc.29 (the roll worked, the build column lied).
# Three times now the ROLL has been right and the READING of it wrong, which
# is why this drill asserts the reading and not just the outcome.
#
# WHAT IT PROVES
#   - `upgrade` EXITS ZERO on a client-TLS fleet (the abort is the bug)
#   - `status` reports the proxy's build, not `-`, when the edge is TLS
#   - the pair nodes and cp report it too, so a green result is not one
#     surface accidentally agreeing with itself
#   - BUG-0217: on a fleet with no co-processor leaf, the upgrade that adds
#     a co-processor mints the leaf alone and changes no other cert file.
#   - ops ADR-0050 D4: a `coproc` line added after bootstrap is started by
#     the next upgrade, the rolled proxy routes VEC. to it, and a second
#     upgrade replaces the running co-processor with the new build
#   - BUG-0205: "routes" means a tenant's VEC.* answers through the edge,
#     not that the proxy's argv names the family (the control plane's table
#     replaces it); removing the line and rolling clears the family
#
# The edge cert is minted by `bootstrap` and signed by the fleet's own
# internal CA, and flintctl's edge trust defaults to that CA — so this needs
# no external certificate and no DNS.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-edgeroll-state 7970 7971 7972 7973 7974
fleet_guard
D=$FLINT_DRILL_ROOT/flint-edgeroll; STATE=$FLINT_DRILL_ROOT/flint-edgeroll-state
INV=$D/cluster.flint
TAG=edge-roll-9
rm -rf "$D" "$STATE"; mkdir -p "$D"

fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane; fleet_kill vec
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop >/dev/null 2>&1
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane; fleet_kill vec
  [ -n "${KEEP:-}" ] || rm -rf "$D" "$STATE"
}
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl -p flint-vec --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

# client-tls on is the whole point. 127.0.0.1 as an edge-san so the edge
# cert matches the address flintctl dials.
cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
tls on
client-tls on
edge-san 127.0.0.1
cp 127.0.0.1:7973
pair 127.0.0.1:7970,127.0.0.1:7971
proxy 127.0.0.1:7972
controller on
EOF
CTL="./target/release/flintctl -f $INV"

echo "== bootstrap (edge cert minted and signed by the fleet's own CA)"
$CTL bootstrap >"$D/boot.log" 2>&1 || { echo "FAIL: bootstrap"; tail -8 "$D/boot.log"; exit 1; }
[ -s "$STATE/certs/edge.crt" ] || { echo "FAIL: no edge cert was minted — this fleet is not client-TLS"; exit 1; }

# POSITIVE CONTROL ON THE TEST ITSELF: if the fleet already reported $TAG,
# every assertion below would pass without the upgrade doing anything.
BEFORE=$($CTL status 2>/dev/null | grep -c "build $TAG" || true)
[ "${BEFORE:-0}" -eq 0 ] \
  || { echo "FAIL: $BEFORE seat(s) already on '$TAG' before the roll — vacuous"; exit 1; }

# ops ADR-0050 D4: a `coproc` line added to a running fleet is started by the
# next upgrade, and the proxies rolled after it route to it. Nothing has
# started this seat: bootstrap ran before the line existed.
#
# BUG-0217: and this fleet stands in for one bootstrapped before the
# co-processor leaf existed (public 2973016, 2026-08-11), which has no
# coproc.crt. The upgrade must mint that leaf alone: the CA, mesh and edge
# files come through it byte for byte. Copies, compared with cmp, because
# shasum is not on the gate box.
rm -f "$STATE/certs/coproc.crt" "$STATE/certs/coproc.key"
mkdir -p "$D/certs-before"
for f in ca.crt ca.key int.crt int.key edge.crt edge.key; do
  cp -p "$STATE/certs/$f" "$D/certs-before/$f" || { echo "FAIL: no $f to compare after the roll"; exit 1; }
done
echo "coproc VEC. 127.0.0.1:7974" >> "$INV"
if (exec 3<>/dev/tcp/127.0.0.1/7974) 2>/dev/null; then
  echo "FAIL: something already listens on 7974 before the upgrade — starting the co-processor would be vacuous"; exit 1
fi
# coproc_info <field>: the co-processor's FLINTINFO over the mesh, as the
# roll and the exporter ask it.
coproc_info() {
  valkey-cli --tls --cacert "$STATE/certs/ca.crt" --cert "$STATE/certs/int.crt" \
    --key "$STATE/certs/int.key" --sni flint-internal -p 7974 FLINTINFO 2>/dev/null \
    | tr -d '\r' | sed -n "s/^$1://p"
}

echo "== upgrade --version-tag $TAG"
# Exit status IS an assertion here. The bug this drill exists for did not
# corrupt anything; it made a successful roll report failure, and a drill
# that only checked the seats afterwards would have called that a pass.
$CTL upgrade --version-tag "$TAG" --soak-ms 1500 >"$D/upgrade.log" 2>&1
RC=$?
if [ "$RC" -ne 0 ]; then
  tail -12 "$D/upgrade.log" | sed 's/^/  | /'
  echo "FAIL: upgrade exited $RC on a client-TLS fleet."
  echo "      If it aborted rolling the proxy for 'would not report a build',"
  echo "      flintctl is refusing to speak TLS to the edge it just rolled"
  echo "      (proxystats_field / edge_tls_client)."
  exit 1
fi

echo "== every seat reports the build THROUGH the TLS edge, not '-'"
ST=$($CTL status 2>&1)
echo "$ST" | sed 's/^/  | /'
PXS=$(echo "$ST" | grep -c "^proxy .*build $TAG" || true)
PAIRS=$(echo "$ST" | grep -c "^pair .*build $TAG" || true)
CPS=$(echo "$ST"  | grep -c "^cp .*build $TAG" || true)
# The proxy row is the one that regressed; the others are here so a green
# result cannot be one surface agreeing with itself.
[ "${PXS:-0}" -ge 1 ] || {
  echo "FAIL: the proxy row does not carry build $TAG."
  echo "      'build -' means PROXYSTATS was never asked over the edge."
  exit 1; }
[ "${PAIRS:-0}" -eq 2 ] && [ "${CPS:-0}" -ge 1 ] || {
  echo "FAIL: pair $PAIRS/2, cp $CPS on $TAG — the roll itself is wrong, not just the report"; exit 1; }
echo "  proxy $PXS, pair $PAIRS/2, cp $CPS — all on $TAG over a TLS edge"

echo "== the upgrade started the co-processor the inventory gained, and the proxy routes to it"
grep -q "vec-7974 reports $TAG" "$D/upgrade.log" || { echo "FAIL: the upgrade log does not show vec-7974 on $TAG:"; grep -n "co-processor\|vec-" "$D/upgrade.log" | sed 's/^/  | /'; exit 1; }
[ "$(coproc_info build)" = "$TAG" ] || { echo "FAIL: vec-7974 answers FLINTINFO build '$(coproc_info build)', expected $TAG"; exit 1; }
PXARGS="$(ps -o args= -p "$(cat "$STATE/pids/proxy-7972.pid")" 2>/dev/null)"
case "$PXARGS" in *"VEC.=127.0.0.1:7974"*) : ;; *) echo "FAIL: the rolled proxy was not given the VEC. family: $PXARGS"; exit 1 ;; esac

echo "== BUG-0217: the upgrade minted the missing co-processor leaf, and only it"
[ -s "$STATE/certs/coproc.crt" ] && [ -s "$STATE/certs/coproc.key" ] \
  || { echo "FAIL: no co-processor leaf after the upgrade"; exit 1; }
grep -q 'minted the co-processor leaf' "$D/upgrade.log" \
  || { echo "FAIL: the upgrade log does not say it minted the leaf:"; grep -n 'leaf\|cert' "$D/upgrade.log" | sed 's/^/  | /'; exit 1; }
for f in ca.crt ca.key int.crt int.key edge.crt edge.key; do
  cmp -s "$D/certs-before/$f" "$STATE/certs/$f" || { echo "FAIL: the upgrade changed $f; it may mint the co-processor leaf only"; exit 1; }
done
# -text, not -ext: macOS's LibreSSL has no -ext, and printed nothing.
EKU=$(openssl x509 -in "$STATE/certs/coproc.crt" -noout -text 2>/dev/null | grep -A1 'Extended Key Usage' | tail -1)
case "$EKU" in
  *"Client Authentication"*) echo "FAIL: the minted co-processor leaf carries clientAuth: $EKU"; exit 1 ;;
  *"Server Authentication"*) : ;;
  *) echo "FAIL: the minted co-processor leaf has no serverAuth EKU: $EKU"; exit 1 ;;
esac
echo "  coproc.crt minted serverAuth-only; ca, int and edge files unchanged"
# BUG-0205: the argv is not the route table. The control plane's snapshot
# replaces a proxy's --families, and only bootstrap registered families with
# it, so the argv above was right while VEC.* answered "unknown command".
# Ask the edge itself, as a tenant.
grep -q "co-processor families on the control plane: VEC.=127.0.0.1:7974" "$D/upgrade.log" \
  || { echo "FAIL: the upgrade did not register VEC. with the control plane:"; grep -n "famil" "$D/upgrade.log" | sed 's/^/  | /'; exit 1; }
$CTL tenant add er tok-er er 1 >/dev/null 2>&1 || { echo "FAIL: tenant add"; exit 1; }
# vec <args>: one command through the TLS edge as tenant er, retrying the
# -LOADING a namespace answers while the co-processor rebuilds it.
vec() {
  local out i
  for i in $(seq 1 40); do
    out=$(valkey-cli -p 7972 --tls --cacert "$STATE/certs/ca.crt" -a tok-er --no-auth-warning "$@" 2>&1)
    case "$out" in *LOADING*) sleep 0.25 ;; *) break ;; esac
  done
  printf '%s' "$out"
}
[ "$(vec VEC.CREATE er DIM 3 METRIC l2)" = "OK" ] || { echo "FAIL: VEC.CREATE through the edge after the upgrade: $(vec VEC.CREATE er2 DIM 3 METRIC l2)"; exit 1; }
[ "$(vec VEC.SET er a 1,0,0)" = "OK" ] || { echo "FAIL: VEC.SET through the edge"; exit 1; }
[ "$(vec VEC.SEARCH er 1,0,0 1 | head -1)" = "a" ] || { echo "FAIL: VEC.SEARCH through the edge: $(vec VEC.SEARCH er 1,0,0 1)"; exit 1; }
echo "  vec-7974 started on $TAG; the proxy routes VEC. to it: a tenant's VEC.CREATE, SET and SEARCH answer through the edge"

echo "== a second upgrade rolls the running co-processor onto the new build"
TAG2=edge-roll-10
VPID="$(cat "$STATE/pids/vec-7974.pid" 2>/dev/null)"
kill -0 "$VPID" 2>/dev/null || { echo "FAIL: no live pid for vec-7974 ($VPID)"; exit 1; }
$CTL upgrade --version-tag "$TAG2" --soak-ms 1500 >"$D/upgrade2.log" 2>&1 \
  || { tail -12 "$D/upgrade2.log" | sed 's/^/  | /'; echo "FAIL: the second upgrade exited $?"; exit 1; }
kill -0 "$VPID" 2>/dev/null && { echo "FAIL: vec-7974's old process $VPID survived the roll"; exit 1; }
[ "$(coproc_info build)" = "$TAG2" ] || { echo "FAIL: after the roll vec-7974 reports '$(coproc_info build)', expected $TAG2"; exit 1; }
echo "  vec-7974: old process gone, new one reports $TAG2"
[ "$(vec VEC.SEARCH er 1,0,0 1 | head -1)" = "a" ] || { echo "FAIL: after the second roll VEC.SEARCH answers: $(vec VEC.SEARCH er 1,0,0 1)"; exit 1; }
echo "  the set survives the roll: rebuilt from durable rows, still routed"

echo "== BUG-0227: start, as flint-supervise runs it every minute, leaves the running co-processor be"
# `start` found a co-processor by its bare seat name, which is no argv token:
# a live one read as dead, `start` tried to spawn it, found the live pid and
# died, before the proxies and the controller. On the playground that failed
# flint-supervise every minute and paged.
VPID="$(cat "$STATE/pids/vec-7974.pid" 2>/dev/null)"
for n in 1 2; do
  $CTL start >"$D/start-coproc-$n.log" 2>&1 || {
    tail -6 "$D/start-coproc-$n.log" | sed 's/^/  | /'
    echo "FAIL: start (run $n) exited non-zero with the co-processor running"; exit 1; }
  grep -q "vec-7974 already up" "$D/start-coproc-$n.log" || {
    grep -n "vec-" "$D/start-coproc-$n.log" | sed 's/^/  | /'
    echo "FAIL: start (run $n) did not find the running co-processor"; exit 1; }
done
[ "$(cat "$STATE/pids/vec-7974.pid" 2>/dev/null)" = "$VPID" ] && kill -0 "$VPID" 2>/dev/null \
  || { echo "FAIL: start replaced or lost vec-7974 ($VPID)"; exit 1; }
echo "  two starts exit 0, each says vec-7974 is already up, and its process is untouched"

echo "== BUG-0205: a coproc line removed and rolled clears the family"
grep -v '^coproc VEC\. 127\.0\.0\.1:7974$' "$INV" > "$INV.new" && mv "$INV.new" "$INV"
TAG3=edge-roll-11
$CTL upgrade --version-tag "$TAG3" --soak-ms 1500 >"$D/upgrade3.log" 2>&1 \
  || { tail -12 "$D/upgrade3.log" | sed 's/^/  | /'; echo "FAIL: the third upgrade exited $?"; exit 1; }
grep -q "co-processor families on the control plane: VEC. cleared" "$D/upgrade3.log" \
  || { echo "FAIL: removing the coproc line did not clear VEC. on the control plane:"; grep -n "famil" "$D/upgrade3.log" | sed 's/^/  | /'; exit 1; }
# The undeclared seat is not the upgrade's to stop (it no longer knows it);
# stop it as an operator does, so nothing could answer if a route remained.
fleet_kill vec
case "$(vec VEC.CREATE gone DIM 3 METRIC l2)" in
  *"unknown command"*) echo "  VEC.* is unknown again: no proxy routes to a co-processor that is gone" ;;
  *) echo "FAIL: after removing the line VEC.CREATE answered: $(vec VEC.CREATE gone DIM 3 METRIC l2)"; exit 1 ;;
esac

echo "== a bare TCP listener must NOT read as a serving proxy"
# THE POSITIVE CONTROL for proxy_up. Until today a client-TLS fleet's
# liveness check was `TcpStream::connect`, which proves only that something
# holds the port — an edge with an expired cert, a failed handshake, or a
# proxy wedged short of RESP all read as UP, and `roll_edge` would accept
# "served after the binary swap" from a proxy that never served.
#
# Asserting the healthy case cannot catch that: a real proxy passes either
# way. So take the proxy away and leave something that accepts the
# connection and says nothing. A TCP-only check calls that UP; a check that
# waits for a RESP reply calls it DOWN. Last assertion in the file, because
# it deliberately ends with no proxy running.
PXPID="$STATE/pids/proxy-7972.pid"
[ -r "$PXPID" ] && kill "$(cat "$PXPID")" 2>/dev/null
for _ in $(seq 1 40); do
  nc -z 127.0.0.1 7972 2>/dev/null || break
  sleep 0.25
done
python3 -c '
import socket
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", 7972)); s.listen(8)
while True:
    c, _ = s.accept()      # accept, then say nothing at all
' &
MUTE=$!
# disown so bash does not print "Terminated: 15" and its whole source when we
# kill it — a drill's output is read for its assertions, not its plumbing.
disown $MUTE 2>/dev/null || true
trap 'kill $MUTE 2>/dev/null; cleanup' EXIT
sleep 1
ROW=$($CTL status 2>&1 | grep "^proxy" | head -1)
kill $MUTE 2>/dev/null
case "$ROW" in
  *DOWN*) echo "  $(echo "$ROW" | tr -s ' ')" ;;
  *)      echo "  $ROW"
          echo "FAIL: a socket that accepts and never replies reads as a serving proxy."
          echo "      proxy_up is measuring the TCP layer, not whether the edge ANSWERS."
          exit 1 ;;
esac

echo "PASS: a client-TLS fleet rolls to completion and every seat REPORTS the build, liveness means ANSWERING rather than merely holding the port — the branch no other drill executes — and an upgrade starts, routes, rolls and (with its line removed) unroutes the fleet's co-processor, which start leaves be"
