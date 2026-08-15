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
#
# The edge cert is minted by `bootstrap` and signed by the fleet's own
# internal CA, and flintctl's edge trust defaults to that CA — so this needs
# no external certificate and no DNS.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-edgeroll-state 7970 7971 7972 7973
fleet_guard
D=$FLINT_DRILL_ROOT/flint-edgeroll; STATE=$FLINT_DRILL_ROOT/flint-edgeroll-state
INV=$D/cluster.flint
TAG=edge-roll-9
rm -rf "$D" "$STATE"; mkdir -p "$D"

fleet_kill server; fleet_kill proxy
fleet_kill controlplane; fleet_kill controller
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; fleet_kill controller
  [ -n "${KEEP:-}" ] || rm -rf "$D" "$STATE"
}
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
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

echo "PASS: a client-TLS fleet rolls to completion and every seat REPORTS the build, and liveness means ANSWERING rather than merely holding the port — the branch no other drill executes"
