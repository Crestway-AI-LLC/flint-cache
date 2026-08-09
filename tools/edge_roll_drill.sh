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
fleet_init /tmp/flint-edgeroll-state 7970 7971 7972 7973
fleet_guard
D=/tmp/flint-edgeroll; STATE=/tmp/flint-edgeroll-state
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

echo "PASS: a client-TLS fleet rolls to completion and every seat REPORTS the build — the branch no other drill executes"
