#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# An edge cert signed by a CA THAT IS NOT THE FLEET'S OWN.
#
# WHY THIS EXISTS. Every client-TLS drill in this suite lets `bootstrap` mint
# the edge cert, which signs it with the fleet's internal CA — and flintctl's
# edge trust DEFAULTS to that same internal CA. So the two halves agree by
# construction, `edge-trust` is never set by any drill in either repository,
# and the entire non-internal-CA path has never been executed.
#
# That path is where a real deployment lives. The playground serves a public
# certificate on a DNS name; its CA is a public root, not `flint-internal-ca`.
# And the gap produced FIVE separate bugs in one day (2026-08-10), every one
# of them the same fact rediscovered:
#
#     the edge cert is not the internal cert, and every component that
#     dials the edge must be told SEPARATELY which CA to trust
#
#   * `verify --probe` dialled the edge in plaintext (#102)
#   * `proxy_up` read a serving proxy as DOWN
#   * `proxystats_field` gave up before asking, so the build column lied
#   * `roll_edge` aborted an upgrade that had already succeeded
#   * the ops box's inventory had no `edge-trust`, so flintctl called the
#     live playground proxy DOWN while it was serving customers
#
# Each was found in production, fixed in isolation, and none of the fixes
# generalised — because nothing exercised the shape they share.
#
# WHAT IT PROVES. With an edge cert signed by a FOREIGN CA:
#   1. without `edge-trust`, flintctl CANNOT read the edge  (the control)
#   2. with `edge-trust <bundle>`, every edge-dialling surface works:
#      liveness (proxy_up), the build column (proxystats_field), and the
#      data plane (verify --probe)
#
# Step 1 is not decoration. Without it a green result is unfalsifiable: if
# the foreign cert were somehow still trusted by the internal CA — or if the
# swap silently did not happen — every assertion in step 2 would pass while
# testing nothing at all. So this drill WATCHES THE CHECK FAIL before it
# trusts the check passing, and separately proves at the file level that the
# cert on disk really did change signers.
#
# NOT COVERED HERE: the agent's own `--edge-ca` dials. The agent is a
# fleet-repository binary, so that half is drilled there. flintctl's half —
# three of the five bugs above — is this repository's, and is what this
# file covers.
#
# Requires: a release build with --features rocks, openssl, valkey-cli.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-edgeca 7451 7452 7453 7454
fleet_guard
D=$FLINT_DRILL_ROOT/flint-edgeca
STATE=$D/state
CERTS=$STATE/certs
OUTER=$D/outer
INV=$D/cluster.flint
A=127.0.0.1:7451
B=127.0.0.1:7452
PROXY=127.0.0.1:7453
CP=127.0.0.1:7454

fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop >/dev/null 2>&1
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane
  [ -n "${KEEP:-}" ] || rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D" "$OUTER"

command -v openssl >/dev/null 2>&1 || { echo "SKIP: no openssl"; exit 0; }

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

# No `edge-trust` line yet — that is deliberate, and it is the control.
cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
tls on
client-tls on
edge-san 127.0.0.1
cp $CP
pair $A,$B
proxy $PROXY
controller on
EOF
CTL="./target/release/flintctl -f $INV"

echo "== bootstrap: the ordinary case, edge signed by the fleet's own CA"
$CTL bootstrap >"$D/boot.log" 2>&1 || { echo "FAIL: bootstrap"; tail -10 "$D/boot.log"; exit 1; }
$CTL tenant add acme tok-acme acme 1 >"$D/tenant.log" 2>&1 \
  || { echo "FAIL: tenant add"; tail -5 "$D/tenant.log"; exit 1; }

# BASELINE. Everything below distinguishes "flintctl cannot trust this cert"
# from "the fleet is broken", and it can only do that if the fleet is known
# good FIRST. Without this line a foreign-CA failure and a bootstrap failure
# are the same output.
OUT=$($CTL verify --probe acme:tok-acme 2>&1)
echo "$OUT" | grep -q "VERIFY OK" || {
  echo "FAIL: the fleet does not verify even with its OWN edge cert."
  echo "      Nothing below would mean anything; fix this first."
  echo "$OUT" | sed 's/^/  | /'; exit 1; }
echo "  fleet healthy on the internal-CA edge cert (VERIFY OK)"

echo "== re-signing the edge cert with a FOREIGN CA"
$CTL stop >/dev/null 2>&1
# A second, entirely unrelated CA — this is the public root a real
# deployment's edge cert chains to. Same openssl invocations `resign_leaves`
# uses, including the explicit -CAserial (LibreSSL writes .srl to the CURRENT
# directory otherwise).
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$OUTER/ca.key" -out "$OUTER/ca.crt" \
  -days 365 -subj /CN=some-public-ca -addext basicConstraints=critical,CA:TRUE 2>/dev/null \
  || { echo "FAIL: could not mint the foreign CA"; exit 1; }
openssl req -newkey rsa:2048 -nodes -keyout "$OUTER/edge.key" -out "$OUTER/edge.csr" \
  -subj /CN=flint-edge 2>/dev/null || { echo "FAIL: edge csr"; exit 1; }
printf 'subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE' \
  > "$OUTER/edge-ext.cnf"
openssl x509 -req -in "$OUTER/edge.csr" -CA "$OUTER/ca.crt" -CAkey "$OUTER/ca.key" \
  -CAcreateserial -CAserial "$OUTER/ca.srl" \
  -out "$OUTER/edge.crt" -days 365 -extfile "$OUTER/edge-ext.cnf" 2>/dev/null \
  || { echo "FAIL: could not sign the foreign edge cert"; exit 1; }
cp "$OUTER/edge.crt" "$CERTS/edge.crt"
cp "$OUTER/edge.key" "$CERTS/edge.key"

# MUTATION CONTROL. Prove the file on disk actually changed signers before
# reading any behaviour off it. A silently-failed copy or a cert that still
# chains to the internal CA would make the whole drill vacuous, and it would
# look exactly like a pass.
openssl verify -CAfile "$CERTS/ca.crt" "$CERTS/edge.crt" >/dev/null 2>&1 && {
  echo "FAIL: the edge cert STILL verifies against the fleet's internal CA."
  echo "      The swap did not take, so this drill would prove nothing."
  exit 1; }
openssl verify -CAfile "$OUTER/ca.crt" "$CERTS/edge.crt" >/dev/null 2>&1 || {
  echo "FAIL: the edge cert does not verify against the foreign CA either —"
  echo "      the signing step is broken, not the fleet."
  exit 1; }
echo "  edge cert now chains to some-public-ca, and NOT to the internal CA"

echo "== THE CONTROL: with no edge-trust, bring-up itself must FAIL"
# `start`, not `bootstrap` — bootstrap would re-mint the cert we just
# replaced. It is expected to FAIL here, and the failure is the point.
#
# flintctl's trust still defaults to the internal CA, which no longer signs
# the cert the proxy serves. `start` spawns every seat and then waits on
# proxy_up, which cannot complete a handshake it cannot validate. So a fleet
# with a PUBLIC edge certificate — which is every real deployment — cannot
# be brought up at all until `edge-trust` is declared.
$CTL start >"$D/start-untrusted.log" 2>&1 && {
  echo "FAIL: start SUCCEEDED against an edge cert signed by an untrusted CA."
  echo "      Either the chain is not being validated or the swap did not reach"
  echo "      the running proxy. Everything after this would be vacuous."
  exit 1; }
echo "  start fails, as it must. What the operator sees:"
sed -n '/never answered PROXYSTATS/,$p' "$D/start-untrusted.log" | head -7 | sed 's/^/    | /'

# THE MESSAGE IS PART OF THE CONTRACT. This failure used to read
# `did not come up (port busy?)`, which sends an operator to `lsof` while a
# healthy proxy serves beside them — the reason the ops-box outage took as
# long to diagnose as it did. A wrong diagnostic is not cosmetic when it is
# the only thing standing between an operator and a one-line fix, so the
# drill asserts the text names the trust anchor it actually used.
grep -q "never answered PROXYSTATS" "$D/start-untrusted.log" || {
  echo "FAIL: the bring-up failure does not say the proxy never ANSWERED."
  echo "      If it blames the port again, proxy_down_help was bypassed."
  exit 1; }
grep -q "$CERTS/ca.crt" "$D/start-untrusted.log" || {
  echo "FAIL: the failure does not name the trust anchor flintctl used."
  echo "      Naming it is the entire diagnostic: an operator who sees the"
  echo "      INTERNAL CA here, on a fleet with a public edge cert, is done."
  exit 1; }
grep -q "edge-trust" "$D/start-untrusted.log" || {
  echo "FAIL: the failure does not mention the inventory line that fixes it."
  exit 1; }
echo "  and it names the trust anchor it used, plus the line that fixes it"
fleet_wait_listen 7453 || exit 1

echo "== the seats ARE up — it is flintctl that cannot read the edge"
ST=$($CTL status 2>&1)
PLINE=$(echo "$ST" | grep "^proxy" | head -1)
case "$PLINE" in
  *DOWN*) echo "  proxy reads DOWN, as it must: $(echo "$PLINE" | tr -s ' ')" ;;
  *) echo "  $PLINE"
     echo "FAIL: flintctl reads a FOREIGN-CA edge as healthy with no edge-trust set."
     echo "      Either the chain is not being validated, or the swap did not"
     echo "      reach the running proxy. Everything after this would be vacuous."
     exit 1 ;;
esac
OUT=$($CTL verify --probe acme:tok-acme 2>&1)
echo "$OUT" | grep -q "VERIFY OK" && {
  echo "FAIL: verify --probe passed against an edge cert signed by an untrusted CA."
  echo "$OUT" | sed 's/^/  | /'; exit 1; }
echo "  and the probe fails:"
# Anchored to verify's own FAIL rows. A loose keyword grep here matched
# section headings and prose from the surrounding report, which reads like
# evidence and is not.
echo "$OUT" | grep -E "^[[:space:]]*FAIL" | head -3 | sed 's/^/    | /'

echo "== declaring edge-trust <foreign bundle> — ONE inventory line"
printf 'edge-trust %s\n' "$OUTER/ca.crt" >> "$INV"

echo "== the same start now completes"
# Same command, same fleet, same certificate — one line of inventory
# different. That pairing is what makes the control above a control rather
# than an anecdote about a broken fleet.
$CTL start >"$D/start-trusted.log" 2>&1 || {
  echo "FAIL: start still fails with edge-trust declared."
  tail -10 "$D/start-trusted.log" | sed 's/^/  | /'
  echo "      proxy_up / edge_tls_client is ignoring the inventory's edge-trust."
  exit 1; }
echo "  start exits 0"

echo "== every edge-dialling surface now works"
# All three of the surfaces that broke separately, asserted together —
# because fixing them one at a time is exactly what happened, and is why
# this file exists.
ST=$($CTL status 2>&1)
echo "$ST" | sed 's/^/  | /'
PLINE=$(echo "$ST" | grep "^proxy" | head -1)
case "$PLINE" in
  *" up "*) ;;
  *) echo "FAIL: proxy still not up with edge-trust declared: ${PLINE:-<no proxy row>}"
     echo "      proxy_up / edge_tls_client is ignoring the inventory's edge-trust."
     exit 1 ;;
esac
echo "$PLINE" | grep -qE "build [^ -]" || {
  echo "FAIL: the proxy is up but its build column is '-'."
  echo "      proxystats_field is not using edge-trust (it reads the edge too)."
  exit 1; }
OUT=$($CTL verify --probe acme:tok-acme 2>&1)
echo "$OUT" | grep -q "VERIFY OK" || {
  echo "FAIL: verify --probe cannot reach a foreign-CA edge that flintctl is"
  echo "      explicitly told to trust."
  echo "$OUT" | sed 's/^/  | /'; exit 1; }
echo "  liveness, build column and the data-plane probe all work over a foreign-CA edge"

echo "PASS: an edge cert signed by a CA the fleet does not own is unreadable without edge-trust and fully readable with it — the shape every real deployment has, and the one no other drill exercises"
