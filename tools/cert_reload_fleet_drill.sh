#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0006 D4 FOLLOW-ON drill — cert hot-reload on EVERY listener/dialer.
#   - full mTLS fleet (client-tls on): CP client port, node data ports,
#     proxy edge port all serve TLS; every dialer holds a reloadable config
#   - record the SERIAL each listener presents, then `flintctl rotate-certs`
#   - within one poll (~2s) every listener presents the NEW serial with the
#     SAME pids (no restarts): CP + proxy edge were load-once before this
#     follow-on and would have kept the old serial
#   - a live edge writer spans the window with zero errors, and fresh
#     post-rotation dials (mesh + edge) succeed — the dial side snapshots
#     the new leaf per dial
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-certreload 7061 7062 7795 7998
fleet_guard
CTL=./target/release/flintctl
D=$FLINT_DRILL_ROOT/flint-certreload; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; fleet_kill controller; sleep 0.4
cleanup() {
  $CTL -f "$D/cluster.flint" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; fleet_kill controller
  rm -rf "$D"
}
trap cleanup EXIT

echo "== bootstrap: mesh mTLS + encrypted front door (client-tls on)"
cat > "$D/cluster.flint" <<EOF
disposable on
statedir $D/state
bins ./target/release
tls on
client-tls on
cp 127.0.0.1:7795
pair 127.0.0.1:7061,127.0.0.1:7062
proxy 127.0.0.1:7998
controller on
EOF
$CTL -f "$D/cluster.flint" bootstrap >/dev/null 2>&1 || { echo "FAIL: bootstrap"; exit 1; }
$CTL -f "$D/cluster.flint" tenant add acme tok-acme acme 1 >/dev/null 2>&1
C="$D/state/certs"
sleep 1

# A valkey-cli WITHOUT TLS fails this drill in a way that reads as a product
# bug. This is the only drill that runs a fleet with `client-tls on`, so it is
# the only one that dials with `valkey-cli --tls`; a cli built without
# BUILD_TLS=yes has no such flag, the dial returns nothing, and the assertion
# below announces "fresh mesh dial with new leaf refused" — i.e. it blames
# cert hot-reload. The first CI run did exactly that while the product was
# fine: every serial had already rotated with zero restarts.
#
# Checked here, up front, so the message names the real cause. Deliberately a
# hard failure and not a SKIP: this drill covers a shipped feature, and
# "quietly not run" is the outcome the whole gate exists to prevent.
valkey-cli --help 2>&1 | grep -q -- '--tls' || {
  echo "FAIL: valkey-cli has no --tls (built without BUILD_TLS=yes)."
  echo "      This drill needs a TLS-capable valkey-cli; it is not a"
  echo "      statement about cert reload. Rebuild valkey with BUILD_TLS=yes."
  exit 1
}

# The serial a live listener PRESENTS (not the file): s_client with the
# current mesh identity for mutual-TLS ports, plain verify for the edge.
mesh_serial() { # <port>
  openssl s_client -connect "127.0.0.1:$1" -cert "$C/int.crt" -key "$C/int.key" \
    -CAfile "$C/ca.crt" </dev/null 2>/dev/null | openssl x509 -noout -serial 2>/dev/null | cut -d= -f2
}
edge_serial() { # <port>
  openssl s_client -connect "127.0.0.1:$1" -CAfile "$C/ca.crt" </dev/null 2>/dev/null \
    | openssl x509 -noout -serial 2>/dev/null | cut -d= -f2
}
# Scoped, because this census is an ASSERTION: line ~105 turns a change in
# this set into "something restarted", a claim about hot-reload. An unscoped
# pgrep -f made that claim about the whole machine — it counted another
# session's fleet, and it counted any process whose COMMAND LINE merely
# contained the string, which on this box includes the Claude agents
# themselves (`--add-dir .../crates/flint-proxy`). Starting or exiting an
# editor between the two samples turned this drill red. _fleet_ours matches
# on the executable's basename and then requires the scope dir or a declared
# port, so neither can enter the set. See docs/bugs/0029.
pids() { _fleet_ours "server proxy controlplane controller" | sort | tr '\n' ' '; }

CP1=$(mesh_serial 7795); N1=$(mesh_serial 7061); E1=$(edge_serial 7998)
[ -n "$CP1" ] && [ -n "$N1" ] && [ -n "$E1" ] || { echo "FAIL: baseline serials unreadable (cp=$CP1 node=$N1 edge=$E1)"; exit 1; }
PIDS1=$(pids)
echo "  baseline: cp=$CP1 node=$N1 edge=$E1"

echo "== live edge writer spans the rotation window"
( ACKED=0; ERRS=0
  END=$((SECONDS+8))
  while [ $SECONDS -lt $END ]; do
    R=$(valkey-cli -p 7998 --tls --cacert "$C/ca.crt" -a tok-acme --no-auth-warning SET "k$ACKED" v 2>/dev/null)
    if [ "$R" = "OK" ]; then ACKED=$((ACKED+1)); else ERRS=$((ERRS+1)); fi
  done
  echo "$ACKED $ERRS" > "$D/writer.out"
) &
WRITER=$!
sleep 1

echo "== rotate-certs (re-signs mesh + edge leaves in place)"
$CTL -f "$D/cluster.flint" rotate-certs >/dev/null 2>&1 || { echo "FAIL: rotate-certs"; exit 1; }
sleep 3.5   # > the 2s reload poll on every component

echo "== every listener presents the NEW serial, same pids"
CP2=$(mesh_serial 7795); N2=$(mesh_serial 7061); E2=$(edge_serial 7998)
PIDS2=$(pids)
echo "  after:    cp=$CP2 node=$N2 edge=$E2"
[ "$CP2" != "$CP1" ] && [ -n "$CP2" ] || { echo "FAIL: CP client port kept the old leaf (load-once regression)"; exit 1; }
[ "$N2" != "$N1" ] && [ -n "$N2" ] || { echo "FAIL: node data port kept the old leaf"; exit 1; }
[ "$E2" != "$E1" ] && [ -n "$E2" ] || { echo "FAIL: proxy edge port kept the old leaf (load-once regression)"; exit 1; }
[ "$PIDS1" = "$PIDS2" ] || { echo "FAIL: pids changed — something restarted"; exit 1; }
echo "  CP + node + proxy-edge all hot-reloaded; zero restarts"

echo "== fresh dials with the NEW leaf succeed on mesh and edge"
NV=$(valkey-cli -p 7061 --tls --cacert "$C/ca.crt" --cert "$C/int.crt" --key "$C/int.key" FLINTINFO 2>/dev/null | head -1)
[ -n "$NV" ] || { echo "FAIL: fresh mesh dial with new leaf refused"; exit 1; }
EV=$(valkey-cli -p 7998 --tls --cacert "$C/ca.crt" -a tok-acme --no-auth-warning GET k0 2>/dev/null)
[ "$EV" = "v" ] || { echo "FAIL: fresh edge dial after rotation (got '$EV')"; exit 1; }
echo "  new-leaf mesh dial + fresh edge session both serve"

wait $WRITER
read -r ACKED ERRS < "$D/writer.out"
echo "  writer through the window: $ACKED acked, $ERRS errors"
[ "$ERRS" = "0" ] && [ "$ACKED" -gt 0 ] || { echo "FAIL: writer saw errors across the reload"; exit 1; }

# VERIFY ON A CLIENT-TLS FLEET. The probe used to dial the client port in
# plaintext, so on every real deployment it decoded the proxy's TLS alert as a
# RESP frame and reported UnknownType(21) for all five data-plane checks. The
# structural checks passed throughout, which is what made it look fine. This
# is the only drill with `client-tls on`, so it is where that has to be caught.
echo "== flintctl verify --probe works THROUGH the encrypted front door"
OUT=$($CTL -f "$D/cluster.flint" verify --probe acme:tok-acme 2>&1)
echo "$OUT" | sed 's/^/  | /'
echo "$OUT" | grep -q "UnknownType" \
  && { echo "FAIL: probe decoded TLS bytes as RESP — it is dialling plaintext"; exit 1; }
echo "$OUT" | grep -q "VERIFY OK" || { echo "FAIL: verify did not pass on a client-tls fleet"; exit 1; }
echo "$OUT" | grep -q "client-tls" || { echo "FAIL: verify did not report which transport it used"; exit 1; }
for c in "auth + ping" "DBSIZE fan-out" "SCAN opens a cursor" "write/read round trip" "inline command accepted"; do
  echo "$OUT" | grep -q "ok   $c" || { echo "FAIL: data-plane check [$c] did not pass over TLS"; exit 1; }
done
echo "  all five data-plane checks passed over client TLS"

echo "PASS: cert hot-reload fleet-wide — CP client port, node data port, proxy edge (and every dialer via per-dial snapshots) pick up a rotated leaf within one poll, no restarts, zero write errors"
