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
CTL=./target/release/flintctl
D=/tmp/flint-certreload; rm -rf "$D"; mkdir -p "$D"
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
pkill -9 -f flint-controlplane 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null; sleep 0.4
cleanup() {
  $CTL -f "$D/cluster.flint" stop >/dev/null 2>&1
  pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
  pkill -9 -f flint-controlplane 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null
  rm -rf "$D"
}
trap cleanup EXIT

echo "== bootstrap: mesh mTLS + encrypted front door (client-tls on)"
cat > "$D/cluster.flint" <<EOF
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
pids() { pgrep -f "flint-(server|proxy|controlplane|controller)" | sort | tr '\n' ' '; }

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

echo "PASS: cert hot-reload fleet-wide — CP client port, node data port, proxy edge (and every dialer via per-dial snapshots) pick up a rotated leaf within one poll, no restarts, zero write errors"
