#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Graceful master handoff + single-node decommission — the exact operator
# sequence: to take a MASTER out you `failover` first (demote->drain->promote,
# ex-master rejoins as a replica), then `decommission-node` drops a member
# while the pair keeps serving. Asserts: roles transition correctly, a live
# writer loses ZERO acked writes across the handoff, data is intact after
# each op, and BOTH guards fire (decommission refuses a live master, and
# refuses the pair's last node as an out-of-scope whole-shard removal).
#
# Mainstream 2-node pair (master + replica). Mesh mTLS on (the failover path
# rides it); the proxy client port is plaintext to keep the writer a plain
# valkey-cli.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-decom-state 7001 7002 7379 7500
fleet_guard
STATE=/tmp/flint-decom-state
INV=/tmp/flint-decom.flint
RUN=/tmp/flint-decom-run
ACKFILE=/tmp/flint-decom-ack
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; fleet_kill controller
sleep 0.4
cleanup() {
  rm -f "$RUN"
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; fleet_kill controller
  rm -rf "$STATE" "$INV" "$ACKFILE"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV" "$RUN" "$ACKFILE"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks

cat > "$INV" <<EOF
statedir $STATE
bins ./target/release
tls on
cp 127.0.0.1:7500
pair 127.0.0.1:7001,127.0.0.1:7002
proxy 127.0.0.1:7379
controller on
EOF

status() { ./target/release/flintctl -f "$INV" status 2>/dev/null; }
master() { status | awk '/master/{print $3; exit}'; }
nodes_live() { status | grep -cE 'master|replica'; }
A="valkey-cli -p 7379 -a tok-acme --no-auth-warning"

echo "== bootstrap a master+replica pair behind a proxy + controller"
./target/release/flintctl -f "$INV" bootstrap >/dev/null 2>&1
./target/release/flintctl -f "$INV" tenant add acme tok-acme acme 1 >/dev/null 2>&1

echo "== seed 5000 keys through the proxy"
awk 'BEGIN{for(i=0;i<5000;i++){k=sprintf("k:%05d",i);v=sprintf("v-%05d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
  | valkey-cli -p 7379 -a tok-acme --no-auth-warning --pipe >/dev/null 2>&1
SEED=$($A DBSIZE)
[ "$SEED" = "5000" ] || { echo "FAIL: seed DBSIZE=$SEED (want 5000)"; exit 1; }
echo "  seeded 5000"

# Background writer FIRST arm the run flag, THEN launch: a monotonic
# counter through the proxy, retrying transient unavailability, recording
# only ACKED values. A failover must never lose the last acked one.
: > "$ACKFILE"; touch "$RUN"
( n=0
  while [ -f "$RUN" ]; do
    n=$((n+1))
    if [ "$($A SET writer:last $n 2>/dev/null)" = "OK" ]; then echo "$n" > "$ACKFILE"; fi
  done ) &
WRITER_PID=$!
sleep 1.5

M0=$(master); echo "== initial master: $M0"
[ -n "$M0" ] || { echo "FAIL: no master"; exit 1; }

echo "== FAILOVER the master ($M0) — graceful handoff"
./target/release/flintctl -f "$INV" failover "$M0" 2>&1 | grep -E 'demoted|complete' | sed 's/^/  /'
sleep 1
M1=$(master); echo "== new master: $M1"
[ -n "$M1" ] && [ "$M1" != "$M0" ] || { echo "FAIL: master unchanged ($M0 -> $M1)"; exit 1; }
[ "$(nodes_live)" -eq 2 ] || { echo "FAIL: pair lost a node after failover"; exit 1; }
echo "  ex-master rejoined as replica; both nodes live"

ACKED=$(cat "$ACKFILE"); GOT=$($A GET writer:last)
[ -n "$ACKED" ] && [ "$GOT" -ge "$ACKED" ] 2>/dev/null \
  || { echo "FAIL: acked write lost (acked=$ACKED, read=$GOT)"; exit 1; }
[ "$($A DBSIZE)" -ge 5000 ] || { echo "FAIL: seeded data lost after failover"; exit 1; }
echo "  zero acked-write loss across failover (acked=$ACKED, read=$GOT); 5000+ keys intact"

echo "== GUARD: decommission the LIVE MASTER ($M1) must be refused"
if ./target/release/flintctl -f "$INV" decommission-node "$M1" 2>/tmp/decom-g1.log; then
  echo "FAIL: decommission of live master not refused"; exit 1; fi
grep -q 'live MASTER' /tmp/decom-g1.log || { echo "FAIL: wrong guard"; cat /tmp/decom-g1.log; exit 1; }
echo "  refused: '$(grep -o 'live MASTER.*failover [^ ]*' /tmp/decom-g1.log | head -1)'"

VICTIM=$(status | awk '/replica/{print $3; exit}')
echo "== DECOMMISSION the replica ($VICTIM) — pair keeps serving master-only"
./target/release/flintctl -f "$INV" decommission-node "$VICTIM" 2>&1 | grep -E 'draining|complete' | sed 's/^/  /'
sleep 1
[ "$(nodes_live)" -eq 1 ] || { echo "FAIL: expected 1 node after decommission, got $(nodes_live)"; exit 1; }
grep -q "7001,127.0.0.1:7002" "$INV" && { echo "FAIL: inventory still lists both nodes"; exit 1; }
[ "$($A DBSIZE)" -ge 5000 ] || { echo "FAIL: data lost after decommission"; exit 1; }
[ "$($A SET post:decom ok)" = "OK" ] || { echo "FAIL: not writable after decommission"; exit 1; }
echo "  pair serves reads+writes on the remaining node; keys intact; inventory updated"

echo "== GUARD: decommission the pair's LAST node ($M1) must be refused"
if ./target/release/flintctl -f "$INV" decommission-node "$M1" 2>/tmp/decom-g2.log; then
  echo "FAIL: decommission of last node not refused"; exit 1; fi
grep -q 'whole shard' /tmp/decom-g2.log || { echo "FAIL: wrong guard"; cat /tmp/decom-g2.log; exit 1; }
echo "  refused: whole-shard removal is out of scope"

rm -f "$RUN"; wait "$WRITER_PID" 2>/dev/null
# The cluster must also AGREE WITH ITSELF, not merely pass the one
# path this drill exercises — the gap two shipped bugs lived in.
echo "== integrity: every view of the cluster reconciles"
./target/release/flintctl -f "$INV" verify --probe acme:tok-acme >/dev/null \
  || { echo "FAIL: cluster does not reconcile (run: flintctl -f $INV verify --probe acme:tok-acme)"; exit 1; }
echo "  verified"

echo "PASS: failover (graceful, zero acked loss) + decommission-node (guarded both ways, pair keeps serving)"
