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
fleet_init $FLINT_DRILL_ROOT/flint-decom-state 7221 7222 7223 7224
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-decom-state
INV=$FLINT_DRILL_ROOT/flint-decom.flint
RUN=$FLINT_DRILL_ROOT/flint-decom-run
ACKFILE=$FLINT_DRILL_ROOT/flint-decom-ack
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
  -p flint-controller -p flint-ctl --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
tls on
cp 127.0.0.1:7224
pair 127.0.0.1:7221,127.0.0.1:7222
proxy 127.0.0.1:7223
controller on
EOF

status() { ./target/release/flintctl -f "$INV" status 2>/dev/null; }
master() { status | awk '/master/{print $3; exit}'; }
nodes_live() { status | grep -cE 'master|replica'; }
A="valkey-cli -p 7223 -a tok-acme --no-auth-warning"

echo "== bootstrap a master+replica pair behind a proxy + controller"
./target/release/flintctl -f "$INV" bootstrap >/dev/null 2>&1
./target/release/flintctl -f "$INV" tenant add acme tok-acme acme 1 >/dev/null 2>&1

echo "== seed 5000 keys through the proxy"
awk 'BEGIN{for(i=0;i<5000;i++){k=sprintf("k:%05d",i);v=sprintf("v-%05d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}' \
  | valkey-cli -p 7223 -a tok-acme --no-auth-warning --pipe >/dev/null 2>&1
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
    # tmp+mv, not `>`: a bare redirect truncates then writes, and the main
    # shell reads this file WHILE the loop runs. CI caught the window — the
    # check read an empty file as "no write was ever acked" while
    # writer:last=713 proved the opposite, and the drill reported acked-write
    # loss against a failover that lost nothing.
    if [ "$($A SET writer:last $n 2>/dev/null)" = "OK" ]; then
      echo "$n" > "$ACKFILE.tmp" && mv "$ACKFILE.tmp" "$ACKFILE"
    fi
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
# Same split as migrate_slots: an empty ack file is a HARNESS failure, not
# evidence of loss. The tmp+mv above should make it impossible, which is
# exactly why it deserves its own message if it ever happens again.
[ -n "$ACKED" ] || {
  echo "FAIL (HARNESS, not the system): no acked write was ever recorded."
  echo "      Says nothing about whether the failover lost data."
  echo "      key reads back as: $GOT"
  exit 1
}
[ "$GOT" -ge "$ACKED" ] 2>/dev/null \
  || { echo "FAIL: acked write lost (acked=$ACKED, read=$GOT)"; exit 1; }
[ "$($A DBSIZE)" -ge 5000 ] || { echo "FAIL: seeded data lost after failover"; exit 1; }
echo "  zero acked-write loss across failover (acked=$ACKED, read=$GOT); 5000+ keys intact"

# POSITIVE CONTROL for `verify` itself, placed here because this is the one
# drill that already owns a two-member pair and a live proxy.
#
# `verify` used to print `master X (1 down)` and count that row as ok, so a
# pair running on ONE node reported VERIFY OK — all views agree. The
# playground ran exactly that way for five days after its replica hit a WAL
# gap and exited: no failover target, one copy on one disk, and a watch that
# a human reads every morning saying OK. A check that has never been shown
# to go red over the condition it exists for is not a check.
DOWN_R=$(status | awk '/replica/{print $3; exit}')
echo "== CONTROL: verify must FAIL while a declared member is missing ($DOWN_R)"
fleet_signal_port "${DOWN_R##*:}" -9
for i in $(seq 1 20); do [ "$(nodes_live)" -eq 1 ] && break; sleep 0.3; done
if ./target/release/flintctl -f "$INV" verify >$FLINT_DRILL_ROOT/decom-sc.log 2>&1; then
  echo "FAIL: verify reported OK with $DOWN_R down — single-copy read as healthy"
  cat $FLINT_DRILL_ROOT/decom-sc.log; exit 1
fi
grep -q 'SINGLE-COPY' $FLINT_DRILL_ROOT/decom-sc.log \
  || { echo "FAIL: verify went red for some other reason"; cat $FLINT_DRILL_ROOT/decom-sc.log; exit 1; }
echo "  red: '$(grep -o 'SINGLE-COPY.*' $FLINT_DRILL_ROOT/decom-sc.log | head -1)'"
./target/release/flintctl -f "$INV" start >/dev/null 2>&1
for i in $(seq 1 40); do [ "$(nodes_live)" -eq 2 ] && break; sleep 0.5; done
./target/release/flintctl -f "$INV" verify >$FLINT_DRILL_ROOT/decom-sc2.log 2>&1 \
  || { echo "FAIL: verify still red after the member came back"; cat $FLINT_DRILL_ROOT/decom-sc2.log; exit 1; }
echo "  green again once the member is back — the check discriminates"

echo "== GUARD: decommission the LIVE MASTER ($M1) must be refused"
if ./target/release/flintctl -f "$INV" decommission-node "$M1" 2>$FLINT_DRILL_ROOT/decom-g1.log; then
  echo "FAIL: decommission of live master not refused"; exit 1; fi
grep -q 'live MASTER' $FLINT_DRILL_ROOT/decom-g1.log || { echo "FAIL: wrong guard"; cat $FLINT_DRILL_ROOT/decom-g1.log; exit 1; }
echo "  refused: '$(grep -o 'live MASTER.*failover [^ ]*' $FLINT_DRILL_ROOT/decom-g1.log | head -1)'"

VICTIM=$(status | awk '/replica/{print $3; exit}')
echo "== DECOMMISSION the replica ($VICTIM) — pair keeps serving master-only"
./target/release/flintctl -f "$INV" decommission-node "$VICTIM" 2>&1 | grep -E 'draining|complete' | sed 's/^/  /'
sleep 1
[ "$(nodes_live)" -eq 1 ] || { echo "FAIL: expected 1 node after decommission, got $(nodes_live)"; exit 1; }
grep -q "7221,127.0.0.1:7222" "$INV" && { echo "FAIL: inventory still lists both nodes"; exit 1; }
[ "$($A DBSIZE)" -ge 5000 ] || { echo "FAIL: data lost after decommission"; exit 1; }
[ "$($A SET post:decom ok)" = "OK" ] || { echo "FAIL: not writable after decommission"; exit 1; }
echo "  pair serves reads+writes on the remaining node; keys intact; inventory updated"

echo "== GUARD: decommission the pair's LAST node ($M1) must be refused"
if ./target/release/flintctl -f "$INV" decommission-node "$M1" 2>$FLINT_DRILL_ROOT/decom-g2.log; then
  echo "FAIL: decommission of last node not refused"; exit 1; fi
grep -q 'whole shard' $FLINT_DRILL_ROOT/decom-g2.log || { echo "FAIL: wrong guard"; cat $FLINT_DRILL_ROOT/decom-g2.log; exit 1; }
echo "  refused: whole-shard removal is out of scope"

rm -f "$RUN"; wait "$WRITER_PID" 2>/dev/null
# The cluster must also AGREE WITH ITSELF, not merely pass the one
# path this drill exercises — the gap two shipped bugs lived in.
echo "== integrity: every view of the cluster reconciles"
./target/release/flintctl -f "$INV" verify --probe acme:tok-acme >/dev/null \
  || { echo "FAIL: cluster does not reconcile (run: flintctl -f $INV verify --probe acme:tok-acme)"; exit 1; }
echo "  verified"

echo "PASS: failover (graceful, zero acked loss) + decommission-node (guarded both ways, pair keeps serving)"
