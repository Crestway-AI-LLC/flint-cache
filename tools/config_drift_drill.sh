#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0014 D2: the members of a pair are compared, and a knob that differs
# between master and replica is reported as drift.
#
# Every operational knob lives in each node's own FLINTINFO and nothing
# aggregated them, so a roll that updated one member and not the other left
# a pair that behaves one way and fails over to a survivor that behaves
# another — invisible until the failover that needed it. `lag_hard_ms`
# differing is not cosmetic: it is the threshold at which writes are shed.
#
# The ADR names the red condition and the trap in the same breath. Set the
# knob differently on the two members and `--json` must report drift naming
# both the knob and the seats. Then restart one member and confirm NO drift
# is reported while it is legitimately young — because a check whose red
# means "unlucky timing" is a check people re-run instead of read.
#
# FLINT_ROLL_GRACE_MS shrinks the two-minute suppression window so both
# sides of that boundary can be exercised without sleeping through it.
#
# Requires: a release build with --features rocks, valkey-cli on PATH.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-drift-state 7421 7422 7423 7424
fleet_guard
STATE=/tmp/flint-drift-state
INV=/tmp/flint-drift.flint
A=127.0.0.1:7421
B=127.0.0.1:7422
PROXY=127.0.0.1:7423
CP=127.0.0.1:7424
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
cp $CP
pair $A,$B
proxy $PROXY
lag-hard-ms 5000
EOF
CTL="./target/release/flintctl -f $INV"

echo "== bootstrap: both members get lag-hard-ms 5000 from the inventory"
$CTL bootstrap >/dev/null 2>&1
for _ in $(seq 1 60); do
  [ "$(valkey-cli -p 7421 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^live_replicas://p')" = "1" ] && break
  sleep 0.5
done
[ "$(valkey-cli -p 7421 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^live_replicas://p')" = "1" ] \
  || { echo "FAIL: no replication after bootstrap — nothing downstream means anything"; exit 1; }

# POSITIVE CONTROL on the knob itself. If the inventory value never reached
# the nodes, the drift check below would pass by comparing two defaults and
# would keep passing against a fleet where the knob does nothing.
for p in 7421 7422; do
  V=$(valkey-cli -p $p FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^lag_hard_ms://p')
  [ "$V" = "5000" ] || { echo "FAIL: port $p reports lag_hard_ms=$V, expected 5000 — the inventory knob never landed"; exit 1; }
done
echo "  both members report lag_hard_ms 5000"

echo "== a healthy pair reports NO drift"
J=$(FLINT_ROLL_GRACE_MS=0 $CTL status --json 2>/dev/null)
echo "$J" | grep -q '"drift": \[\]' || {
  echo "FAIL: a matched pair reported drift:"; echo "$J" | grep -A2 '"drift"'; exit 1; }
echo "  drift: []"

echo "== uptime_ms is present and advancing (the suppression depends on it)"
U1=$(valkey-cli -p 7421 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^uptime_ms://p')
[ -n "$U1" ] || { echo "FAIL: FLINTINFO has no uptime_ms — the roll-grace check has nothing to read"; exit 1; }
sleep 1.2
U2=$(valkey-cli -p 7421 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^uptime_ms://p')
[ "$U2" -gt "$U1" ] || { echo "FAIL: uptime_ms did not advance ($U1 -> $U2) — a frozen clock suppresses drift forever"; exit 1; }
echo "  uptime_ms $U1 -> $U2"

echo "== now make them DISAGREE: restart the replica with a different lag-hard-ms"
REPLICA=$(for p in 7421 7422; do
  [ "$(valkey-cli -p $p FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^role://p')" = "replica" ] && echo $p
done | head -1)
[ -n "$REPLICA" ] || { echo "FAIL: no replica found"; exit 1; }
MASTER=$([ "$REPLICA" = "7421" ] && echo 7422 || echo 7421)
$CTL kill-node "127.0.0.1:$REPLICA" >/dev/null 2>&1
sleep 1
./target/release/flint-server --port "$REPLICA" --engine rocks \
  --data-dir "$STATE/n$REPLICA" --replica-of "127.0.0.1:$MASTER" \
  --lag-hard-ms 9999 > "$STATE/drift-replica.log" 2>&1 &
for _ in $(seq 1 60); do
  [ "$(valkey-cli -p $REPLICA FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^lag_hard_ms://p')" = "9999" ] && break
  sleep 0.5
done
GOT=$(valkey-cli -p $REPLICA FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^lag_hard_ms://p')
[ "$GOT" = "9999" ] || { echo "FAIL: could not create drift — replica reports lag_hard_ms=$GOT"; exit 1; }
echo "  master $MASTER=5000  replica $REPLICA=9999"

echo "== SUPPRESSED while the restarted member is young (a roll, not drift)"
J=$(FLINT_ROLL_GRACE_MS=600000 $CTL status --json 2>/dev/null)
echo "$J" | grep -q '"drift": \[\]' || {
  echo "FAIL: drift reported for a member that just restarted — this is the"
  echo "      cry-wolf case the ADR calls out; every roll would go red:"
  echo "$J" | grep -A2 '"drift"'; exit 1; }
echo "  drift: []  (correctly held back)"

echo "== REPORTED once the grace window has passed"
J=$(FLINT_ROLL_GRACE_MS=0 $CTL status --json 2>/dev/null)
echo "$J" | grep -q '"drift": \[\]' && {
  echo "FAIL: real drift was NOT reported — the check verifies nothing"; exit 1; }
echo "$J" | grep -o '"drift": \[[^]]*\]' | sed 's/^/  /'
echo "$J" | grep -q "lag_hard_ms" || { echo "FAIL: drift does not name the knob"; exit 1; }
echo "$J" | grep -q "127.0.0.1:$REPLICA" || { echo "FAIL: drift does not name the seat"; exit 1; }
echo "$J" | grep -q "127.0.0.1:$MASTER" || { echo "FAIL: drift does not name the other seat"; exit 1; }

echo "== the document is valid JSON, not just greppable text"
echo "$J" | python3 -c "
import json,sys
d = json.load(sys.stdin)
assert isinstance(d['drift'], list) and d['drift'], 'drift must be a non-empty list'
assert d['pairs'] and d['pairs'][0]['members'], 'pairs/members missing'
# State must NOT be compared: a master and its replica differ on role by
# definition, and a drift check that flagged it would be red on every
# healthy fleet.
assert not any('role' in x for x in d['drift']), f'role must never be drift: {d[\"drift\"]}'
print('  parsed: %d pair(s), %d drift row(s)' % (len(d['pairs']), len(d['drift'])))
" || { echo "FAIL: --json did not parse"; exit 1; }

echo "PASS: config drift — a knob differing between pair members is reported with the knob and both seats named, and is held back while a member is younger than a roll window"
