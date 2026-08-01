#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Chaos against a fleet the harness did NOT spawn.
#
# Every chaos run so far has been self-dealt: the harness starts the nodes,
# kills them with pkill, and promotes the survivor itself. That tests the
# engine, but it tests NONE of the machinery an operator actually has — the
# inventory, flintctl's kill/restart, the controller's promotion decision —
# and it cannot be pointed at a fleet spread over several machines, which is
# the whole of #99.
#
# So this runs the SAME workload and the SAME ledger oracle against a real
# bootstrapped fleet: TLS on, control plane, controller supervising, faults
# injected through `flintctl kill-node` and repaired with `restart-node`.
#
# It runs on ONE machine on purpose. Every seat resolves local, so flintctl
# takes the local Runner path — which means this drill covers the attach
# mode, the two new commands, the controller's promotion and the oracle
# without AWS. Pointing it at a multi-host inventory is then the same command
# with a different file; only the ssh transport is left untested here, and
# nothing local can test that honestly.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-attached 7361 7362 7363 7364 7692 7744
fleet_guard
D=/tmp/flint-attached; INV=$D/cluster.flint
CTL=./target/release/flintctl
ITER="${1:-6}"
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; fleet_kill controller
sleep 0.4
cleanup() {
  $CTL -f "$INV" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; fleet_kill controller
  rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl -p flint-chaos --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:7744
pair 127.0.0.1:7361,127.0.0.1:7362
pair 127.0.0.1:7363,127.0.0.1:7364
proxy 127.0.0.1:7692
controller on
poll-ms 150
confirm 3
lease-ttl-ms 3000
EOF

echo "== bootstrap a real fleet (TLS, control plane, controller supervising)"
$CTL -f "$INV" bootstrap >/dev/null 2>&1 || { echo "FAIL: bootstrap"; exit 1; }
$CTL -f "$INV" verify >/dev/null 2>&1 || { echo "FAIL: fleet did not verify before chaos"; exit 1; }
echo "  VERIFY OK"

# A tenant, so the workload can go through the PROXY EDGE rather than dialling
# each master. That is the path a real client takes, and until now it was
# covered only by the local proxy_chaos drill — the attached (operator-path)
# chaos still bypassed the proxy entirely, so "the proxy chases a promotion"
# was never tested on a CP-fed proxy with a gated tenant.
$CTL -f "$INV" tenant add chaos tok-chaos chaos 1 >/dev/null 2>&1 \
  || { echo "FAIL: could not create the chaos tenant"; exit 1; }
echo "  tenant chaos created; workload will drive the edge at 127.0.0.1:7692"

echo "== the two fault verbs behave before we lean on them"
# restart-node must REFUSE the master: bringing the master back as a replica
# of itself is meaningless, and doing it silently would wipe the only copy.
MASTER=$($CTL -f "$INV" status 2>/dev/null | awk '/master/ {print $3}')
[ -n "$MASTER" ] || { echo "FAIL: no master reported"; exit 1; }
if $CTL -f "$INV" restart-node "$MASTER" >/dev/null 2>&1; then
  echo "FAIL: restart-node accepted the MASTER — that would re-seed the live copy from itself"
  exit 1
fi
echo "  restart-node refuses the master ($MASTER)"

echo "== chaos: $ITER kills through flintctl, promotion by the fleet's controller"
FLINTCTL_BIN=$CTL ./target/release/flint-chaos \
  --inventory "$INV" --iterations "$ITER" --keys 300 --mode mixed \
  --edge 127.0.0.1:7692 --auth chaos:tok-chaos \
  2>&1 | tee "$D/chaos.log" | sed 's/^/  /'
grep -q "^PASS:" "$D/chaos.log" || { echo "FAIL: chaos oracle did not pass"; exit 1; }

# The oracle's own invariants are asserted inside the harness (it panics on
# corruption, time travel or cross-key bleed). What THIS drill adds is that
# the kills were real and went through the operator path.
MK=$(sed -n 's/.*(\([0-9]*\) master, \([0-9]*\) replica).*/\1/p' "$D/chaos.log")
RK=$(sed -n 's/.*(\([0-9]*\) master, \([0-9]*\) replica).*/\2/p' "$D/chaos.log")
[ "$(( ${MK:-0} + ${RK:-0} ))" -eq "$ITER" ] \
  || { echo "FAIL: expected $ITER kills, ledger reports ${MK:-0}+${RK:-0}"; exit 1; }
[ "${MK:-0}" -ge 1 ] \
  || { echo "FAIL: no MASTER was ever killed — the failover path went untested"; exit 1; }
# BOTH pairs must take at least one kill. The 7-host chaos runs reported
# "16 cross-host kills" while every kill landed on pair 0 and pair 1 was
# scenery — the harness opened a single pair index defaulting to 0. This
# fleet declares two pairs precisely so that regression cannot come back
# quietly: a harness that only ever kills pair 0 fails here.
grep -q "pair 0: killed" "$D/chaos.log" \
  || { echo "FAIL: no kill ever landed on pair 0"; exit 1; }
grep -q "pair 1: killed" "$D/chaos.log" \
  || { echo "FAIL: no kill ever landed on pair 1 — the harness is single-pair again"; exit 1; }
echo "  $MK master kill(s), $RK replica kill(s), spread across both pairs, all via flintctl"

echo "== the fleet is intact afterwards, by its own reckoning"
# The point of routing faults through flintctl is that the fleet stays a
# fleet: registry, manifests and the proxy must still agree once the dust
# settles. A chaos run that leaves verify unhappy has broken something the
# oracle does not look at.
for _ in $(seq 1 30); do
  $CTL -f "$INV" verify >/dev/null 2>&1 && break
  sleep 1
done
$CTL -f "$INV" verify 2>&1 | tail -12 | sed 's/^/  /'
$CTL -f "$INV" verify >/dev/null 2>&1 || { echo "FAIL: fleet does not verify after chaos"; exit 1; }

echo "PASS: attached chaos — the ledger oracle survives $ITER kills injected through the operator path (flintctl kill-node/restart-node) with the fleet's own controller promoting, and the cluster still verifies"
