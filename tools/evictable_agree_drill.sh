#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0069: eviction is per-TENANT policy that had only a per-SEAT switch.
#
# The value of `evictable-ns` is a NAMESPACE LIST, so the policy belongs to a
# tenant; the only ways to set it were a start flag and a live FLINTCONFIG,
# both per seat, and nothing composed them. `flint-server/src/main.rs` already
# names the cost: a pair whose members disagree has one side reclaiming while
# the other fills to -QUOTA -- divergent POLICY rather than divergent
# decisions, "strictly worse, and nothing else would surface it".
#
# EVICTABLE_AGREE detects that and FLINTINFO reports it, but a detector is not
# a guard: it tells an operator afterwards, and there was no deploy step for it
# to refuse at. This drill exercises the step that now exists.
#
# WHAT IS ASSERTED, each with its control:
#   1. POSITIVE CONTROL: the inventory key reaches BOTH seats. Without it every
#      assertion below compares two defaults and would keep passing against a
#      fleet where the key does nothing.
#   2. A matched pair verifies clean -- the check must not be red on a healthy
#      fleet, which is the failure that trains people to ignore it.
#   3. A mismatch is REFUSED, naming the pair and both seats.
#   4. --allow-evictable-mismatch lets the same fleet through. Refusing without
#      an escape hatch strands a half-converted roll.
#   5. A member inside the roll window is HELD BACK, not refused: `roll` walks
#      members one at a time and passes through a legitimate disagreement, so a
#      refusal there would fail the very roll that is converging them.
#
# FLINT_ROLL_GRACE_MS shrinks the suppression window so both sides of that
# boundary are exercised without sleeping through it.
#
# Requires: a release build with --features rocks, valkey-cli on PATH.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"

fleet_init "$FLINT_DRILL_ROOT/flint-evictagree-state" 7431 7432 7433 7434
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-evictagree-state
INV=$FLINT_DRILL_ROOT/flint-evictagree.flint
A=127.0.0.1:7431
B=127.0.0.1:7432
PROXY=127.0.0.1:7433
CP=127.0.0.1:7434
fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane
  rm -rf "$STATE" "$INV"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
cp $CP
pair $A,$B
proxy $PROXY
evictable-ns cache
EOF
CTL="./target/release/flintctl -f $INV"

echo "== bootstrap: the inventory declares evictable-ns cache"
$CTL bootstrap >/dev/null 2>&1
for _ in $(seq 1 60); do
  [ "$(valkey-cli -p 7431 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^live_replicas://p')" = "1" ] && break
  sleep 0.5
done
[ "$(valkey-cli -p 7431 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^live_replicas://p')" = "1" ] \
  || { echo "FAIL: no replication after bootstrap — nothing downstream means anything"; exit 1; }

# POSITIVE CONTROL on the inventory key itself.
for p in 7431 7432; do
  V=$(valkey-cli -p $p FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^evictable_ns://p')
  [ "$V" = "cache" ] || {
    echo "FAIL: port $p reports evictable_ns='$V', expected 'cache' — the inventory"
    echo "      key never reached the seat, so every check below would compare two"
    echo "      defaults and pass against a fleet where the key does nothing."
    exit 1; }
done
echo "  both members report evictable_ns cache"

echo "== a matched pair verifies clean"
FLINT_ROLL_GRACE_MS=0 $CTL verify >"$STATE/v-clean.txt" 2>&1 || {
  echo "FAIL: a MATCHED pair was refused — a check that is red on a healthy"
  echo "      fleet is a check people learn to skip:"
  grep -iE "evictable|FAIL" "$STATE/v-clean.txt" | head -5 | sed 's/^/      /'
  exit 1; }
echo "  verify OK"

echo "== now make them DISAGREE (FLINTCONFIG on one member)"
valkey-cli -p 7432 FLINTCONFIG evictable-ns other >/dev/null 2>&1
GOT=$(valkey-cli -p 7432 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^evictable_ns://p')
[ "$GOT" = "other" ] || { echo "FAIL: could not create the mismatch — 7432 reports '$GOT'"; exit 1; }
echo "  7431=cache  7432=other"

echo "== REFUSED once the grace window has passed, naming the pair and both seats"
if FLINT_ROLL_GRACE_MS=0 $CTL verify >"$STATE/v-drift.txt" 2>&1; then
  echo "FAIL: divergent eviction policy was ACCEPTED — the guard verifies nothing"
  exit 1
fi
grep -q "evictable-ns agreement" "$STATE/v-drift.txt" \
  || { echo "FAIL: the refusal does not name the check:"; tail -5 "$STATE/v-drift.txt"; exit 1; }
for want in "pair 0" "127.0.0.1:7431" "127.0.0.1:7432"; do
  grep -q "$want" "$STATE/v-drift.txt" \
    || { echo "FAIL: the refusal does not name '$want' — an operator cannot act on it"; exit 1; }
done
grep -oE "evictable-ns agreement.*" "$STATE/v-drift.txt" | head -1 | cut -c1-150 | sed 's/^/  /'

echo "== HELD BACK while a member is inside the roll window (a roll, not drift)"
if ! FLINT_ROLL_GRACE_MS=600000 $CTL verify >"$STATE/v-young.txt" 2>&1; then
  echo "FAIL: refused a pair whose members are legitimately mid-roll. \`roll\`"
  echo "      converges the two sides one at a time and passes through exactly"
  echo "      this state; refusing here fails the roll that would fix it:"
  grep -iE "evictable" "$STATE/v-young.txt" | head -3 | sed 's/^/      /'
  exit 1
fi
grep -q "held back" "$STATE/v-young.txt" || {
  echo "FAIL: the difference was suppressed with NOTHING emitted. A suppression"
  echo "      nobody can see is indistinguishable from a clean fleet."
  exit 1; }
echo "  reported as held back, not as clean"

echo "== --allow-evictable-mismatch lets the same fleet through"
FLINT_ROLL_GRACE_MS=0 $CTL verify --allow-evictable-mismatch >"$STATE/v-allow.txt" 2>&1 || {
  echo "FAIL: the override did not allow a deliberate half-applied rollout:"
  grep -iE "evictable|FAIL" "$STATE/v-allow.txt" | head -5 | sed 's/^/      /'
  exit 1; }
grep -q "ALLOWED by --allow-evictable-mismatch" "$STATE/v-allow.txt" || {
  echo "FAIL: the override passed SILENTLY. An operator who forgets the flag is"
  echo "      set has no way to see that a mismatch is being carried."
  exit 1; }
echo "  allowed, and said so"

echo "== and the override does not blanket-disable verify"
valkey-cli -p 7432 FLINTCONFIG evictable-ns cache >/dev/null 2>&1
FLINT_ROLL_GRACE_MS=0 $CTL verify --allow-evictable-mismatch >/dev/null 2>&1 \
  || { echo "FAIL: a matched pair was refused even WITH the override"; exit 1; }
echo "  a re-converged pair still verifies clean"

echo "PASS: evictable-ns agreement — the inventory key reaches both seats, a divergent pair is REFUSED naming both, a member inside the roll window is held back rather than refused, and --allow-evictable-mismatch carries a deliberate half-applied rollout while saying so (BUG-0069)"
