#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0074 — a min-replicas-to-write that cannot survive a failover.
#
# After a failover the survivors are `members - 1`, and one of them IS the new
# master, so live replicas = `members - 2`. A gate above that number does not
# shed writes occasionally; it sheds EVERY write after EVERY failover until the
# dead seat rejoins. On a two-member pair `min-replicas-to-write 1` is therefore
# a guaranteed outage per failover — a rewind at best, a full re-seed at worst,
# and BUG-0071 measured 94.2 s of one.
#
# The server honours whatever value it is given, deliberately: "never accept a
# write that is not on two copies" is a real posture. What was missing is anyone
# saying so at DEPLOY, while the fleet is still being shaped. The number is
# chosen once and its consequence arrives months later at the worst moment.
#
# WHAT IS ASSERTED:
#   1. CONTROL: the default (0) on a two-member pair verifies clean. A guard
#      that is red on a healthy fleet is one people learn to skip.
#   2. min-replicas 1 on a TWO-member pair is REFUSED, with the arithmetic and
#      the remedies in the message.
#   3. --allow-blocking-min-replicas carries it, and SAYS so rather than
#      passing silently.
#   4. The refusal is about SURVIVABILITY, not about the number 1: the same
#      value on a THREE-member pair is fine, because members-2 = 1.
#
# (4) is the one that would catch a guard written as `mr == 0` instead of
# `mr <= members - 2`.
#
# Requires: a release build with --features rocks, valkey-cli on PATH.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"

fleet_init "$FLINT_DRILL_ROOT/flint-minrepl-state" 7425 7426 7427 7428
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-minrepl-state
INV=$FLINT_DRILL_ROOT/flint-minrepl.flint
INV3=$FLINT_DRILL_ROOT/flint-minrepl3.flint
A=127.0.0.1:7425
B=127.0.0.1:7426
PROXY=127.0.0.1:7427
CP=127.0.0.1:7428
fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
  rm -rf "$STATE" "$INV" "$INV3"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV" "$INV3"

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
EOF
CTL="./target/release/flintctl -f $INV"

echo "== bootstrap a TWO-member pair at the default min-replicas 0"
$CTL bootstrap >"$STATE-boot.log" 2>&1 || {
  # Capture it and STOP. This discarded bootstrap's output and
  # ignored its exit status, so a failed bootstrap ran on into the
  # assertions below and was reported as whichever one broke first
  # -- a product fault asserted for what was really "bootstrap
  # failed and nobody looked" (BUG-0064).
  echo "FAIL: bootstrap"; tail -25 "$STATE-boot.log"; exit 1; }
for _ in $(seq 1 60); do
  [ "$(valkey-cli -p 7425 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^live_replicas://p')" = "1" ] && break
  sleep 0.5
done
[ "$(valkey-cli -p 7425 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^live_replicas://p')" = "1" ] \
  || { echo "FAIL: no replication after bootstrap"; exit 1; }
for p in 7425 7426; do
  V=$(valkey-cli -p $p FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^min_replicas_to_write://p')
  [ "$V" = "0" ] || { echo "FAIL: port $p reports min_replicas_to_write=$V, expected the default 0"; exit 1; }
done
echo "  both members at the default 0"

echo "== the default verifies clean (a guard red on a healthy fleet is skipped)"
$CTL verify >"$STATE/v-ok.txt" 2>&1 || {
  echo "FAIL: the DEFAULT configuration was refused:"; grep -iE "min-replicas|FAIL" "$STATE/v-ok.txt" | head -5 | sed 's/^/    /'; exit 1; }
echo "  verify OK"

echo "== now set min-replicas 1 on a TWO-member pair — it cannot survive a failover"
valkey-cli -p 7425 FLINTCONFIG min-replicas-to-write 1 >/dev/null 2>&1
GOT=$(valkey-cli -p 7425 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^min_replicas_to_write://p')
[ "$GOT" = "1" ] || { echo "FAIL: could not stage the condition — 7425 reports '$GOT'"; exit 1; }
echo "  7425 now requires 1 live replica; a failover leaves 0"

echo "== REFUSED, with the arithmetic and the remedies"
if $CTL verify >"$STATE/v-bad.txt" 2>&1; then
  echo "FAIL: a configuration that sheds every write after every failover was ACCEPTED"
  exit 1
fi
grep -q "min-replicas survivability" "$STATE/v-bad.txt" \
  || { echo "FAIL: the refusal does not name the check:"; tail -5 "$STATE/v-bad.txt"; exit 1; }
for want in "pair 0" "127.0.0.1:7425" "widowed-grace-ms"; do
  grep -q "$want" "$STATE/v-bad.txt" \
    || { echo "FAIL: the refusal does not mention '$want' — an operator cannot act on it"; exit 1; }
done
grep -oE "min-replicas survivability.{0,120}" "$STATE/v-bad.txt" | head -1 | sed 's/^/  /'

echo "== --allow-blocking-min-replicas carries it, and says so"
$CTL verify --allow-blocking-min-replicas >"$STATE/v-allow.txt" 2>&1 || {
  echo "FAIL: the override did not allow a deliberate durability posture:"
  grep -iE "min-replicas|FAIL" "$STATE/v-allow.txt" | head -5 | sed 's/^/    /'; exit 1; }
grep -q "ALLOWED by --allow-blocking-min-replicas" "$STATE/v-allow.txt" || {
  echo "FAIL: the override passed SILENTLY; an operator cannot see what is being carried"; exit 1; }
echo "  allowed, and said so"

echo "== the same value on a THREE-member pair is FINE (survivability, not the number 1)"
# members-2 = 1, so one replica survives a failover and the gate is satisfiable.
# A guard written as `mr == 0` would wrongly refuse this.
$CTL stop >/dev/null 2>&1; sleep 0.5
rm -rf "$STATE"
cat > "$INV3" <<EOF
disposable on
statedir $STATE
bins ./target/release
cp $CP
pair $A,$B,127.0.0.1:7427
EOF
CTL3="./target/release/flintctl -f $INV3"
$CTL3 bootstrap >"$STATE-boot.log" 2>&1 || {
  # Capture it and STOP. This discarded bootstrap's output and
  # ignored its exit status, so a failed bootstrap ran on into the
  # assertions below and was reported as whichever one broke first
  # -- a product fault asserted for what was really "bootstrap
  # failed and nobody looked" (BUG-0064).
  echo "FAIL: bootstrap"; tail -25 "$STATE-boot.log"; exit 1; }
for _ in $(seq 1 60); do
  [ "$(valkey-cli -p 7425 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^live_replicas://p')" = "2" ] && break
  sleep 0.5
done
LR=$(valkey-cli -p 7425 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^live_replicas://p')
[ "${LR:-0}" = "2" ] || { echo "FAIL: the three-member fixture never reached 2 replicas (got ${LR:-none}) — the arm would prove nothing"; exit 1; }
valkey-cli -p 7425 FLINTCONFIG min-replicas-to-write 1 >/dev/null 2>&1
$CTL3 verify >"$STATE/v-three.txt" 2>&1 || {
  echo "FAIL: min-replicas 1 was refused on a THREE-member pair, where a failover"
  echo "      still leaves one live replica. The guard is keyed on the NUMBER"
  echo "      rather than on survivability:"
  grep -iE "min-replicas|FAIL" "$STATE/v-three.txt" | head -5 | sed 's/^/    /'; exit 1; }
echo "  accepted on three members, where a failover still leaves one replica"

echo "PASS: min-replicas survivability — the default verifies clean, a value a failover cannot satisfy is REFUSED with its arithmetic and remedies, --allow-blocking-min-replicas carries a deliberate posture and says so, and the same value on a larger pair is accepted because the check is about survivability rather than the number (BUG-0074)"
