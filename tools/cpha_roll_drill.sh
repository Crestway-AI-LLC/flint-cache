#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# `flintctl upgrade` against a THREE-SEAT Raft control plane.
#
# WHY THIS EXISTS. Two drills run a 3-seat CP (controlplane_ha, ctl_cpha) and
# neither one rolls. Two drills roll (upgrade, edge_roll) and both declare
# exactly one `cp`. So the intersection — upgrading an HA control plane, which
# is the production topology — had never executed, and a defect sat in it:
#
#     ha.rs CPINFO:      version: proxies: pairs: tenants: node: leader: ...
#     main.rs CPINFO:    build: registry_version: version: ... {controller rows}
#
# ADR-0014 D1 landed the build stamp, the registry_version rename and the
# controller registry on the SINGLE-NODE control plane only. `flintctl
# upgrade` calls assert_build("control plane", cpinfo_field(…, "build:")) and
# its None arm DIES, so every Raft fleet was unrollable: the seats swapped,
# then the roll aborted saying the control plane would not report a build.
# Exactly #146's shape — the roll worked, the reading of it did not — in the
# one topology no drill covered.
#
# WHAT IT PROVES
#   - `upgrade` EXITS ZERO against a 3-seat Raft CP
#   - EVERY CP seat reports the new build, not just the one flintctl asked
#   - the controller's registration reaches CPINFO on the HA path too, so
#     the roll's controller gate can be satisfied
#   - `registry_version:` is present alongside the `version:` alias
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-cpharoll-state 7603 7604 7605 7606 7607 7608
fleet_guard
D=/tmp/flint-cpharoll; STATE=/tmp/flint-cpharoll-state
INV=$D/cluster.flint
TAG=cpha-roll-3
rm -rf "$D" "$STATE"; mkdir -p "$D"

fleet_kill server; fleet_kill proxy; fleet_kill controlplane; fleet_kill controller
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill proxy; fleet_kill controlplane; fleet_kill controller
  [ -n "${KEEP:-}" ] || rm -rf "$D" "$STATE"
}
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
cp 127.0.0.1:7603
cp 127.0.0.1:7604
cp 127.0.0.1:7605
pair 127.0.0.1:7606,127.0.0.1:7607
proxy 127.0.0.1:7608
controller on
EOF
CTL="./target/release/flintctl -f $INV"

echo "== bootstrap a 3-seat Raft control plane"
$CTL bootstrap >"$D/boot.log" 2>&1 || { echo "FAIL: bootstrap"; tail -12 "$D/boot.log"; exit 1; }
UP=$($CTL status 2>&1 | grep -c "^cp .*up")
[ "$UP" = "3" ] || { echo "FAIL: $UP/3 CP seats up before the roll"; exit 1; }
echo "  3/3 seats up"

# POSITIVE CONTROL ON THE DRILL ITSELF: if the fleet already reported $TAG,
# every assertion below would pass without the upgrade doing anything.
BEFORE=$($CTL status 2>/dev/null | grep -c "build $TAG" || true)
[ "${BEFORE:-0}" -eq 0 ] \
  || { echo "FAIL: $BEFORE seat(s) already on '$TAG' before the roll — vacuous"; exit 1; }

echo "== every CP seat reports build: and registry_version: (ADR-0014 D1)"
for p in 7603 7604 7605; do
  # Plaintext on purpose: this inventory sets no `tls on`, so the CP's RESP
  # port takes a bare client and the drill needs no cert plumbing to read
  # one field.
  INFO=$(valkey-cli -p "$p" CPINFO 2>/dev/null | tr -d '\r')
  printf '%s' "$INFO" | grep -q '^build:' || {
    echo "FAIL: CP seat $p serves no build: in CPINFO."
    echo "      flintctl upgrade's assert_build dies on that, so this fleet"
    echo "      cannot be rolled — ADR-0014 D1 reached only one of the two"
    echo "      control-plane implementations."
    printf '%s\n' "$INFO" | head -6 | sed 's/^/  | /'
    exit 1; }
  printf '%s' "$INFO" | grep -q '^registry_version:' || {
    echo "FAIL: CP seat $p has no registry_version: — the D1 rename never"
    echo "      landed on the HA path, so 'version:' is still doing two jobs."
    exit 1; }
done
echo "  build: and registry_version: on all three"

echo "== upgrade --version-tag $TAG"
# Exit status IS the assertion. The defect this drill exists for did not
# corrupt anything: it made a successful roll report failure, and a drill
# that only checked the seats afterwards would have called that a pass.
$CTL upgrade --version-tag "$TAG" --soak-ms 1500 >"$D/upgrade.log" 2>&1
RC=$?
if [ "$RC" -ne 0 ]; then
  tail -14 "$D/upgrade.log" | sed 's/^/  | /'
  echo "FAIL: upgrade exited $RC against a 3-seat Raft control plane."
  # Report what the log SAYS, not what this drill was written expecting. An
  # earlier version asserted the cause was a missing build: — and the first
  # real run aborted on a bound port instead, so the message named a defect
  # that was already fixed and hid the one actually in front of it. A drill
  # that explains a failure it did not diagnose is worse than one that just
  # prints the log.
  case "$(grep -o 'UPGRADE ABORTED[^\\n]*' "$D/upgrade.log" | head -1)" in
    *"would not report a build"*)
      echo "      A seat came up and served no build. On the control plane that"
      echo "      means ha.rs's CPINFO is missing build: — ADR-0014 D1 reached"
      echo "      only the single-node implementation." ;;
    *"still bound"*)
      echo "      A seat's port was still held after its process went away."
      echo "      On a Raft CP each seat has peers actively connected to it, so"
      echo "      this is where a single-seat roll and an HA roll differ." ;;
    *) echo "      See the aborted line above; this drill does not guess." ;;
  esac
  exit 1
fi

echo "== all three CP seats carry the new build, not just the one asked"
ST=$($CTL status 2>&1)
echo "$ST" | grep '^cp ' | sed 's/^/  | /'
CPS=$(echo "$ST" | grep -c "^cp .*build $TAG" || true)
[ "${CPS:-0}" -eq 3 ] || {
  echo "FAIL: $CPS/3 CP seats report $TAG. A roll that leaves one seat behind"
  echo "      is a mixed-build control plane, which is what the gate exists"
  echo "      to make impossible."
  exit 1; }
echo "  3/3 on $TAG"

echo "== the controller registered THROUGH the HA path"
# The controller has no listener; its build arrives only by CPCONTROLLER.
# ha.rs had no handler at all, so this row could not exist on a Raft fleet
# and the roll's controller gate would wait its 45s and fail.
grep -q "controller reports $TAG" "$D/upgrade.log" || {
  echo "FAIL: the roll never confirmed the controller's build."
  grep -i controller "$D/upgrade.log" | tail -4 | sed 's/^/  | /'
  exit 1; }
ROWS=$(echo "$ST" | grep -c "^controller .*build=$TAG" || true)
[ "${ROWS:-0}" -ge 1 ] || {
  echo "FAIL: status shows no controller row on $TAG — CPCONTROLLER is not"
  echo "      being recorded by the HA control plane."
  exit 1; }
echo "  controller registered and reported"

echo "PASS: cpha roll — a three-seat Raft control plane rolls to completion, every seat REPORTS the new build, and the controller's registration reaches CPINFO on the HA path"
