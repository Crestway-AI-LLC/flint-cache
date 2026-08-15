#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# `flintctl upgrade` rolls the fleet, and every node REPORTS the build you
# asked for.
#
# WHY THIS EXISTS. v0.1.0-rc.29 shipped a regression in `flintctl status`:
# the build column rewrote any string that was not a release-shaped tag to
# "unstamped". The roll itself worked perfectly; the report of it did not.
# That column is how an operator confirms an upgrade took, so it stopped
# being able to tell two builds apart, and it disagreed with the build label
# the exporter publishes for the very same node.
#
# The public gate went green on that, start to finish. The only thing that
# caught it was the FLEET repository's canary drill — coverage sitting behind
# a private repo, for a binary that ships from this one. Anyone building from
# source, or running CI on a fork, had none at all.
#
# So this drill is narrow on purpose. It does NOT re-test what canary tests
# (the journal soak, mixed-version replication, the abort-on-incident gate,
# the agent and the exporter); those are fleet concerns and they stay there.
# It asserts the one thing this repository owns and could not see:
#
#     the roll happens, and the build a node reports is the build you named.
#
# THE TAG BELOW IS DELIBERATELY NOT RELEASE-SHAPED. A `v<major>.<minor>.<patch>`
# tag is exactly the case that never broke — using one would reproduce the
# blind spot rather than close it. Operators do use their own build numbers;
# docs/self-hosting.md invites it.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-upgrade 6845 6846 7145 7845
fleet_guard

CTL=./target/release/flintctl
D=$FLINT_DRILL_ROOT/flint-upgrade; rm -rf "$D"; mkdir -p "$D"
TAG=build-1234

cleanup() {
  $CTL -f "$D/cluster.flint" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; fleet_kill controller
  [ -n "${KEEP:-}" ] || rm -rf "$D"
}
trap cleanup EXIT
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; fleet_kill controller
sleep 0.4

cargo build --release -q -p flint-server --features rocks || { echo "FAIL: build"; exit 1; }
cargo build --release -q -p flint-ctl -p flint-proxy -p flint-controlplane -p flint-controller \
  || { echo "FAIL: build"; exit 1; }

cat > "$D/cluster.flint" <<EOF
# Throwaway: a from-source build carries no release version, so the fleet
# must admit it is disposable before flintctl will mutate it.
disposable on
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:7145
pair 127.0.0.1:6845,127.0.0.1:6846
proxy 127.0.0.1:7845
controller on
EOF

echo "== bootstrap"
$CTL -f "$D/cluster.flint" bootstrap >"$D/bootstrap.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -8 "$D/bootstrap.log"; exit 1; }
$CTL -f "$D/cluster.flint" tenant add acme tok-acme acme 1 >/dev/null 2>&1 \
  || { echo "FAIL: tenant add"; exit 1; }

CLI=""
for c in valkey-cli redis-cli; do command -v "$c" >/dev/null 2>&1 && { CLI=$c; break; }; done
[ -n "$CLI" ] || { echo "SKIP: no valkey-cli or redis-cli"; exit 0; }

# Written BEFORE the roll: an upgrade is a binary swap plus a warm restart,
# never a resync, so losing this would be a real defect and not a reporting
# one. Cheap to check while we are here.
sleep 1
$CLI -p 7845 -a tok-acme --no-auth-warning SET before-roll "kept" >/dev/null 2>&1 \
  || { echo "FAIL: could not write through the proxy before the roll"; exit 1; }

# POSITIVE CONTROL ON THE TEST ITSELF. If the fleet already reported $TAG,
# every assertion below would pass without the upgrade doing anything.
BEFORE=$($CTL -f "$D/cluster.flint" status 2>/dev/null | grep -c "build $TAG" || true)
[ "${BEFORE:-0}" -eq 0 ] \
  || { echo "FAIL: $BEFORE node(s) already report '$TAG' before the upgrade — the test would pass vacuously"; exit 1; }

echo "== upgrade --version-tag $TAG (an operator's own build number, not a release tag)"
$CTL -f "$D/cluster.flint" upgrade --version-tag "$TAG" --soak-ms 1500 >"$D/upgrade.log" 2>&1 \
  || { echo "FAIL: upgrade exited non-zero"; tail -15 "$D/upgrade.log"; exit 1; }

echo "== EVERY seat reports the build that was asked for, not just the pair"
# This counted a flat `grep -c "build $TAG"` against 2, because the pair
# nodes were the only seats that carried a stamp. ADR-0014 D1 gave the
# control plane and the proxy one too, and the count became 4 — the drill
# went red on a fleet that had rolled correctly, which is the shape of
# "a drill can assert the OLD contract" from the field notes.
#
# Counting 4 instead would fix the red and waste the change. #105 made
# `upgrade` roll all five seat kinds while it could only verify one, and
# the whole point of D1 is that the other tiers are now checkable. So
# assert them BY KIND: a magic total would also go stale the next time the
# inventory grows a seat.
ST=$($CTL -f "$D/cluster.flint" status 2>/dev/null || true)
PAIRS=$(echo "$ST" | grep -c "^pair .*build $TAG" || true)
CPS=$(echo "$ST"   | grep -c "^cp .*build $TAG" || true)
PXS=$(echo "$ST"   | grep -c "^proxy .*build $TAG" || true)
if [ "${PAIRS:-0}" -ne 2 ] || [ "${CPS:-0}" -lt 1 ] || [ "${PXS:-0}" -lt 1 ]; then
  echo "$ST" | sed 's/^/  | /'
  echo "FAIL: after the roll, seats on '$TAG': ${PAIRS:-0}/2 pair nodes, ${CPS:-0} cp, ${PXS:-0} proxy"
  echo "      the roll may have worked — check whether status is REPORTING it."
  echo "      A cp or proxy at 0 means the EDGE roll did not land, which is"
  echo "      the half that used to be invisible."
  exit 1
fi
echo "  pair $PAIRS/2, cp $CPS, proxy $PXS — all on $TAG"

echo "== and so does HELLO, the only version string a CLIENT ever reads"
# Every assertion above reads an OPERATOR surface. HELLO is the one a client
# library reads, and it was wrong for the entire life of the project: both
# handlers passed env!("CARGO_PKG_VERSION") — the workspace version, the
# literal 0.0.1 — so a fleet where status, CPINFO, PROXYSTATS and
# --build-version all correctly said v0.1.0-rc.37 told redis-py 0.0.1.
#
# ADR-0014 D1 is marked implemented in full and shipped every operator-facing
# stamp; nothing in this repository looked at the client-facing one. It was
# found by speaking RESP to the playground edge from outside, which is the
# only vantage point from which it is visible at all.
#
# $TAG is what gives this teeth. In a from-source build the crate fallback IS
# 0.0.1, so asserting "HELLO agrees with the build" against an unstamped
# fleet would pass on the broken code — the self-fulfilling shape this file's
# sibling drill warns about. After the roll the build is `build-1234`, which
# the old code could not produce by any route.
HOUT=$($CLI -p 7845 -a tok-acme --no-auth-warning HELLO 2>/dev/null | tr -d '\r"')
echo "$HOUT" | grep -qxF "$TAG" || {
  echo "$HOUT" | sed 's/^/  | /'
  echo "FAIL: HELLO does not report '$TAG'. If it says 0.0.1 the reply is"
  echo "      carrying the crate version instead of the build (flint_build::wire)."
  exit 1; }
echo "$HOUT" | grep -qxF "0.0.1" && {
  echo "$HOUT" | sed 's/^/  | /'
  echo "FAIL: HELLO still carries the crate version 0.0.1 alongside the build"; exit 1; }
echo "  HELLO version = $TAG"

echo "== the data survived the roll (warm restart, not a resync)"
GOT=$($CLI -p 7845 -a tok-acme --no-auth-warning GET before-roll 2>/dev/null | tr -d '\r')
[ "$GOT" = "kept" ] \
  || { echo "FAIL: key written before the roll reads back '$GOT'"; exit 1; }

echo "== and the fleet still agrees with itself"
$CTL -f "$D/cluster.flint" verify --probe acme:tok-acme >"$D/verify.log" 2>&1 \
  || { echo "FAIL: verify after the upgrade"; tail -12 "$D/verify.log"; exit 1; }

echo "PASS: upgrade rolls the fleet and every node reports the operator-chosen build '$TAG' — the roll and the REPORT of it are checked separately, because rc.29 got the first right and the second wrong."
