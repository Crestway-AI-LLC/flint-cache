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
fleet_init /tmp/flint-upgrade 6845 6846 7145 7845
fleet_guard

CTL=./target/release/flintctl
D=/tmp/flint-upgrade; rm -rf "$D"; mkdir -p "$D"
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

echo "== every pair node reports the build that was asked for"
ST=$($CTL -f "$D/cluster.flint" status 2>/dev/null || true)
N=$(echo "$ST" | grep -c "build $TAG" || true)
if [ "${N:-0}" -ne 2 ]; then
  echo "$ST" | sed 's/^/  | /'
  echo "FAIL: expected 2 nodes reporting build '$TAG', got ${N:-0}"
  echo "      the roll may have worked — check whether status is REPORTING it."
  exit 1
fi
echo "  both nodes on $TAG"

echo "== the data survived the roll (warm restart, not a resync)"
GOT=$($CLI -p 7845 -a tok-acme --no-auth-warning GET before-roll 2>/dev/null | tr -d '\r')
[ "$GOT" = "kept" ] \
  || { echo "FAIL: key written before the roll reads back '$GOT'"; exit 1; }

echo "== and the fleet still agrees with itself"
$CTL -f "$D/cluster.flint" verify --probe acme:tok-acme >"$D/verify.log" 2>&1 \
  || { echo "FAIL: verify after the upgrade"; tail -12 "$D/verify.log"; exit 1; }

echo "PASS: upgrade rolls the fleet and every node reports the operator-chosen build '$TAG' — the roll and the REPORT of it are checked separately, because rc.29 got the first right and the second wrong."
