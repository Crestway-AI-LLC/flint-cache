#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0014 D1: every seat says what build it is running.
#
# Before this, only flint-server, flint-ctl and flint-backup carried a
# stamp. `flintctl status` printed
#
#     cp    127.0.0.1:7500  up
#     proxy 0.0.0.0:7379    up
#     pair 0 127.0.0.1:7001 master ... build v0.1.0-rc.33
#
# — three of five seat kinds with nothing to report, rolled by an `upgrade`
# that therefore could not verify them. A half-completed edge roll looked
# exactly like a finished one. The recorded consequence is in sweep_orphans:
# "a controller from a previous start survived two upgrade cycles", which
# went unnoticed for two cycles precisely because nothing could be asked
# what build it was.
#
# WHAT THIS DRILL PROVES, and what it does not:
#
#   PROVES  every seat kind reports a build, the values agree with what the
#           binaries say about themselves via --build-version, and the
#           controller — which has no listener and must not gain one —
#           reaches `status` by registering with the CP.
#
#   DOES NOT PROVE  that `upgrade` aborts on a build MISMATCH. With dev
#           binaries it cannot: `upgrade --version-tag T` exports
#           FLINT_BUILD_VERSION=T to the seats it spawns, and an unbaked
#           binary reports the environment, so the assertion would be
#           reading back the value it just set. That is the self-fulfilling
#           check flint_build's own comment describes, and the reason the
#           baked FLINT_RELEASE_TAG deliberately WINS over the environment.
#           Proving the abort needs two RELEASE builds with different baked
#           tags — a release-box run, not a drill. Stated here rather than
#           faked, because a drill that appears to cover this and does not
#           is worse than one that says it doesn't.
#
# Requires: a release build with --features rocks, valkey-cli on PATH.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-buildstamp-state 7411 7412 7413 7414
fleet_guard
STATE=/tmp/flint-buildstamp-state
INV=/tmp/flint-buildstamp.flint
A=127.0.0.1:7411
B=127.0.0.1:7412
PROXY=127.0.0.1:7413
CP=127.0.0.1:7414
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
controller on
EOF
CTL="./target/release/flintctl -f $INV"

# The binaries' own answer, which every other surface must agree with. If
# these disagree the surfaces are reporting something other than the build.
echo "== each binary answers --build-version"
for b in flint-server flint-proxy flint-controlplane flint-controller; do
  V=$(./target/release/$b --build-version 2>/dev/null | head -1)
  [ -n "$V" ] || { echo "FAIL: $b --build-version printed nothing"; exit 1; }
  echo "  $b $V"
done
WANT=$(./target/release/flint-controlplane --build-version | head -1)

echo "== bootstrap"
$CTL bootstrap >/dev/null 2>&1
for _ in $(seq 1 60); do
  [ "$(valkey-cli -p 7411 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^live_replicas://p')" = "1" ] && break
  sleep 0.5
done

echo "== CPINFO carries build:, registry_version:, and the version: alias"
CPI=$(valkey-cli -p 7414 CPINFO 2>/dev/null | tr -d '\r')
echo "$CPI" | grep -q "^build:$WANT$" || { echo "FAIL: CPINFO build: is not $WANT"; echo "$CPI"; exit 1; }
echo "$CPI" | grep -q "^registry_version:" || { echo "FAIL: CPINFO has no registry_version:"; exit 1; }
# The alias is the whole reason the rename is safe to ship: CPWATCH clients
# parse `version:`. Dropping it here would break the watch protocol, which
# is the one thing the rename was explicitly not allowed to do.
echo "$CPI" | grep -q "^version:" || {
  echo "FAIL: CPINFO dropped the version: alias — CPWATCH clients parse it"; exit 1; }
RV=$(echo "$CPI" | sed -n 's/^registry_version://p')
AV=$(echo "$CPI" | sed -n 's/^version://p')
[ "$RV" = "$AV" ] || { echo "FAIL: alias disagrees with registry_version ($AV vs $RV)"; exit 1; }
echo "  build:$WANT  registry_version:$RV  version:$AV (alias)"

echo "== PROXYSTATS carries build:"
PS=$(valkey-cli -p 7413 PROXYSTATS 2>/dev/null | tr -d '\r')
echo "$PS" | grep -q "^build:$WANT$" || { echo "FAIL: PROXYSTATS build: is not $WANT"; echo "$PS" | head -3; exit 1; }
echo "  build:$WANT"

echo "== the controller registers itself (it has no listener to ask)"
FOUND=""
for _ in $(seq 1 40); do
  CPI=$(valkey-cli -p 7414 CPINFO 2>/dev/null | tr -d '\r')
  FOUND=$(echo "$CPI" | grep "^controller:" | head -1)
  [ -n "$FOUND" ] && break
  sleep 0.5
done
[ -n "$FOUND" ] || { echo "FAIL: no controller registered with the CP after 20s"; exit 1; }
echo "$FOUND" | grep -q "build=$WANT" || { echo "FAIL: controller reported a different build: $FOUND"; exit 1; }
echo "$FOUND" | grep -q " live " || { echo "FAIL: a controller that just registered reads as stale: $FOUND"; exit 1; }
echo "  $FOUND"

echo "== status shows a build for EVERY seat kind, not just pair nodes"
OUT=$($CTL status 2>&1)
echo "$OUT" | sed 's/^/  | /'
for row in "cp" "proxy" "pair 0" "controller"; do
  echo "$OUT" | grep -q "^$row" || { echo "FAIL: status has no '$row' row"; exit 1; }
done
# Compared against the PAIR NODE's rendering rather than a literal: a dev
# build displays the crate-version fallback as "unstamped" and a release
# build displays its tag, and the property that matters in both cases is
# that every seat agrees. Hard-coding either form would make this drill
# pass in one build mode and fail in the other for no real reason.
SHOWN=$(echo "$OUT" | grep "^pair 0" | head -1 | sed -n 's/.*build \([^ ]*\).*/\1/p')
[ -n "$SHOWN" ] || { echo "FAIL: could not read the pair node's build from status"; exit 1; }
# The point of the whole ADR item: these two used to have no build to
# print. A '-' here means the seat answered but would not say, which is the
# pre-D1 behaviour and must fail.
for row in "cp" "proxy"; do
  LINE=$(echo "$OUT" | grep "^$row" | head -1)
  case "$LINE" in
    *"build $SHOWN"*) ;;
    *"build -"*) echo "FAIL: $row reports no build — the pre-ADR-0014 behaviour: $LINE"; exit 1 ;;
    *) echo "FAIL: $row disagrees with the pair nodes (expected build $SHOWN): $LINE"; exit 1 ;;
  esac
done
echo "  every seat shows build $SHOWN"
echo "$OUT" | grep "^controller" | grep -q "build=$WANT" || {
  echo "FAIL: status controller row missing build=$WANT"; exit 1; }
echo "$OUT" | grep -q "NONE REPORTING" && {
  echo "FAIL: status says no controller reported, but one is running"; exit 1; }

echo "PASS: build stamps — cp, proxy and controller all report a build and status shows all five seat kinds; CPINFO renames version: to registry_version: while keeping the alias CPWATCH parses"
