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
#           binaries say about themselves via --build-version, the
#           controller — which has no listener and must not gain one —
#           reaches `status` by registering with the CP, and the RESP
#           `HELLO` reply carries the same build to CLIENTS.
#
#           That last one was added after the fact. D1 shipped every
#           operator-facing stamp and left the only client-facing one
#           reading the crate version, which no drill here could see
#           because none of them had ever asked a seat `HELLO`.
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
fleet_init $FLINT_DRILL_ROOT/flint-buildstamp-state 7415 7416 7417 7418
fleet_guard

# A STAMP THE CRATE VERSION CANNOT IMITATE, exported before anything runs so
# the binaries, the seats flintctl spawns, and every surface below all read
# the same value.
#
# Without it the HELLO assertion at the end would be vacuous in exactly the
# way this file's header warns about: a from-source build's fallback IS
# `0.0.1`, which is also the wrong value the broken code produced, so
# "HELLO agrees with the build" would pass on both. Read at RUNTIME by
# flint_build::version (option_env! FLINT_RELEASE_TAG is the compile-time
# one), so this does not touch what cargo bakes in.
#
# Release-SHAPED on purpose — the leading `v` is the half of flint_build::wire
# that only a real release exercises, and shipping a `v` prefix to a client
# library that parses this field is the failure mode being guarded against.
export FLINT_BUILD_VERSION=v9.9.9-stamp-probe
WIRE=9.9.9-stamp-probe
STATE=$FLINT_DRILL_ROOT/flint-buildstamp-state
INV=$FLINT_DRILL_ROOT/flint-buildstamp.flint
A=127.0.0.1:7415
B=127.0.0.1:7416
PROXY=127.0.0.1:7417
CP=127.0.0.1:7418
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

# UNGUARDED, this silently tests whatever binaries were already in
# target/release. A build that fails (a disk that filled, a compile error in
# an unrelated crate) then leaves the drill asserting against the PREVIOUS
# build, which is how a drill certifies a change it never compiled. Every
# other drill in this suite checks it; these two did not, and one of them
# produced a HELLO failure that vanished on re-run with no cause found.
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
  [ "$(valkey-cli -p 7415 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^live_replicas://p')" = "1" ] && break
  sleep 0.5
done

echo "== CPINFO carries build:, registry_version:, and the version: alias"
CPI=$(valkey-cli -p 7418 CPINFO 2>/dev/null | tr -d '\r')
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
PS=$(valkey-cli -p 7417 PROXYSTATS 2>/dev/null | tr -d '\r')
echo "$PS" | grep -q "^build:$WANT$" || { echo "FAIL: PROXYSTATS build: is not $WANT"; echo "$PS" | head -3; exit 1; }
echo "  build:$WANT"

echo "== the controller registers itself (it has no listener to ask)"
FOUND=""
for _ in $(seq 1 40); do
  CPI=$(valkey-cli -p 7418 CPINFO 2>/dev/null | tr -d '\r')
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

echo "== HELLO carries the build too — the one stamp a CLIENT reads"
# The surface D1 missed. Every assertion above reads an operator surface;
# this is the field a client library parses to decide what the server can
# do, and both handlers passed env!("CARGO_PKG_VERSION") instead of the
# build. On the playground that meant a uniformly v0.1.0-rc.37 fleet
# answering redis-py with `0.0.1`.
#
# Checked at the NODE as well as the proxy because they are two separate
# handlers in two crates that were wrong in the same way — and the node's
# is unreachable from tools/upgrade_drill.sh, whose fleet puts internal
# TLS in front of it. `WIRE` is the tag WITHOUT its leading `v`: real Redis
# answers `7.2.4` here, so a parser must not meet a `v`.
#
# Matched with -x against the whole line: piped valkey-cli prints each array
# element raw, one per line, so a substring match would also accept
# `v9.9.9-stamp-probe` — the exact case the second assertion exists to catch.
for p in 7415 7417; do
  H=$(valkey-cli -p $p HELLO 2>/dev/null | tr -d '\r"')
  echo "$H" | grep -qxF "$WIRE" || {
    echo "$H" | sed 's/^/  | /'
    echo "FAIL: port $p HELLO does not report '$WIRE'"
    echo "      '0.0.1' means the crate version, 'v$WIRE' means flint_build::wire was skipped."
    exit 1; }
  echo "$H" | grep -qxF "v$WIRE" && {
    echo "FAIL: port $p HELLO ships the leading 'v' — clients parse this field"; exit 1; }
  echo "$H" | grep -qxF "0.0.1" && {
    echo "FAIL: port $p HELLO still carries the crate version 0.0.1"; exit 1; }
  echo "  port $p HELLO version = $WIRE"
done

echo "PASS: build stamps — cp, proxy and controller all report a build and status shows all five seat kinds; HELLO carries it to clients as well; CPINFO renames version: to registry_version: while keeping the alias CPWATCH parses"
