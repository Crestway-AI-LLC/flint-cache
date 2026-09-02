#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A roll that CANNOT ASK a seat for its build must abort saying so, and must
# not report the seat as having answered (BUG-0083).
#
# BUG-0083: `proxystats_field` and `cpinfo_field` read a seat's build with one
# attempt and collapsed every failure -- refused connect, unfinished TLS
# handshake, read timeout, `-NOAUTH`, and a seat genuinely naming no build --
# into the same `None`. `roll_edge` treats None as fatal AFTER rolling every
# seat, so one missed read turned a finished roll into "came up but would not
# report a build", and the failure tail could not say which of the five it was.
#
# The fix routes all three tiers through `field_from_reply`, which answers
# three ways. THAT CLASSIFICATION HAS UNIT TESTS. Its consequence did not: the
# bug was marked FIXED with "no drill asserts the end-to-end behaviour"
# recorded as owed, because forcing a real read failure mid-roll means a
# refused connect against an otherwise healthy seat, which a drill cannot
# produce without testing something else. FLINT_BUILD_READ_FAIL arms it.
#
# TWO ARMS, because "it aborted correctly" means nothing unless the same roll
# succeeds without the seam:
#
#   A (negative control): no seam. `upgrade` completes, exit 0.
#   B (positive control): seam set. `upgrade` aborts non-zero, and the message
#     says the read FAILED and names the injected reason -- not "reports build
#     ''", which is what the node half printed before the fix and is a claim
#     about something the seat never said.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-brf-state 6502 6503 6504 6505 6506
fleet_guard
D=$FLINT_DRILL_ROOT/flint-brf; STATE=$FLINT_DRILL_ROOT/flint-brf-state
INV=$D/cluster.flint
rm -rf "$D" "$STATE"; mkdir -p "$D"
fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop >/dev/null 2>&1
  fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
  [ -n "${KEEP:-}" ] || rm -rf "$D" "$STATE"
}
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

# TWO members, not one. `upgrade` refuses with "need at least one replica",
# and arm A caught that on the first run of this drill -- which is the negative
# control earning its place: a single-member pair would have made arm B pass
# against a fleet that could never have rolled at all.
cat > "$INV" <<INVEOF
disposable on
statedir $STATE
bins ./target/release
cp 127.0.0.1:6504
pair 127.0.0.1:6502,127.0.0.1:6503
controller 127.0.0.1:6506
proxy 127.0.0.1:6505
proxy-advertise 127.0.0.1:6505
INVEOF

CTL=./target/release/flintctl
# --version-tag is REQUIRED for this drill to test anything. `assert_build` is
# skipped without it -- deliberately, since there is nothing to compare against
# -- so an armed seam on a tagless roll exits 0 and the drill would report a
# pass having exercised none of the path. The first run did exactly that.
TAG=build-read-fail-1
$CTL -f "$INV" bootstrap >"$D/bootstrap.log" 2>&1 || { echo "FAIL: bootstrap"; tail -5 "$D/bootstrap.log"; exit 1; }
grep -q master <<<"$($CTL -f "$INV" status 2>/dev/null)" || { echo "FAIL: no master after bootstrap"; exit 1; }
echo "  fleet up"

# ARM A -- NEGATIVE CONTROL. Without the seam the roll must SUCCEED. A drill
# whose positive arm fails against a fleet that could never roll anyway proves
# nothing, which is the trap OPS-0094 and the rc.47 edge-roll both fell into.
echo "== A: no seam — the roll must complete (--version-tag $TAG, so the build IS asserted)"
if $CTL -f "$INV" upgrade --version-tag "$TAG" >"$D/upgrade-a.log" 2>&1; then
  echo "  upgrade exited 0"
else
  echo "FAIL: upgrade FAILED without the seam armed — arm B would prove nothing."
  tail -8 "$D/upgrade-a.log" | sed 's/^/    /'
  exit 1
fi

# ARM B -- POSITIVE CONTROL. The read fails; the roll must refuse AND say why.
echo "== B: FLINT_BUILD_READ_FAIL — the roll must abort naming the cause"
if FLINT_BUILD_READ_FAIL=1 $CTL -f "$INV" upgrade --version-tag "$TAG" >"$D/upgrade-b.log" 2>&1; then
  echo "FAIL: upgrade exited 0 with the build read FORCED to fail."
  echo "      A roll that cannot verify itself must not report success."
  exit 1
fi
echo "  upgrade refused, as it must"

# The message is the entire point of BUG-0083. Aborting is not enough; the
# abort has to distinguish "I could not ask" from "the seat answered wrongly".
if ! grep -q "could not be asked for its build\|could not be asked" "$D/upgrade-b.log"; then
  echo "FAIL: the abort does not say the READ failed."
  echo "      Before BUG-0083 this path printed 'reports build \"\"', which"
  echo "      asserts the seat answered with an empty string -- a claim about"
  echo "      something it never said."
  tail -8 "$D/upgrade-b.log" | sed 's/^/    /'
  exit 1
fi
if ! grep -q "FLINT_BUILD_READ_FAIL" "$D/upgrade-b.log"; then
  echo "FAIL: the abort names no underlying reason."
  echo "      The injected cause must survive into the message, or the next"
  echo "      real occurrence is as undiagnosable as the one that started this."
  tail -8 "$D/upgrade-b.log" | sed 's/^/    /'
  exit 1
fi
# And it must NOT claim the seat reported something.
if grep -q 'reports build ""' "$D/upgrade-b.log"; then
  echo "FAIL: the abort says the seat 'reports build \"\"' — the fabricated"
  echo "      quotation BUG-0083 removed."
  exit 1
fi
echo "  the abort names the failed read AND the injected reason"

echo "PASS: a roll that cannot ask a seat for its build refuses and says so — the read failure is named, not disguised as a seat that answered with nothing (BUG-0083)"
