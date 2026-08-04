#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A pair whose members share a failure domain must FAIL verify.
#
# WHY THIS EXISTS. The inventory has always required a pair's two members on
# separate HOSTS, and that is the check people assume protects them. It does
# not: two hosts in one availability zone survive a host failure and not the
# loss of the zone, and the zone is by far the likelier event. On
# instance-store families the data goes with them — stopping or retiring an
# instance destroys its copy — so a zone event is dual DATA loss, not a
# temporary outage. See docs/slo.md.
#
# `zone <host> <name>` declares the domain and `verify` asserts pair members
# never share one. Four states, because only the first is interesting alone:
#
#   none declared        -> verify passes AND SAYS it is not checking
#   distinct domains     -> the check reports ok
#   shared domain        -> the check reports FAIL
#   partial declaration  -> the check reports FAIL
#   malformed zone line  -> refused at parse, not silently dropped
#
# The partial case is the one worth the extra assertion. A half-declared
# topology reports anti-affinity it never checked, which is worse than
# declaring nothing at all — it turns "unknown" into "confirmed safe".
#
# TWO HARNESS RULES LEARNED HERE, both the hard way:
#
#   1. CLEANUP USES A PRISTINE INVENTORY. The first version rewrote
#      cluster.flint for each case and let the trap call `stop` on whatever
#      was left — which, for the malformed case, was a file flintctl refuses
#      to parse. `stop` died, five seats leaked, and every later drill in the
#      suite failed with "REFUSING TO RUN: this box already has Flint
#      processes". The live topology now lives in its own file that no case
#      ever touches.
#   2. ASSERT THE SPECIFIC LINE, NOT THE EXIT CODE. Cases that need two
#      distinct hosts use 127.0.0.2, which is a loopback alias on Linux and
#      usually absent on macOS — so verify fails those for LIVENESS whatever
#      the zones say. An exit-code assertion there would pass for the wrong
#      reason. Only the shared-domain case, whose topology is genuinely live,
#      asserts a non-zero exit.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-affinity 6855 6856 7155 7855
fleet_guard

CTL=./target/release/flintctl
D=/tmp/flint-affinity; rm -rf "$D"; mkdir -p "$D"
LIVE="$D/live.flint"     # the running topology. NEVER rewritten.
CASE="$D/case.flint"     # rewritten per assertion.

cleanup() {
  # Always the pristine file: see harness rule 1 in the header.
  $CTL -f "$LIVE" stop >/dev/null 2>&1
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

cat > "$LIVE" <<EOF
disposable on
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:7155
pair 127.0.0.1:6855,127.0.0.1:6856
proxy 127.0.0.1:7855
controller on
EOF

echo "== bootstrap (no zones: nothing asserted, and it says so)"
$CTL -f "$LIVE" bootstrap >"$D/bootstrap.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -8 "$D/bootstrap.log"; exit 1; }

OUT=$($CTL -f "$LIVE" verify 2>&1) \
  || { echo "FAIL: a zone-less inventory must still verify"; echo "$OUT"; exit 1; }
echo "$OUT" | grep -q "not declared" \
  || { echo "FAIL: a zone-less inventory must SAY the check is not armed, not stay silent"
       echo "$OUT"; exit 1; }
echo "  verifies, and reports the check is not armed"

# --- shared domain -------------------------------------------------------
# The only case whose topology is genuinely live, so the only one where a
# non-zero exit can be attributed to the zone check.
echo "== shared domain -> FAIL (and verify exits non-zero)"
{ cat "$LIVE"; echo "zone 127.0.0.1 az-a"; } > "$CASE"
set +e
OUT=$($CTL -f "$CASE" verify 2>&1); RC=$?
set -e
[ "$RC" -ne 0 ] \
  || { echo "FAIL: both members in az-a must fail verify"; echo "$OUT"; exit 1; }
echo "$OUT" | grep -q "FAIL pair 0 members are in distinct failure domains" \
  || { echo "FAIL: the failure must be the anti-affinity check, named"; echo "$OUT"; exit 1; }
echo "  failed, naming the anti-affinity check"

# --- distinct domains ----------------------------------------------------
two_host_base() {
  sed 's|^pair 127.0.0.1:6855,127.0.0.1:6856$|pair 127.0.0.1:6855,127.0.0.2:6856|' "$LIVE"
}

echo "== distinct domains -> ok"
{ two_host_base; echo "zone 127.0.0.1 az-a"; echo "zone 127.0.0.2 az-b"; } > "$CASE"
OUT=$($CTL -f "$CASE" verify 2>&1 || true)
echo "$OUT" | grep -q "ok   pair 0 members are in distinct failure domains" \
  || { echo "FAIL: distinct domains must report ok"; echo "$OUT"; exit 1; }
echo "  reported ok"

# --- partial declaration -------------------------------------------------
echo "== partial declaration -> FAIL (the false-assurance case)"
{ two_host_base; echo "zone 127.0.0.1 az-a"; } > "$CASE"   # 127.0.0.2 unzoned
OUT=$($CTL -f "$CASE" verify 2>&1 || true)
echo "$OUT" | grep -q "FAIL pair 0 every member has a zone" \
  || { echo "FAIL: a half-declared topology must be refused by the zone check"
       echo "$OUT"; exit 1; }
echo "$OUT" | grep -q "no .zone. line for 127.0.0.2" \
  || { echo "FAIL: the refusal must name the unzoned host"; echo "$OUT"; exit 1; }
echo "  refused, naming the unzoned host"

# --- malformed line ------------------------------------------------------
echo "== malformed zone line -> refused at parse"
{ cat "$LIVE"; echo "zone 127.0.0.1"; } > "$CASE"
set +e
OUT=$($CTL -f "$CASE" verify 2>&1); RC=$?
set -e
[ "$RC" -ne 0 ] \
  || { echo "FAIL: 'zone <host>' with no name must be refused"; echo "$OUT"; exit 1; }
echo "$OUT" | grep -q "takes exactly a host and a name" \
  || { echo "FAIL: the refusal must explain the syntax"; echo "$OUT"; exit 1; }
echo "  refused at parse, with the syntax explained"

# The live fleet must be untouched by all of the above — none of those cases
# was allowed to mutate the running topology.
$CTL -f "$LIVE" verify >/dev/null 2>&1 \
  || { echo "FAIL: the live fleet stopped verifying after the case files"; exit 1; }

echo "PASS: failure-domain anti-affinity — a zone-less inventory says it is not checking, distinct domains verify, a shared domain fails naming the check, a PARTIAL declaration fails rather than reporting anti-affinity it never checked, and a malformed zone line is refused at parse instead of silently dropped."
