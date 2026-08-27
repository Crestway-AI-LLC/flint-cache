#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Chaos against the edge posture a CUSTOMER runs: TLS, not plaintext.
#
# WHY THIS EXISTS. `flint-chaos --edge` dialled the proxy with
# `Client::connect_addr(&e.addr, &None)` — plaintext, unconditionally, with a
# comment saying frontend TLS was "a separate concern from the internal mesh".
# It is a separate concern, and it is also the concern: every release note
# claiming the fleet is chaos-tested described a plaintext edge, while a
# client-TLS fleet is what a customer deploys. The client path was the one
# thing `--edge` was added to cover, and it covered it in the wrong posture.
#
# AND IT FAILED BY MISATTRIBUTION, which is worse than failing quietly.
# Pointed at a TLS edge the TCP connect succeeds, the proxy waits for a
# handshake that never arrives, AUTH times out, and `connect()` returns None
# — until the post-kill stall detector trips and panics with "the proxy never
# recovered". The run DOES end, and it ends by accusing a perfectly healthy
# proxy. Someone reading that goes and debugs the fleet. (Measured here, not
# assumed: it is verbatim what the negative control below produces. An
# earlier draft of this comment claimed an infinite hang, which is what the
# code looks like it should do and is not what it does.)
#
# THE NEGATIVE CONTROL IS THE POINT OF THE DRILL. Without it a green result
# here is unfalsifiable: if the edge were not really serving TLS, or if chaos
# silently fell back to plaintext, the positive run would pass while proving
# nothing. So this WATCHES CHAOS FAIL to dial the TLS edge without
# `--edge-ca` before it trusts the run that succeeds with it — the same
# discipline edge_ca_trust_drill.sh uses for flintctl.
#
# NOT COVERED HERE: a FOREIGN edge CA. bootstrap signs the edge cert with the
# fleet's own CA, so this drill proves chaos can speak edge TLS and verify a
# server name; edge_ca_trust_drill.sh is where the not-our-CA path lives.
#
# ADR-0018 item 9 (#20). Requires: a release build with --features rocks.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-chaostls 7191 7192 7193 7194
fleet_guard

D=$FLINT_DRILL_ROOT/flint-chaostls
STATE=$D/state
INV=$D/cluster.flint
PROXY=127.0.0.1:7193
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
  -p flint-controller -p flint-ctl -p flint-chaos --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

# client-tls on is the whole point; edge-san 127.0.0.1 so the edge cert
# matches the address chaos dials (connect_edge uses the dialed host as the
# server name, unlike the mesh's fixed SNI).
cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
tls on
client-tls on
edge-san 127.0.0.1
cp 127.0.0.1:7194
pair 127.0.0.1:7191,127.0.0.1:7192
proxy 127.0.0.1:7193
controller on
EOF
CTL="./target/release/flintctl -f $INV"

echo "== bootstrap (client-TLS fleet, edge cert signed by the fleet CA)"
$CTL bootstrap >"$D/boot.log" 2>&1 || { echo "FAIL: bootstrap"; tail -8 "$D/boot.log"; exit 1; }
[ -s "$STATE/certs/edge.crt" ] || {
  echo "FAIL: no edge cert was minted — this fleet is not client-TLS, so"
  echo "      everything below would be testing a plaintext edge."; exit 1; }
[ -s "$STATE/certs/ca.crt" ] || { echo "FAIL: no CA to trust"; exit 1; }
echo "  edge cert present; the proxy at $PROXY serves TLS"

$CTL tenant add chaos tok-chaos chaos 1 >/dev/null 2>&1 \
  || { echo "FAIL: could not create the chaos tenant"; exit 1; }

# `timeout` is not on macOS. perl's alarm is, everywhere this runs.
bounded() { s=$1; shift; perl -e 'alarm shift; exec @ARGV' "$s" "$@"; }

# ORDER: the positive run goes FIRST, and that is not the order the argument
# runs in. The negative control kills nodes before it panics, so running it
# first hands the positive run a pair with no reachable master — which is how
# it failed the first time this drill was assembled, on a TLS path that was
# working. Execution order is not evidence order: the control's job is to
# fail, and WHY it fails is established by the assertions on its output, not
# by what ran before it. Those assertions are specific enough to tell the
# difference — a pair-has-no-master panic carries neither the plaintext
# banner nor the dial hint, so a control that failed for the wrong reason is
# reported as a failure rather than banked as a pass.

echo
echo "== chaos through the TLS edge, trusting the fleet CA"
bounded 180 ./target/release/flint-chaos \
  --inventory "$INV" --iterations 3 --keys 200 --mode mixed \
  --edge "$PROXY" --auth chaos:tok-chaos --edge-ca "$STATE/certs/ca.crt" \
  2>&1 | tee "$D/tls.log" | sed 's/^/  /'
grep -q "^PASS:" "$D/tls.log" || {
  echo "FAIL: the chaos oracle did not pass over the TLS edge"
  exit 1; }

# The run must have gone through the EDGE, not silently fallen back to
# dialling masters directly — which would still pass the oracle while
# testing the thing this drill exists to stop testing.
grep -q "client path: proxy edge $PROXY .*TLS, trusting" "$D/tls.log" || {
  echo "FAIL: chaos did not report a TLS client path — it may have dialled"
  echo "      the pair masters instead of the edge."
  grep -i "client path" "$D/tls.log" | sed 's/^/  | /'
  exit 1; }

# …and the workload actually wrote something. An oracle with no writes has
# nothing to be right about, and "PASS: 3 kills (...), 0 writes" is a real
# possible output — so this reads the COUNT rather than grepping for the word
# "writes", which the PASS line contains either way.
WRITES=$(sed -n 's/^PASS:.*, \([0-9][0-9]*\) writes.*/\1/p' "$D/tls.log" | head -1)
[ -n "$WRITES" ] || { echo "FAIL: could not read the write count from the PASS line"; exit 1; }
[ "$WRITES" -gt 0 ] 2>/dev/null || {
  echo "FAIL: the oracle passed over $WRITES writes — nothing was exercised"
  exit 1; }
echo "  $WRITES writes acked through the TLS edge"

echo
echo "== NEGATIVE CONTROL: chaos with no --edge-ca must NOT get through"
# 25s is generous: a working edge completes this workload in a few seconds,
# and the broken case is an infinite retry, so anything short of a hang is a
# pass for the wrong reason. Exit status alone is not the assertion — a
# hang and a clean refusal both exit non-zero here, and only one of them is
# the behaviour being described.
# --iterations 0: NO KILLS. The control needs to prove chaos cannot DIAL a
# TLS edge, which needs no failover at all — and a control that killed nodes
# would hand the other run a broken pair. Both orderings of this drill failed
# that way before the fallback below was removed: negative-first left the
# positive run with no reachable master, positive-first left the negative run
# unable to restart a node, and neither failure had anything to do with TLS.
# With no kills the two runs are independent and the order is free.
#
# It reaches the dial through the FINAL WALK, which in edge mode now refuses
# to fall back to the pair masters. That refusal is a fix in its own right:
# with the fallback, this control passed — chaos failed to reach the TLS
# edge, silently verified against the masters instead, and printed PASS.
bounded 25 ./target/release/flint-chaos \
  --inventory "$INV" --iterations 0 --keys 40 --mode mixed \
  --edge "$PROXY" --auth chaos:tok-chaos \
  >"$D/plain.log" 2>&1
PLAIN_RC=$?
if grep -q "^PASS:" "$D/plain.log"; then
  echo "FAIL: chaos PASSED against a TLS edge without --edge-ca."
  echo "      Either the proxy is not actually serving TLS, or chaos is"
  echo "      still dialling it in plaintext and something answered. Either"
  echo "      way the positive run below would prove nothing."
  tail -6 "$D/plain.log" | sed 's/^/  | /'
  exit 1
fi
echo "  plaintext dial did not complete (rc=$PLAIN_RC), as it must not"
# And say WHY it did not, so a future failure for an unrelated reason is not
# read as this one.
if grep -qi "PLAINTEXT — not the posture" "$D/plain.log" \
   && grep -qi "suspect the dial before the fleet" "$D/plain.log"; then
  echo "  and it announced the plaintext posture, then failed naming the dial"
  # The failure must be the STALL detector, not a crash on the way in — a
  # panic in argument handling would also exit non-zero and would prove
  # nothing about TLS.
  grep -oE "(edge served fewer than 50 writes|final walk cannot reach the edge)[^\"]*" "$D/plain.log" | head -1 | sed 's/^/    | /'
else
  echo "FAIL: chaos did not announce a plaintext edge — it may have failed"
  echo "      for some other reason, which makes this control worthless."
  tail -6 "$D/plain.log" | sed 's/^/  | /'
  exit 1
fi

echo
echo "PASS  chaos drives the customer TLS edge, and cannot drive it plaintext"
