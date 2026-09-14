#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# THE NEAR-CACHE'S CROSS-CLIENT WINDOW (ADR-0031), which nothing measured.
#
# `flint-proxy/src/cache.rs` states the contract: "stale reads are ALLOWED,
# bounded by the TTL. A write through THIS proxy invalidates its local entry;
# a write through ANOTHER proxy -- or straight to a node -- becomes visible
# here only when the TTL lapses. The TTL is the contract."
#
# So read-your-own-writes holds for the client that WROTE, and the exposure is
# cross-CLIENT: A writes through proxy 1, B reads through proxy 2 and sees the
# old value. That had no drill at all, in either direction -- neither the
# staleness nor the read-your-own-writes it is often confused with.
#
# THIS DRILL PINS THE CONTRACT AS WRITTEN. The "B still sees the old value"
# assertion is the DOCUMENTED behaviour, not a property worth protecting: when
# ADR-0031's v2 lands it goes RED and tells whoever landed it to flip it,
# rather than passing silently against a fleet that now invalidates across
# proxies. The other three assertions survive v2 unchanged.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-ncx-state 6644 6645 6646 7567
fleet_guard
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; sleep 0.4
B=./target/release/flint-server
CPB=./target/release/flint-controlplane
PX=./target/release/flint-proxy
STATE=$FLINT_DRILL_ROOT/flint-ncx-state
# Long enough that the stale window is not a race with the assertions, short
# enough that waiting it out does not dominate the drill. The product default
# is 5s and a tenant may set 60s; this is the shape, not the value.
TTL_MS=4000
cleanup() {
  fleet_kill proxy; fleet_kill controlplane; fleet_kill server
  rm -rf "$STATE" "$STATE.tmp" $FLINT_DRILL_ROOT/flint-ncx-*
}
trap cleanup EXIT
rm -rf "$STATE"

$B --port 6644 --engine rocks --data-dir $FLINT_DRILL_ROOT/flint-ncx-data 2>"${FLEET_SCOPE}server.log" &
fleet_wait_listen 6644
$CPB --port 7567 --state "$STATE" 2>$FLINT_DRILL_ROOT/flint-ncx-cp.log &
fleet_wait_listen 7567
sleep 0.4
fleet_cp 7567 CPADDPROXY 127.0.0.1:6645
fleet_cp 7567 CPADDPROXY 127.0.0.1:6646
fleet_cp 7567 CPADDPAIR 127.0.0.1:6644
fleet_cp 7567 CPADDTENANT acme tok-acme acme 1 >/dev/null
# The near-cache is opt-in per tenant (D6) AND the proxy must have a TTL.
fleet_cp 7567 CPTENANTCACHE acme on >/dev/null
for p in 6645 6646; do
  $PX --port $p --control-plane 127.0.0.1:7567 --advertise 127.0.0.1:$p \
      --cache-ttl-ms $TTL_MS 2>$FLINT_DRILL_ROOT/flint-ncx-px$p.log &
  fleet_wait_listen $p
done
sleep 1.2

p1() { valkey-cli -p 6645 -a tok-acme --no-auth-warning "$@"; }
p2() { valkey-cli -p 6646 -a tok-acme --no-auth-warning "$@"; }

echo "== seed, then let client B populate proxy 2's cache"
[ "$(p1 SET ncx v1)" = "OK" ] || { echo "FAIL: seed write through proxy 1"; exit 1; }
[ "$(p2 GET ncx)" = "v1" ] || { echo "FAIL: B could not read the seed through proxy 2"; exit 1; }
# THE CONTROL FOR THE WHOLE DRILL. Everything below is about a CACHED entry,
# and a proxy with the cache off would satisfy the freshness assertions while
# failing to demonstrate anything. Prove proxy 2 is actually holding one:
# with the backend value changed underneath it, a cached read still answers v1.
valkey-cli -p 6644 SET "acme:ncx" v_backend_poke >/dev/null 2>&1 || true
CACHED=$(p2 GET ncx)
[ "$CACHED" = "v1" ] || {
  echo "FAIL: proxy 2 is not caching (read '$CACHED' straight through), so the"
  echo "      staleness this drill exists to pin cannot be demonstrated. Check"
  echo "      CPTENANTCACHE and --cache-ttl-ms."; exit 1; }
echo "  proxy 2 holds a cached entry for ncx"

echo "== client A writes through proxy 1"
T_WRITE=$(date +%s)
[ "$(p1 SET ncx v2)" = "OK" ] || { echo "FAIL: A's write through proxy 1"; exit 1; }

echo "== read-your-own-writes holds for the client that wrote"
[ "$(p1 GET ncx)" = "v2" ] || { echo "FAIL: A read its OWN write as stale through proxy 1 — same-proxy invalidation is broken"; exit 1; }
echo "  A sees v2 through proxy 1"

echo "== and B does NOT, through proxy 2 — the documented cross-client window"
SAW=$(p2 GET ncx)
ELAPSED=$(( $(date +%s) - T_WRITE ))
if [ "$SAW" != "v1" ]; then
  echo "FAIL (AND THIS MAY BE GOOD NEWS): B saw '$SAW' ${ELAPSED}s after the write."
  echo "      This drill PINS cache.rs's stated contract — a write through"
  echo "      ANOTHER proxy is visible only when the TTL lapses. If something"
  echo "      now invalidates across proxies, that is ADR-0031's v2 and THIS"
  echo "      ASSERTION IS WHAT NEEDS UPDATING, not the proxy."
  exit 1
fi
echo "  B still sees v1 ${ELAPSED}s after A's write (ttl ${TTL_MS}ms)"

echo "== the TTL is the contract, so it must actually expire"
# Without this the drill would pass against a cache that never expires, which
# is a far worse defect than the one above and looks identical from here.
sleep $(( TTL_MS / 1000 + 2 ))
AFTER=$(p2 GET ncx)
[ "$AFTER" = "v2" ] || { echo "FAIL: B still sees '$AFTER' after the TTL lapsed — the bound the contract rests on does not hold"; exit 1; }
echo "  B sees v2 once the TTL lapses"

echo "PASS: read-your-own-writes holds through one proxy, the cross-client window is real and is bounded by the TTL"
