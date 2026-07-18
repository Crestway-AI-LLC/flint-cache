#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# CP publication drill: the last two control-plane M3 items.
#   A) DELTA SUPPRESSION: a registry version bump whose FILTERED view for a
#      proxy is unchanged is ACKed locally, not pushed — with shuffle
#      sharding most mutations touch few proxies, so most pushes are no-ops.
#      The suppressed watcher must NOT be wedged: a later change that DOES
#      affect it still arrives.
#   B) DNS SUBSET PUBLICATION: CPDNSZONE renders the authoritative zone —
#      each tenant's name resolves to ONLY its subset's hosts (the client's
#      path to its sub-group without a bootstrap service).
set -u
cd "$(dirname "$0")/.."
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
D=/tmp/flint-pub; rm -rf "$D"; mkdir -p "$D"
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
pkill -9 -f flint-controlplane 2>/dev/null; sleep 0.4
cleanup() {
  pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
  pkill -9 -f flint-controlplane 2>/dev/null; rm -rf "$D"
}
trap cleanup EXIT

$B --port 6890 --engine rocks --data-dir "$D/n" 2>/dev/null &
sleep 0.6
$CP --port 7595 --state "$D/cp" 2>"$D/cp.log" &
sleep 0.5
valkey-cli -p 7595 CPADDPROXY 127.0.0.1:7821 >/dev/null
valkey-cli -p 7595 CPADDPROXY 127.0.0.1:7822 >/dev/null
valkey-cli -p 7595 CPADDPAIR 127.0.0.1:6890 >/dev/null
$PX --port 7821 --control-plane 127.0.0.1:7595 --advertise 127.0.0.1:7821 2>/dev/null &
$PX --port 7822 --control-plane 127.0.0.1:7595 --advertise 127.0.0.1:7822 2>/dev/null &
sleep 1.5

echo "== A) tenant pinned to proxy1 only: proxy2's push is SUPPRESSED"
valkey-cli -p 7595 CPADDTENANT acme tok-acme acme 1 >/dev/null
valkey-cli -p 7595 CPSETSUBSET acme 127.0.0.1:7821 >/dev/null
sleep 1.2
# Deterministic no-op bump: re-assert the same subset. The version advances
# but NEITHER proxy's filtered view changes — both watchers must suppress.
valkey-cli -p 7595 CPSETSUBSET acme 127.0.0.1:7821 >/dev/null
sleep 1.2
grep -q "watch 127.0.0.1:7822: suppressed push" "$D/cp.log" \
  || { echo "FAIL: no suppression logged for the unaffected proxy"; grep watch "$D/cp.log"; exit 1; }
[ "$(valkey-cli -p 7821 -a tok-acme --no-auth-warning SET pk pv)" = "OK" ] \
  || { echo "FAIL: affected proxy did not receive the push"; exit 1; }
V2=$(valkey-cli -p 7822 -a tok-acme --no-auth-warning PING 2>&1)
echo "$V2" | grep -q "WRONGPASS" || { echo "FAIL: proxy2 should not hold acme's token (got: $V2)"; exit 1; }
echo "  proxy1 pushed + serves; proxy2 suppressed + holds no foreign token"

echo "== A2) the suppressed watcher is NOT wedged: a change FOR proxy2 arrives"
valkey-cli -p 7595 CPSETSUBSET acme 127.0.0.1:7822 >/dev/null
OK=0
for i in $(seq 1 20); do
  [ "$(valkey-cli -p 7822 -a tok-acme --no-auth-warning GET pk 2>/dev/null)" = "pv" ] && { OK=1; break; }
  sleep 0.4
done
[ "$OK" = "1" ] || { echo "FAIL: proxy2 never received its own update after suppression"; exit 1; }
echo "  re-subset to proxy2 -> pushed and serving (suppression is per-view, not a wedge)"

echo "== B) CPDNSZONE renders each tenant -> ONLY its subset"
valkey-cli -p 7595 CPADDTENANT globex tok-glx globex 2 >/dev/null
ZONE=$(valkey-cli -p 7595 CPDNSZONE flint.test)
echo "$ZONE" | sed 's/^/  | /'
# acme is pinned to exactly one member; globex shuffle-sharded across 2.
A_COUNT=$(echo "$ZONE" | grep -c "^acme.flint.test. 30 IN A ")
G_COUNT=$(echo "$ZONE" | grep -c "^globex.flint.test. 30 IN A ")
[ "$A_COUNT" = "1" ] || { echo "FAIL: acme should have exactly 1 A record (got $A_COUNT)"; exit 1; }
[ "$G_COUNT" = "2" ] || { echo "FAIL: globex should have exactly 2 A records (got $G_COUNT)"; exit 1; }
echo "$ZONE" | grep -q "acme.flint.test. 30 IN A 127.0.0.1" || { echo "FAIL: acme A record wrong"; exit 1; }
echo "  zone: acme=1 record (pinned), globex=2 (shuffle subset) — publication-ready"

echo "PASS: CP publication — no-op pushes suppressed without wedging watchers; DNS zone maps each tenant to exactly its subset"
