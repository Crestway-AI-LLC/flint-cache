#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Control plane v1 drill:
#   - CP owns the tenant registry + topology; two proxies subscribe (CPWATCH)
#     and are fed ONLY their assigned tenants (shuffle-shard sub-groups)
#   - tenants added AT RUNTIME appear on their proxies within a push cycle,
#     no restarts; AUTH on a non-assigned proxy is refused (sub-group
#     enforcement = blast-radius/connection bounding)
#   - CPSETSUBSET re-assigns live (whale isolation / drain)
#   - CP state survives restart; CP OUTAGE does not touch the data path
set -u
cd "$(dirname "$0")/.."
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null; pkill -9 -f flint-controlplane 2>/dev/null; sleep 0.4
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
STATE=/tmp/flint-cp-drill-state
cleanup() {
  pkill -9 -f "flint-server --port 673" 2>/dev/null
  pkill -9 -f flint-proxy 2>/dev/null
  pkill -9 -f flint-controlplane 2>/dev/null
  rm -rf /tmp/flint-cpd-* "$STATE" "$STATE.tmp"
}
trap cleanup EXIT
rm -f "$STATE"

echo "== data plane: two single-master pairs"
for p in 6730 6740; do
  d="/tmp/flint-cpd-$p"; rm -rf "$d"
  $B --port $p --engine rocks --data-dir "$d" 2>/dev/null &
done
sleep 0.8

echo "== control plane + registrations"
$CP --port 7500 --state "$STATE" 2>/tmp/flint-cpd-cp.log &
sleep 0.5
valkey-cli -p 7500 CPADDPROXY 127.0.0.1:7601 >/dev/null
valkey-cli -p 7500 CPADDPROXY 127.0.0.1:7602 >/dev/null
valkey-cli -p 7500 CPADDPAIR 127.0.0.1:6730 >/dev/null
valkey-cli -p 7500 CPADDPAIR 127.0.0.1:6740 >/dev/null

echo "== two proxies in control-plane mode (no --pairs/--tenants flags)"
$PX --port 7601 --control-plane 127.0.0.1:7500 --advertise 127.0.0.1:7601 2>/tmp/flint-cpd-p1.log &
$PX --port 7602 --control-plane 127.0.0.1:7500 --advertise 127.0.0.1:7602 2>/tmp/flint-cpd-p2.log &
sleep 1.5

echo "== add tenant with k=1: assigned to exactly one proxy (shuffle shard)"
R=$(valkey-cli -p 7500 CPADDTENANT acme tok-acme acme 1)
echo "  $R"
SUB=$(echo "$R" | grep -oE '\[[^]]*\]' | tr -d '[]')
[ -n "$SUB" ] || { echo "FAIL: no subset in reply: $R"; exit 1; }
APORT=${SUB##*:}
OPORT=$([ "$APORT" = "7601" ] && echo 7602 || echo 7601)
sleep 1.5   # push cycle

echo "== sub-group enforcement: AUTH works ONLY on the assigned proxy"
W=$(valkey-cli -p "$APORT" -a tok-acme --no-auth-warning SET hello world 2>&1)
[ "$W" = "OK" ] || { echo "FAIL: assigned proxy :$APORT rejected tenant: $W"; tail -4 /tmp/flint-cpd-p1.log /tmp/flint-cpd-p2.log; exit 1; }
G=$(valkey-cli -p "$APORT" -a tok-acme --no-auth-warning GET hello)
[ "$G" = "world" ] || { echo "FAIL: data path via CP-fed proxy: '$G'"; exit 1; }
X=$(valkey-cli -p "$OPORT" -a tok-acme --no-auth-warning GET hello 2>&1)
echo "$X" | grep -q "WRONGPASS" || { echo "FAIL: non-assigned proxy :$OPORT accepted the token: $X"; exit 1; }
echo "  assigned :$APORT serves; other :$OPORT refuses (WRONGPASS) — blast radius bounded"

echo "== runtime add with k=2: appears on BOTH proxies, no restarts"
valkey-cli -p 7500 CPADDTENANT globex tok-glx globex 2 >/dev/null
sleep 1.5
for p in 7601 7602; do
  W=$(valkey-cli -p $p -a tok-glx --no-auth-warning SET g 1 2>&1)
  [ "$W" = "OK" ] || { echo "FAIL: globex not live on :$p: $W"; exit 1; }
done
echo "  globex live on both proxies within one push cycle"

echo "== CPSETSUBSET: drain globex to :$APORT only (live re-assignment)"
valkey-cli -p 7500 CPSETSUBSET globex "127.0.0.1:$APORT" >/dev/null
sleep 1.5
X=$(valkey-cli -p "$OPORT" -a tok-glx --no-auth-warning SET g 2 2>&1)
echo "$X" | grep -q "WRONGPASS" || { echo "FAIL: drained proxy :$OPORT still accepts globex: $X"; exit 1; }
W=$(valkey-cli -p "$APORT" -a tok-glx --no-auth-warning SET g 2 2>&1)
[ "$W" = "OK" ] || { echo "FAIL: retained proxy :$APORT lost globex: $W"; exit 1; }
echo "  drained live: :$OPORT refuses, :$APORT serves"

echo "== CP outage: data path unaffected; restart restores durable state"
V_BEFORE=$(valkey-cli -p 7500 CPINFO | tr '\r' '\n' | grep "^version" | cut -d: -f2)
pkill -9 -f flint-controlplane; sleep 0.5
W=$(valkey-cli -p "$APORT" -a tok-acme --no-auth-warning SET during-outage ok 2>&1)
[ "$W" = "OK" ] || { echo "FAIL: data path depends on CP being up: $W"; exit 1; }
[ "$(valkey-cli -p "$APORT" -a tok-acme --no-auth-warning GET during-outage)" = "ok" ] || { echo "FAIL: read during outage"; exit 1; }
$CP --port 7500 --state "$STATE" 2>>/tmp/flint-cpd-cp.log &
sleep 1
V_AFTER=$(valkey-cli -p 7500 CPINFO | tr '\r' '\n' | grep "^version" | cut -d: -f2)
[ "$V_AFTER" = "$V_BEFORE" ] || { echo "FAIL: state lost across restart ($V_BEFORE -> $V_AFTER)"; exit 1; }
echo "  wrote+read during CP outage; restart restored version $V_AFTER"

echo "== tenant added after CP restart still propagates (watch reconnected)"
valkey-cli -p 7500 CPADDTENANT initech tok-ini initech 2 >/dev/null
OK=0
for i in $(seq 1 10); do
  [ "$(valkey-cli -p 7601 -a tok-ini --no-auth-warning SET i 1 2>&1)" = "OK" ] && { OK=1; break; }
  sleep 0.5
done
[ "$OK" = "1" ] || { echo "FAIL: post-restart tenant never propagated"; exit 1; }
echo "  initech live after CP restart — subscriptions self-heal"

echo "PASS: control plane v1 — durable registry, shuffle-shard sub-groups enforced, live pushes, CP outage off the data path"
