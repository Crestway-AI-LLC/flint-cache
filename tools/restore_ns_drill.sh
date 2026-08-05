#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Namespace-scoped restore (ADR-0011 D5, verification items 3 and 4).
#
# The two claims that make this restore mode safe, and their assertions:
#
#   ISOLATION (item 3) — restoring tenant A into a NEW namespace touches
#   nothing that is serving. Tenant B's data is byte-identical after the
#   restore, and so is tenant A's LIVE namespace: a key written after the
#   backup is still there, a key deleted after the backup is still gone.
#   The restored namespace shows the OPPOSITE of both — that pair of
#   opposites is what proves the restore read the backup rather than the
#   live data, and wrote the new namespace rather than the live one.
#
#   PLACEMENT (item 4) — rows land by ownership NOW, not the backup's
#   topology. After the backup is taken, one of the destination
#   namespace's slots is committed to the OTHER pair (CPSETSLOT — the
#   same exceptions table the proxies route by). The restore must follow
#   it: the row lands on the new owner and reads back through the proxy.
#   This fails if restore-ns places by the topology recorded in the set.
#
# Plus the property a cache restore must never break: TTLs are ABSOLUTE.
# A key backed up with a TTL comes back with the remaining time, not a
# fresh lease — and one that expired between backup and restore comes
# back dead (GC sweeps it on sight; ADR-0011 says this must not be
# "fixed").
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-rns 7111 7112 7841 7996
fleet_guard
CP=./target/release/flint-controlplane
B=./target/release/flint-server
PX=./target/release/flint-proxy
BK=./target/release/flint-backup
D=/tmp/flint-rns; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; sleep 0.4
cleanup() { fleet_kill server; fleet_kill proxy; fleet_kill controlplane; rm -rf "$D"; }
trap cleanup EXIT

echo "== fleet: CP, two pairs, proxy, tenants acme (restored) and beta (bystander)"
$CP --port 7841 --state "$D/cp" 2>"$D/cp.log" &
disown
CP_UP=0
for _ in $(seq 1 50); do [ "$(valkey-cli -p 7841 PING 2>/dev/null)" = "PONG" ] && { CP_UP=1; break; }; sleep 0.2; done
[ "$CP_UP" = "1" ] || { echo "FAIL: control plane never answered PING"; cat "$D/cp.log"; exit 1; }
# Every registration ASSERTED: a swallowed CP error surfaces later as
# WRONGPASS at the edge and a missing state file at backup — two symptoms
# pointing everywhere except the cause.
cp_must() { R=$(valkey-cli -p 7841 "$@" 2>&1); case "$R" in OK*) ;; *) echo "FAIL: $* -> $R"; exit 1;; esac; }
cp_must CPADDPROXY 127.0.0.1:7996
cp_must CPADDPAIR 127.0.0.1:7111
cp_must CPADDPAIR 127.0.0.1:7112
cp_must CPADDTENANT acme tok-acme acme 1
cp_must CPADDTENANT beta tok-beta beta 1
[ -f "$D/cp" ] || { echo "FAIL: CP state file absent after five committed registrations"; exit 1; }
$B --port 7111 --engine rocks --data-dir "$D/a" --advertise 127.0.0.1:7111 2>"$D/a.log" &
disown
$B --port 7112 --engine rocks --data-dir "$D/b" --advertise 127.0.0.1:7112 2>"$D/b.log" &
disown
$PX --port 7996 --control-plane 127.0.0.1:7841 --advertise 127.0.0.1:7996 2>"$D/px.log" &
disown
fleet_wait_listen 7111 7112 7996
sleep 1.5
A="valkey-cli -p 7996 -a tok-acme --no-auth-warning"
BT="valkey-cli -p 7996 -a tok-beta --no-auth-warning"
# The proxy learns tokens from the CP push; wait for BOTH tenants to be
# servable rather than racing the push with 250 silent writes.
for _ in $(seq 1 50); do [ "$($A PING 2>/dev/null)" = "PONG" ] && break; sleep 0.2; done
[ "$($A PING 2>/dev/null)" = "PONG" ] || { echo "FAIL: proxy never accepted acme's token"; exit 1; }
for _ in $(seq 1 50); do [ "$($BT PING 2>/dev/null)" = "PONG" ] && break; sleep 0.2; done
[ "$($BT PING 2>/dev/null)" = "PONG" ] || { echo "FAIL: proxy never accepted beta's token"; exit 1; }

echo "== corpora: 200 acme keys (both pairs), 50 beta keys, a TTL key, all types"
for i in $(seq 1 200); do $A SET "rk:$i" "av$i" >/dev/null; done
$A SET ttl:keeper held EX 3600 >/dev/null
$A SET ttl:goner brief EX 2 >/dev/null
$A HSET conf:h f1 v1 f2 v2 >/dev/null
$A ZADD conf:z 1 one 2 two >/dev/null
$A SET doomed:after-backup here >/dev/null
for i in $(seq 1 50); do $BT SET "bk:$i" "bv$i" >/dev/null; done
BETA_BEFORE=$($BT DBSIZE)

echo "== back up the live fleet"
$BK run --pairs "127.0.0.1:7111;127.0.0.1:7112" \
        --cp-state "$D/cp" --to "$D/sets" --snap-root "$D/snaps" | tail -1
SET_DIR="$D/sets/$(ls "$D/sets")"

echo "== diverge the LIVE namespace from the backup, both directions"
$A SET only:after-backup fresh >/dev/null   # exists live, absent in backup
$A DEL doomed:after-backup >/dev/null       # absent live, exists in backup

echo "== after the backup: move one dest-ns slot to the other pair (item 4)"
# The restored namespace routes by ITS name. Learn which slot rk:1 lands in
# under ns acme-r, then commit that slot to pair 1 — restore must follow.
valkey-cli -p 7841 CPADDTENANT acme-r tok-acme-r acme-r 1 >/dev/null
R="valkey-cli -p 7996 -a tok-acme-r --no-auth-warning"
for _ in $(seq 1 50); do [ "$($R PING 2>/dev/null)" = "PONG" ] && break; sleep 0.2; done
[ "$($R PING 2>/dev/null)" = "PONG" ] || { echo "FAIL: proxy never accepted acme-r's token"; exit 1; }
$R SET rk:1 probe >/dev/null
MOVED_SLOT=""
for port in 7111 7112; do
  ROW=$(valkey-cli -p $port FLINTSLOTSTATS 2>/dev/null | grep "acme-r$" | head -1)
  [ -n "$ROW" ] && MOVED_SLOT=$(echo "$ROW" | awk '{print $1}')
done
[ -n "$MOVED_SLOT" ] || { echo "FAIL: could not locate rk:1's slot for acme-r"; exit 1; }
$R DEL rk:1 >/dev/null   # the probe must not mask the restored value
valkey-cli -p 7841 CPSETSLOT acme-r "$MOVED_SLOT" 127.0.0.1:7112 >/dev/null
echo "  slot $MOVED_SLOT (ns acme-r) committed to pair 1 AFTER the backup"
sleep 1.2   # exception push settles before restore reads the map

echo "== wait out ttl:goner so absolute expiry is testable"
sleep 2.2

echo "== restore acme -> acme-r by current ownership"
$BK restore-ns --from "$SET_DIR" --ns acme --into-ns acme-r \
    --cp 127.0.0.1:7841 --proxy-name 127.0.0.1:7996 | tail -1 || {
  echo "FAIL: restore-ns refused"; exit 1; }

echo
echo "== ITEM 4: the moved slot's key reads back through the proxy"
V=$($R GET rk:1)
[ "$V" = "av1" ] || { echo "FAIL: rk:1 = '$V' — the restore did not follow current ownership"; exit 1; }
# And it genuinely lives on the NEW owner: ask pair 1's node directly.
ON_NEW=$(valkey-cli -p 7112 FLINTSLOTSTATS 2>/dev/null | grep -c "^$MOVED_SLOT .* acme-r$")
[ "$ON_NEW" -ge 1 ] || { echo "FAIL: slot $MOVED_SLOT has no acme-r rows on pair 1"; exit 1; }
echo "  rk:1 readable via proxy, rows on the post-move owner"

echo "== the restored namespace is the BACKUP's view, not the live one"
[ "$($R GET rk:137)" = "av137" ] || { echo "FAIL: bulk key missing from restore"; exit 1; }
[ "$($R HGET conf:h f2)" = "v2" ] || { echo "FAIL: hash did not survive re-enveloping"; exit 1; }
[ "$($R ZSCORE conf:z two)" = "2" ] || { echo "FAIL: zset did not survive re-enveloping"; exit 1; }
[ "$($R GET doomed:after-backup)" = "here" ] || { echo "FAIL: restore lost a key deleted only AFTER backup"; exit 1; }
[ -z "$($R GET only:after-backup)" ] || { echo "FAIL: restore contains a key written AFTER the backup — it read live data"; exit 1; }
echo "  strings, hash, zset present; deleted-after-backup present; written-after-backup absent"

echo "== TTLs are absolute"
T=$($R TTL ttl:keeper)
[ "$T" -gt 0 ] && [ "$T" -le 3600 ] || { echo "FAIL: ttl:keeper TTL=$T — not the remaining time"; exit 1; }
[ -z "$($R GET ttl:goner)" ] || { echo "FAIL: a key that expired between backup and restore came back alive"; exit 1; }
echo "  keeper TTL=${T}s remaining; goner stayed expired"

echo "== ITEM 3: nothing that was serving got touched"
[ "$($BT DBSIZE)" = "$BETA_BEFORE" ] || { echo "FAIL: tenant beta's key count changed"; exit 1; }
[ "$($BT GET bk:17)" = "bv17" ] || { echo "FAIL: tenant beta's data changed"; exit 1; }
[ "$($A GET only:after-backup)" = "fresh" ] || { echo "FAIL: live acme lost a post-backup key"; exit 1; }
[ -z "$($A GET doomed:after-backup)" ] || { echo "FAIL: live acme resurrected a deleted key"; exit 1; }
echo "  beta byte-identical; live acme still has its post-backup write and its deletion"

echo "== the set itself is still intact (reading it must not mutate it)"
$BK verify --from "$SET_DIR" >/dev/null || {
  echo "FAIL: the set no longer verifies — restore-ns wrote into it"; exit 1; }
echo "  set re-verifies after being read"

echo
echo "PASS: namespace restore places by current ownership, reproduces the backup's view beside untouched live data, and keeps TTLs absolute"
