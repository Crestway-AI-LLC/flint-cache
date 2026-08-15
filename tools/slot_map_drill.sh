#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Option B drill — the CP is the durable source of truth for slot ownership.
#   - migrate one (ns, slot) between pairs, then commit CPSETSLOT
#   - a COLD-restarted proxy routes the migrated slot correctly from its
#     FIRST snapshot: moved_learned_total stays 0 (zero -MOVED discovery)
#   - the exception survives a CP restart (durable), lists via CPSLOTS,
#     counts in CPINFO, and retires via CPCLEARSLOT
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-slotmap 7101 7102 7840 7995
fleet_guard
CP=./target/release/flint-controlplane
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=$FLINT_DRILL_ROOT/flint-slotmap; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; sleep 0.4
cleanup() {
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; rm -rf "$D"
}
trap cleanup EXIT

echo "== fleet: CP, two pairs, proxy, one tenant"
$CP --port 7840 --state "$D/cp" 2>/dev/null &
fleet_wait_ping 7840
fleet_cp 7840 CPADDPROXY 127.0.0.1:7995
fleet_cp 7840 CPADDPAIR 127.0.0.1:7101
fleet_cp 7840 CPADDPAIR 127.0.0.1:7102
fleet_cp 7840 CPADDTENANT acme tok-acme acme 1
$B --port 7101 --engine rocks --data-dir "$D/a" --advertise 127.0.0.1:7101 2>/dev/null &
$B --port 7102 --engine rocks --data-dir "$D/b" --advertise 127.0.0.1:7102 2>/dev/null &
$PX --port 7995 --control-plane 127.0.0.1:7840 --advertise 127.0.0.1:7995 2>/dev/null &
fleet_wait_listen 7101 7102 7995
sleep 1.5
A="valkey-cli -p 7995 -a tok-acme --no-auth-warning"

echo "== seed 50 keys under one hash tag through the proxy"
for i in $(seq 1 50); do $A SET "{ob}:key$i" "v$i" >/dev/null; done
[ "$($A GET '{ob}:key7')" = "v7" ] || { echo "FAIL: seed"; exit 1; }

# Who holds the slot? (ns=acme rows live on exactly one node.)
SRC=""; DST=""; SLOT=""
for port in 7101 7102; do
  ROW=$(valkey-cli -p $port FLINTSLOTSTATS 2>/dev/null | grep "acme$" | head -1)
  if [ -n "$ROW" ]; then SRC=$port; SLOT=$(echo "$ROW" | awk '{print $1}'); fi
done
[ "$SRC" = "7101" ] && DST=7102 || DST=7101
[ -n "$SLOT" ] || { echo "FAIL: could not locate the tag slot"; exit 1; }
echo "  slot $SLOT (ns acme) lives on :$SRC; migrating to :$DST"

echo "== migrate + commit the truth (CPSETSLOT by member address)"
RES=$(valkey-cli -p $DST FLINTMIGRATEIN "127.0.0.1:$SRC" "$SLOT" "127.0.0.1:$DST" acme 2>&1)
echo "$RES" | grep -q "MIGRATEIN-OK" || { echo "FAIL: migrate: $RES"; exit 1; }
[ "$(valkey-cli -p 7840 CPSETSLOT acme "$SLOT" "127.0.0.1:$DST")" = "OK" ] || { echo "FAIL: CPSETSLOT"; exit 1; }
valkey-cli -p 7840 CPSLOTS | grep -q "acme $SLOT" || { echo "FAIL: CPSLOTS missing row"; exit 1; }
valkey-cli -p 7840 CPINFO | grep -q "slot_exceptions:1" || { echo "FAIL: CPINFO count"; exit 1; }
echo "  committed: CPSLOTS row + CPINFO slot_exceptions:1"

echo "== the truth survives a CP restart (durable, not session state)"
pkill -9 -f "flint-controlplane --port 7840"; sleep 0.3
$CP --port 7840 --state "$D/cp" 2>/dev/null &
fleet_wait_ping 7840
valkey-cli -p 7840 CPSLOTS | grep -q "acme $SLOT" || { echo "FAIL: exception lost across CP restart"; exit 1; }
echo "  CP restarted; exception row intact"

echo "== COLD proxy: correct routing from the first snapshot, zero -MOVED"
pkill -9 -f "flint-proxy --port 7995"; sleep 0.3
$PX --port 7995 --control-plane 127.0.0.1:7840 --advertise 127.0.0.1:7995 2>/dev/null &
fleet_wait_listen 7995
sleep 1.5
OK=1
for i in 1 13 27 42 50; do
  [ "$($A GET "{ob}:key$i")" = "v$i" ] || { OK=0; echo "  MISS key$i"; }
done
[ "$OK" = "1" ] || { echo "FAIL: cold proxy misrouted migrated keys"; exit 1; }
MOVED=$(valkey-cli -p 7995 PROXYSTATS 2>/dev/null | tr -d '\r' | grep "^moved_learned_total:" | cut -d: -f2)
[ "$MOVED" = "0" ] || { echo "FAIL: cold proxy learned $MOVED -MOVED redirects (should be 0)"; exit 1; }
echo "  all migrated keys served; moved_learned_total=0 — snapshot truth, not discovery"

echo "== writes land on the new owner through the cold proxy"
$A SET "{ob}:fresh" hello >/dev/null
V=$(valkey-cli -p $DST FLINTNS acme >/dev/null 2>&1; python3 - <<PY
import socket
def resp(a): return f"*{len(a)}\r\n".encode()+b"".join(f"\${len(x)}\r\n{x}\r\n".encode() for x in a)
s=socket.create_connection(("127.0.0.1",$DST),timeout=3)
s.sendall(resp(["FLINTNS","acme"])); s.recv(64)
s.sendall(resp(["GET","{ob}:fresh"]))
b=b""
while not b.endswith(b"\r\n"): b+=s.recv(256)
print(b.decode(errors="replace").strip().split("\r\n")[-1])
PY
)
[ "$V" = "hello" ] || { echo "FAIL: fresh write not on new owner (got '$V')"; exit 1; }
echo "  fresh write present on :$DST (the committed owner)"

echo "== retire the row (consolidation path)"
[ "$(valkey-cli -p 7840 CPCLEARSLOT acme "$SLOT")" = "OK" ] || { echo "FAIL: CPCLEARSLOT"; exit 1; }
valkey-cli -p 7840 CPSLOTS | grep -q "acme $SLOT" && { echo "FAIL: row survived clear"; exit 1; }
echo "  cleared; CPSLOTS empty"

echo "== consolidation: adjacent commits compress; move-backs self-retire"
fleet_cp 7840 CPSETSLOT acme 1000 1
fleet_cp 7840 CPSETSLOT acme 1001 1
fleet_cp 7840 CPSETSLOT acme 1002 1
ROWS=$(valkey-cli -p 7840 CPSLOTS | grep -c "acme")
RUN=$(valkey-cli -p 7840 CPSLOTS | grep "acme 1000")
[ "$ROWS" = "1" ] && echo "$RUN" | grep -q "acme 1000 1002 1" || { echo "FAIL: adjacent commits did not compress ($ROWS rows: $RUN)"; exit 1; }
SNAP=$(valkey-cli -p 7840 CPSNAPSHOT 127.0.0.1:7995 | sed -n '6p')
echo "$SNAP" | grep -q "acme:1000-1002:1" || { echo "FAIL: run form missing from snapshot ($SNAP)"; exit 1; }
echo "  three commits -> ONE row (acme 1000 1002 1); snapshot carries acme:1000-1002:1"
# Interior move-back: splits the run AND self-retires (default owner).
fleet_cp 7840 CPSETSLOT acme 1001 0
ROWS=$(valkey-cli -p 7840 CPSLOTS | grep -c "acme")
[ "$ROWS" = "2" ] || { echo "FAIL: interior move-back should split to 2 rows (got $ROWS)"; exit 1; }
[ "$(valkey-cli -p 7840 CPCONSOLIDATE)" = "2" ] || { echo "FAIL: CPCONSOLIDATE count"; exit 1; }
# Full move-back retires everything without CPCLEARSLOT.
fleet_cp 7840 CPSETSLOT acme 1000 0
fleet_cp 7840 CPSETSLOT acme 1002 0
[ "$(valkey-cli -p 7840 CPSLOTS | grep -c acme)" = "0" ] || { echo "FAIL: move-backs did not self-retire"; exit 1; }
echo "  interior split -> 2 rows; CPCONSOLIDATE=2; move-backs self-retired to 0 rows"

echo "PASS: Option B — slot ownership committed at cutover is durable CP truth; a cold proxy routes fragmented ownership from its first snapshot with ZERO -MOVED discovery; rows list, count, survive restart, retire — and adjacent commits consolidate into runs while move-backs self-retire"
