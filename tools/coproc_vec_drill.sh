#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0017: the first real co-processor — flint-vec — end to end through the
# proxy, and its crash-durability (D3 rebuild + LOADING).
#
# Unlike coproc_forward (a stand-in that only stores), this runs the ACTUAL
# flint-vec binary as the VEC. family's co-processor and asserts:
#   - VEC.CREATE/SET/SEARCH/GET/INFO/DEL serve end to end; SEARCH returns the
#     correct nearest-neighbour order; a write's durable rows land in KV
#   - CRASH DURABILITY (D3): kill the co-processor (its index is memory-only),
#     restart it EMPTY, and the first SEARCH answers -LOADING, rebuilds the
#     index from the durable rows, and then returns the SAME results — proving
#     the vectors survived in the namespace, not just in the co-processor
#   - the ordinary tenant data path is untouched (control)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-vecd 6676 6677 6678
fleet_guard
B=./target/release/flint-server
PX=./target/release/flint-proxy
VEC=./target/release/flint-vec
D=$FLINT_DRILL_ROOT/flint-vecd; rm -rf "$D"; mkdir -p "$D"
COPROC_PID=""
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() {
  [ -n "$COPROC_PID" ] && kill -9 "$COPROC_PID" 2>/dev/null
  fleet_kill server; fleet_kill proxy; rm -rf "$D"
}
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy -p flint-vec --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

echo "== cluster: master + proxy (static --families) + the real flint-vec co-processor"
$VEC --port 6678 2>"$D/vec1.log" & COPROC_PID=$!
fleet_wait_listen 6678
# --engine mem: the server stays up across the co-processor restart and holds
# the durable rows, so this drill isolates the CO-PROCESSOR's rebuild, not the
# server's own restart durability (that is repl/warm_restart's job).
$B --port 6676 --engine mem 2>"$D/n.log" & fleet_wait_listen 6676; fleet_wait_ping 6676
$PX --port 6677 --pairs 127.0.0.1:6676 --tenants "tok=ns" \
    --families "VEC.=127.0.0.1:6678" --edge-advertise 127.0.0.1:6677 2>"$D/px.log" & fleet_wait_listen 6677
for _ in $(seq 1 60); do case "$(valkey-cli -p 6677 PING 2>&1)" in *NOAUTH*|PONG) break;; esac; sleep 0.1; done

A="valkey-cli -p 6677 -a tok --no-auth-warning"
# The client contract for a warming index: retry on -LOADING. The first touch of
# a cold namespace warms it (an empty rebuild here), then commands serve.
vexec() {
  local i out
  for i in $(seq 1 50); do
    out="$($A "$@" 2>&1 | tr -d '\r' | tr '\n' ' ')"
    case "$out" in *LOADING*) sleep 0.15 ;; *) echo "$out"; return 0 ;; esac
  done
  echo "$out"; return 1
}

echo "== VEC.* end to end"
[ "$(vexec VEC.CREATE docs DIM 3 METRIC l2)" = "OK " ] \
  || { echo "FAIL: VEC.CREATE"; exit 1; }
for kv in "a 1,0,0" "b 0,1,0" "c 0,0,1"; do
  set -- $kv
  [ "$(vexec VEC.SET docs "$1" "$2")" = "OK " ] || { echo "FAIL: VEC.SET $1"; exit 1; }
done
PRE="$(vexec VEC.SEARCH docs 0.9,0.1,0 2)"
case "$PRE" in *a*b*) : ;; *) echo "FAIL: SEARCH order, got: $PRE"; exit 1 ;; esac
case "$(vexec VEC.GET docs a)" in *"1,0,0"*) : ;; *) echo "FAIL: VEC.GET"; exit 1 ;; esac
case "$(vexec VEC.INFO docs)" in *count*3*) : ;; *) echo "FAIL: VEC.INFO count"; exit 1 ;; esac
echo "  VEC.SEARCH -> $PRE"

echo "== a second set on the HNSW engine (INDEX hnsw) serves through the same surface"
[ "$(vexec VEC.CREATE docsh DIM 3 METRIC l2 INDEX hnsw)" = "OK " ] \
  || { echo "FAIL: VEC.CREATE INDEX hnsw"; exit 1; }
for kv in "a 1,0,0" "b 0,1,0" "c 0,0,1"; do
  set -- $kv
  [ "$(vexec VEC.SET docsh "$1" "$2")" = "OK " ] || { echo "FAIL: VEC.SET docsh $1"; exit 1; }
done
PREH="$(vexec VEC.SEARCH docsh 0.9,0.1,0 2)"
case "$PREH" in *a*b*) : ;; *) echo "FAIL: HNSW SEARCH order, got: $PREH"; exit 1 ;; esac
case "$(vexec VEC.INFO docsh)" in *index*hnsw*) : ;; *) echo "FAIL: VEC.INFO should report the hnsw engine"; exit 1 ;; esac
echo "  HNSW VEC.SEARCH -> $PREH"

echo "== durable rows exist in KV independent of the co-processor"
# 15 = docs (1 config + 3 vectors) + docsh (1 config + 3 vectors)
#    +  1 set-name index ('s', one per namespace, unbucketed)
#    +  6 id-index buckets ('i', one per (set, bucket) that holds an id;
#       a,b,c hash to distinct buckets in each of the two sets)
# The last two are #194's durable index — the co-processor used to find its
# vectors by SCANning the whole tenant keyspace on every cold start. A change
# to the durable layout is SUPPOSED to move this number; the breakdown above
# and the key dump below are what make the next move take a minute.
[ "$($A DBSIZE 2>&1 | tr -d '\r')" = "15" ] \
  || { echo "FAIL: expected 15 durable keys (2 configs + 6 vectors + 1 set-name index + 6 id-index buckets), got $($A DBSIZE)"
       $A SCAN 0 COUNT 200 2>/dev/null | sed 's/^/    key| /'; exit 1; }

echo "== CRASH DURABILITY: kill the co-processor, restart it EMPTY, SEARCH rebuilds"
kill -9 "$COPROC_PID" 2>/dev/null; wait "$COPROC_PID" 2>/dev/null; COPROC_PID=""
sleep 0.3
$VEC --port 6678 2>"$D/vec2.log" & COPROC_PID=$!
fleet_wait_listen 6678
POST="$(vexec VEC.SEARCH docs 0.9,0.1,0 2)"
[ "$POST" = "$PRE" ] \
  || { echo "FAIL: rebuild did not restore the index. pre=[$PRE] post=[$POST]"; exit 1; }
case "$(vexec VEC.INFO docs)" in *count*3*) : ;; *) echo "FAIL: count after rebuild"; exit 1 ;; esac
case "$(vexec VEC.GET docs a)" in *"1,0,0"*) : ;; *) echo "FAIL: GET after rebuild"; exit 1 ;; esac
grep -qi "rebuilt ns" "$D/vec2.log" || { echo "FAIL: no rebuild logged"; exit 1; }
echo "  post-restart VEC.SEARCH -> $POST  (rebuilt from durable rows)"

# The HNSW set must rebuild AS hnsw: the durable config records the engine kind,
# so the co-processor restores the graph, not the flat default.
POSTH="$(vexec VEC.SEARCH docsh 0.9,0.1,0 2)"
[ "$POSTH" = "$PREH" ] \
  || { echo "FAIL: HNSW rebuild changed results. pre=[$PREH] post=[$POSTH]"; exit 1; }
case "$(vexec VEC.INFO docsh)" in *index*hnsw*) : ;; *) echo "FAIL: rebuilt set is not hnsw (kind lost in the durable config)"; exit 1 ;; esac
echo "  post-restart HNSW VEC.SEARCH -> $POSTH  (rebuilt as hnsw)"

echo "== VEC.DEL is durable too"
[ "$(vexec VEC.DEL docs a)" = "1 " ] || { echo "FAIL: VEC.DEL"; exit 1; }
case "$(vexec VEC.SEARCH docs 1,0,0 5)" in *a*) echo "FAIL: 'a' still present after DEL"; exit 1 ;; *) : ;; esac
# Captured, not asserted: VEC.DEL drops the vector row and may drop its
# id-index bucket too (if 'a' was alone in it), so the absolute number is
# layout-coupled. What D4 needs is only that it does not MOVE.
DBSIZE_BEFORE_D4="$($A DBSIZE 2>&1 | tr -d '\r')"

echo "== D4: a tiny per-namespace index-memory cap sheds new writes; reads unaffected"
kill -9 "$COPROC_PID" 2>/dev/null; wait "$COPROC_PID" 2>/dev/null; COPROC_PID=""
sleep 0.3
# 100 bytes is far below the namespace's already-durable footprint. Rebuild
# loads those rows regardless (they are already durable, ADR-0017 D3); the cap
# governs only NEW writes, so this restart comes up ALREADY over budget.
$VEC --port 6678 --index-mem-bytes 100 2>"$D/vec3.log" & COPROC_PID=$!
fleet_wait_listen 6678
# Reads still serve (the index rebuilt past the cap from the durable rows)...
case "$(vexec VEC.SEARCH docs 1,0,0 3)" in *b*) : ;; *) echo "FAIL: read after cap-restart"; exit 1 ;; esac
# ...but a fresh VEC.SET is refused with -VECFULL, BEFORE any durable write.
R="$(vexec VEC.SET docs znew 0,0,1)"
case "$R" in *VECFULL*) : ;; *) echo "FAIL: expected VECFULL over the cap, got: $R"; exit 1 ;; esac
# The shed write did not persist: durable key count is unchanged.
[ "$($A DBSIZE 2>&1 | tr -d '\r')" = "$DBSIZE_BEFORE_D4" ] \
  || { echo "FAIL: a VECFULL-shed write still persisted (DBSIZE moved off $DBSIZE_BEFORE_D4)"; exit 1; }
echo "  new VEC.SET -> ${R}(refused, not persisted); reads still served"

echo "== CONTROL: the ordinary tenant data path is untouched"
cli_ok $A SET plain value
[ "$($A GET plain)" = "value" ] || { echo "FAIL (control): tenant SET/GET broke"; exit 1; }

echo "== TTL (D7): a PX vector expires from the index; a permanent one in the same set survives"
# Restart WITHOUT the D4 cap so these writes are not shed (the prior section left
# the namespace deliberately over a 100-byte cap).
kill -9 "$COPROC_PID" 2>/dev/null; wait "$COPROC_PID" 2>/dev/null; COPROC_PID=""
sleep 0.3
$VEC --port 6678 2>"$D/vec4.log" & COPROC_PID=$!
fleet_wait_listen 6678
[ "$(vexec VEC.CREATE sess DIM 3 METRIC l2)" = "OK " ] || { echo "FAIL: VEC.CREATE sess"; exit 1; }
[ "$(vexec VEC.SET sess keep 1,0,0)" = "OK " ] || { echo "FAIL: VEC.SET sess keep"; exit 1; }
# PX 1200ms: searchable now; gone within one sweep (--sweep-ms 1000) after it lapses.
[ "$(vexec VEC.SET sess gone 0,1,0 PX 1200)" = "OK " ] || { echo "FAIL: VEC.SET sess gone PX"; exit 1; }
case "$(vexec VEC.SEARCH sess 0,1,0 2)" in *gone*) : ;; *) echo "FAIL: TTL'd vector not searchable before expiry"; exit 1 ;; esac
case "$(vexec VEC.GET sess gone)" in *"0,1,0"*) : ;; *) echo "FAIL: TTL'd VEC.GET before expiry"; exit 1 ;; esac
# Introspection: VEC.TTL -> -1 for permanent 'keep', a positive ms for 'gone'.
case "$(vexec VEC.TTL sess keep)" in *-1*) : ;; *) echo "FAIL: VEC.TTL of a permanent id should be -1"; exit 1 ;; esac
TT="$(vexec VEC.TTL sess gone)"; case "$TT" in -*) echo "FAIL: VEC.TTL 'gone' negative ($TT)"; exit 1 ;; *[0-9]*) : ;; *) echo "FAIL: VEC.TTL 'gone' not numeric ($TT)"; exit 1 ;; esac
# Management: EXPIRE then PERSIST round-trip a TTL onto 'keep' and back off, leaving it permanent.
[ "$(vexec VEC.EXPIRE sess keep 100)" = "1 " ] || { echo "FAIL: VEC.EXPIRE should return 1"; exit 1; }
case "$(vexec VEC.TTL sess keep)" in *-1*) echo "FAIL: 'keep' should carry a TTL after EXPIRE"; exit 1 ;; esac
[ "$(vexec VEC.PERSIST sess keep)" = "1 " ] || { echo "FAIL: VEC.PERSIST should return 1"; exit 1; }
case "$(vexec VEC.TTL sess keep)" in *-1*) : ;; *) echo "FAIL: 'keep' should be permanent after PERSIST"; exit 1 ;; esac
# INFO's expiring count sees the one live TTL'd id ('gone').
case "$(vexec VEC.INFO sess)" in *expiring*1*) : ;; *) echo "FAIL: INFO expiring should be 1 before expiry"; exit 1 ;; esac
echo "  before expiry: 'gone' searchable, VEC.TTL positive; EXPIRE/PERSIST round-trip on 'keep'"
sleep 2.4   # past PX (1.2s) plus at least one sweep tick
# The expired id reads absent and is masked from SEARCH; the permanent 'keep' stays.
case "$(vexec VEC.GET sess gone)" in *"0,1,0"*) echo "FAIL: expired VEC.GET still returns the vector"; exit 1 ;; esac
case "$(vexec VEC.SEARCH sess 0,1,0 5)" in *gone*) echo "FAIL: expired vector still in SEARCH"; exit 1 ;; esac
case "$(vexec VEC.SEARCH sess 1,0,0 5)" in *keep*) : ;; *) echo "FAIL: permanent 'keep' vanished with the TTL'd one"; exit 1 ;; esac
# VEC.TTL of the now-absent id is -2.
case "$(vexec VEC.TTL sess gone)" in *-2*) : ;; *) echo "FAIL: VEC.TTL of an expired id should be -2"; exit 1 ;; esac
# The sweep reclaimed it from the index: INFO count is back to 1 (only 'keep'), expiring 0.
case "$(vexec VEC.INFO sess)" in *count*1*) : ;; *) echo "FAIL: sweep did not reclaim the expired id (count != 1)"; exit 1 ;; esac
case "$(vexec VEC.INFO sess)" in *expiring*0*) : ;; *) echo "FAIL: INFO expiring should be 0 after expiry"; exit 1 ;; esac
grep -qi "swept" "$D/vec4.log" || { echo "FAIL: no expiry sweep logged"; exit 1; }
echo "  after expiry: 'gone' masked/swept, VEC.TTL -2; 'keep' still served"

echo "PASS: flint-vec serves VEC.* end to end (flat + hnsw), writes are durable, a"
echo "      restarted co-processor rebuilds each set from KV (D3) as its own engine kind,"
echo "      a per-namespace index-memory cap (D4) sheds new writes with -VECFULL, and a"
echo "      per-vector TTL (D7) expires+sweeps while permanent ids stay — with VEC.TTL/"
echo "      EXPIRE/PERSIST introspection and the INFO expiring count over the wire."
