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
#   - a QUANT sq8 set (ADR-0049) ranks right, GETs the exact vector, and
#     rebuilds as sq8, not as plain hnsw; with --vec-dir its full vectors are
#     in a local file (step 2), which a restart sweeps and the rebuild refills,
#     and a second co-processor cannot take the same directory
#   - ADR-0049's verifications 4-6 on quantized sets: a bin set's restart
#     answers -LOADING and rebuilds to the same results; a burst of searches
#     over every engine opens no channel (the proxy's connection count, with a
#     write as its control); TTL expires and sweeps on sq8 and bin sets; and a
#     second tenant's bin set searches and takes writes while the first tenant
#     is at the D4 cap
#   - the ordinary tenant data path is untouched (control)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-vecd 6676 6677 6678 6679
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
VD="$D/vecs"
$VEC --port 6678 --vec-dir "$VD" 2>"$D/vec1.log" & COPROC_PID=$!
fleet_wait_listen 6678
# The directory is locked: a second co-processor on it must refuse to start
# rather than delete the first one's vector files at its own startup sweep.
$VEC --port 6679 --vec-dir "$VD" 2>"$D/vec-second.log" & SECOND=$!
for _ in $(seq 1 50); do kill -0 "$SECOND" 2>/dev/null || break; sleep 0.2; done
if kill -0 "$SECOND" 2>/dev/null; then
  kill -9 "$SECOND"; echo "FAIL: a second flint-vec started on a locked --vec-dir"; exit 1
fi
wait "$SECOND" && { echo "FAIL: a second flint-vec on a locked --vec-dir exited 0"; exit 1; }
grep -q "held by another flint-vec" "$D/vec-second.log" \
  || { echo "FAIL: the second flint-vec did not say why it refused:"; sed 's/^/    /' "$D/vec-second.log"; exit 1; }
# --engine mem: the server stays up across the co-processor restart and holds
# the durable rows, so this drill isolates the CO-PROCESSOR's rebuild, not the
# server's own restart durability (that is repl/warm_restart's job).
$B --port 6676 --engine mem 2>"$D/n.log" & fleet_wait_listen 6676; fleet_wait_ping 6676
# Four workers, pinned: the BUG-0200 section writes more than that on one
# connection, whatever the box's core count. A second tenant (ns2) is the
# isolation check's: it shares the co-processor and its cap.
$PX --port 6677 --workers 4 --pairs 127.0.0.1:6676 --tenants "tok=ns,tok2=ns2" \
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
A2="valkey-cli -p 6677 -a tok2 --no-auth-warning"
vexec2() { local A="$A2"; vexec "$@"; }
# The proxy's accepted-connection count. A co-processor's PROXYCHAN dial-back is
# a connection like any client's, so it moves this too.
conns() { valkey-cli -p 6677 PROXYSTATS 2>/dev/null | tr -d '\r' | sed -n 's/^conns_total://p'; }

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
# COUNTED BY KIND, not compared against a total. The total was 8 until #194
# added the durable id index, and the drill then reported seven NEW keys as
# seven LOST ones. Correcting 8 to 15 fixed that instance and left the shape:
# a single number that says nothing about WHICH part moved when it next moves.
#
# The branch fix for this re-derived the total by reimplementing FNV-1a and
# INDEX_BUCKETS in Python, inside the drill. That trades a stale constant for a
# second copy of the hash which can drift from flint_vec::bucket_of silently —
# and two implementations of one invariant is the thing this repo keeps paying
# for. Counting kinds needs no hash at all.
#
# Keys are KEY_PREFIX + kind + NUL + set [+ NUL + id] (flint-vec durable_key),
# so each kind is countable directly:
#   s  one set-name index per namespace
#   c  one config per set
#   v  one row per (set, id)
#   i  one per (set, DISTINCT bucket) — BOUNDED by the ids, not predicted from
#      them, because a hash collision may only ever REDUCE this count
# The total survives only as an equality, whose single job is to catch keys of
# a kind nobody expected. That is what the bare number was really guarding.
VKEYS=$($A --no-raw SCAN 0 COUNT 500 2>/dev/null)
vkind() { printf '%s\n' "$VKEYS" | grep -cF "\\x00vec\\x00$1\\x00" || true; }
N_S=$(vkind s); N_C=$(vkind c); N_V=$(vkind v); N_I=$(vkind i)
N_TOT=$($A DBSIZE 2>&1 | tr -d '\r')
vfail() { echo "FAIL: durable layout — $1"
          echo "      counted: s=$N_S c=$N_C v=$N_V i=$N_I  dbsize=$N_TOT"
          $A --no-raw SCAN 0 COUNT 500 2>/dev/null | sed 's/^/    key| /'; exit 1; }
[ "$N_S" = 1 ] || vfail "expected 1 set-name index ('s'), got $N_S"
[ "$N_C" = 2 ] || vfail "expected 2 configs ('c', one per set), got $N_C"
[ "$N_V" = 6 ] || vfail "expected 6 vector rows ('v', 3 ids x 2 sets), got $N_V"
[ "$N_I" -ge 2 ] && [ "$N_I" -le 6 ] \
  || vfail "id-index buckets ('i') = $N_I, outside 2..6 (>=1 and <=3 per set)"
[ "$N_TOT" = "$(( N_S + N_C + N_V + N_I ))" ] \
  || vfail "dbsize $N_TOT != s+c+v+i = $(( N_S + N_C + N_V + N_I )) — a key of an unexpected kind exists"

# Created after the layout count above, which describes the first two sets.
echo "== a third set ranks by 8-bit codes (QUANT sq8) and re-ranks on the full vectors"
[ "$(vexec VEC.CREATE docsq DIM 3 METRIC l2 INDEX hnsw QUANT sq8)" = "OK " ] \
  || { echo "FAIL: VEC.CREATE QUANT sq8"; exit 1; }
for kv in "a 0.123,0.456,0.789" "b 0.9,0.1,0.2" "c 0.5,0.5,0.5"; do
  set -- $kv
  [ "$(vexec VEC.SET docsq "$1" "$2")" = "OK " ] || { echo "FAIL: VEC.SET docsq $1"; exit 1; }
done
PREQ="$(vexec VEC.SEARCH docsq 0.85,0.1,0.2 2)"
case "$PREQ" in *b*c*) : ;; *) echo "FAIL: sq8 SEARCH order, got: $PREQ"; exit 1 ;; esac
case "$(vexec VEC.INFO docsq)" in *quant*sq8*) : ;; *) echo "FAIL: VEC.INFO should report quant sq8"; exit 1 ;; esac
# The code is lossy; what GET returns must not be.
case "$(vexec VEC.GET docsq a)" in *"0.123,0.456,0.789"*) : ;; *) echo "FAIL: sq8 GET is not the exact vector: $(vexec VEC.GET docsq a)"; exit 1 ;; esac
case "$(vexec VEC.INFO docsq)" in *vectors_on*disk*) : ;; *) echo "FAIL: with --vec-dir the sq8 set's vectors should be on disk: $(vexec VEC.INFO docsq)"; exit 1 ;; esac
NFILES=$(ls "$VD"/*.vecs 2>/dev/null | wc -l | tr -d ' ')
[ "$NFILES" = 1 ] || { echo "FAIL: expected one vector file in $VD (the sq8 set's), found $NFILES"; ls -la "$VD"; exit 1; }
echo "  sq8 VEC.SEARCH -> $PREQ  (full vectors in $VD)"

echo "== a 1-bit set (QUANT bin: ADR-0049 item 2) serves the same way"
[ "$(vexec VEC.CREATE docsb DIM 3 METRIC l2 INDEX hnsw QUANT bin)" = "OK " ] \
  || { echo "FAIL: VEC.CREATE QUANT bin"; exit 1; }
for kv in "a 0.123,0.456,0.789" "b 0.9,0.1,0.2" "c 0.5,0.5,0.5"; do
  set -- $kv
  [ "$(vexec VEC.SET docsb "$1" "$2")" = "OK " ] || { echo "FAIL: VEC.SET docsb $1"; exit 1; }
done
R="$(vexec VEC.SEARCH docsb 0.85,0.1,0.2 2)"
[ "$R" = "$PREQ" ] || { echo "FAIL: bin SEARCH should rank as sq8 did. bin=[$R] sq8=[$PREQ]"; exit 1; }
case "$(vexec VEC.INFO docsb)" in *quant*bin*vectors_on*disk*) : ;; *) echo "FAIL: VEC.INFO docsb: $(vexec VEC.INFO docsb)"; exit 1 ;; esac
case "$(vexec VEC.GET docsb a)" in *"0.123,0.456,0.789"*) : ;; *) echo "FAIL: bin GET is not the exact vector"; exit 1 ;; esac
NFILES=$(ls "$VD"/*.vecs 2>/dev/null | wc -l | tr -d ' ')
[ "$NFILES" = 2 ] || { echo "FAIL: expected two vector files in $VD (sq8, bin), found $NFILES"; ls -la "$VD"; exit 1; }
echo "  bin VEC.SEARCH -> $PREQ"

echo "== CRASH DURABILITY: kill the co-processor, restart it EMPTY, SEARCH rebuilds"
kill -9 "$COPROC_PID" 2>/dev/null; wait "$COPROC_PID" 2>/dev/null; COPROC_PID=""
# A file the dead process might have held for a set the new one never makes:
# the restart must remove it, not just overwrite the names it reuses.
: > "$VD/99.vecs"
sleep 0.3
$VEC --port 6678 --vec-dir "$VD" 2>"$D/vec2.log" & COPROC_PID=$!
fleet_wait_listen 6678
# The first touch of the cold namespace answers -LOADING, here on the bin set
# whose vector file the restart has just swept (ADR-0049 verification 4).
R="$($A VEC.SEARCH docsb 0.85,0.1,0.2 2 2>&1 | tr -d '\r')"
case "$R" in *LOADING*) : ;; *) echo "FAIL: the first command after the restart should answer -LOADING, got: $R"; exit 1 ;; esac
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

# And the sq8 set AS sq8: QUANT rides the config's fourth field.
POSTQ="$(vexec VEC.SEARCH docsq 0.85,0.1,0.2 2)"
[ "$POSTQ" = "$PREQ" ] \
  || { echo "FAIL: sq8 rebuild changed results. pre=[$PREQ] post=[$POSTQ]"; exit 1; }
case "$(vexec VEC.INFO docsq)" in *quant*sq8*) : ;; *) echo "FAIL: rebuilt set is not sq8 (quant lost in the durable config)"; exit 1 ;; esac
case "$(vexec VEC.GET docsq a)" in *"0.123,0.456,0.789"*) : ;; *) echo "FAIL: sq8 GET after rebuild is not the exact vector"; exit 1 ;; esac
# The dead process's file was swept at startup (derived, never trusted) and the
# rebuild wrote a new one.
grep -q "removed 3 vector file" "$D/vec2.log" \
  || { echo "FAIL: the restart did not sweep the old vector files:"; sed 's/^/    /' "$D/vec2.log"; exit 1; }
[ ! -e "$VD/99.vecs" ] || { echo "FAIL: a vector file no set owns survived the restart"; exit 1; }
case "$(vexec VEC.INFO docsq)" in *vectors_on*disk*) : ;; *) echo "FAIL: the rebuilt sq8 set is not on disk"; exit 1 ;; esac
echo "  post-restart sq8 VEC.SEARCH -> $POSTQ  (rebuilt as sq8)"
R="$(vexec VEC.SEARCH docsb 0.85,0.1,0.2 2)"
[ "$R" = "$PREQ" ] || { echo "FAIL: bin rebuild changed results. pre=[$PREQ] post=[$R]"; exit 1; }
case "$(vexec VEC.INFO docsb)" in *quant*bin*vectors_on*disk*) : ;; *) echo "FAIL: rebuilt docsb is not bin on disk: $(vexec VEC.INFO docsb)"; exit 1 ;; esac
case "$(vexec VEC.GET docsb a)" in *"0.123,0.456,0.789"*) : ;; *) echo "FAIL: bin GET after rebuild is not the exact vector"; exit 1 ;; esac
echo "  post-restart bin VEC.SEARCH -> $PREQ  (rebuilt as bin)"

echo "== search opens no channel (ADR-0017 verification 3, ADR-0049 verification 5)"
# Every set is warm now. A burst of searches over all four engines on ONE
# connection moves the proxy's connection count by exactly two: the burst's
# connection and the second count's own. A search that dialled a PROXYCHAN
# channel would add one each.
C0="$(conns)"
OUT="$(for _ in $(seq 1 10); do
  for q in "docs 0.9,0.1,0" "docsh 0.9,0.1,0" "docsq 0.85,0.1,0.2" "docsb 0.85,0.1,0.2"; do
    echo "VEC.SEARCH $q 2"
  done
done | $A 2>&1 | tr -d '\r')"
C1="$(conns)"
case "$C0$C1" in ''|*[!0-9]*) echo "FAIL: PROXYSTATS conns_total unreadable ($C0, $C1)"; exit 1 ;; esac
# 'b' is in every set's top two for its query, so each answer holds it once.
NB="$(grep -cx b <<<"$OUT")"
[ "$NB" = 40 ] || { echo "FAIL: 40 searches on one connection, $NB answered with 'b':"; sort <<<"$OUT" | uniq -c | sed 's/^/    /'; exit 1; }
[ $(( C1 - C0 )) = 2 ] || { echo "FAIL: 40 searches moved conns_total by $(( C1 - C0 )), not 2: a search opened a channel"; exit 1; }
# Control: one write does open a channel, so the same count moves by three.
C0="$(conns)"
[ "$(vexec VEC.SET docsb ctl 0,0,1)" = "OK " ] || { echo "FAIL: VEC.SET docsb ctl"; exit 1; }
C1="$(conns)"
[ $(( C1 - C0 )) = 3 ] || { echo "FAIL (control): a VEC.SET moved conns_total by $(( C1 - C0 )), not 3; the count does not see channels"; exit 1; }
echo "  40 searches: +0 channels; one VEC.SET: +1 channel"

echo "== VEC.DEL is durable too"
[ "$(vexec VEC.DEL docs a)" = "1 " ] || { echo "FAIL: VEC.DEL"; exit 1; }
case "$(vexec VEC.SEARCH docs 1,0,0 5)" in *a*) echo "FAIL: 'a' still present after DEL"; exit 1 ;; *) : ;; esac
# Captured, not asserted: VEC.DEL drops the vector row and may drop its
# id-index bucket too (if 'a' was alone in it), so the absolute number is
# layout-coupled. What D4 needs is only that it does not MOVE.
DBSIZE_BEFORE_D4="$($A DBSIZE 2>&1 | tr -d '\r')"

echo "== D4: a tiny per-namespace index-memory cap sheds new writes; reads unaffected"
# The second tenant's 1-bit set, made before the cap (the isolation check below).
[ "$(vexec2 VEC.CREATE iso DIM 3 METRIC l2 INDEX hnsw QUANT bin)" = "OK " ] || { echo "FAIL: tenant 2 VEC.CREATE iso"; exit 1; }
[ "$(vexec2 VEC.SET iso t2a 1,0,0)" = "OK " ] || { echo "FAIL: tenant 2 VEC.SET iso t2a"; exit 1; }
kill -9 "$COPROC_PID" 2>/dev/null; wait "$COPROC_PID" 2>/dev/null; COPROC_PID=""
sleep 0.3
# 2,000 bytes is below this namespace's already-durable footprint: twelve
# vectors, eleven of them HNSW nodes the meter charges 320 B and more each.
# Rebuild loads those rows regardless (they are already durable, ADR-0017 D3);
# the cap governs only NEW writes, so this restart comes up ALREADY over
# budget. The second tenant's one vector is well under the same cap.
$VEC --port 6678 --vec-dir "$VD" --index-mem-bytes 2000 2>"$D/vec3.log" & COPROC_PID=$!
fleet_wait_listen 6678
# Reads still serve (the index rebuilt past the cap from the durable rows)...
case "$(vexec VEC.SEARCH docs 1,0,0 3)" in *b*) : ;; *) echo "FAIL: read after cap-restart"; exit 1 ;; esac
# ...but a fresh VEC.SET is refused with -VECFULL, BEFORE any durable write.
R="$(vexec VEC.SET docs znew 0,0,1)"
case "$R" in *VECFULL*) : ;; *) echo "FAIL: expected VECFULL over the cap, got: $R"; exit 1 ;; esac
case "$(vexec VEC.SET docsb znew 0,0,1)" in *VECFULL*) : ;; *) echo "FAIL: the bin set took a write over the cap"; exit 1 ;; esac
# The shed writes did not persist: durable key count is unchanged.
[ "$($A DBSIZE 2>&1 | tr -d '\r')" = "$DBSIZE_BEFORE_D4" ] \
  || { echo "FAIL: a VECFULL-shed write still persisted (DBSIZE moved off $DBSIZE_BEFORE_D4)"; exit 1; }
echo "  new VEC.SET -> ${R}(refused, not persisted); reads still served"

echo "== isolation (ADR-0049 verification 6): tenant 1 is at its cap; tenant 2's bin set is not"
case "$(vexec2 VEC.SEARCH iso 1,0,0 1)" in *t2a*) : ;; *) echo "FAIL: tenant 2's search beside a full tenant: $(vexec2 VEC.SEARCH iso 1,0,0 1)"; exit 1 ;; esac
R="$(vexec2 VEC.SET iso t2b 0,1,0)"
[ "$R" = "OK " ] || { echo "FAIL: tenant 2's write under its own cap was refused: $R"; exit 1; }
case "$(vexec2 VEC.SEARCH iso 0,1,0 1)" in *t2b*) : ;; *) echo "FAIL: tenant 2's new vector is not searchable"; exit 1 ;; esac
I2="$(vexec2 VEC.INFO iso)"
case "$I2" in *"count 2 "*"quant bin vectors_on disk "*) : ;; *) echo "FAIL: tenant 2's set is not two bin vectors on disk: $I2"; exit 1 ;; esac
# Each namespace is metered on its own: the same 2,000-byte cap, two sides of it.
mem() { sed -n 's/.*ns_mem_bytes \([0-9]*\) ns_mem_cap \([0-9]*\) .*/\1 \2/p' <<<"$1"; }
read -r M1 CAP <<<"$(mem "$(vexec VEC.INFO docsb)")"
read -r M2 _ <<<"$(mem "$I2")"
[ "$CAP" = 2000 ] && [ "${M1:-0}" -gt 2000 ] && [ "${M2:-9999}" -lt 2000 ] \
  || { echo "FAIL: expected tenant 1 over and tenant 2 under a 2000-byte cap: $M1, $M2 of $CAP"; exit 1; }
# Tenant 2's write did not free tenant 1, and tenant 1 cannot reach tenant 2's set.
case "$(vexec VEC.SET docs znew 0,0,1)" in *VECFULL*) : ;; *) echo "FAIL: tenant 1 left its cap after tenant 2's write"; exit 1 ;; esac
case "$(vexec VEC.SEARCH iso 1,0,0 1)" in *t2a*) echo "FAIL: tenant 1 searched tenant 2's set"; exit 1 ;; esac
echo "  tenant 2 ($M2 of $CAP B): search and write served; tenant 1 ($M1 B): still -VECFULL, blind to tenant 2's set"

echo "== CONTROL: the ordinary tenant data path is untouched"
cli_ok $A SET plain value
[ "$($A GET plain)" = "value" ] || { echo "FAIL (control): tenant SET/GET broke"; exit 1; }

echo "== TTL (D7): a PX vector expires from the index; a permanent one in the same set survives"
# Restart WITHOUT the D4 cap so these writes are not shed (the prior section left
# the namespace deliberately over a 2,000-byte cap).
kill -9 "$COPROC_PID" 2>/dev/null; wait "$COPROC_PID" 2>/dev/null; COPROC_PID=""
sleep 0.3
$VEC --port 6678 --vec-dir "$VD" 2>"$D/vec4.log" & COPROC_PID=$!
fleet_wait_listen 6678
[ "$(vexec VEC.CREATE sess DIM 3 METRIC l2)" = "OK " ] || { echo "FAIL: VEC.CREATE sess"; exit 1; }
[ "$(vexec VEC.SET sess keep 1,0,0)" = "OK " ] || { echo "FAIL: VEC.SET sess keep"; exit 1; }
# PX 1200ms: searchable now; gone within one sweep (--sweep-ms 1000) after it lapses.
[ "$(vexec VEC.SET sess gone 0,1,0 PX 1200)" = "OK " ] || { echo "FAIL: VEC.SET sess gone PX"; exit 1; }
# The same on the quantized sets (ADR-0049 verification 6), whose full vectors
# are in the local file.
for qs in "sessq sq8" "sessb bin"; do
  set -- $qs
  [ "$(vexec VEC.CREATE "$1" DIM 3 METRIC l2 INDEX hnsw QUANT "$2")" = "OK " ] || { echo "FAIL: VEC.CREATE $1"; exit 1; }
  [ "$(vexec VEC.SET "$1" keep 1,0,0)" = "OK " ] || { echo "FAIL: VEC.SET $1 keep"; exit 1; }
  [ "$(vexec VEC.SET "$1" gone 0,1,0 PX 1200)" = "OK " ] || { echo "FAIL: VEC.SET $1 gone PX"; exit 1; }
  case "$(vexec VEC.SEARCH "$1" 0,1,0 2)" in *gone*) : ;; *) echo "FAIL: TTL'd vector not searchable in $1 before expiry"; exit 1 ;; esac
done
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
for qs in "sessq sq8" "sessb bin"; do
  set -- $qs
  case "$(vexec VEC.GET "$1" gone)" in *"0,1,0"*) echo "FAIL: expired VEC.GET in $1 still returns the vector"; exit 1 ;; esac
  case "$(vexec VEC.SEARCH "$1" 0,1,0 5)" in *gone*) echo "FAIL: expired vector still in $1's SEARCH"; exit 1 ;; *keep*) : ;; *) echo "FAIL: 'keep' vanished from $1"; exit 1 ;; esac
  I="$(vexec VEC.INFO "$1")"
  case "$I" in *"count 1 expiring 0 "*) : ;; *) echo "FAIL: the sweep did not reclaim $1's expired id: $I"; exit 1 ;; esac
  case "$I" in *"quant $2 vectors_on disk "*) : ;; *) echo "FAIL: $1 is not $2 on disk: $I"; exit 1 ;; esac
done
echo "  after expiry: 'gone' masked/swept, VEC.TTL -2; 'keep' still served"

echo "== BUG-0200: more writes on ONE client connection than the proxy has workers"
# Every write above opens a connection of its own. A family command used to
# block its proxy worker until the co-processor replied, and the
# co-processor's PROXYCHAN dial-back, dealt round-robin over the workers,
# landed on that blocked worker once in --workers (4 here) and waited out its
# 5 s token: on one connection, every 4th of these failed.
[ "$(vexec VEC.CREATE pipe DIM 3 METRIC l2)" = "OK " ] || { echo "FAIL: VEC.CREATE pipe"; exit 1; }
T0=$(date +%s)
OUT="$(for i in $(seq 1 24); do echo "VEC.SET pipe p$i $i,1,0"; done | $A 2>&1 | tr -d '\r')"
SECS=$(( $(date +%s) - T0 ))
OKS="$(grep -cx OK <<<"$OUT")"
if [ "$OKS" != 24 ]; then
  echo "FAIL (BUG-0200): $OKS of 24 VEC.SETs on one connection answered OK, in ${SECS}s:"
  grep -vx OK <<<"$OUT" | sort | uniq -c | sed 's/^/    /'
  exit 1
fi
case "$(vexec VEC.INFO pipe)" in *count*24*) : ;; *) echo "FAIL: VEC.INFO pipe should count 24"; exit 1 ;; esac
echo "  24 VEC.SETs on one connection: all OK in ${SECS}s"

echo "PASS: flint-vec serves VEC.* end to end (flat + hnsw + sq8 and bin on a local file), writes are durable, a"
echo "      restarted co-processor rebuilds each set from KV (D3) as its own engine kind,"
echo "      a per-namespace index-memory cap (D4) sheds new writes with -VECFULL, and a"
echo "      per-vector TTL (D7) expires+sweeps while permanent ids stay — with VEC.TTL/"
echo "      EXPIRE/PERSIST introspection and the INFO expiring count over the wire, and a"
echo "      client connection takes more writes than the proxy has workers (BUG-0200). Searches"
echo "      open no channel, TTL holds on quantized sets, and a full tenant leaves another's"
echo "      bin set serving (ADR-0049 verifications 4-6)."
