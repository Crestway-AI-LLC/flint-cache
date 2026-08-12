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
fleet_init /tmp/flint-vecd 6676 6677 6678
fleet_guard
B=./target/release/flint-server
PX=./target/release/flint-proxy
VEC=./target/release/flint-vec
D=/tmp/flint-vecd; rm -rf "$D"; mkdir -p "$D"
COPROC_PID=""
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() {
  [ -n "$COPROC_PID" ] && kill -9 "$COPROC_PID" 2>/dev/null
  fleet_kill server; fleet_kill proxy; rm -rf "$D"
}
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy -p flint-vec --features flint-server/rocks

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

echo "== durable rows exist in KV independent of the co-processor"
[ "$($A DBSIZE 2>&1 | tr -d '\r')" = "4" ] \
  || { echo "FAIL: expected 4 durable keys (1 config + 3 vectors), got $($A DBSIZE)"; exit 1; }

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

echo "== VEC.DEL is durable too"
[ "$(vexec VEC.DEL docs a)" = "1 " ] || { echo "FAIL: VEC.DEL"; exit 1; }
case "$(vexec VEC.SEARCH docs 1,0,0 5)" in *a*) echo "FAIL: 'a' still present after DEL"; exit 1 ;; *) : ;; esac

echo "== CONTROL: the ordinary tenant data path is untouched"
cli_ok $A SET plain value
[ "$($A GET plain)" = "value" ] || { echo "FAIL (control): tenant SET/GET broke"; exit 1; }

echo "PASS: flint-vec serves VEC.* end to end, writes are durable, and a restarted"
echo "      co-processor rebuilds its index from KV (D3) and returns identical results."
