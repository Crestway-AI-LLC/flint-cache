#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0017 D3: a set larger than one channel's budget rebuilds ACROSS channels,
# and a partial index is never served.
#
# coproc_vec_drill proves rebuild with a generous budget (one channel covers the
# whole set). This forces the opposite: a deliberately tiny --family-budget so
# no single channel can load the set, and asserts the resumable rebuild still
# reaches a COMPLETE index — each -LOADING retry donates another channel, the
# co-processor resumes from where the last budget ran out, and the namespace is
# marked warm only when the final row is in (never a silently-partial k-NN).
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-vecrb 6705 6706 6707
fleet_guard
B=./target/release/flint-server
PX=./target/release/flint-proxy
VEC=./target/release/flint-vec
D=$FLINT_DRILL_ROOT/flint-vecrb; rm -rf "$D"; mkdir -p "$D"
COPROC_PID=""
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() {
  [ -n "$COPROC_PID" ] && kill -9 "$COPROC_PID" 2>/dev/null
  fleet_kill server; fleet_kill proxy; rm -rf "$D"
}
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy -p flint-vec --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

$VEC --port 6707 2>"$D/vec1.log" & COPROC_PID=$!
fleet_wait_listen 6707
$B --port 6705 --engine mem 2>"$D/n.log" & fleet_wait_listen 6705; fleet_wait_ping 6705
# --family-budget 4: each channel gets FOUR data commands. Ordinary VEC.SET (one
# durable SET) and VEC.SEARCH (no channel I/O) fit easily; only a rebuild's
# SCAN + many GETs overflows it, which is exactly the path under test.
$PX --port 6706 --pairs 127.0.0.1:6705 --tenants "tok=ns" \
    --families "VEC.=127.0.0.1:6707" --edge-advertise 127.0.0.1:6706 \
    --family-budget 4 2>"$D/px.log" & fleet_wait_listen 6706
for _ in $(seq 1 60); do case "$(valkey-cli -p 6706 PING 2>&1)" in *NOAUTH*|PONG) break;; esac; sleep 0.1; done

A="valkey-cli -p 6706 -a tok --no-auth-warning"
vexec() {
  local i out
  for i in $(seq 1 80); do
    out="$($A "$@" 2>&1 | tr -d '\r' | tr '\n' ' ')"
    case "$out" in *LOADING*) sleep 0.1 ;; *) echo "$out"; return 0 ;; esac
  done
  echo "$out"; return 1
}

echo "== a set of 8 vectors (SCAN + 1 config + 8 GETs = 10 channel commands, > one budget of 4)"
[ "$(vexec VEC.CREATE big DIM 2 METRIC l2)" = "OK " ] || { echo "FAIL: VEC.CREATE"; exit 1; }
for i in 0 1 2 3 4 5 6 7; do
  [ "$(vexec VEC.SET big "v$i" "$i,0")" = "OK " ] || { echo "FAIL: VEC.SET v$i"; exit 1; }
done
PRE="$(vexec VEC.SEARCH big 0,0 8)"
case "$(vexec VEC.INFO big)" in *count*8*) : ;; *) echo "FAIL: expected 8 before restart"; exit 1 ;; esac
echo "  8 vectors set; pre-restart nearest -> $PRE"

echo "== restart the co-processor EMPTY: the rebuild must span several channels"
kill -9 "$COPROC_PID" 2>/dev/null; wait "$COPROC_PID" 2>/dev/null; COPROC_PID=""
sleep 0.3
$VEC --port 6707 2>"$D/vec2.log" & COPROC_PID=$!
fleet_wait_listen 6707

# Drive the rebuild to completion through the client's -LOADING retry loop.
POST="$(vexec VEC.SEARCH big 0,0 8)"
case "$(vexec VEC.INFO big)" in *count*8*) : ;; *) echo "FAIL: rebuild did not reach a COMPLETE index (count != 8) — a partial was served or it stalled"; sed 's/^/    /' "$D/vec2.log"; exit 1 ;; esac
[ "$POST" = "$PRE" ] || { echo "FAIL: post-rebuild result differs from pre-restart (partial index?). pre=[$PRE] post=[$POST]"; exit 1; }

# Prove it actually took MULTIPLE channels (not one oversized one): the resume
# log line must appear at least twice, i.e. >=2 chunks before the final "rebuilt".
CHUNKS=$(grep -c "rebuild chunk loaded" "$D/vec2.log")
[ "${CHUNKS:-0}" -ge 2 ] || { echo "FAIL: rebuild did not span multiple channels (chunk-resume lines: ${CHUNKS:-0})"; sed 's/^/    /' "$D/vec2.log"; exit 1; }
grep -qi "rebuilt ns" "$D/vec2.log" || { echo "FAIL: rebuild never completed"; exit 1; }
echo "  rebuilt across $CHUNKS+1 channels; post-restart nearest -> $POST  (complete, matches pre)"

echo "== every id survived the multi-channel rebuild (nothing dropped at a chunk boundary)"
for i in 0 1 2 3 4 5 6 7; do
  case "$(vexec VEC.GET big "v$i")" in *"$i,0"*) : ;; *) echo "FAIL: v$i missing after resumable rebuild"; exit 1 ;; esac
done
echo "  all 8 ids present"

echo "PASS: a set that overflows one channel's budget rebuilds across channels (D3),"
echo "      each -LOADING retry resuming from the last budget's end, reaching a"
echo "      COMPLETE index — the co-processor never serves a partial k-NN."
