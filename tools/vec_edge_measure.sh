#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0049 verification 3: VEC.SEARCH latency through the edge, with the
# re-rank reading the co-processor's local file, next to today's full-vector
# HNSW at the same N. Run on a measurement box that already holds the corpus
# (tools/vec_real_measure.sh fetched and converted it), through
# packaging/aws/gate-box/run.sh (ops) with FLINT_GATE_CMD='bash
# tools/vec_edge_measure.sh' and FLINT_GATE_PULL=/tmp/flint-vecedge. Not a
# drill and in no gate.
#
# One flint-server, on the instance store, holds both sets' durable rows. Each
# arm has its own co-processor and proxy, since a proxy sends the VEC. family
# to one co-processor: plain HNSW, and HNSW with QUANT $VEC_CODE and its full
# vectors in --vec-dir on the instance store. The two load at once; the
# searches run one arm at a time, so neither is timed against the other's
# work. The quantized arm is timed warm, and with its vector file dropped from
# the page cache before each query: a re-rank that reads the device, as one
# does when the file is bigger than spare RAM.
#
#   VEC_N       vectors (default: all 999,000)
#   VEC_CODE    the quantized arm's code (default bin)
#   VEC_ENGINE  the server's engine (default rocks)
#   VEC_DATA    the corpus (default /mnt/d/vecdata)
#   VEC_WORK    servers' state (default /mnt/d/vecedge)
#   VEC_OUT     results (default /tmp/flint-vecedge)
set -euo pipefail
cd "$(dirname "$0")/.."
D="${VEC_DATA:-/mnt/d/vecdata}"
W="${VEC_WORK:-/mnt/d/vecedge}"
OUT="${VEC_OUT:-/tmp/flint-vecedge}"
N="${VEC_N:-}"
CODE="${VEC_CODE:-bin}"
ENGINE="${VEC_ENGINE:-rocks}"
[ -s "$D/base.fbin" ] || { echo "FAIL: no corpus in $D: run tools/vec_real_measure.sh first"; exit 1; }
mkdir -p "$OUT"
rm -f "$OUT"/*.txt "$OUT"/*.log
rm -rf "$W"
mkdir -p "$W"

FEATURES=()
[ "$ENGINE" = rocks ] && FEATURES=(--features flint-server/rocks)
cargo build --release -q -p flint-server -p flint-proxy -p flint-vec ${FEATURES[@]+"${FEATURES[@]}"}
T=./target/release
PIDS=()
cleanup() {
  for p in ${PIDS[@]+"${PIDS[@]}"}; do kill "$p" 2>/dev/null || true; done
}
trap cleanup EXIT
listen() {
  for _ in $(seq 1 300); do
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && return 0
    sleep 0.1
  done
  echo "FAIL: nothing listening on $1"
  exit 1
}

"$T/flint-server" --port 7100 --bind 127.0.0.1 --engine "$ENGINE" --data-dir "$W/server" \
  2>"$OUT/server.log" & PIDS+=($!)
listen 7100
"$T/flint-vec" --port 7111 2>"$OUT/vec-plain.log" & VP=$!
PIDS+=($VP)
"$T/flint-vec" --port 7121 --vec-dir "$W/vecs" 2>"$OUT/vec-$CODE.log" & VQ=$!
PIDS+=($VQ)
listen 7111
listen 7121
for arm in plain "$CODE"; do
  [ "$arm" = plain ] && { P=7110; V=7111; } || { P=7120; V=7121; }
  "$T/flint-proxy" --port $P --pairs 127.0.0.1:7100 --tenants "tok=$arm" \
    --families "VEC.=127.0.0.1:$V" --edge-advertise "127.0.0.1:$P" 2>"$OUT/proxy-$arm.log" & PIDS+=($!)
  listen $P
done

edge() { # port label [bench args...]
  local port="$1" label="$2"
  shift 2
  "$T/bench" --edge "127.0.0.1:$port" --auth tok --data "$D" --label "$label" ${N:+--n "$N"} "$@"
}
edge 7110 plain --load-only >"$OUT/load-plain.txt" 2>"$OUT/load-plain.log" & L1=$!
edge 7120 "$CODE-disk" --quant "$CODE" --load-only >"$OUT/load-$CODE.txt" 2>"$OUT/load-$CODE.log" & L2=$!
wait $L1
wait $L2
for pid in $VP $VQ; do
  echo "co-processor $pid: $(grep -E 'VmRSS|RssAnon' /proc/$pid/status | tr -s ' \n' ' ')"
done | tee "$OUT/rss.txt"
edge 7110 plain --search-only >"$OUT/search-plain.txt" 2>"$OUT/search-plain.log"
edge 7120 "$CODE-disk" --quant "$CODE" --search-only --cold-dir "$W/vecs" \
  >"$OUT/search-$CODE.txt" 2>"$OUT/search-$CODE.log"
du -sh "$W/server" "$W/vecs" | tee "$OUT/disk.txt"
cat "$OUT"/load-*.txt "$OUT"/search-*.txt
