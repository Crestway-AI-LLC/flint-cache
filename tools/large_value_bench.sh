#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Large values: what they cost, and what they cost EVERYONE ELSE.
#
# Every published Flint number is measured at 1 KB, while the value cap is
# 512 MB — six orders of magnitude we permit and had never measured. This
# sweeps the range people actually cache (JSON documents, serialized
# features, rendered fragments) and reports two different things:
#
#   1. throughput and latency for the large values themselves, which is the
#      obvious measurement; and
#   2. p99 for ORDINARY 1 KB traffic running concurrently, which is the one
#      that matters. On a single-threaded-per-shard engine a big value is a
#      head-of-line blocking problem: it monopolises the shard and every
#      small request behind it waits. Flint is not single-threaded per
#      shard, so "small keys keep their latency while large values move" is
#      a claim it can make and Redis structurally cannot — but a claim is
#      worth nothing unmeasured, and this is where it gets measured.
#
# Usage: tools/large_value_bench.sh [--engine rocks|mem] [--port N]
set -u
cd "$(dirname "$0")/.."

ENGINE=rocks; PORT=6395
while [ $# -gt 0 ]; do
  case "$1" in
    --engine) ENGINE=$2; shift 2 ;;
    --port) PORT=$2; shift 2 ;;
    *) echo "unknown arg $1"; exit 2 ;;
  esac
done

command -v memtier_benchmark >/dev/null || {
  echo "SKIP: memtier_benchmark not installed (brew install memtier_benchmark)"; exit 0; }

D=/tmp/flint-lv; rm -rf $D; mkdir -p $D
pkill -9 -f "flint-server.*$PORT" 2>/dev/null; sleep 0.3
cleanup() { pkill -9 -f "flint-server.*$PORT" 2>/dev/null; rm -rf $D; }
trap cleanup EXIT

cargo build --release -q -p flint-server --features rocks

ENGINE_ARGS="--engine $ENGINE"
[ "$ENGINE" = "rocks" ] && ENGINE_ARGS="--engine rocks --data-dir $D/data"
./target/release/flint-server --port $PORT $ENGINE_ARGS >$D/server.log 2>&1 &
for _ in $(seq 1 40); do
  [ "$(valkey-cli -p $PORT PING 2>/dev/null)" = "PONG" ] && break; sleep 0.25
done
[ "$(valkey-cli -p $PORT PING 2>/dev/null)" = "PONG" ] || { echo "FAIL: server did not start"; tail -5 $D/server.log; exit 1; }
echo "engine: $ENGINE   value cap: $(valkey-cli -p $PORT CONFIG GET maxmemory 2>/dev/null | tail -1)"

# Keep the DATASET roughly constant (~2 GB) across sizes, so what changes
# between rows is the value size and not how much data is in play.
DATASET=$((2 * 1024 * 1024 * 1024))

run() { # size_bytes label
  local sz=$1 label=$2
  local keys=$((DATASET / sz)); [ $keys -lt 200 ] && keys=200
  valkey-cli -p $PORT FLUSHALL >/dev/null
  memtier_benchmark -s 127.0.0.1 -p $PORT --protocol=redis --hide-histogram \
    -n allkeys --key-maximum="$keys" --key-pattern=P:P --ratio=1:0 \
    -d "$sz" --random-data -c 4 -t 2 >/dev/null 2>&1
  local out
  out=$(memtier_benchmark -s 127.0.0.1 -p $PORT --protocol=redis --hide-histogram \
    --test-time=20 -d "$sz" --random-data --key-maximum="$keys" \
    --key-pattern=R:R --ratio=1:9 -c 4 -t 2 2>/dev/null | tail -12)
  local ops p50 p99
  ops=$(echo "$out" | awk '/^Totals/ {print $2}')
  p50=$(echo "$out" | awk '/^Totals/ {print $6}')
  p99=$(echo "$out" | awk '/^Totals/ {print $8}')
  printf "  %-10s %-9s %12s ops/s   p50 %8s ms   p99 %8s ms\n" "$label" "$keys" "$ops" "$p50" "$p99"
}

echo
echo "== value-size sweep (~2 GB dataset each, 1:9 write:read)"
printf "  %-10s %-9s %12s   %12s   %12s\n" "value" "keys" "throughput" "p50" "p99"
run 1024      "1 KB"
run 65536     "64 KB"
run 1048576   "1 MB"
run 16777216  "16 MB"

# ---------------------------------------------------------------------------
# The fairness measurement. Small keys and large values on the SAME node at
# the same time: does the 1 KB p99 hold while 1 MB values are in flight?
#
# METHODOLOGY, because the obvious way to write this is wrong. Loading 2 GB
# of 1 MB values leaves RocksDB flushing and compacting for a long time
# afterwards. Measure the baseline immediately after that load and you
# measure the compaction backlog, not the baseline — and because the
# CONTENDED run happens later, once the backlog has drained, the contended
# number comes out BETTER than the quiet one and the bench reports large
# values making small keys faster. They do not. The first version of this
# script did exactly that.
#
# So: load everything, settle, and then measure the quiet baseline TWICE,
# once either side of the contended run. If the two baselines disagree the
# machine was not in a steady state and no ratio computed from them means
# anything, so the script says so instead of printing a number.
# ---------------------------------------------------------------------------
SETTLE=${SETTLE:-60}
probe_small() { # -> "ops p99"
  memtier_benchmark -s 127.0.0.1 -p $PORT --hide-histogram \
    --test-time=20 -d 1024 --random-data --key-prefix=small: \
    --key-maximum=200000 --key-pattern=R:R --ratio=1:9 -c 4 -t 2 2>/dev/null \
    | awk '/^Totals/ {print $2, $8}'
}

echo
echo "== isolation: 1 KB p99 quiet vs. with 1 MB traffic alongside"
valkey-cli -p $PORT FLUSHALL >/dev/null
memtier_benchmark -s 127.0.0.1 -p $PORT --hide-histogram -n allkeys \
  --key-prefix=small: --key-maximum=200000 --key-pattern=P:P --ratio=1:0 \
  -d 1024 --random-data -c 4 -t 2 >/dev/null 2>&1
memtier_benchmark -s 127.0.0.1 -p $PORT --hide-histogram -n allkeys \
  --key-prefix=big: --key-maximum=2000 --key-pattern=P:P --ratio=1:0 \
  -d 1048576 --random-data -c 2 -t 1 >/dev/null 2>&1
echo "  settling ${SETTLE}s after the load (no compaction signal to poll)"
sleep "$SETTLE"

read -r BASE_OPS BASE_P99 <<<"$(probe_small)"
printf "  1 KB quiet (before)         %12s ops/s   p99 %8s ms\n" "$BASE_OPS" "$BASE_P99"

# The contending load is READ-ONLY, deliberately. Writing 1 MB values at
# this rate leaves ~2 GB of compaction debt behind, which then degrades the
# second quiet baseline and makes the whole comparison unusable — the same
# confound as the load phase, in the middle of the test.
#
# It also separates two questions that were tangled together. "Does a large
# request block small ones while it is being served?" is the head-of-line
# question this section exists to answer, and reads isolate it. "Does a
# burst of large WRITES stall the write path?" is a different question, and
# the sweep above already answers it (loudly).
memtier_benchmark -s 127.0.0.1 -p $PORT --hide-histogram --test-time=26 \
  -d 1048576 --random-data --key-prefix=big: --key-maximum=2000 \
  --key-pattern=R:R --ratio=0:1 -c 2 -t 1 >$D/big.log 2>&1 &
BIG=$!
sleep 3
read -r WITH_OPS WITH_P99 <<<"$(probe_small)"
wait $BIG 2>/dev/null
BIG_OPS=$(grep -E '^Totals' $D/big.log | awk '{print $2}')
printf "  1 KB + 1 MB reads alongside %12s ops/s   p99 %8s ms   (large: %s ops/s)\n" \
  "$WITH_OPS" "$WITH_P99" "${BIG_OPS:-n/a}"

sleep "$SETTLE"
read -r BASE2_OPS BASE2_P99 <<<"$(probe_small)"
printf "  1 KB quiet (after)          %12s ops/s   p99 %8s ms\n" "$BASE2_OPS" "$BASE2_P99"

echo
python3 - "$BASE_P99" "$WITH_P99" "$BASE2_P99" <<'PY'
import sys
try:
    base, with_, base2 = (float(x) for x in sys.argv[1:4])
except ValueError:
    print("  (could not parse p99s — read the rows above)"); raise SystemExit(0)
if base <= 0 or base2 <= 0:
    print("  (zero baseline — invalid run)"); raise SystemExit(0)
drift = abs(base2 - base) / max(base, base2)
print(f"  baseline drift between the two quiet runs: {drift*100:.0f}%")
if drift > 0.30:
    print("  UNSTABLE — the two quiet baselines disagree by more than 30%, so the")
    print("  node was still settling and no contention ratio from this run is")
    print("  meaningful. Raise SETTLE (currently exported seconds) and re-run.")
    raise SystemExit(0)
quiet = (base + base2) / 2
print(f"  small-key p99 inflation under large-value load: {with_/quiet:.2f}x "
      f"({quiet:.3f} ms quiet -> {with_:.3f} ms contended)")
print("  A single-threaded-per-shard engine serialises the big value ahead of")
print("  every small request behind it; this is how much of that Flint avoids.")
PY
echo
echo "NOTE: a measurement, not a gate — it prints numbers and asserts nothing."
echo "Record results in docs/bench/ and re-run when the write path changes."
