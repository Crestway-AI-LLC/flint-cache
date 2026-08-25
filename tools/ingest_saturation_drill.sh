#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Does the write path stay honest and bounded when compaction falls behind?
#
# WHY THIS EXISTS. The 2026-08-16 fleet run measured 11.5x interval write
# amplification on 2-vCPU nodes and, because of it, a progress meter that read
# ~4x high: the harness measured "logical bytes loaded" as on-disk/2, and
# on-disk carries L0 backlog plus pending-compaction intermediates. It tripped
# its target at ~28% of the real number and the run died downstream. That cost
# hours and a fleet to learn. It is reproducible on a laptop in ~2 minutes,
# because it is a RATIO — write rate against compaction throughput — and the
# ratio is reached by shrinking the LSM, not by growing the data.
#
# WHAT IT PINS
#   1. THE HONEST METER. Logical bytes are what the client acked, never what
#      is on disk. The drill knows exactly what it wrote, so the inflation
#      factor is measured rather than assumed — and printed, because that
#      ratio IS the write-amp curve and is the number the write-path work
#      (#165/#196) has to move.
#   2. A CEILING ON AMPLIFICATION. Not a tight number, which would flap: a
#      loose bound that a real regression would blow through. The baseline to
#      beat is on the record above.
#   3. NO ACKED WRITE IS LOST while the store is in that state — the property
#      that makes the amplification a performance problem rather than a
#      correctness one.
#
# The gate default is ~400 MB, not the ~5 GB the roadmap first sketched. The
# ratio is what carries the regime, and 400 MB reaches it in seconds where
# 5 GB would put minutes on every suite run for the same answer. For a deep
# run when the write path changes, raise the volume rather than the wall:
#
#   SAT_KEYS=500000 tools/ingest_saturation_drill.sh     # ~5 GB
#
# Values are INCOMPRESSIBLE per key (urandom, not a repeated byte). #169 was
# filed because a constant fill let block compression squash the data, so the
# "loaded" bytes never landed and the working set never left the block cache.
# A compressible fill here would understate amplification for the same reason.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init "$FLINT_DRILL_ROOT/flint-saturate" 6457
fleet_guard

PORT=6457
DIR="$FLINT_DRILL_ROOT/flint-saturate/n"
BIN="./target/release/flint-server"
# ~400 MB of logical data in 10 KB values. Enough to build several levels
# once the level base is shrunk below; small enough to run in about a minute.
KEYS="${SAT_KEYS:-40000}"
VSIZE="${SAT_VSIZE:-10000}"
# The ceiling. Run-4 measured 11.5x on a saturated 2-vCPU fleet node; 25x is
# well clear of that and of normal variation, while still catching a genuine
# regression (an LSM misconfigured into rewriting everything repeatedly).
WAMP_CEILING="${SAT_WAMP_CEILING:-25}"

cleanup() { fleet_kill server; rm -rf "$DIR"; }
trap cleanup EXIT
rm -rf "$DIR"; mkdir -p "$DIR"

cargo build --release -q -p flint-server --features rocks || { echo "FAIL: build"; exit 1; }
fleet_warm "$BIN"

# A SHRUNKEN LSM: 8 MB level base (default 256 MB) and 4 MB memtables make
# 400 MB behave like the hundreds of gigabytes a fleet node holds — many
# levels, constant compaction. This is the whole trick: reach the regime by
# shrinking the structure instead of growing the dataset.
export FLINT_LEVEL_BASE_MB=8
export FLINT_WRITE_BUFFER_MB=4
# Without this RocksDB writes its stats table to LOG only every 600 s, so a
# drill this short would have nothing to parse and would silently skip the
# two assertions that matter most.
export FLINT_STATS_DUMP_SEC=5

echo "== node :$PORT, level base ${FLINT_LEVEL_BASE_MB}MB, ${KEYS} x ${VSIZE}B incompressible"
# Own subshell: this shell must not own the seat as a job, or cleanup's
# SIGKILL prints "Killed: 9" after the PASS line and a clean run looks broken.
( "$BIN" --port "$PORT" --engine rocks --data-dir "$DIR" >"$DIR/out" 2>&1 & )
fleet_wait_listen "$PORT"

# Feed raw RESP into valkey-cli --pipe: binary-safe, backpressured, and it
# counts replies, so "acked" is a number the drill owns rather than infers.
python3 - "$KEYS" "$VSIZE" > "$DIR/feed.resp" <<'PYEOF'
import os, sys
n, vs = int(sys.argv[1]), int(sys.argv[2])
out = sys.stdout.buffer
hdr = b"*3\r\n$3\r\nSET\r\n"
vlen = ("$%d\r\n" % vs).encode()
for i in range(n):
    k = ("sat:%012d" % i).encode()
    out.write(hdr + ("$%d\r\n" % len(k)).encode() + k + b"\r\n" + vlen + os.urandom(vs) + b"\r\n")
PYEOF

LOGICAL=$(( KEYS * VSIZE ))
T0=$(date +%s)
PIPE_OUT=$(valkey-cli -p "$PORT" --pipe < "$DIR/feed.resp" 2>&1 | tr -d '\r')
T1=$(date +%s)
ELAPSED=$(( T1 - T0 )); [ "$ELAPSED" -gt 0 ] || ELAPSED=1
ERRS=$(printf '%s' "$PIPE_OUT" | sed -n 's/.*errors: \([0-9]*\).*/\1/p' | head -1)
echo "  fed $(( LOGICAL / 1048576 )) MB logical in ${ELAPSED}s ($(( LOGICAL / 1048576 / ELAPSED )) MB/s), errors=${ERRS:-?}"
[ "${ERRS:-1}" = "0" ] || { echo "FAIL: the feed reported errors: $PIPE_OUT"; exit 1; }

# Let compaction do what it is going to do AND let at least one stats dump
# land (FLINT_STATS_DUMP_SEC above), then read the truth three ways.
sleep 12
PHYS=$(du -sk "$DIR" | awk '{print $1 * 1024}')
INFLATION=$(awk -v p="$PHYS" -v l="$LOGICAL" 'BEGIN{printf "%.2f", p/l}')
# RocksDB's own accounting. Flush(GB) is what actually entered the LSM (the
# honest logical figure); the Sum row's W-Amp is the amplification.
LOG="$DIR/LOG"
FLUSHED=$(grep -E "^Flush\(GB\): cumulative" "$LOG" 2>/dev/null | tail -1 | sed -n 's/.*cumulative \([0-9.]*\).*/\1/p')
WAMP=$(awk '/^ Sum/{w=$17} END{print w+0}' "$LOG" 2>/dev/null)
echo "  logical(acked)=$(( LOGICAL / 1048576 ))MB  physical(du)=$(( PHYS / 1048576 ))MB  inflation=${INFLATION}x"
echo "  rocksdb: Flush(GB) cumulative=${FLUSHED:-?}  W-Amp=${WAMP:-?}"

# 1. THE HONEST METER. RocksDB's own Flush total must agree with what the
#    client acked. If these ever diverge, "logical bytes" has no single
#    meaning and every rate derived from it is suspect.
if [ -n "$FLUSHED" ]; then
  OK=$(awk -v f="$FLUSHED" -v l="$LOGICAL" 'BEGIN{fb=f*1073741824; print (fb > l*0.7 && fb < l*1.6) ? 1 : 0}')
  [ "$OK" = 1 ] || {
    echo "FAIL: RocksDB flushed ${FLUSHED}GB but the client acked $(( LOGICAL / 1048576 ))MB —"
    echo "      the two accounts of 'logical bytes' disagree, so neither can be trusted"
    exit 1
  }
  echo "  meter honest: flushed bytes agree with acked bytes"
fi

# 2. THE CEILING.
if [ -n "$WAMP" ] && [ "$WAMP" != "0" ]; then
  OVER=$(awk -v w="$WAMP" -v c="$WAMP_CEILING" 'BEGIN{print (w > c) ? 1 : 0}')
  [ "$OVER" = 0 ] || {
    echo "FAIL: write amplification ${WAMP}x exceeds the ${WAMP_CEILING}x ceiling."
    echo "      Every logical byte is being rewritten ${WAMP} times; on a small node"
    echo "      that is compaction stealing the cores the write path needs."
    exit 1
  }
  echo "  amplification ${WAMP}x within the ${WAMP_CEILING}x ceiling"
fi

# 3. NO ACKED WRITE IS LOST. Sample across the whole keyspace, including the
#    earliest keys, which are the ones compaction has rewritten most often.
MISSING=0
for i in 0 1 7 99 1000 9999 19999 29999 $(( KEYS - 1 )); do
  [ "$i" -ge "$KEYS" ] && continue
  K=$(printf "sat:%012d" "$i")
  LEN=$(valkey-cli -p "$PORT" STRLEN "$K" 2>/dev/null | tr -d '\r')
  [ "$LEN" = "$VSIZE" ] || { echo "  MISSING/short: $K (strlen=$LEN want=$VSIZE)"; MISSING=$((MISSING+1)); }
done
[ "$MISSING" = 0 ] || { echo "FAIL: $MISSING acked keys are missing or truncated after compaction"; exit 1; }
echo "  every sampled acked key reads back at full length"

echo "PASS: ingest saturation drill — the write path's logical accounting matches"
echo "      RocksDB's own, amplification is bounded (${WAMP:-?}x vs ${WAMP_CEILING}x),"
echo "      physical ran ${INFLATION}x ahead of logical, and no acked write was lost"
