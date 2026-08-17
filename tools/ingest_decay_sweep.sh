#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# #196 step 3: measure the ingest DECAY CURVE, and what compaction settings do
# to it, on a host shaped like the seat we ship.
#
# WHY THIS IS NOT ingest_saturation_drill.sh. That drill asks a yes/no question
# — is the meter honest, is amplification bounded, was an acked write lost —
# and answers it in a minute on any machine. This asks a QUANTITATIVE one:
# how far does ingest fall as the LSM deepens, and does giving compaction more
# background jobs move that curve. A pass/fail gate cannot answer it, and a
# fast machine cannot either.
#
# WHY IT REFUSES TO RUN ON A BIG BOX. The whole finding behind #196 is that
# `flint-storage` sets no compaction parallelism at all — the single
# `increase_parallelism` call in the tree is in flint-bench, not the engine —
# so every seat runs RocksDB's default 2 background jobs. On 8 or 16 cores,
# compaction and the write path do not contend, so raising the job count looks
# free whatever the truth is on a saturated 2-vCPU seat. A sweep run there
# produces a number that has to be retracted later, which is worse than no
# number. So the host asserts come first and they are hard.
#
# WHAT IT MEASURES, per configuration:
#   * the curve: MB/s per interval across a sustained feed, so decay is a
#     shape rather than a single average that hides it
#   * where the cores went: engine background threads (RocksDB's compaction
#     and flush pool, `rocksdb:*`) against everything else, sampled from
#     /proc per interval. Run 4's "compaction ~0.43 core, serve ~1.2" came
#     from this split, and it is the quantity a job-count change is supposed
#     to move.
#   * RocksDB's own verdict: cumulative W-Amp and stall percentage. Run 4 saw
#     ZERO stalls, which is the finding that decides what tuning can even do
#     here — with no stalls the write path is not waiting on compaction, it is
#     competing with it for CPU, and more background jobs can only make that
#     worse. A sweep that reports stalls instead would mean the regime moved.
#
# THE POSITIVE CONTROL. If the last interval is not materially slower than the
# first, the run never reached the regime and every comparison below it is
# noise about a warm-up. That is a failure of the MEASUREMENT, and it exits
# non-zero, because a flat curve reported as "no decay" is exactly the
# flattering result this file exists to avoid.
#
# Usage:
#   DECAY_PIN=0,1 tools/ingest_decay_sweep.sh   # seat on 2 cores of a big box
#   tools/ingest_decay_sweep.sh                 # on a genuinely small host
#   DECAY_JOBS='0 2 4 6' tools/ingest_decay_sweep.sh
#   DECAY_KEYS=120000 tools/ingest_decay_sweep.sh    # deeper, slower
#
# `0` means UNSET — RocksDB's own default, which is what every seat runs
# today and therefore the only honest baseline column.
#
# ON AN i4i, PUT THE SCRATCH ON THE INSTANCE STORE. A stock AL2023 image does
# not mount it, so FLINT_DRILL_ROOT would land on the root EBS volume, whose
# gp3 baseline is below the bar this script checks — it will refuse rather
# than hand back a disk curve labelled as a compaction curve:
#
#   sudo mkfs.ext4 -F /dev/nvme1n1 && sudo mkdir -p /mnt/d \
#     && sudo mount /dev/nvme1n1 /mnt/d && sudo chown ec2-user /mnt/d
#   FLINT_DRILL_ROOT=/mnt/d tools/ingest_decay_sweep.sh
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init "$FLINT_DRILL_ROOT/flint-decay" 6459
fleet_guard

PORT=6459
ROOT="$FLINT_DRILL_ROOT/flint-decay"
OUT="${DECAY_OUT:-/tmp/flint-decay}"
BIN="./target/release/flint-server"

KEYS="${DECAY_KEYS:-80000}"          # x VSIZE = ~800 MB logical
VSIZE="${DECAY_VSIZE:-10000}"
INTERVALS="${DECAY_INTERVALS:-8}"    # ~100 MB per interval
JOBS="${DECAY_JOBS:-0 2 4 6}"
SETTLE="${DECAY_SETTLE:-20}"         # seconds after the feed, for the stats dump

# CONSTRAIN THE SERVER, NOT THE MACHINE — and prefer pinning to a small
# instance. DECAY_PIN taskset-pins the SEAT to a couple of cores on a big host
# and leaves the load generator the rest.
#
# That is not a convenience, it is the more faithful arrangement. On a real
# 2-vCPU seat the load arrives from OTHER machines, so both cores are the
# server's. Running the driver locally on a 2-vCPU box would have it competing
# with the thing under test for exactly the resource in question, and every
# column would carry the driver's appetite. Pinning separates them the way the
# fleet does. It also sidesteps a cold RocksDB build on two cores, which costs
# more wall-clock than the measurement.
# A cpuset has more than one spelling for the same set — "0,1" and "0-1" are
# the same two CPUs, and the kernel reports back whichever form is shorter. So
# canonicalise before counting or comparing, or the verification below rejects
# a pin that is exactly right.
cpuset_expand() {
  python3 - "$1" <<'PYEOF'
import sys
out = []
for part in sys.argv[1].split(","):
    part = part.strip()
    if not part:
        continue
    if "-" in part:
        a, b = part.split("-"); out.extend(range(int(a), int(b) + 1))
    else:
        out.append(int(part))
print(",".join(str(c) for c in sorted(set(out))))
PYEOF
}

PIN="${DECAY_PIN:-}"
if [ -n "$PIN" ]; then
  command -v taskset >/dev/null || {
    echo "REFUSED: DECAY_PIN=$PIN but no taskset on this host."; exit 2; }
  PIN_SET=$(cpuset_expand "$PIN")
  SEAT_CPUS=$(printf '%s' "$PIN_SET" | tr ',' '\n' | grep -c .)
  RUN=(taskset -c "$PIN")
else
  PIN_SET=""
  SEAT_CPUS=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 0)
  RUN=()
fi

# THE HOST ASSERTS. Refusals, not warnings: a warning in a log is how a number
# from the wrong machine ends up in a document.
if [ "${DECAY_ANY_HOST:-0}" != "1" ] && [ "$SEAT_CPUS" -gt 4 ]; then
  echo "REFUSED: the seat would have $SEAT_CPUS cores. This sweep is only"
  echo "  meaningful where compaction and the write path contend — the 2-vCPU"
  echo "  seat we ship. With more, background jobs are close to free, so the"
  echo "  answer is 'more is better' regardless of the truth on a real node."
  echo "  Pin the seat (DECAY_PIN=0,1) or run on an i4i.large; DECAY_ANY_HOST=1"
  echo "  if you are deliberately measuring something else."
  exit 2
fi

# DOES THE ARITHMETIC EVALUATE? Asked here, in the first second, and runnable
# anywhere with DECAY_SELFTEST=1 — including the laptop, which cannot run the
# sweep itself.
#
# Every expression below was once written as `printf "%.1f", x > y ? a : b`,
# which awk parses as `printf "%.1f", x` REDIRECTED TO THE FILE `y`, and then
# chokes on the `?`. It is a syntax error in awk, not a runtime one, so
# `bash -n` on this file says nothing about it: the shell only sees a quoted
# string. It cost a full run on a paid box to find, and the run did not even
# fail — it printed empty columns for four configurations and PASSED.
#
# So the expressions are exercised against known inputs before anything else
# happens. Cheap, and it catches the whole class.
selftest() {
  local fail=0 got
  got=$(awk -v a=1.0 -v b=2.5 'BEGIN{printf "%.2f", ((b-a)>0.01?(b-a):0.01)}')
  [ "$got" = "1.50" ] || { echo "  elapsed: got '$got' want 1.50"; fail=1; }
  got=$(awk -v m=200 -v s=8 'BEGIN{printf "%.1f", (s>0 ? m/s : 0)}')
  [ "$got" = "25.0" ] || { echo "  mean: got '$got' want 25.0"; fail=1; }
  got=$(awk -v f=100 -v l=60 'BEGIN{printf "%.0f", (f>0 ? (1 - l/f) * 100 : 0)}')
  [ "$got" = "40" ] || { echo "  decay: got '$got' want 40"; fail=1; }
  got=$(awk -v a=100 -v b=105 'BEGIN{printf "%.1f", (a>0 ? (b>a?(b-a):(a-b))/a*100 : 0)}')
  [ "$got" = "5.0" ] || { echo "  noise: got '$got' want 5.0"; fail=1; }
  got=$(awk -v a=1 -v b=3 'BEGIN{d=b-a; if(d<0.01)d=0.01; printf "%d", 512/d}')
  [ "$got" = "256" ] || { echo "  disk: got '$got' want 256"; fail=1; }
  return $fail
}
if ! selftest; then
  echo "REFUSED: this script's own arithmetic does not evaluate on this awk."
  echo "  Fix that before spending a machine on it — every number it would"
  echo "  print derives from those expressions."
  exit 2
fi
if [ "${DECAY_SELFTEST:-0}" = "1" ]; then
  echo "selftest OK: the arithmetic evaluates on $(awk --version 2>/dev/null | head -1 || echo 'this awk')"
  exit 0
fi

if [ ! -r /proc/self/stat ]; then
  echo "REFUSED: no /proc — the CPU split between compaction and the write"
  echo "  path is read per-thread from /proc, and without it this measures"
  echo "  only throughput, which is the half that does not explain itself."
  exit 2
fi

mkdir -p "$OUT" "$ROOT"
cleanup() { fleet_kill server; rm -rf "$ROOT"; }
trap cleanup EXIT

cargo build --release -q -p flint-server --features rocks || { echo "FAIL: build"; exit 1; }
fleet_warm "$BIN"

# THE DISK MUST NOT BE THE BOTTLENECK, and that is measured rather than
# assumed from a device name. A gp3 root volume at its 125 MB/s baseline would
# turn a CPU experiment into a disk experiment and every column would move
# together for the wrong reason. The bar is 4x the fastest per-interval rate
# we expect to see, written with fsync so the page cache cannot answer for the
# device.
CHUNK_MB=$(( KEYS * VSIZE / INTERVALS / 1048576 ))
DISK_MIN=250
# Timed here rather than parsed out of dd's summary line, whose units switch
# between MB/s and GB/s depending on the device — the one place a fast disk
# would be misread as a slow one and refuse a valid run.
DT0=$(date +%s.%N)
dd if=/dev/zero of="$ROOT/.probe" bs=1M count=512 conv=fsync >/dev/null 2>&1
DT1=$(date +%s.%N)
rm -f "$ROOT/.probe"
DD_MBPS=$(awk -v a="$DT0" -v b="$DT1" 'BEGIN{d=b-a; if(d<0.01)d=0.01; printf "%d", 512/d}')
echo "== seat on $SEAT_CPUS core(s)${PIN:+ (pinned to $PIN)}, scratch writes at ${DD_MBPS} MB/s (need >= ${DISK_MIN})"
if [ "$DD_MBPS" -lt "$DISK_MIN" ]; then
  echo "REFUSED: $FLINT_DRILL_ROOT sustains only ${DD_MBPS} MB/s. At that speed"
  echo "  the curve below would be the disk's, not compaction's. On an i4i,"
  echo "  put FLINT_DRILL_ROOT on the NVMe instance store, not the root EBS"
  echo "  volume."
  exit 2
fi

# PRE-GENERATE every chunk before the first timer starts. Generating
# incompressible values costs real CPU, and on a 2-vCPU box that CPU would be
# taken from the thing being measured — the feed would look slower exactly
# when the generator was busiest, which is not a property of the write path.
# (#169: values must be incompressible, or block compression squashes them and
# the working set never leaves the block cache.)
echo "== generating $INTERVALS x ${CHUNK_MB}MB of incompressible feed"
PER=$(( KEYS / INTERVALS ))
python3 - "$PER" "$INTERVALS" "$VSIZE" "$ROOT" <<'PYEOF'
import os, sys
per, n, vs, root = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
hdr = b"*3\r\n$3\r\nSET\r\n"
vlen = ("$%d\r\n" % vs).encode()
for c in range(n):
    with open("%s/feed.%02d" % (root, c), "wb") as f:
        for i in range(c * per, (c + 1) * per):
            k = ("dec:%012d" % i).encode()
            f.write(hdr + ("$%d\r\n" % len(k)).encode() + k + b"\r\n" + vlen + os.urandom(vs) + b"\r\n")
PYEOF

# Per-thread CPU, split by who owns the thread. RocksDB names its background
# pool `rocksdb:low*` / `rocksdb:high*` (comm is truncated at 15 chars); every
# other thread in the process is the server's own. utime+stime are in clock
# ticks, so the caller converts with getconf CLK_TCK.
cpu_snapshot() {
  python3 - "$1" <<'PYEOF'
import sys, os
pid = sys.argv[1]
eng = other = 0
d = "/proc/%s/task" % pid
try:
    tids = os.listdir(d)
except OSError:
    print("0 0"); sys.exit(0)
for t in tids:
    try:
        with open("%s/%s/comm" % (d, t)) as f: name = f.read().strip()
        with open("%s/%s/stat" % (d, t)) as f: fields = f.read().rsplit(") ", 1)[1].split()
    except OSError:
        continue
    # utime, stime are fields 14,15 in proc(5); after the rsplit above the
    # first entry is field 3, so they land at indices 11 and 12.
    ticks = int(fields[11]) + int(fields[12])
    if name.startswith("rocksdb"): eng += ticks
    else: other += ticks
print("%d %d" % (eng, other))
PYEOF
}
HZ=$(getconf CLK_TCK 2>/dev/null || echo 100)

TSV="$OUT/decay.tsv"
: > "$TSV"
printf 'bg_jobs\tinterval\tmb\tsecs\tmb_s\teng_cores\tother_cores\n' >> "$TSV"
SUMMARY="$OUT/summary.txt"
: > "$SUMMARY"

for J in $JOBS; do
  DIR="$ROOT/n"
  rm -rf "$DIR"; mkdir -p "$DIR"
  if [ "$J" = "0" ]; then
    unset FLINT_BG_JOBS; LABEL="default"
  else
    export FLINT_BG_JOBS="$J"; LABEL="$J"
  fi
  # The shrunken LSM from ingest_saturation_drill: reach the fleet's regime by
  # shrinking the structure rather than growing the dataset. Held FIXED across
  # the sweep — the only thing that varies is the job count, or the columns
  # are not comparable.
  export FLINT_LEVEL_BASE_MB=8 FLINT_WRITE_BUFFER_MB=4 FLINT_STATS_DUMP_SEC=5

  echo
  echo "== FLINT_BG_JOBS=$LABEL  ($KEYS x ${VSIZE}B, $INTERVALS intervals, seat on $SEAT_CPUS core(s))"
  "${RUN[@]+"${RUN[@]}"}" "$BIN" --port "$PORT" --engine rocks --data-dir "$DIR" >"$DIR/out" 2>&1 &
  # Ready, not merely listening: since #176 a node binds before it can serve,
  # and a first interval that started against a node still coming up would be
  # charged to compaction.
  fleet_wait_ready "$PORT"
  SPID=$(pgrep -f "flint-server --port $PORT( |$)" | head -1)
  [ -n "$SPID" ] || { echo "FAIL: no server pid for :$PORT"; exit 1; }

  # THE PIN IS VERIFIED, NOT ASSUMED. taskset can succeed and still leave the
  # process on every CPU if the mask was misread, and an unpinned seat on a
  # 16-core box produces exactly the flattering "more jobs is free" answer
  # this whole file refuses to produce. Ask the kernel what it actually did.
  if [ -n "$PIN" ]; then
    ALLOWED=$(grep -m1 '^Cpus_allowed_list:' "/proc/$SPID/status" 2>/dev/null | awk '{print $2}')
    ALLOWED_SET=$(cpuset_expand "${ALLOWED:-}")
    [ "$ALLOWED_SET" = "$PIN_SET" ] || {
      echo "FAIL: asked for CPUs [$PIN_SET], the seat is running on [${ALLOWED_SET:-unknown}]."
      echo "  An unpinned seat measures the host, not the node we ship."
      exit 1
    }
  fi

  read -r E0 O0 <<<"$(cpu_snapshot "$SPID")"
  FIRST=""; LAST=""; ENG_TICKS=0; TOT_SECS=0
  printf '  %-4s %8s %8s %10s %10s\n' int MB secs MB/s "eng|other"
  for i in $(seq 0 $(( INTERVALS - 1 ))); do
    c=$(printf '%02d' "$i")
    T0=$(date +%s.%N)
    ERR=$(valkey-cli -p "$PORT" --pipe < "$ROOT/feed.$c" 2>&1 | tr -d '\r' \
          | sed -n 's/.*errors: \([0-9]*\).*/\1/p' | head -1)
    T1=$(date +%s.%N)
    read -r E1 O1 <<<"$(cpu_snapshot "$SPID")"
    [ "${ERR:-1}" = "0" ] || { echo "FAIL: interval $c reported errors=$ERR"; exit 1; }
    SECS=$(awk -v a="$T0" -v b="$T1" 'BEGIN{printf "%.2f", ((b-a)>0.01?(b-a):0.01)}')
    RATE=$(awk -v m="$CHUNK_MB" -v s="$SECS" 'BEGIN{printf "%.1f", m/s}')
    # FAIL ON THE FIRST BLANK, not after four configs of empty columns. An
    # arithmetic slip that yields "" propagates silently: every subsequent
    # number derives from it, the summary prints "mean=  first= MB/s", and the
    # final control has nothing to compare, so the run ends in a PASS made of
    # holes. One empty cell is the whole measurement.
    case "$SECS$RATE" in
      *[!0-9.]*|"") echo "FAIL: interval $c produced no timing (secs='$SECS' rate='$RATE')."
                    echo "  The arithmetic is broken; nothing below it would mean anything."
                    exit 1 ;;
    esac
    EC=$(awk -v d="$(( E1 - E0 ))" -v hz="$HZ" -v s="$SECS" 'BEGIN{printf "%.2f", d/hz/s}')
    OC=$(awk -v d="$(( O1 - O0 ))" -v hz="$HZ" -v s="$SECS" 'BEGIN{printf "%.2f", d/hz/s}')
    ENG_TICKS=$(( ENG_TICKS + E1 - E0 ))
    E0=$E1; O0=$O1
    printf '  %-4s %8s %8s %10s %10s\n' "$c" "$CHUNK_MB" "$SECS" "$RATE" "$EC|$OC"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$LABEL" "$c" "$CHUNK_MB" "$SECS" "$RATE" "$EC" "$OC" >> "$TSV"
    TOT_SECS=$(awk -v a="$TOT_SECS" -v b="$SECS" 'BEGIN{printf "%.2f", a+b}')
    [ -z "$FIRST" ] && FIRST=$RATE
    LAST=$RATE
  done
  MEAN=$(awk -v m="$(( CHUNK_MB * INTERVALS ))" -v s="$TOT_SECS" 'BEGIN{printf "%.1f", (s>0 ? m/s : 0)}')

  # POSITIVE CONTROL ON THE INSTRUMENT. RocksDB names its background threads
  # only under glibc; where it does not, every thread falls into "other" and
  # the compaction column reads 0.00 — a plausible-looking number meaning
  # "compaction is free" when it actually means the split is fiction. An
  # 800 MB feed into an 8 MB level base cannot leave compaction idle, so zero
  # here is the instrument failing, not the engine resting.
  if [ "$ENG_TICKS" -eq 0 ]; then
    echo "FAIL: no CPU attributed to any rocksdb:* thread across the whole run."
    echo "  The engine/serve split is not being measured — thread naming is"
    echo "  glibc-only and this host is not providing it. Every eng column"
    echo "  above is 0.00 for that reason and none of it is evidence."
    exit 1
  fi

  sleep "$SETTLE"
  LOG="$DIR/LOG"
  WAMP=$(awk '/^ Sum/{w=$17} END{print w+0}' "$LOG" 2>/dev/null)
  STALLPCT=$(grep -E '^Cumulative stall:' "$LOG" 2>/dev/null | tail -1 \
             | sed -n 's/.*, \([0-9.]*\) percent.*/\1/p')
  PHYS=$(du -sk "$DIR" | awk '{print $1 / 1024}')
  DECAY=$(awk -v f="$FIRST" -v l="$LAST" 'BEGIN{printf "%.0f", (f>0 ? (1 - l/f) * 100 : 0)}')
  LINE=$(printf 'bg_jobs=%-8s mean=%s  first=%s MB/s  last=%s MB/s  decay=%s%%  W-Amp=%s  stall=%s%%  phys=%.0fMB' \
    "$LABEL" "$MEAN" "$FIRST" "$LAST" "$DECAY" "${WAMP:-?}" "${STALLPCT:-0}" "$PHYS")
  echo "  $LINE"
  echo "$LINE" >> "$SUMMARY"

  fleet_kill server
  sleep 1
done

echo
echo "== summary"
cat "$SUMMARY"
echo
echo "  per-interval rows: $TSV"

# THE POSITIVE CONTROL, applied to the BASELINE column. If the default
# configuration showed no decay, the run never entered the regime the fleet is
# in, and every difference between the columns above is warm-up noise dressed
# as a finding. Deeper is the fix — DECAY_KEYS — not a softer threshold.
BASE_DECAY=$(awk '/^bg_jobs=default/{ for (i=1;i<=NF;i++) if ($i ~ /^decay=/) { sub(/decay=/,"",$i); sub(/%/,"",$i); print $i } }' "$SUMMARY" | head -1)
# ABSENCE IS NOT SUCCESS. This guard was written `[ -n "$BASE_DECAY" ] && [
# "$BASE_DECAY" -lt 15 ]`, so a MISSING figure skipped the check entirely and
# the run printed "PASS: the curve is real (baseline decayed ?%)". That is the
# exact failure this file exists to prevent, in the file itself, and it is the
# same shape as bugs/0009 — a check that verifies nothing and reads as green.
# It fired for real: an awk slip emptied every rate and the sweep still passed.
# So: no baseline figure is a HARDER failure than a bad one.
if [ -z "${BASE_DECAY:-}" ]; then
  echo
  echo "MEASUREMENT INVALID: no decay figure for the baseline column at all."
  echo "  Something above failed to produce a number, so there is nothing to"
  echo "  check and nothing here is evidence. Read the run output, not this line."
  exit 1
fi
if [ "$BASE_DECAY" -lt 15 ]; then
  echo
  echo "MEASUREMENT INVALID: the default configuration decayed only ${BASE_DECAY}%"
  echo "  across the run, so the LSM never got deep enough for compaction to"
  echo "  cost anything. Nothing above is evidence about compaction settings."
  echo "  Raise DECAY_KEYS (currently $KEYS) and run it again."
  exit 1
fi
# THE NOISE FLOOR, FOR FREE. RocksDB's own default for max_background_jobs is
# 2 (options.h), so the `default` and `2` columns configure the SAME engine by
# two routes and any gap between them is run-to-run variance. That number is
# the error bar on every other comparison here, and without it a 5% "win" and
# a 5% coin-toss look identical. Reported rather than asserted when the sweep
# includes both, since a sweep that omits one is still a valid sweep.
D_MEAN=$(awk '/^bg_jobs=default/{for(i=1;i<=NF;i++) if($i ~ /^mean=/){sub(/mean=/,"",$i); print $i}}' "$SUMMARY" | head -1)
T_MEAN=$(awk '/^bg_jobs=2 /{for(i=1;i<=NF;i++) if($i ~ /^mean=/){sub(/mean=/,"",$i); print $i}}' "$SUMMARY" | head -1)
if [ -n "${D_MEAN:-}" ] && [ -n "${T_MEAN:-}" ]; then
  NOISE=$(awk -v a="$D_MEAN" -v b="$T_MEAN" 'BEGIN{printf "%.1f", (a>0 ? (b>a?(b-a):(a-b))/a*100 : 0)}')
  echo "  noise floor: default vs an explicit 2 (the same engine) differ by ${NOISE}%."
  echo "  Treat any column gap smaller than that as nothing."
fi

echo "PASS: the curve is real (baseline decayed ${BASE_DECAY}%) — read the summary above"
