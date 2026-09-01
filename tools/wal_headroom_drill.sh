#!/usr/bin/env bash
# WAL headroom backpressure must fire BEFORE a replica meets a deleted segment.
#
# WHY THIS EXISTS. BUG-0012, twice on the playground in three weeks. A replica
# tails the master's WAL; when its cursor falls into a segment RocksDB has
# already recycled, `Repl::updates_since` raises WalGap and the seat exits to
# re-seed — then full-syncs, tails, falls behind, and hits the same wall. Round
# two churned a pair into a mutual --replica-of cycle with no master for nine
# hours. The tell was in the numbers and free to read:
#
#     sequence 90776415 is no longer in the WAL (latest is 90776416)
#
# Off by ONE. Not a slow replica — retention that knew nothing about replicas.
#
# ADR-0022 answered it with backpressure: the master tracks its slowest LIVE
# replica and sheds writes once it has run `--wal-headroom-seq` sequences
# ahead, so the replica meets `-THROTTLED` instead of a deleted segment. All of
# that shipped. NONE of it was gated, which is the whole reason round two
# happened silently after round one was "fixed" by adding detection.
#
# WHAT IS ASSERTED, and each is a control, not an observation:
#   1. The gate SHIPS ON. A protection defaulting to 0 is one nobody has.
#   2. NEGATIVE control: at the shipped threshold, ordinary traffic sheds
#      NOTHING. A gate that fires on normal load is worse than none.
#   3. POSITIVE control: tightened, the same traffic IS refused, the refusal is
#      the HEADROOM one by name — not the lag or deadline gate, which shed the
#      same command with a different cause — and `writes_shed_headroom` moves.
#   4. RECOVERABLE: restoring the threshold restores writes. Backpressure that
#      cannot be released is an outage with a nicer message.
#   5. NO WALGAP. The replica must never hit the fatal path while backpressure
#      is doing its job. This is the property the other four exist to protect.
#
# THE THRESHOLD IS RAMPED DOWN, not fixed, for the reason BUG-0030 records:
# a fixed control is a guess about the machine. Headroom is `latest_seq -
# min_acked_live`, so how far the master runs ahead of a live replica during a
# burst depends on the box. Tighten until the control actually arms, and say
# which rung armed it.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-walhr 6490 6491
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-walhr; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; sleep 0.3
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }
fleet_warm ./target/release/flint-server

info() { valkey-cli -p "$1" FLINTINFO | tr '\r' '\n' | grep "^$2:" | cut -d: -f2; }

echo "== master + replica"
$B --port 6490 --engine rocks --data-dir "$D/m" 2>"$D/m.log" &
fleet_wait_ping 6490
$B --port 6491 --engine rocks --data-dir "$D/r" --replica-of 127.0.0.1:6490 2>"$D/r.log" &
fleet_wait_ping 6491

# The headroom gate is defined over the slowest LIVE replica and returns false
# with none — deliberately, since a widowed master is the widowed-grace gate's
# problem and two gates firing on one cause report the wrong one. So a run that
# never gets a live replica cannot test this at all, and must say so rather
# than pass quietly with the gate structurally unable to fire.
for _ in $(seq 1 60); do
  [ "$(info 6490 live_replicas)" = "1" ] && break; sleep 0.25
done
[ "$(info 6490 live_replicas)" = "1" ] \
  || { echo "FAIL: no live replica after 15s — the headroom gate cannot fire without one,"; \
       echo "      so this drill would be structurally incapable of testing anything."; exit 1; }
echo "  live_replicas=1, headroom=$(info 6490 wal_headroom_seq) min_acked=$(info 6490 wal_min_acked_seq)"

echo "== 1. the gate ships on"
DEFAULT=$(valkey-cli -p 6490 FLINTCONFIG | tr '\r' '\n' | grep '^wal-headroom-seq:' | cut -d: -f2)
echo "  wal-headroom-seq: $DEFAULT"
[ "${DEFAULT:-0}" -gt 0 ] \
  || { echo "FAIL: wal-headroom-seq is 0 — ADR-0022's backpressure ships DISABLED,"; \
       echo "      which is BUG-0012 with the fix present and switched off."; exit 1; }

burst() { # $1=port $2=count $3=tag -> "ok=N throttled=N other=N"
  local ok=0 thr=0 oth=0 r
  for i in $(seq 1 "$2"); do
    r=$(valkey-cli -p "$1" SET "$3:$i" "v$i" 2>&1)
    case "$r" in
      OK) ok=$((ok+1)) ;;
      *"too far behind the retained WAL"*) thr=$((thr+1)) ;;
      *) oth=$((oth+1)) ;;
    esac
  done
  echo "ok=$ok throttled=$thr other=$oth"
}

echo "== 2. negative control: ordinary load at the shipped threshold"
S0=$(info 6490 writes_shed_headroom)
OUT=$(burst 6490 400 neg)
S1=$(info 6490 writes_shed_headroom)
echo "  $OUT  writes_shed_headroom ${S0}->${S1}"
case "$OUT" in *"throttled=0"*) ;; *) echo "FAIL: the headroom gate shed ordinary traffic at the ${DEFAULT}-sequence default"; exit 1 ;; esac
[ "$S0" = "$S1" ] || { echo "FAIL: server counted a headroom shed under ordinary load"; exit 1; }

echo "== 3. positive control: tighten until it arms"
ARMED=0; RUNG=""; POUT=""
for T in 1000 100 10 2 1; do
  valkey-cli -p 6490 FLINTCONFIG wal-headroom-seq "$T" >/dev/null
  NOW=$(valkey-cli -p 6490 FLINTCONFIG | tr '\r' '\n' | grep '^wal-headroom-seq:' | cut -d: -f2)
  [ "$NOW" = "$T" ] || { echo "FAIL: FLINTCONFIG did not move wal-headroom-seq (asked $T, got '$NOW')"; exit 1; }
  P0=$(info 6490 writes_shed_headroom)
  POUT=$(burst 6490 300 "arm$T")
  P1=$(info 6490 writes_shed_headroom)
  echo "  wal-headroom-seq=$T -> $POUT  shed ${P0}->${P1}"
  case "$POUT" in *"throttled=0"*) ;; *) ARMED=1; RUNG=$T; break ;; esac
done
[ "$ARMED" = "1" ] \
  || { echo "FAIL: headroom backpressure never armed, down to 1 sequence. Either the gate"; \
       echo "      is not wired to the write path, or this replica never lagged at all —"; \
       echo "      headroom was $(info 6490 wal_headroom_seq) at the end. Both are findings; neither is a pass."; exit 1; }
echo "  armed at wal-headroom-seq=$RUNG"
[ "$(info 6490 writes_shed_headroom)" -gt 0 ] || { echo "FAIL: refusals seen but writes_shed_headroom did not move"; exit 1; }

echo "== 4. recoverable: restore the threshold and writes resume"
valkey-cli -p 6490 FLINTCONFIG wal-headroom-seq "$DEFAULT" >/dev/null
for _ in $(seq 1 40); do
  [ "$(valkey-cli -p 6490 SET recover:probe v 2>&1)" = "OK" ] && break; sleep 0.25
done
[ "$(valkey-cli -p 6490 SET recover:probe2 v 2>&1)" = "OK" ] \
  || { echo "FAIL: writes still refused after restoring wal-headroom-seq=$DEFAULT"; exit 1; }
echo "  writes accepted again at the shipped threshold"

echo "== 5. the replica never hit the fatal WAL gap"
if grep -q 'WALGAP' "$D/r.log" 2>/dev/null; then
  echo "FAIL: replica hit WALGAP while backpressure was supposed to prevent it:"
  grep 'WALGAP' "$D/r.log" | head -3 | sed 's/^/  | /'
  exit 1
fi
echo "  no WALGAP in the replica log"

echo "== 6. the threshold's CONVERSION FACTOR is observed, not assumed"
# The threshold is a byte budget expressed in SEQUENCES, and RocksDB numbers
# sequences per KEY -- so one sequence is one write and the conversion is the
# size of a record. It shipped as a fixed 16 KiB that had never been measured:
# right at 16 KiB values, and wrong by the ratio everywhere else, which made
# the gate fire ~16x early at the ~1 KiB its own call site tells operators to
# expect.
#
# `headroom_shed_seq_for` is unit-tested to convert correctly. What only a
# running node can show is that the OBSERVATION is real, so this asserts
# `wal_bytes_per_seq` tracks the record size actually being written.
#
# A fixed constant passes nothing here: it would report the same number for
# both sizes, which is exactly what the comparison rejects.
sizes_ok=1
val_small=$(head -c 64 /dev/zero | tr '\0' 'a')
val_large=$(head -c 8192 /dev/zero | tr '\0' 'b')
for i in $(seq 1 200); do valkey-cli -p 6490 SET "bps:small:$i" "$val_small" >/dev/null; done
OBS_SMALL=$(info 6490 wal_bytes_per_seq)
for i in $(seq 1 200); do valkey-cli -p 6490 SET "bps:large:$i" "$val_large" >/dev/null; done
OBS_LARGE=$(info 6490 wal_bytes_per_seq)
echo "  64 B records -> wal_bytes_per_seq=$OBS_SMALL | 8 KiB records -> $OBS_LARGE"

[ "${OBS_SMALL:-0}" -gt 0 ] || {
  echo "FAIL: wal_bytes_per_seq is 0 after 200 writes. Nothing is observing the"
  echo "      record size, so the threshold is still the old assumption under a"
  echo "      new name."
  exit 1; }
# 4x is a wide margin on a 128x change in record size: this is asserting that
# the number MOVES with the workload, not that it equals any particular value.
awk -v a="$OBS_SMALL" -v b="$OBS_LARGE" 'BEGIN { exit !(b > a * 4) }' || {
  echo "FAIL: 64 B and 8 KiB records both report ~the same bytes/sequence"
  echo "      ($OBS_SMALL vs $OBS_LARGE). The conversion factor is not tracking"
  echo "      what is being written, which is the defect this replaced: a fixed"
  echo "      number is right at one value size and wrong at every other."
  exit 1; }
echo "  the conversion factor tracks the workload"

echo "PASS: WAL headroom — ships on, quiet at the default, arms at $RUNG, recovers, no WAL gap, and its bytes/sequence is observed ($OBS_SMALL -> $OBS_LARGE) rather than assumed"
