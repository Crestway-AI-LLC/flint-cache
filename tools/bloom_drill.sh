#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Bloom filter drill (ADR-0016 Verification 2): a Bloom filter may say yes
# wrongly, and may NEVER say no wrongly. The unit tests prove that in
# memory across a scale-out; this proves it across the three things a unit
# test cannot reach — a warm restart, a full sync to a fresh replica, and a
# slot migration.
#
# Slot migration is the reason this drill exists. A filter's blocks are
# subkey rows under a versioned prefix, moved by the same generic rekey the
# collections use. That it works has been REASONED about, not run. If the
# blocks and the metadata row ever part company — moved separately, or the
# version rewritten on one and not the other — the filter does not error.
# It answers NO for items it holds, which is the one answer it is not
# allowed to give, and nothing else in the system would notice.
#
# The filter is deliberately built past its reserved capacity so it carries
# a CHAIN. Items added before a scale-out live in older links, so any
# operation that drops or mis-rekeys a link shows up here and would not in
# a single-link filter.
#
# Requires: a release build with --features rocks, valkey-cli on PATH.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-bloom- 6402 6403 6404
fleet_guard
fleet_kill server; sleep 0.4

B=./target/release/flint-server
MPORT=6402; RPORT=6403; DPORT=6404
MDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-bloom-m.XXXXXX)
RDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-bloom-r.XXXXXX)
DDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-bloom-d.XXXXXX)

# Hash tag pins the filter to one known slot, which phase 3 needs.
TAG="{bloomer}"
KEY="$TAG:filter"
N=5000              # items added
CAPACITY=500        # reserved for far fewer, so the chain grows
ERROR=0.01
PROBES=2000         # never-added items sampled for the vacuity control

cleanup() {
  pkill -9 -f "flint-server --port 640" 2>/dev/null
  rm -rf "$MDIR" "$RDIR" "$DDIR"
}
trap cleanup EXIT

# How many of items [lo,hi) the filter at $1 reports present. Batched
# through BF.MEXISTS rather than one BF.EXISTS per item: 5000 round trips
# is slow enough that someone would shrink N to make the drill quick, and
# a drill made quick by weakening its sample is the failure this whole
# suite exists to avoid.
present_count() {
  local port="$1" key="$2" lo="$3" hi="$4" prefix="$5" i j end args n=0
  for (( i=lo; i<hi; i+=500 )); do
    end=$(( i + 500 )); [ "$end" -gt "$hi" ] && end="$hi"
    args=""
    for (( j=i; j<end; j++ )); do args="$args ${prefix}:$j"; done
    # shellcheck disable=SC2086
    n=$(( n + $(valkey-cli -p "$port" BF.MEXISTS "$key" $args | tr -d '\r' | grep -c '^1$') ))
  done
  printf '%s' "$n"
}

# Every added item present, AND the filter not merely saying yes to
# everything. The second half is not decoration: a filter with every bit
# set has no false negatives either, and would pass the first check
# perfectly while being completely broken. Whatever the operation under
# test did, "it still answers yes to things it never stored" is the
# failure this catches.
assert_filter_intact() {
  local port="$1" label="$2" hits misses fpr
  hits=$(present_count "$port" "$KEY" 0 "$N" item)
  if [ "$hits" != "$N" ]; then
    echo "FAIL [$label]: $hits/$N added items still present — $(( N - hits )) FALSE NEGATIVES"
    exit 1
  fi
  misses=$(present_count "$port" "$KEY" 0 "$PROBES" absent)
  fpr=$(( misses * 100 / PROBES ))
  if [ "$misses" -gt $(( PROBES / 10 )) ]; then
    echo "FAIL [$label]: ${misses}/${PROBES} never-added items read present (${fpr}%)."
    echo "      The no-false-negative check above is vacuous when the filter"
    echo "      answers yes to everything. Expected well under 10% at ${ERROR}."
    exit 1
  fi
  echo "  [$label] $hits/$N present, ${misses}/${PROBES} false positives (${fpr}%)"
}

add_items() {
  local port="$1" lo="$2" hi="$3"
  awk -v k="$KEY" -v lo="$lo" -v hi="$hi" 'BEGIN{
    for (i = lo; i < hi; i++) {
      it = sprintf("item:%d", i)
      printf "*3\r\n$6\r\nBF.ADD\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, length(it), it
    }
  }' | valkey-cli -p "$port" --pipe | tail -1
}

echo "== bloom drill: $N items into a filter reserved for $CAPACITY at ${ERROR}"

$B --port $MPORT --engine rocks --data-dir "$MDIR" 2>/dev/null &
fleet_wait_listen $MPORT
fleet_wait_ping $MPORT

cli_ok valkey-cli -p $MPORT BF.RESERVE "$KEY" "$ERROR" "$CAPACITY"
add_items $MPORT 0 $N

LINKS=$(valkey-cli -p $MPORT BF.INFO "$KEY" FILTERS | tr -d '\r')
[ "${LINKS:-1}" -gt 1 ] || {
  echo "FAIL: filter has $LINKS link(s) — the chain never grew, so every"
  echo "      assertion below about older links proves nothing. Raise N or"
  echo "      lower CAPACITY."
  exit 1
}
CARD=$(valkey-cli -p $MPORT BF.CARD "$KEY" | tr -d '\r')
SIZE=$(valkey-cli -p $MPORT BF.INFO "$KEY" SIZE | tr -d '\r')
echo "== filter built: $LINKS links, card $CARD, $SIZE bytes materialised"
# BF.CARD counts items the filter ACCEPTED, so insert-time false positives
# are never counted and the card reads at or below N. Only an OVER-count
# would mean something is wrong.
[ "$CARD" -le "$N" ] || { echo "FAIL: card $CARD exceeds the $N items added"; exit 1; }
assert_filter_intact $MPORT "built"

echo "== phase 1: kill -9 and warm restart"
fleet_signal_port $MPORT 9
sleep 0.3
$B --port $MPORT --engine rocks --data-dir "$MDIR" 2>/dev/null &
fleet_wait_listen $MPORT
fleet_wait_ping $MPORT
[ "$(valkey-cli -p $MPORT BF.INFO "$KEY" FILTERS | tr -d '\r')" = "$LINKS" ] \
  || { echo "FAIL: link count changed across the restart"; exit 1; }
assert_filter_intact $MPORT "warm restart"

echo "== phase 2: full sync to a FRESH replica (empty data dir)"
$B --port $RPORT --engine rocks --data-dir "$RDIR" --replica-of "127.0.0.1:$MPORT" 2>/dev/null &
fleet_wait_listen $RPORT
fleet_wait_ping $RPORT
for _ in $(seq 1 200); do
  [ "$(valkey-cli -p $RPORT BF.CARD "$KEY" | tr -d '\r')" = "$CARD" ] && break
  sleep 0.1
done
RCARD=$(valkey-cli -p $RPORT BF.CARD "$KEY" | tr -d '\r')
[ "$RCARD" = "$CARD" ] || { echo "FAIL: replica card $RCARD != master $CARD after sync"; exit 1; }
assert_filter_intact $RPORT "replica after full sync"

echo "== phase 3: slot migration to a third node"
SLOT=$(python3 - <<'PY'
def crc16(d):
    poly = 0x1021; crc = 0
    for b in d:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ poly) & 0xffff if crc & 0x8000 else (crc << 1) & 0xffff
    return crc
print(crc16(b"bloomer") % 16384)
PY
)
$B --port $DPORT --engine rocks --data-dir "$DDIR" 2>/dev/null &
fleet_wait_listen $DPORT
fleet_wait_ping $DPORT
echo "  filter lives in slot $SLOT; pulling it to :$DPORT"
RES=$(valkey-cli -p $DPORT FLINTMIGRATEIN "127.0.0.1:$MPORT" "$SLOT" 2>&1 | tr -d '\r')
echo "  result: $RES"
echo "$RES" | grep -q "MIGRATEIN-OK" || { echo "FAIL: migration did not complete: $RES"; exit 1; }

# The destination must hold the filter as a FILTER — metadata row and every
# block row, under a consistent version. A migration that shipped the
# metadata and dropped the blocks would leave a key that still answers
# BF.INFO and has forgotten its contents.
DTYPE=$(valkey-cli -p $DPORT TYPE "$KEY" | tr -d '\r')
[ "$DTYPE" = "bloom" ] || { echo "FAIL: destination TYPE is '$DTYPE', not bloom"; exit 1; }
DLINKS=$(valkey-cli -p $DPORT BF.INFO "$KEY" FILTERS | tr -d '\r')
DCARD=$(valkey-cli -p $DPORT BF.CARD "$KEY" | tr -d '\r')
DSIZE=$(valkey-cli -p $DPORT BF.INFO "$KEY" SIZE | tr -d '\r')
[ "$DLINKS" = "$LINKS" ] || { echo "FAIL: destination has $DLINKS links, source had $LINKS"; exit 1; }
[ "$DCARD" = "$CARD" ] || { echo "FAIL: destination card $DCARD != source $CARD"; exit 1; }
[ "$DSIZE" = "$SIZE" ] || { echo "FAIL: destination holds $DSIZE bytes, source had $SIZE"; exit 1; }
assert_filter_intact $DPORT "destination after slot migration"

echo "== phase 4: the filter still WORKS after the move, not just reads"
valkey-cli -p $DPORT BF.ADD "$KEY" "post-move" >/dev/null
[ "$(valkey-cli -p $DPORT BF.EXISTS "$KEY" post-move | tr -d '\r')" = "1" ] \
  || { echo "FAIL: an item added after the migration does not read back"; exit 1; }
DCARD2=$(valkey-cli -p $DPORT BF.CARD "$KEY" | tr -d '\r')
[ "$DCARD2" = "$(( DCARD + 1 ))" ] \
  || { echo "FAIL: card $DCARD2 did not advance by one from $DCARD"; exit 1; }
assert_filter_intact $DPORT "destination after a further add"

echo "PASS: no false negatives across warm restart, full sync, and slot migration"
echo "      ($N items, $LINKS chain links, filter sampled ${PROBES}x for vacuity at each step)"
