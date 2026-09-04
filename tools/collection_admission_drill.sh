#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0060: prove the collection-read admission bound actually refuses.
#
# The unit tests use a seam (a fixed available-memory figure) so the
# arithmetic can be proven on a host with no /proc/meminfo. That leaves the
# WIRING untested: the flag, the guard's placement around dispatch AND
# encoding, and the FLINTINFO fields. This drill exercises the whole path
# through a real server and a real client.
#
# THE CONTROL MATTERS MORE THAN THE ASSERTION. The same read is issued twice:
# once with admission OFF, where it must SUCCEED, and once with a budget it
# cannot fit, where it must be REFUSED. Without the first, a refusal proves
# only that something went wrong -- a broken build refuses too.
#
# On a host whose memory cannot be read (macOS has no /proc/meminfo) admission
# admits everything by design, so this drill CANNOT test it. It then prints a
# `SKIP:` line and exits 0 -- the repo's convention -- which FLINT_GATE_STRICT=1
# promotes to a FAIL. The gate sets STRICT and runs on Linux, so it must never
# reach that line; a developer's Mac gets a quiet skip instead of a false pass.
set -euo pipefail
. "$(dirname "$0")/lib/fleet.sh"

fleet_init $FLINT_DRILL_ROOT/flint-colladmit 6307
PORT=6307
fleet_guard
fleet_kill server
sleep 0.3

fail() { echo "FAIL: $*"; exit 1; }
cli() { valkey-cli -p "$PORT" "$@"; }
info_field() { cli FLINTINFO 2>/dev/null | tr -d '\r' | sed -n "s/^$1://p"; }

D="$FLINT_DRILL_ROOT/flint-colladmit-data"
LOG="$FLINT_DRILL_ROOT/flint-colladmit.log"
VSIZE=100000

cargo build --release -q -p flint-server --features rocks || fail "build"

start_node() {   # $1 = extra args (may be empty)
  rm -rf "$D"
  # shellcheck disable=SC2086
  ./target/release/flint-server --port "$PORT" --engine rocks --data-dir "$D" \
    $1 >"$LOG" 2>&1 &
  fleet_wait_listen "$PORT"
  fleet_wait_ping "$PORT"
}

# Build a hash big enough that 1% of this node's available memory cannot hold
# its estimated peak. Sized from the node's OWN reading rather than a guess:
# a hardcoded size passes trivially on a small box and silently fails to
# provoke anything on a large one.
build_hash() {   # $1 = fields
  local n="$1" i args
  args=""
  for ((i = 0; i < n; i++)); do
    args="$args f$(printf '%05d' "$i") $(head -c "$VSIZE" /dev/zero | tr '\0' 'x')"
    if (( (i + 1) % 10 == 0 )); then
      # shellcheck disable=SC2086
      cli HSET h $args >/dev/null
      args=""
    fi
  done
  [ -n "$args" ] && cli HSET h $args >/dev/null
  return 0
}

echo "== control: admission OFF, the read must succeed"
start_node ""
PCT_OFF=$(info_field collection_read_budget_pct)
[ "$PCT_OFF" = "0" ] || fail "expected admission off by default, got pct=$PCT_OFF"

MEM_SRC=$(info_field mem_src)
AVAIL=$(info_field mem_avail_bytes)
if [ "$MEM_SRC" != "proc" ]; then
  fleet_kill server
  echo "SKIP: node memory reads as '$MEM_SRC', so admission admits everything by"
  echo "  design and this drill cannot exercise a refusal -- the case on macOS."
  echo "  Exits 0 as a SKIP rather than a pass, and FLINT_GATE_STRICT=1 (which the"
  echo "  gate box always sets) turns that into a FAIL. So a developer Mac is quiet"
  echo "  and the gate, which runs on Linux and must never reach this line, is not."
  exit 0
fi

# budget = AVAIL x 1%; a read is estimated at 3.5x its bytes. Pick bytes 20%
# past what that budget can hold, then convert to whole fields.
NEED=$(( AVAIL / 100 * 12 / 10 * 10 / 35 ))
FIELDS=$(( NEED / VSIZE + 1 ))
BYTES=$(( FIELDS * (VSIZE + 6) ))
if [ "$BYTES" -gt $((400 * 1024 * 1024)) ]; then
  fleet_kill server
  fail "this node has ${AVAIL}B available, so provoking a refusal at 1% needs a
  ${BYTES}B collection -- past what this drill will build. Raise PORT-side
  memory pressure or run on a smaller box; do NOT lower the assertion."
fi
echo "   avail=${AVAIL}B -> building a ${BYTES}B hash ($FIELDS x ${VSIZE}B)"
build_hash "$FIELDS"

OUT=$(cli HGETALL h | head -c 200 || true)
case "$OUT" in
  *THROTTLED*) fail "admission is OFF but the read was refused: $OUT" ;;
  "") fail "the control read returned nothing -- it must SUCCEED with admission off" ;;
esac
echo "   control read returned $(cli HLEN h) fields, as it must"
fleet_kill server
sleep 0.3

echo "== armed: 1% budget, the same read must be refused"
start_node "--collection-read-budget-pct 1"
PCT_ON=$(info_field collection_read_budget_pct)
[ "$PCT_ON" = "1" ] || fail "flag not applied: collection_read_budget_pct=$PCT_ON"
build_hash "$FIELDS"

OUT=$(cli HGETALL h 2>&1 | head -c 400 || true)
case "$OUT" in
  *THROTTLED*) : ;;
  *) fail "expected a THROTTLED refusal at a 1% budget, got: ${OUT:0:200}" ;;
esac
# The refusal must name its terms, not just its verdict.
for term in "collection read needs" "in-flight" "--collection-read-budget-pct 1" "retry with backoff"; do
  case "$OUT" in
    *"$term"*) : ;;
    *) fail "the refusal omits '$term': $OUT" ;;
  esac
done
echo "   refused, naming its terms"

REFUSED=$(info_field collection_read_refused)
[ "${REFUSED:-0}" -ge 1 ] || fail "collection_read_refused=$REFUSED after a refusal"
INFLIGHT=$(info_field collection_read_in_flight_bytes)
[ "${INFLIGHT:-x}" = "0" ] || fail "a refused read reserved ${INFLIGHT}B -- it must reserve nothing"
UNMEASURED=$(info_field collection_read_unmeasured)
[ "${UNMEASURED:-0}" = "0" ] || fail "reads were admitted with no budget checked: $UNMEASURED"
echo "   FLINTINFO: refused=$REFUSED in_flight=$INFLIGHT unmeasured=$UNMEASURED"

# A small read must still pass while the bound is armed -- otherwise the
# refusal above proves the feature is broken, not that it is working.
cli HSET small f v >/dev/null
SMALL=$(cli HGETALL small 2>&1 | head -c 100 || true)
case "$SMALL" in
  *THROTTLED*) fail "a 1-field hash was refused: the bound is refusing everything" ;;
esac
echo "   a small read still passes, so the bound discriminates by size"

fleet_kill server
rm -rf "$D"
echo "PASSED"
