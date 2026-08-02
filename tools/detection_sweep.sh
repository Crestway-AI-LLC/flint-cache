#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# What does tightening failure DETECTION actually buy, and what does it cost?
#
# Client-visible stall through the proxy edge measured p50 646ms / worst 911ms
# at the shipped controller defaults. That number is dominated by detection:
# poll-ms x confirm is 450ms of it, roughly 70%. Everything a promotion-notice
# push could remove (#91 — the proxy's reactive rediscovery plus its 50-100ms
# retry sleep) lives in the remaining ~196ms. So the cheap lever is the two
# numbers below, and this measures them before anyone writes code.
#
# TWO HALVES, because only measuring the first is how you ship a regression:
#
#   SENSITIVITY — how fast does a REAL master death become writable again?
#     Lower poll-ms x confirm should shrink the stall roughly linearly.
#
#   SPECIFICITY — how often does the controller promote when NOTHING died?
#     `confirm` exists to ride out a transient miss: one dropped probe, a GC
#     pause, a scheduler hiccup. Cutting it trades that tolerance away. A
#     spurious promotion is not a slow failover, it is an UNNECESSARY one —
#     it demotes a healthy master, forces a replacement full sync, and burns
#     an epoch. Faster detection that invents failovers is worse than slow
#     detection that does not.
#
# The specificity half runs a fleet under continuous write load with NO kills
# at all and asserts the fleet journal records ZERO promotions. That is the
# only way the cost side of this trade shows up as a number.
#
# Usage: tools/detection_sweep.sh [soak-seconds]   (default 45)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"

SOAK=${1:-45}
OUT=/tmp/flint-detection-sweep
rm -rf "$OUT"; mkdir -p "$OUT"

# poll-ms:confirm. 200:3 FIRST because that is the actual shipped default —
# flint-controller's compiled arg_or("--poll-ms", 200)/confirm 3, what
# self-hosting.md documents, and what the multi-host runner renders.
#
# The first version of this sweep called 150:3 "the shipped default" and
# never measured 200:3 at all. 150:3 is only what attached_chaos_drill.sh
# happens to configure. Every improvement was therefore quoted against a
# baseline 150ms of detection FASTER than anything a customer runs, which
# understated the win — the same shape of error as the RTO number in #119,
# where a label claimed one thing and the measurement was of another.
SETTINGS="${FLINT_SWEEP_SETTINGS:-200:3 150:3 100:3 100:2}"

echo "== detection sweep: sensitivity (real kills) + specificity (no kills)"
echo "   soak ${SOAK}s per setting; SHIPPED DEFAULT is 200:3 (first row)"
echo

printf '%-12s %-11s %-26s %s\n' "poll:confirm" "detect(ms)" "stall p50/worst (kills)" "spurious promotions"
printf '%-12s %-11s %-26s %s\n' "------------" "----------" "--------------------------" "-------------------"

for s in $SETTINGS; do
  POLL=${s%%:*}; CONF=${s##*:}
  DETECT=$((POLL * CONF))

  # --- SENSITIVITY -------------------------------------------------------
  # Reuse the attached drill: a real fleet, kills through the operator path,
  # workload through the proxy edge. Its own oracle still asserts zero acked
  # loss, so a setting that is fast AND wrong fails here rather than scoring.
  FLINT_POLL_MS=$POLL FLINT_CONFIRM=$CONF \
    bash tools/attached_chaos_drill.sh > "$OUT/kills-$POLL-$CONF.log" 2>&1
  RC=$?
  # Read the ORACLE, not the drill's exit code. The drill also asserts its own
  # coverage ("a kill landed on each pair"), which is randomized and can trip
  # without anything being wrong with the setting under test — 75:2 did
  # exactly that on the first sweep, and reporting it as "DRILL FAILED" made a
  # perfectly good configuration look broken. Correctness lives in the PASS
  # line; that is what qualifies a measurement.
  STALL=$(grep -oE 'p50 [0-9]+ms, worst [0-9]+ms' "$OUT/kills-$POLL-$CONF.log" | head -1)
  if ! grep -q "PASS:" "$OUT/kills-$POLL-$CONF.log"; then
    STALL="ORACLE FAILED"
  elif [ -z "$STALL" ]; then
    STALL="(no stall line)"
  elif [ $RC -ne 0 ]; then
    STALL="$STALL [coverage flake]"
  fi

  # --- SPECIFICITY -------------------------------------------------------
  # Same fleet shape, no kills, continuous writes. Any PromoteIssued in the
  # journal is the controller inventing a failover.
  SPUR=$(FLINT_POLL_MS=$POLL FLINT_CONFIRM=$CONF SOAK=$SOAK \
         bash tools/lib/detection_soak.sh "$OUT/soak-$POLL-$CONF" 2>&1 | tail -1)

  printf '%-12s %-11s %-26s %s\n' "$s" "$DETECT" "$STALL" "$SPUR"
done

echo
echo 'Read the last column first. A setting is only better if it is faster'
echo 'AND still reports 0 spurious promotions - confirm is the tolerance'
echo 'for a transient miss, and it is what gets traded away.'
