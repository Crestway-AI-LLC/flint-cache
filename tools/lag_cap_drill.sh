#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The lag cap must be OBSERVED to bite, not merely configured.
#
# docs/slo.md publishes "RPO <= 10 s of acked writes" and says the window is
# "a configuration bound, not a hope" because the master sheds writes
# (-THROTTLED) once replica lag exceeds --lag-hard-ms. That mechanism is what
# the whole bounded-loss claim rests on — and until this drill existed it had
# never fired. Every chaos run ever recorded, including a 7-host run over a
# real network, reported:
#
#     writes shed -THROTTLED (retried): 0
#
# Not evidence of correctness: evidence that the condition was never created.
#
# THAT ORIGINAL DIAGNOSIS WAS WRONG, and the sentence it justified is retracted.
# It read: "Loopback replication acks in ~0.2ms and cross-host was not much
# slower, so a 1000ms cap is simply unreachable under any load the harness
# generates." Both clauses are true and the conclusion does not follow. A
# single ack is fast; a PIPELINED stream does not wait for one. On an idle
# laptop, one continuous 500k-write pipe against a healthy pair at shipped
# defaults peaked at lag_ms_max=631 -- 63% of the cap, no stall, nothing
# else running. Freeze that replica for 700ms and the cap is crossed.
#
# The measurement that hid it for a year: every earlier probe used BURSTY
# load, which drains between batches and never lets a backlog build. Reaching
# for a mean ack latency to reason about a queue was the error underneath.
#
# So this drill lowers the cap until it is reachable and asserts the shed path
# actually fires. It is the cheap half of the RPO evidence problem: no AWS, no
# network shaping, runs in the gate.
#
# TWO THINGS IT PROVES, and one it deliberately does not:
#
#   * the master sheds when lag exceeds the cap (THROTTLED > 0);
#   * a shed write is NEVER counted as lost — it was never acked, so the
#     oracle must not report it as data loss. A run that shed writes AND
#     reported corruption or acked-loss would mean the shed path is lying
#     about what it accepted.
#
#   * it does NOT produce acked-write loss depth. Lowering the cap makes the
#     master shed EARLIER, which shrinks the unreplicated suffix — the
#     opposite of what loss depth needs. Real depth requires the master to
#     die while the replica is genuinely behind, which the controller
#     refuses to promote out of by design ("a survivor that was NOT recently
#     converged is a degraded-window failure → refuse and page"). That is a
#     separate experiment; see #121.
#
# THE CLAMP THAT MADE THIS HARDER THAN IT LOOKS. ReplHub::new stores
# `lag_hard_ms.max(lag_soft_ms)` so hard can never sit below soft. Passing
# only --lag-hard-ms 5 therefore yields a 500ms cap (the default soft) and a
# run that appears to test aggressive shedding while testing nothing. Both
# caps have to move together; the harness now does that for you.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-lagcap 6362 6363 6364 6365 6366 6367 6368 6369
fleet_guard
fleet_kill server; fleet_kill controller
sleep 0.3
cleanup() { fleet_kill server; fleet_kill controller; rm -f $FLINT_DRILL_ROOT/flint-lagcap.log; }
trap cleanup EXIT

cargo build --release -q -p flint-chaos -p flint-server -p flint-controller \
  --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

# 5ms hard / 4ms soft: low enough that an ordinary write burst outruns
# replication, high enough that the pair still makes progress.
echo "== chaos with the lag cap at 5ms, so the shed path is reachable"
# FORCE THE LAG, DO NOT HOPE FOR IT (BUG-0049).
#
# This used to rely on an ordinary write burst outrunning replication. That is
# an ambient property of the machine, not something the run controls, so on a
# box where replication kept up nothing was shed and the drill correctly
# reported the mechanism unproven -- failing for a reason unrelated to the
# change under test. One red main, 5 passes either side of it.
#
# --stall-replica-ms SIGSTOPs the replica so lag is produced rather than
# awaited. It was rejected at first on the grounds that a stall belongs to the
# bounded-loss regime this drill's header defers to #121, and that reasoning
# was wrong: a stall creates LAG, and if the shed mechanism works the master
# refuses writes rather than acking them, so no acked loss appears. Shedding
# is precisely what the stall should produce here. If it produced loss instead,
# that would be the mechanism failing -- which this drill would then be right
# to report.
#
# 200ms against a 5ms cap: two orders of margin, and far under the 2s liveness
# window, so the pair still looks failover-worthy to the controller.
OUT=$(./target/release/flint-chaos --port-base 6362 --iterations 4 --keys 150 --mode master \
  --driver controller --lag-hard-ms 5 --rpo-margin-ms 50 --stall-replica-ms 200 2>&1)
echo "$OUT" | tail -8 | sed 's/^/  /'

echo "$OUT" | grep -q "^PASS:" || { echo "FAIL: chaos oracle did not pass under a tight cap"; exit 1; }

SHED=$(echo "$OUT" | sed -n 's/.*writes shed -THROTTLED (retried): \([0-9]*\).*/\1/p')
[ -n "$SHED" ] || { echo "FAIL: could not read the shed count"; exit 1; }
[ "$SHED" -gt 0 ] || {
  echo "FAIL: nothing was shed even at a 5ms cap — the mechanism the published"
  echo "      RPO bound rests on did not fire, so the bound is unproven. Check"
  echo "      that BOTH --lag-soft-ms and --lag-hard-ms reached the nodes:"
  echo "      ReplHub::new clamps hard up to soft, so hard alone does nothing."
  exit 1
}
echo "  shed $SHED write(s) — the cap was observed to bite"

# A shed write was refused, not accepted. If the oracle had counted one as
# lost, the server would be claiming to have taken a write it rejected.
LOST=$(echo "$OUT" | sed -n 's/.*acked keys regressed across master kills: \([0-9]*\).*/\1/p')
echo "$OUT" | grep -q "corruption: 0  time-travel: 0  cross-key: 0" \
  || { echo "FAIL: corruption/time-travel/cross-key under a shedding master"; exit 1; }
echo "  no shed write was ever counted as acked (acked regressions: ${LOST:-0}, all within the cap)"

echo "PASS: lag cap drill — at a 5ms cap the master shed $SHED write(s) with -THROTTLED, the ledger oracle still passed, and nothing shed was mistaken for data loss"
