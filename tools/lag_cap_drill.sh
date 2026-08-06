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
# Loopback replication acks in ~0.2ms and cross-host was not much slower, so a
# 1000ms cap is simply unreachable under any load the harness generates.
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
fleet_init /tmp/flint-lagcap 6306 6307 6471 6472 6473
fleet_guard
fleet_kill server; fleet_kill controller
sleep 0.3
cleanup() { fleet_kill server; fleet_kill controller; rm -f /tmp/flint-lagcap.log; }
trap cleanup EXIT

cargo build --release -q -p flint-chaos -p flint-server -p flint-controller \
  --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

# 5ms hard / 4ms soft: low enough that an ordinary write burst outruns
# replication, high enough that the pair still makes progress.
echo "== chaos with the lag cap at 5ms, so the shed path is reachable"
OUT=$(./target/release/flint-chaos --iterations 4 --keys 150 --mode master \
  --driver controller --lag-hard-ms 5 --rpo-margin-ms 50 2>&1)
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
