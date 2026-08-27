#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Hot-key write path under controller-driven failover: a writer hammers a
# handful of keys THROUGH each master kill (it never pauses for the kill),
# so every failover's loss window lands on the keys being written that
# instant. Ledger-verified (no corruption / cross-key / time-travel /
# phantom; regressions accounted), and the client-observed write blackout
# of every failover is measured against the published 10 s RTO — the drill
# FAILS if any failover exceeds it.
set -euo pipefail
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-hotkeychaos 6354 6355 6356 6357 6358 6359 6360 6361
fleet_guard
fleet_kill controller
fleet_kill server
fleet_kill proxy
sleep 0.5
cargo build --release -q -p flint-server --features rocks
cargo build --release -q -p flint-chaos
KILLS="${KILLS:-6}"
KEYS="${KEYS:-8}"
WRITERS="${WRITERS:-4}"

echo "== phase 1: inline (sync) write path"
./target/release/hotkey --port-base 6354 --kills "$KILLS" --keys "$KEYS" --writers "$WRITERS"
fleet_kill controller
fleet_kill server
fleet_kill proxy
sleep 0.5

echo "== phase 2: ASYNC WRITE QUEUE (ADR-0005 D4) — the hot-key mitigation"
# The open-mode proxy pins namespace "0"; opting it in routes every hot-key
# write through the queue (group-committed batches, ack after apply), on
# every node the harness spawns — including promote-replace respawns.
FLINT_CHAOS_ASYNC_WRITES=0 \
  ./target/release/hotkey --port-base 6354 --kills "$KILLS" --keys "$KEYS" --writers "$WRITERS"
fleet_kill controller
fleet_kill server
fleet_kill proxy
echo "HOTKEY CHAOS PASSED (sync + async-queue)"
