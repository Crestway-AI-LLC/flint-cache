#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Proxy-in-the-loop chaos gate: the SAME ledger oracle as chaos_drill, but the
# whole workload flows through the proxy (client->proxy->node) while a real
# controller drives failover. Proves no acked write is lost or corrupted across
# the FULL path under repeated controller-promoted, proxy-chased failovers —
# the production path the direct-to-node chaos test deliberately bypasses.
#
# The direct-to-node chaos_drill.sh stays the storage-engine regression gate;
# this is the routing-plane end-to-end gate. Both share one oracle.
set -euo pipefail
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-chaos-
fleet_guard
fleet_kill server
fleet_kill proxy
fleet_kill controller
sleep 0.5
cargo build --release -q -p flint-server --features rocks
cargo build --release -q -p flint-proxy -p flint-controller -p flint-chaos
SEEDS="${SEEDS:-7 19 42}"
ITERS="${ITERS:-12}"
for seed in $SEEDS; do
  echo "== proxy-chaos seed=$seed iters=$ITERS"
  ./target/release/proxy_chaos --iterations "$ITERS" --keys 300 --seed "$seed"
  fleet_kill server
  fleet_kill proxy
  fleet_kill controller
  sleep 0.3
done
echo "ALL SEEDS PASSED"
