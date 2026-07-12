#!/usr/bin/env bash
# Chaos gate: random master/replica kills under load, ledger-verified.
# Requires a release build with --features rocks. Kills stray servers first.
set -euo pipefail
pkill -9 -f flint-server 2>/dev/null || true
sleep 0.5
cargo build --release -q -p flint-server --features rocks
cargo build --release -q -p flint-chaos
SEEDS="${SEEDS:-7 19 42}"
ITERS="${ITERS:-12}"
for seed in $SEEDS; do
  echo "== chaos seed=$seed iters=$ITERS"
  ./target/release/flint-chaos --iterations "$ITERS" --keys 300 --seed "$seed"
  pkill -9 -f flint-server 2>/dev/null || true
  sleep 0.3
done
echo "== chain traversal (200k elements) under failover"
./target/release/chain --elements "${ELEMENTS:-200000}" --kills 12 --seed 13
pkill -9 -f flint-server 2>/dev/null || true
echo "ALL SEEDS PASSED"
