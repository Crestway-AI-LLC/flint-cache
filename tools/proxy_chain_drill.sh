#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Reply-correlation gate for the async proxy (ADR-0021).
#
# The proxy correlates backend replies BY POSITION — the Nth reply on a
# connection belongs to the Nth request, no request ids, the same invariant
# twemproxy and Envoy rely on. Each worker owns its backend connections and
# many client connections share them, so if that correlation is ever wrong one
# client receives another client's value.
#
# Nothing else in the suite can see that. Throughput tests do not check whose
# value came back; the ledger oracle catches a CORRUPTED value but not a
# well-formed value delivered to the wrong asker. A concurrent chain walk
# catches it on the first hop and names it.
#
# Deliberately over-subscribed: 16 workers on a box with fewer cores, so more
# independent FIFO streams interleave on one node than a tuned deployment would
# ever run. Worker count is a correctness variable here, not just a perf knob.
set -euo pipefail
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-proxychain 6460 6461 6462 6463 6464 6465 6466 6467
fleet_guard
fleet_kill controller
fleet_kill server
fleet_kill proxy
sleep 0.5

cargo build --release -q -p flint-server --features rocks
cargo build --release -q -p flint-proxy -p flint-controller -p flint-chaos

CHAINS="${CHAINS:-16}"
ELEMENTS="${ELEMENTS:-25000}"
KILLS="${KILLS:-6}"
WORKERS="${WORKERS:-16}"

echo "== concurrent chain traversal through the proxy under failover"
echo "   ${CHAINS} chains x ${ELEMENTS} elements, ${KILLS} kills, ${WORKERS} proxy workers"
./target/release/proxy_chain \
  --port-base 6460 \
  --chains "$CHAINS" \
  --elements "$ELEMENTS" \
  --kills "$KILLS" \
  --workers "$WORKERS"

# The binary asserts its own overlap: a run whose walkers finish before the
# first kill fails rather than reporting green, because it would have tested
# the chain oracle and not failover at all.
echo "PASS: proxy_chain_drill"
