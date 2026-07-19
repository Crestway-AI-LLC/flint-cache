#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Standard-tool benchmark harness: memtier_benchmark (the industry-standard
# cache load generator) against a running Flint endpoint. Publishes-ready
# output: a markdown table of the three canonical scenarios.
#
# Usage: memtier_bench.sh <host> <port> [--tls-ca <ca.crt>] [--auth <token>]
#
# Scenarios (memtier 2.x):
#   A  hot GET        100% GET over a small keyspace (page-cache resident)
#   B  mixed 1:10     SET:GET=1:10, gaussian keys over the loaded range
#   C  pipelined GET  scenario A with --pipeline=16 (throughput shape)
# Each runs 30s after a data load. Values 1024B RANDOM (incompressible —
# constant values compress ~15:1 in the engine and fake residency).
#
# HONESTY RULES for publishing results from this script:
#   - name the exact instance type, dataset size vs RAM, and whether the
#     dataset exceeds RAM (the row that matters for Flint);
#   - never publish numbers from a laptop or shared box;
#   - publish the competitor row from the SAME box, same script, same day.
set -euo pipefail
HOST="${1:?host}"; PORT="${2:?port}"; shift 2
EXTRA=()
while [ $# -gt 0 ]; do case "$1" in
  --tls-ca) EXTRA+=(--tls --cacert "$2"); shift 2;;
  --auth)   EXTRA+=(-a "$2"); shift 2;;
  *) echo "unknown arg $1"; exit 2;;
esac; done
M(){ memtier_benchmark -s "$HOST" -p "$PORT" ${EXTRA[@]+"${EXTRA[@]}"} --hide-histogram "$@"; }

KEYS="${MEMTIER_KEYS:-1000000}"
echo "== load: $KEYS keys x 1KB random values"
M -n allkeys --key-maximum="$KEYS" --key-pattern=P:P --ratio=1:0 -d 1024 --random-data -c 8 -t 4 >/dev/null 2>&1

run(){ # <label> <extra memtier args...>
  local label="$1"; shift
  local out
  out=$(M --test-time=30 -d 1024 --random-data --key-maximum="$KEYS" -c 8 -t 4 "$@" 2>/dev/null | tail -20)
  local line
  line=$(echo "$out" | awk '/^Totals/ {printf "%.0f | %.3f | %.3f | %.3f | %.3f", $2, $5, $6, $7, $8}')
  echo "| $label | $line |"
  echo "$out" > "/tmp/memtier-$label.out"
}
echo
echo "| scenario | ops/s | avg ms | p50 ms | p99 ms | p99.9 ms |"
echo "|---|---|---|---|---|---|"
run "A-hot-get"      --ratio=0:1 --key-pattern=G:G --key-stddev=$((KEYS/100))
run "B-mixed-1-10"   --ratio=1:10 --key-pattern=G:G
run "C-get-pipe16"   --ratio=0:1 --key-pattern=G:G --pipeline=16
echo
echo "raw outputs: /tmp/memtier-*.out (Totals line: ops/s, avg, p50, p99, p99.9 latency ms)"
