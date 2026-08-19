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
#   D  SET only       100% SET — the PUT number, isolated
# Each runs 30s after a data load. Values 1024B RANDOM (incompressible —
# constant values compress ~15:1 in the engine and fake residency).
#
# HONESTY RULES for publishing results from this script:
#   - name the exact instance type, dataset size vs RAM, and whether the
#     dataset exceeds RAM (the row that matters for Flint);
#   - never publish numbers from a laptop or shared box;
#   - publish the competitor row from the SAME box, same script, same day;
#   - say where the CLIENT ran. Every Flint latency published up to
#     2026-08-16 was loopback (client and server on one box), which omits
#     the two hops a real caller pays. A loopback row and an off-box row
#     are different measurements and must never share a table without
#     saying which is which;
#   - NEVER quote an ops/s figure as capacity unless it came from a SWEEP.
#     In a closed loop `throughput == connections / latency` is an IDENTITY,
#     true at saturation and below it, so a single-concurrency run reports
#     latency in throughput units and nothing else. Every published Flint
#     number up to 2026-08-19 was taken at exactly 32 connections, which is
#     why "ElastiCache does 3x our ops/s" was never a statement about either
#     engine's ceiling. Use MEMTIER_SWEEP and quote the KNEE, with the
#     client-CPU column that says whose ceiling it is;
#   - scenario D is NOT comparable across engines without its contract.
#     Flint acks a SET fsync-bounded (WAL cadence, --wal-fsync-ms); Redis,
#     Valkey and ElastiCache ack from RAM and fsync later (or never). The
#     same column holds two different promises, so print the promise.
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
# -c is clients PER THREAD, so connections = CLIENTS x THREADS. Defaults
# reproduce the historical `-c 8 -t 4` = 32 exactly, so every run before
# 2026-08-19 remains directly comparable to a default run today.
THREADS="${MEMTIER_THREADS:-4}"
CLIENTS="${MEMTIER_CLIENTS:-8}"
# Space-separated CONNECTION counts. Empty = single run at the default, which
# is the old behaviour. Each entry is divided by THREADS to get -c.
SWEEP="${MEMTIER_SWEEP:-}"

# Client-side CPU during a run, because a throughput plateau is only a
# statement about the SERVER if the load generator still had headroom. A
# sweep without this column cannot tell "the server is saturated" from "the
# client cannot offer more", and those are the two readings that matter.
# Reports n/a rather than 0 where /proc/stat is unavailable: an unmeasured
# CPU must never render as an idle one.
_cpu_snap(){ [ -r /proc/stat ] && awk '/^cpu /{t=0; for(i=2;i<=NF;i++) t+=$i; print t, $5}' /proc/stat || echo ""; }
_cpu_busy_pct(){ # <snap_before> <snap_after>
  local a="$1" b="$2"
  [ -n "$a" ] && [ -n "$b" ] || { echo "n/a"; return; }
  awk -v a="$a" -v b="$b" 'BEGIN{
    split(a,x," "); split(b,y," ");
    dt=y[1]-x[1]; di=y[2]-x[2];
    if (dt<=0) { print "n/a"; exit }
    printf "%.0f%%", 100.0*(dt-di)/dt
  }'
}
echo "== load: $KEYS keys x 1KB random values"
M -n allkeys --key-maximum="$KEYS" --key-pattern=P:P --ratio=1:0 -d 1024 --random-data -c "$CLIENTS" -t "$THREADS" >/dev/null 2>&1

run(){ # <label> <extra memtier args...>
  local label="$1"; shift
  local conns=$(( CLIENTS * THREADS ))
  local out before after cpu
  before=$(_cpu_snap)
  out=$(M --test-time=30 -d 1024 --random-data --key-maximum="$KEYS" -c "$CLIENTS" -t "$THREADS" "$@" 2>/dev/null | tail -20)
  after=$(_cpu_snap)
  cpu=$(_cpu_busy_pct "$before" "$after")
  local line
  line=$(echo "$out" | awk '/^Totals/ {printf "%.0f | %.3f | %.3f | %.3f | %.3f", $2, $5, $6, $7, $8}')
  echo "| $label | $conns | $line | $cpu |"
  echo "$out" > "/tmp/memtier-$label-c$conns.out"
  # Machine-readable, for the knee analysis below.
  echo "$label $conns $(echo "$out" | awk '/^Totals/ {print $2}') $cpu" >> "$SWEEPDATA"
}
SWEEPDATA=$(mktemp /tmp/memtier-sweep.XXXXXX)
scenarios(){
  run "A-hot-get"      --ratio=0:1 --key-pattern=G:G --key-stddev=$((KEYS/100))
  run "B-mixed-1-10"   --ratio=1:10 --key-pattern=G:G
  run "C-get-pipe16"   --ratio=0:1 --key-pattern=G:G --pipeline=16
  # Over the SAME loaded keyspace, so these are overwrites — the steady-state
  # write a cache actually serves. Writing fresh keys instead would measure
  # the fill path (and grow the dataset mid-run, moving the read rows).
  run "D-set-only"     --ratio=1:0 --key-pattern=G:G
}

echo
echo "| scenario | conns | ops/s | avg ms | p50 ms | p99 ms | p99.9 ms | client cpu |"
echo "|---|---|---|---|---|---|---|---|"
if [ -z "$SWEEP" ]; then
  scenarios
else
  for c in $SWEEP; do
    if [ $(( c % THREADS )) -ne 0 ]; then
      echo "FAIL: sweep entry $c is not divisible by THREADS=$THREADS" >&2; exit 2
    fi
    CLIENTS=$(( c / THREADS ))
    scenarios
  done
fi
echo

if [ -n "$SWEEP" ]; then
  echo "## Knee"
  echo
  echo "The capacity question is where ops/s STOPS rising as concurrency doubles."
  echo "Scored four ways, because a flat curve has more than one cause and one"
  echo "of them is the load generator running out of CPU:"
  echo
  awk '{ops[$1"|"$2]=$3; cpu[$1"|"$2]=$4; if(!seen[$1]++) order[++n]=$1; conns[$1]=conns[$1]" "$2}
    END{
      for(i=1;i<=n;i++){
        s=order[i]; split(conns[s], cs, " ");
        best=0; bestc=0; prev=0; prevc=0; verdict="STILL RISING"; at=0;
        for(j=1;j<=length(cs);j++){
          c=cs[j]; if(c=="") continue;
          o=ops[s"|"c]+0;
          if(prevc>0){
            gain=(prev>0)?(o-prev)/prev:1;
            if(gain<0.05 && verdict=="STILL RISING"){
              cpus=cpu[s"|"c]; gsub("%","",cpus);
              if(cpus=="n/a") verdict="FLAT, client CPU UNKNOWN — cannot attribute";
              else if(cpus+0>=90) verdict="FLAT but CLIENT is at " cpu[s"|"c] " — this is the LOAD GENERATOR ceiling, not the server";
              else verdict="SERVER CEILING (client " cpu[s"|"c] ")";
              at=c;
            }
          }
          if(o>best){best=o; bestc=c}
          prev=o; prevc=c;
        }
        if(at>0) printf "- **%s**: flat by %s connections, peak %d ops/s — %s\n", s, at, best, verdict;
        else printf "- **%s**: STILL RISING at the top of the sweep (%s conns, %d ops/s) — the ceiling is ABOVE this range, do not quote it as capacity\n", s, bestc, best;
      }
    }' "$SWEEPDATA"
  echo
  echo "raw sweep data: $SWEEPDATA"
fi
echo "raw outputs: /tmp/memtier-*.out (Totals line: ops/s, avg, p50, p99, p99.9 latency ms)"
echo "scenario D (SET) carries a durability contract — state it alongside the"
echo "number: Flint acks fsync-bounded, RAM caches ack before any disk write."
