#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0059: what a rollback below the release that introduces vector filters
# does to a set made with them, and why that release keeps them behind
# --vec-filters.
#
# THIS build's flint-vec, with --vec-filters, makes a set with fields and
# answers filtered searches. The previous release's flint-vec
# (FLINT_PREV_VEC, built from its tag) then rebuilds the same set from the
# same durable rows: it loads it, answers unfiltered searches the same, and
# answers a FILTER as if there were none, which is the hazard the flag holds
# back. THIS build then rebuilds it again and filters as it did.
#
# Not in the gate: it needs a second build. Run it by hand when touching how
# a set's config row or VEC.SEARCH's options are read, with the release a
# rollback would go to:
#   git worktree add --detach ../prev v0.1.0-rc.83
#   (cd ../prev && cargo build --release -p flint-vec)
#   FLINT_PREV_VEC=../prev/target/release/flint-vec tools/vec_filters_rollback_drill.sh
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
PREV=${FLINT_PREV_VEC:-}
if [ -z "$PREV" ] || [ ! -x "$PREV" ]; then
  echo "SKIP: set FLINT_PREV_VEC to the previous release's flint-vec"
  exit 0
fi
fleet_init "$FLINT_DRILL_ROOT/flint-vfroll" 7945 7946 7947
fleet_guard
D=$FLINT_DRILL_ROOT/flint-vfroll
COPROC_PID=""
fleet_kill server; fleet_kill proxy; sleep 0.3
cleanup() {
  [ -n "$COPROC_PID" ] && kill -9 "$COPROC_PID" 2>/dev/null
  fleet_kill server; fleet_kill proxy; rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"
cargo build --release -q -p flint-server -p flint-proxy -p flint-vec --features flint-server/rocks || { echo "FAIL: build"; exit 1; }
NOW=./target/release/flint-vec
# --engine mem: the server holds the durable rows across every co-processor
# restart, so each co-processor rebuilds from the same rows.
# Each disowned, so the cleanup's kills are not announced as job deaths.
./target/release/flint-server --port 7945 --engine mem 2>"$D/n.log" & disown
fleet_wait_listen 7945; fleet_wait_ping 7945
./target/release/flint-proxy --port 7946 --pairs 127.0.0.1:7945 --tenants "tok=ns" \
    --families "VEC.=127.0.0.1:7947" --edge-advertise 127.0.0.1:7946 2>"$D/px.log" & disown
fleet_wait_listen 7946
for _ in $(seq 1 60); do case "$(valkey-cli -p 7946 PING 2>&1)" in *NOAUTH*|PONG) break;; esac; sleep 0.1; done
A="valkey-cli -p 7946 -a tok --no-auth-warning"
vexec() {   # retry on -LOADING, the client contract for a warming index
  local i out
  for i in $(seq 1 50); do
    out="$($A "$@" 2>&1 | tr -d '\r' | tr '\n' ' ')"
    case "$out" in *LOADING*) sleep 0.15 ;; *) echo "$out"; return 0 ;; esac
  done
  echo "$out"; return 1
}
ids() { vexec "$@" | awk '{for (i = 1; i <= NF; i += 2) printf "%s ", $i}'; }
start_vec() {   # start_vec <binary> <log> <flags...>
  local bin="$1" log="$2"; shift 2
  [ -n "$COPROC_PID" ] && kill -9 "$COPROC_PID" 2>/dev/null
  sleep 0.3
  "$bin" --port 7947 "$@" 2>"$D/$log" & COPROC_PID=$!
  disown
  fleet_wait_listen 7947
}

echo "== this build, with --vec-filters: a set with fields"
start_vec "$NOW" vec1.log --vec-filters
for s in "fl" "hn INDEX hnsw" "bn INDEX hnsw QUANT bin"; do
  set -- $s
  name=$1; shift
  [ "$(vexec VEC.CREATE "$name" DIM 3 METRIC l2 "$@" FIELD t TAG)" = "OK " ] || { echo "FAIL: VEC.CREATE $name"; exit 1; }
  set -f
  for row in 'a 1,0,0 {"t":"x"}' 'b 0.9,0.1,0 {"t":"y"}' 'c 0.8,0.2,0 {"t":"x"}'; do
    set -- $row
    [ "$(vexec VEC.SET "$name" "$1" "$2" META "$3")" = "OK " ] || { echo "FAIL: VEC.SET $name $1"; exit 1; }
  done
  set +f
done
check() {   # check <label>: every set filters to a c
  for name in fl hn bn; do
    [ "$(ids VEC.SEARCH $name 1,0,0 3)" = "a b c " ] || { echo "FAIL: $1: unfiltered $name: $(ids VEC.SEARCH $name 1,0,0 3)"; exit 1; }
    [ "$(ids VEC.SEARCH $name 1,0,0 3 FILTER '@t:{x}')" = "a c " ] || { echo "FAIL: $1: filtered $name: $(ids VEC.SEARCH $name 1,0,0 3 FILTER '@t:{x}')"; exit 1; }
  done
}
check "this build"
echo "  flat, hnsw and bin sets filter to the matches"

echo "== the previous release, on the same rows"
start_vec "$PREV" vec2.log --vec-filters
for name in fl hn bn; do
  [ "$(ids VEC.SEARCH $name 1,0,0 3)" = "a b c " ] || { echo "FAIL: the previous release did not load $name: $(vexec VEC.SEARCH $name 1,0,0 3)"; exit 1; }
  # The hazard: an option it does not know is passed over, so the filter is
  # not applied. This is why the release that introduces filters keeps them
  # behind --vec-filters, and why the next release can turn them on.
  [ "$(ids VEC.SEARCH $name 1,0,0 3 FILTER '@t:{x}')" = "a b c " ] || { echo "FAIL: the previous release answered FILTER otherwise than as no filter: $(vexec VEC.SEARCH $name 1,0,0 3 FILTER '@t:{x}')"; exit 1; }
done
case "$(vexec VEC.CREATE nf DIM 3 METRIC l2 FIELD t TAG)" in *ERR*) : ;; *) echo "FAIL: the previous release made a set with FIELD"; exit 1 ;; esac
echo "  it loads every set, searches it the same, and answers FILTER as no filter"

echo "== this build again"
start_vec "$NOW" vec3.log --vec-filters
check "after the previous release"
grep -qi "rebuilt ns" "$D/vec3.log" || { echo "FAIL: no rebuild logged"; exit 1; }
echo "  the fields came back from the config row: the same filtered answers"

echo "PASS: a set made with vector filters loads on the previous release, which ignores FILTER,"
echo "      and filters again on this one (ADR-0059's two releases)."
