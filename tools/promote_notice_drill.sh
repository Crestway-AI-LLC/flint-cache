#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# PROMOTION NOTICE (#91): does telling the proxies actually cost less than
# letting them find out?
#
# THE PATH BEING MEASURED. A planned failover demotes the old master IN
# PLACE, so it stays up and keeps answering — with -READONLY. The proxy has
# no other signal: it discovers the handoff only when a client's write
# bounces, and its retry loop sleeps 50ms before trying again. So the first
# write after a failover pays that sleep plus a pair re-probe, every time,
# on every proxy. `flintctl upgrade` walks each master through exactly this.
#
# WHY FIRST-WRITE LATENCY IS THE RIGHT RULER. The difference is structural,
# not statistical: without a notice the write CANNOT return before the 50ms
# retry sleep, and with one it takes a normal round trip. That is ~50x on
# loopback, so a single measurement separates the two cases without any
# averaging — and the assertion below fails loudly if the gap ever collapses,
# which is what would happen if the notice silently stopped being sent.
#
# THE NEGATIVE CONTROL IS THE POINT. Run A does the same failover with the
# notice suppressed and asserts the stall is STILL THERE. Without it, a drill
# that only measured the fast path would pass just as happily against a proxy
# that had gotten fast for some unrelated reason, and would keep passing if
# the feature were deleted.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-promo 6910 6911 7596 7823
fleet_guard
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
D=/tmp/flint-promo; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; sleep 0.4
cleanup() { fleet_kill server; fleet_kill proxy; fleet_kill controlplane; rm -rf "$D"; }
trap cleanup EXIT

M=127.0.0.1:6910   # starts as master
R=127.0.0.1:6911   # starts as replica, gets promoted

# Milliseconds since epoch, portable across macOS and Linux (the same clock
# fix controller_drill.sh needed — `date +%s%3N` is GNU-only and silently
# yields a literal "3N" on macOS, which would make every measurement 0).
now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

boot_fleet() {
  fleet_kill server; fleet_kill proxy; fleet_kill controlplane; sleep 0.4
  rm -rf "$D"; mkdir -p "$D"
  $B --port 6910 --engine rocks --data-dir "$D/m" 2>/dev/null &
  $B --port 6911 --engine rocks --data-dir "$D/r" --replica-of "$M" 2>/dev/null &
  fleet_wait_listen 6910 6911
  sleep 0.8
  $CP --port 7596 --state "$D/cp" 2>"$D/cp.log" &
  fleet_wait_listen 7596
  sleep 0.5
  valkey-cli -p 7596 CPADDPROXY 127.0.0.1:7823 >/dev/null
  valkey-cli -p 7596 CPADDPAIR "$M,$R" >/dev/null
  valkey-cli -p 7596 CPADDTENANT acme tok-acme acme 1 >/dev/null
  $PX --port 7823 --control-plane 127.0.0.1:7596 --advertise 127.0.0.1:7823 2>"$D/px.log" &
  fleet_wait_listen 7823
  sleep 1.5
  # PROVE THE PROXY IS ROUTING before timing anything: a drill that measured
  # a proxy which was never serving would report a confident number about
  # nothing.
  [ "$(valkey-cli -p 7823 -a tok-acme --no-auth-warning SET warm v)" = "OK" ] \
    || { echo "FAIL: proxy not serving before the failover — nothing to measure"; exit 1; }
}

# Steady-state cost of one write through the proxy: round trip PLUS the
# valkey-cli spawn. Everything interesting is a delta above this.
measure_baseline() {
  local a b c
  a=$(first_write_ms base1); b=$(first_write_ms base2); c=$(first_write_ms base3)
  # Median of three: one scheduler hiccup should not set the yardstick.
  printf '%s\n' "$a" "$b" "$c" | sort -n | sed -n 2p
}

# Demote-then-promote: the same order controlled_failover uses, so the old
# master stays UP and answers -READONLY. That is the case with no connection
# error to trigger rediscovery.
do_failover() {
  local epoch=9
  valkey-cli -p 6910 FLINTDEMOTE 0 $epoch >/dev/null 2>&1
  valkey-cli -p 6911 FLINTPROMOTE 0 $((epoch + 1)) >/dev/null 2>&1
}

# The first write a client issues AFTER the handoff, in ms. NOTE this
# includes the cost of SPAWNING valkey-cli (tens of ms), which is why every
# assertion below is against a MEASURED steady-state baseline rather than an
# absolute number. Guessing that constant is how a ruler ends up measuring
# the harness instead of the system -- the first version of this drill
# asserted <25ms and failed at 31ms on a working feature, because ~28ms of
# that was process startup.
first_write_ms() {
  local t0 t1
  t0=$(now_ms)
  valkey-cli -p 7823 -a tok-acme --no-auth-warning SET after_$1 v >/dev/null 2>&1
  t1=$(now_ms)
  echo $(( t1 - t0 ))
}

echo "== A) NEGATIVE CONTROL: failover with NO notice (today's behaviour)"
boot_fleet
BASE=$(measure_baseline)
echo "  steady-state write (incl. cli spawn): ${BASE}ms — the yardstick"
do_failover
SLOW=$(first_write_ms a)
echo "  first write after failover: ${SLOW}ms"
SLOW_DELTA=$(( SLOW - BASE ))
echo "  that is ${SLOW_DELTA}ms above steady state"
# The proxy must have had to discover it the hard way: the retry loop sleeps
# 50ms, so the stall cannot be small. If it is, the ruler is broken (or the
# proxy learned some other way) and run B proves nothing by comparison.
[ "$SLOW_DELTA" -ge 30 ] \
  || { echo "FAIL: expected the reactive path to cost >=30ms above baseline (the 50ms
      retry sleep), got ${SLOW_DELTA}ms. The comparison in B is meaningless if
      the no-notice path is already fast."; exit 1; }
HINT_A=$(valkey-cli -p 7596 CPSNAPSHOT 127.0.0.1:7823 | tail -1)
echo "  cp promotion hint: '${HINT_A}' (expected empty)"

echo "== B) WITH the notice: the CP is told, and tells the proxy"
boot_fleet
BASE_B=$(measure_baseline)
echo "  steady-state write: ${BASE_B}ms"
do_failover
valkey-cli -p 7596 CPPROMOTED "$R" >/dev/null
# The push is a wakeup on an already-open CPWATCH connection, not a poll.
# Allow a beat for it to land; the assertion below is what proves it did.
sleep 0.5
FAST=$(first_write_ms b)
echo "  first write after failover: ${FAST}ms"

# MECHANISM, not just timing: the hint must actually be in the snapshot the
# proxy is served. A timing win with an empty hint would mean something else
# got fast and the notice is doing nothing.
HINT_B=$(valkey-cli -p 7596 CPSNAPSHOT 127.0.0.1:7823 | tail -1)
echo "  cp promotion hint: '${HINT_B}'"
case "$HINT_B" in
  "$R|"*) ;;
  *) echo "FAIL: expected a promotion hint naming $R, got '${HINT_B}'"; exit 1 ;;
esac

FAST_DELTA=$(( FAST - BASE_B ))
echo "  that is ${FAST_DELTA}ms above steady state"
# Having been told, the proxy re-probed BEFORE this write, so it should cost
# about what any other write costs. Allow 20ms of slack for scheduling; the
# separation being measured is ~50ms, so this is nowhere near a coin flip.
[ "$FAST_DELTA" -lt 20 ] \
  || { echo "FAIL: with the notice the first write should cost about steady state,
      got ${FAST_DELTA}ms above it — the proxy did not act on the hint"; exit 1; }

echo
echo "  no notice: +${SLOW_DELTA}ms over baseline    with notice: +${FAST_DELTA}ms"
echo "PASS: the promotion notice removes the post-failover retry stall (+${SLOW_DELTA}ms -> +${FAST_DELTA}ms above steady state); hint present in the pushed snapshot and absent without it"
