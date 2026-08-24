#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Controller drill: a master/replica pair plus flint-controller. Kill the
# master and verify the controller AUTOMATICALLY promotes the survivor —
# no manual FLINTPROMOTE — with data intact. Then bring the old master back
# and verify the controller fences it (FLINTDEMOTE).
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
# The scope declared here must COVER every data dir this drill creates
# (BUG-0047). The harness attributes a seat to a drill by this prefix or
# by a declared port; a seat matching neither is unattributable, and the
# one such seat in the suite — failover's zombie, started outside
# fleet.sh's tracking — is what broke the parallel gate.
fleet_init $FLINT_DRILL_ROOT/flint-ctl- 6440 6441 6370 6371 6372 6373 6374 6375 6376 6377
fleet_guard
fleet_kill server; fleet_kill controller; sleep 0.4
MDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-ctl-m.XXXXXX); RDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-ctl-r.XXXXXX)
B=./target/release/flint-server
MPORT=6440; RPORT=6441
cleanup() {
  pkill -9 -f "flint-server --port 644" 2>/dev/null
  fleet_kill controller
  rm -rf "$MDIR" "$RDIR"
}
trap cleanup EXIT

$B --port $MPORT --engine rocks --data-dir "$MDIR" 2>/dev/null &
fleet_wait_listen $MPORT
sleep 0.5
$B --port $RPORT --engine rocks --data-dir "$RDIR" --replica-of 127.0.0.1:$MPORT 2>/dev/null &
fleet_wait_listen $RPORT
sleep 0.9

echo "== loading 20000 keys"
# Loaded through fleet_load_resp, which replays anything the master sheds.
# Piping once and then asserting on key:0019999 made this drill print
# "FAIL: tail lost" for a write the master had openly REFUSED — reporting
# acked-write loss across a failover for a write that was never acked
# (BUG-0035). The tail assertion below is only meaningful once the load is
# known complete.
_ctl_load_gen() {
  awk 'BEGIN{for(i=0;i<20000;i++){k=sprintf("key:%07d",i);v=sprintf("value-%07d",i);printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",length(k),k,length(v),v}}'
}
fleet_load_resp "$MPORT" _ctl_load_gen || exit 1
# The head and tail this drill asserts on must exist before the assertion
# means anything. Repaired individually so a shed write cannot masquerade as
# failover data loss.
fleet_ensure_keys "$MPORT" "key:0000000=value-0000000" "key:0019999=value-0019999" || exit 1

echo "== starting controller"
./target/release/flint-controller --nodes 127.0.0.1:$MPORT,127.0.0.1:$RPORT --id ctl \
  --poll-ms 150 --confirm 3 2> $FLINT_DRILL_ROOT/flint-ctl.log &
sleep 1.5   # let it observe convergence

# A REAL clock, because this drill's number is quoted as evidence in
# docs/slo.md and the old one measured the ruler more than the event.
#
# The old measurement was `i * 200` over a loop that slept 200ms per probe.
# That reports the SLEEP budget, not elapsed time: it ignores the cost of
# each `valkey-cli` fork+connect, quantizes everything to a 200ms grid, and
# prints "200ms" for a promotion that took five. It also cannot report a
# number smaller than its own poll interval, so the published 0.6-1.2 s
# range was partly an artefact of the ruler.
#
# WHAT THIS DRILL'S NUMBER IS, and what it is not. This is an IDLE failover
# measured by a client that reconnects per probe: no write load, a fresh
# `valkey-cli` each attempt. That costs a process spawn and a connect per
# poll, which is why it reads slightly HIGHER than flint-chaos does for the
# same event (idle p50 ~514ms here vs ~404ms there over 14 promotions) even
# though chaos hammers writes straight through the kill.
#
# The loaded vantage belongs to flint-chaos, which holds a connection and
# writes continuously from inside the process. A shell loop cannot stand in
# for it: spawning valkey-cli per write tops out around a hundred writes a
# second, nowhere near enough to leave the replica a backlog, so a
# "--load" knob here would report a number while testing nothing. Measured
# before it was removed: loaded-by-shell 436-440ms vs idle 491-530ms — the
# load was invisible.
ms_now() { python3 -c 'import time;print(int(time.time()*1000))'; }

echo "== KILL master (no manual promotion — the controller must act)"
KILL_T=$(ms_now)
pkill -9 -f "flint-server --port $MPORT"

echo "== waiting for the controller to auto-promote the replica"
PROMOTED=0
DEADLINE=$(( $(date +%s) + 12 ))
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  W=$(valkey-cli -p $RPORT SET ctl-probe ok 2>&1)
  if [ "$W" = "OK" ]; then PROMOTED=1; break; fi
  sleep 0.02   # tight: the ruler must be finer than the thing measured
done
RTO_MS=$(( $(ms_now) - KILL_T ))
[ "$PROMOTED" = "1" ] || { echo "FAIL: controller did not auto-promote in 12s"; echo "--- controller log:"; cat $FLINT_DRILL_ROOT/flint-ctl.log; exit 1; }
echo "auto-promotion OK (${RTO_MS}ms after kill, idle, real clock, reconnect per probe)"
# The published budget, asserted rather than admired. docs/slo.md commits to
# RTO <= 10 s; a drill that prints a number nobody checks is not evidence.
[ "$RTO_MS" -lt 10000 ] || { echo "FAIL: RTO ${RTO_MS}ms exceeds the published 10s budget"; exit 1; }

echo "== data intact on the new master"
[ "$(valkey-cli -p $RPORT GET key:0000000)" = "value-0000000" ] || { echo "FAIL: head lost"; exit 1; }
[ "$(valkey-cli -p $RPORT GET key:0019999)" = "value-0019999" ] || { echo "FAIL: tail lost"; exit 1; }
[ "$(valkey-cli -p $RPORT GET ctl-probe)" = "ok" ] || { echo "FAIL: post-promotion write lost"; exit 1; }
echo "$(grep -oE 'PROMOTED .* at \(0,[0-9]+\)' $FLINT_DRILL_ROOT/flint-ctl.log | head -1)"

echo "== bring the OLD master back; controller must fence it"
$B --port $MPORT --engine rocks --data-dir "$MDIR" 2>/dev/null &
fleet_wait_listen $MPORT
sleep 2.0
FENCED=0
for i in $(seq 1 40); do
  RO=$(valkey-cli -p $MPORT SET zombie bad 2>&1 || true)
  if echo "$RO" | grep -q "READONLY"; then FENCED=1; break; fi
  sleep 0.2
done
[ "$FENCED" = "1" ] || { echo "FAIL: controller did not fence the returned master"; echo "--- controller log:"; tail -8 $FLINT_DRILL_ROOT/flint-ctl.log; exit 1; }
echo "$(grep -oE 'FENCED zombie .* at \(0,[0-9]+\)' $FLINT_DRILL_ROOT/flint-ctl.log | head -1)"

echo "PASS: hands-free failover + automatic zombie fencing, data intact"
