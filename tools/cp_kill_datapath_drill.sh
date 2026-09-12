#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# M3 exit, clause four: "kill -9 any single control node with no data-path
# impact."
#
# WHY THIS EXISTS. That clause spent two months ORPHANED (OPS-0145): an
# annotation inserted mid-sentence closed M3's exit after its second clause and
# stranded the last three below the paragraph declaring the milestone complete.
# Nothing was deleted and every sentence stayed true, so nobody noticed — and
# nobody traces coverage for a criterion that is not attached to anything. When
# it was restored, the nearest three drills each turned out to assert something
# else:
#
#   controlplane_ha  kills the CP leader, but it REGISTERS proxy addresses
#                    (CPADDPROXY) and starts no proxy and no server. There is
#                    no data path present to be disturbed.
#   cpha_roll        has a full fleet, but it ROLLS the control plane — a
#                    controlled restart through flintctl, not kill -9.
#   cp-quorum (ops)  kills a seat, including the remote one, and asserts a
#                    MUTATION still lands. That is the CONTROL path, which is
#                    the half the clause is not about.
#
# So the claim had three neighbours and no evidence.
#
# WHAT IT PROVES. With a tenant reading and writing through the proxy
# CONTINUOUSLY, kill -9 on the CP LEADER costs the data path nothing: zero
# failed operations spanning the kill and the election that follows.
#
# WHY THE CONTROL AT THE END IS NOT OPTIONAL. "Zero errors" is exactly what a
# loop that never ran reports, and this drill's whole verdict is a zero. So it
# asserts a floor on operations attempted, and then DELIBERATELY breaks the
# data path and requires the same loop to report errors. A detector that cannot
# be shown to fire is not evidence, and a durability drill whose pass condition
# is a zero has to earn it twice.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-cpkilldp 6992 7571 7572 7573 7581 7582 7583 7914
fleet_guard
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
D=$FLINT_DRILL_ROOT/flint-cpkilldp; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; sleep 0.4
cleanup() {
  [ -n "${LOOP_PID:-}" ] && kill "$LOOP_PID" 2>/dev/null
  fleet_kill server; fleet_kill proxy; fleet_kill controlplane; rm -rf "$D"
}
trap cleanup EXIT

PEERS="1=127.0.0.1:7581,2=127.0.0.1:7582,3=127.0.0.1:7583"
CLIENTS="1=127.0.0.1:7571,2=127.0.0.1:7572,3=127.0.0.1:7573"
CLIENT_PORT=(x 7571 7572 7573)

start_seat() { # $1 = node id
  local id=$1
  $CP --raft --node-id "$id" --port "${CLIENT_PORT[$id]}" --raft-port "758$id" \
     --peers "$PEERS" --client-addrs "$CLIENTS" --state "$D/n$id" \
     >"$D/n$id.log" 2>&1 &
}

cpw() { # $1 = client port, rest = command; follows a LEADER redirect
  local port=$1; shift; local R
  for _ in 1 2 3 4 5 6; do
    R=$(valkey-cli -p "$port" "$@" 2>&1)
    if echo "$R" | grep -qoE "LEADER 127.0.0.1:[0-9]+"; then
      port=$(echo "$R" | grep -oE "127.0.0.1:[0-9]+" | head -1 | cut -d: -f2)
      sleep 0.4; continue
    fi
    if echo "$R" | grep -q "no leader elected"; then sleep 0.5; continue; fi
    echo "$R"; return
  done
  echo "$R"
}
leader_of() { valkey-cli -p "$1" CPINFO 2>/dev/null | tr '\r' '\n' | grep "^leader:" | cut -d: -f2; }

echo "== 3-seat Raft control plane + one pair + one proxy"
for id in 1 2 3; do start_seat $id; done
LEADER=""
for _ in $(seq 1 40); do
  L=$(leader_of 7571); [ -n "$L" ] && [ "$L" != "none" ] && { LEADER=$L; break; }
  sleep 0.5
done
[ -n "$LEADER" ] || { echo "FAIL: no leader elected"; tail -6 "$D"/n*.log; exit 1; }
echo "  leader elected: node $LEADER"

cpw 7571 CPADDPROXY 127.0.0.1:7914 >/dev/null
cpw 7571 CPADDPAIR 127.0.0.1:6992 >/dev/null
cpw 7571 CPADDTENANT acme tok-acme acme 1 >/dev/null
$B --port 6992 --engine rocks --data-dir "$D/m" 2>"${FLEET_SCOPE}server.log" &
fleet_wait_listen 6992
sleep 0.7
$PX --port 7914 --control-plane 127.0.0.1:7571 --advertise 127.0.0.1:7914 2>"$D/px.log" &
fleet_wait_listen 7914
sleep 1.5

a() { valkey-cli -p 7914 -a "$1" --no-auth-warning "${@:2}" 2>&1; }
[ "$(a tok-acme SET witness before-kill)" = "OK" ] \
  || { echo "FAIL: the data path does not work BEFORE the kill — nothing below would mean anything"; exit 1; }
echo "  data path up: SET/GET through the proxy as acme"

# The loop writes one line per operation: `ok` or `ERR <reply>`. It runs in its
# own process so the kill below happens WHILE it is in flight, which is the
# whole point — a before/after probe cannot see an outage between the probes.
LOG="$D/datapath.log"
: > "$LOG"
(
  i=0
  while :; do
    i=$((i + 1))
    r=$(valkey-cli -p 7914 -a tok-acme --no-auth-warning SET "k$i" "v$i" 2>&1)
    if [ "$r" = "OK" ]; then echo ok >> "$LOG"; else echo "ERR $r" >> "$LOG"; fi
    r=$(valkey-cli -p 7914 -a tok-acme --no-auth-warning GET "k$i" 2>&1)
    if [ "$r" = "v$i" ]; then echo ok >> "$LOG"; else echo "ERR $r" >> "$LOG"; fi
  done
) &
LOOP_PID=$!
sleep 1.5

echo "== kill -9 the CP LEADER (node $LEADER) while the tenant is reading and writing"
# SCOPED TO THIS DRILL'S OWN STATE DIR (BUG-0136). The pattern was
# `flint-controlplane --raft --node-id $LEADER ` and nothing else -- no port,
# no path. Three drills spawn a three-seat raft CP and all three elect node 1,
# so under the parallel gate each one's kill destroyed the others' node 1 as
# well. This drill is the only one of the three that checked pkill's exit
# status, so it is the only one that ever reported it: `FAIL: could not kill
# seat 1`, deterministic at 4-way parallelism and green when run alone.
# `--state $D/` is unique per drill because $D is under FLINT_DRILL_ROOT, and
# this seat spawns with --node-id BEFORE --state.
pkill -9 -f "flint-controlplane --raft --node-id $LEADER .*--state $D/" \
  || { echo "FAIL: could not kill seat $LEADER — no CP process matched this drill's own state dir"; exit 1; }

NEW=""
for _ in $(seq 1 40); do
  for p in 7571 7572 7573; do
    [ "$p" = "${CLIENT_PORT[$LEADER]}" ] && continue
    L=$(leader_of "$p"); [ -n "$L" ] && [ "$L" != "none" ] && [ "$L" != "$LEADER" ] && { NEW=$L; break 2; }
  done
  sleep 0.5
done
[ -n "$NEW" ] || { echo "FAIL: no new leader after killing $LEADER"; tail -6 "$D"/n*.log; exit 1; }
echo "  new leader elected: node $NEW"
sleep 1.5
kill "$LOOP_PID" 2>/dev/null; wait "$LOOP_PID" 2>/dev/null || true
LOOP_PID=""

OPS=$(wc -l < "$LOG" | tr -d ' ')
BAD=$(grep -c '^ERR' "$LOG" || true)
# A FLOOR ON WHAT WAS ATTEMPTED. Zero errors out of zero operations is what a
# loop that died on its first call reports, and it is indistinguishable from a
# clean run unless the count is asserted.
[ "$OPS" -ge 50 ] || { echo "FAIL: the data-path loop attempted only $OPS operation(s) across the kill — too few to have observed anything"; exit 1; }
[ "$BAD" -eq 0 ] || {
  echo "FAIL: $BAD of $OPS data-path operations failed while a CP seat was killed."
  echo "      M3's exit says kill -9 on any single control node has NO data-path impact."
  grep -m5 '^ERR' "$LOG" | sed 's/^/        /'
  exit 1
}
echo "  $OPS data-path operations spanning the kill and the election, 0 failed"

echo "== CONTROL: break the data path and require the SAME client path to notice"
# POLLED, NOT TIMED. The first version killed the pair, slept a fixed 1.5s and
# counted a loop's errors -- and got 131 successes and zero errors, which reads
# as "the detector cannot fire". It could: `fleet_kill` signals without waiting
# and its ps scan consumed most of the sleep, so nearly the whole window was
# still BEFORE the pair died. A control that depends on out-guessing a teardown
# is a control that reports on the sleep.
#
# So: kill the pair, then poll the same client path until it fails, with a
# budget. This asserts the detector CAN fail without assuming when.
fleet_kill server
SAW=""
for _ in $(seq 1 40); do
  r=$(a tok-acme SET "control-$$" probe)
  [ "$r" = "OK" ] || { SAW="$r"; break; }
  sleep 0.25
done
[ -n "$SAW" ] || {
  echo "FAIL: the pair was killed and the data path kept answering OK for 10s."
  echo "      Either this detector cannot fail -- making the 0 of $OPS above"
  echo "      worthless -- or the proxy acks writes with no live pair behind it."
  exit 1
}
echo "  detector fires once the pair is gone: [$SAW]"

echo "PASS: cp kill data-path — kill -9 on the Raft CP leader, with a tenant reading and writing throughout, cost the data path 0 of $OPS operations; the same client path fails when the pair itself is killed, so the zero was earned"
