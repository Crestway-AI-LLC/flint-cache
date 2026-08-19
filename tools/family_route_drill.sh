#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0010 D1 step 3: the family route table, and the gate that it changes
# nothing it must not.
#
# The proxy learns a table of command PREFIXES (`VEC.`) that route to a
# co-processor. The safety argument for shipping it (D1) is entirely negative:
# resolution order is known write → known read → registered family → unknown,
# so a command that is NOT a registered family routes exactly as before — a
# known command by its key, an unknown one to the master, which errors. A
# registered family with no reachable co-processor answers `-COPROCUNAVAIL`
# (the FORWARD to a co-processor is the next slice; this is its recognition and
# routing decision).
#
# This drill is that gate:
#   - the whole ordinary command surface behaves normally with a table present
#   - a known command whose PREFIX overlaps a registered family is NOT
#     intercepted (known write/read wins the resolution order)
#   - a registered family command -> -COPROCUNAVAIL
#   - an UNREGISTERED dotted command -> the server's unknown-command error,
#     exactly as it would without a table — so the table's effect is scoped to
#     the prefixes it registers and nothing else
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-fam 6690 6691
fleet_guard
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=$FLINT_DRILL_ROOT/flint-fam; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() { fleet_kill server; fleet_kill proxy; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

$B --port 6690 --engine mem 2>"$D/node.log" &
fleet_wait_listen 6690
fleet_wait_ping 6690
# A real family (VEC.) AND a pathological prefix (SE) that overlaps the known
# write SET — to prove the resolution order puts known commands first. The
# endpoints are placeholders: step 3 answers -COPROCUNAVAIL without dialing.
$PX --port 6691 --pairs "127.0.0.1:6690" --tenants "tokA=nsA" \
    --families "VEC.=coproc-a:9;SE=coproc-b:9" 2>"$D/proxy.log" &
fleet_wait_listen 6691
for _ in $(seq 1 100); do
  case "$(valkey-cli -p 6691 PING 2>&1)" in *NOAUTH*|PONG) break ;; esac
  sleep 0.1
done
grep -q "listening" "$D/proxy.log" 2>/dev/null || true
# If --families were rejected the proxy would not be serving; the NOAUTH/PONG
# wait above already proves it came up.

A="valkey-cli -p 6691 -a tokA --no-auth-warning"

echo "== the ordinary command surface is unchanged with a table present"
cli_ok $A SET k1 v1
[ "$($A GET k1)" = "v1" ]        || { echo "FAIL: GET"; exit 1; }
[ "$($A APPEND k1 xy)" = "4" ]   || { echo "FAIL: APPEND"; exit 1; }
[ "$($A DEL k1)" = "1" ]         || { echo "FAIL: DEL"; exit 1; }
[ "$($A INCR n)" = "1" ]         || { echo "FAIL: INCR"; exit 1; }
[ "$($A HSET h f v)" = "1" ]     || { echo "FAIL: HSET"; exit 1; }
[ "$($A HGET h f)" = "v" ]       || { echo "FAIL: HGET"; exit 1; }
[ "$($A SADD s a b c)" = "3" ]   || { echo "FAIL: SADD"; exit 1; }
[ "$($A LPUSH l x)" = "1" ]      || { echo "FAIL: LPUSH"; exit 1; }
echo "  strings/hash/set/list all behave normally"

echo "== resolution order: a known command overlapping a family prefix is NOT intercepted"
# SET begins with the registered family prefix SE, but SET is a known WRITE, so
# known-write wins the order and it routes to the backend exactly as always.
cli_ok $A SET sekey sev
[ "$($A GET sekey)" = "sev" ] || { echo "FAIL: SET was intercepted by the SE family"; exit 1; }
echo "  SET works despite the SE family — known write wins the resolution order"

echo "== a REGISTERED family command -> -COPROCUNAVAIL"
VE=$($A VEC.SET vk vv 2>&1)
echo "$VE" | grep -qi "COPROCUNAVAIL" \
  || { echo "FAIL: registered VEC.SET did not answer COPROCUNAVAIL ($VE)"; exit 1; }
echo "  VEC.SET -> $VE"

echo "== an UNREGISTERED dotted command -> the server's unknown-command error (unchanged)"
# MAT. is not in the table, so it routes to the master exactly as it would with
# no table at all — the proof that the table changes only what it registers.
MA=$($A MAT.MUL mk 2>&1)
echo "$MA" | grep -qi "COPROCUNAVAIL" \
  && { echo "FAIL: an unregistered command was intercepted by the family path ($MA)"; exit 1; }
echo "$MA" | grep -qi "unknown command" \
  || { echo "FAIL: unregistered MAT.MUL did not get the master's unknown-command error ($MA)"; exit 1; }
echo "  MAT.MUL -> ${MA#*ERR } (routed to the master, which errors — as without a table)"

echo "PASS: the family table routes registered prefixes to the co-processor path"
echo "      (-COPROCUNAVAIL until one is reachable) and leaves every other"
echo "      command — known or unknown — routed exactly as before."
