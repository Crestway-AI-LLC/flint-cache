#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0010 D3 step 4: the channel resource class (the BOUND half).
#
# A co-processor's channel is not an unbounded pipe. A channel is minted with a
# fixed budget of DATA commands; it serves exactly that many, then the next is
# refused and the connection is closed. A looping co-processor is cut off at the
# bound (a -ERR plus a dropped socket), not discovered later in a latency graph.
# Non-data commands (PING/HELLO) are free — a channel can keep-alive without
# spending its budget. And the whole time, the channel's data commands are
# ordinary namespace-scoped storage: they land in the granted namespace and
# nowhere else.
#
# The claims:
#   - a channel serves EXACTLY its budget of data commands, then the next is
#     refused "channel budget exhausted" and the connection is closed
#   - non-data commands (PING) do NOT draw the budget down
#   - a channel's budgeted writes land in the granted namespace and ONLY there
#   - the ordinary tenant path is untouched (control)
#
# NOT here: the D1 quota EXEMPTION (a channel's data commands are exempt from
# the granting tenant's ops/s quota). A quota needs a rate, a rate needs the
# control plane (static `--tenants token=ns` carries no quota — asserted at
# boot), and every co-processor drill is deliberately CP-less. The exemption's
# decision is unit-tested (`channel_data_is_quota_exempt`); its end-to-end proof
# under a real rate lives with the CP-based fleet drills (#152).
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-budget 6688 6689
fleet_guard
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=$FLINT_DRILL_ROOT/flint-budget; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() { fleet_kill server; fleet_kill proxy; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy --features flint-server/rocks

ADMIN="admin-secret-token"
$B --port 6688 --engine mem 2>"$D/node.log" &
fleet_wait_listen 6688
fleet_wait_ping 6688
$PX --port 6689 --pairs "127.0.0.1:6688" --tenants "tokA=nsA,tokB=nsB" \
    --admin-token "$ADMIN" 2>"$D/proxy.log" &
fleet_wait_listen 6689
for _ in $(seq 1 100); do
  case "$(valkey-cli -p 6689 PING 2>&1)" in *NOAUTH*|PONG) break ;; esac
  sleep 0.1
done

A="valkey-cli -p 6689 -a tokA --no-auth-warning"
BEE="valkey-cli -p 6689 -a tokB --no-auth-warning"
ADM="valkey-cli -p 6689 -a $ADMIN --no-auth-warning"
# raw <printf-fmt> <args…> — one connection carrying several inline commands,
# CRs stripped for grepping.
raw() { local f="$1"; shift; printf "$f" "$@" | nc -w 2 127.0.0.1 6689 | tr -d '\r'; }

echo "== a channel serves EXACTLY its budget of data commands, then is cut off"
# budget 3: the open (+OK) plus three SETs (+OK) is four +OK; the fourth data
# command is refused and the socket closed.
T=$($ADM PROXYCHANMINT nsA 3 60000)
[ "${#T}" = 64 ] || { echo "FAIL: mint did not return a 64-hex token (got '${T}')"; exit 1; }
OUT=$(raw 'PROXYCHAN %s\nSET k1 v1\nSET k2 v2\nSET k3 v3\nSET k4 v4\n' "$T")
OKS=$(printf '%s\n' "$OUT" | grep -c "^+OK$")
[ "$OKS" = 4 ] \
  || { echo "FAIL: expected 4 +OK (open + 3 data), got $OKS: $OUT"; exit 1; }
echo "$OUT" | grep -qi "channel budget exhausted" \
  || { echo "FAIL: the 4th data command was not refused for budget: $OUT"; exit 1; }
echo "  budget 3 -> 3 data commands served, 4th refused: channel budget exhausted"

echo "== non-data commands do NOT draw the budget down (PING is free)"
# budget 1, but TWO PINGs precede the SET. If PING were charged, the SET would
# already be over budget. The airtight discriminator is on the tenant side: the
# SET's write is only visible if the SET actually ran; the overflow SET is not.
T2=$($ADM PROXYCHANMINT nsA 1 60000)
raw 'PROXYCHAN %s\nPING\nPING\nSET freebie yes\nSET overflow no\n' "$T2" >/dev/null
[ "$($A GET freebie)" = "yes" ] \
  || { echo "FAIL: the SET after two PINGs did not run — PING drew the budget down"; exit 1; }
[ -z "$($A GET overflow)" ] \
  || { echo "FAIL: a SET past the budget still wrote (budget not enforced)"; exit 1; }
echo "  two PINGs cost nothing; the one budgeted SET landed; the next did not"

echo "== a channel's budgeted writes land in the granted namespace and ONLY there"
[ "$($A GET k1)" = "v1" ] || { echo "FAIL: nsA cannot see the channel's write"; exit 1; }
[ -z "$($BEE GET k1)" ]   || { echo "FAIL: the channel's write leaked into nsB"; exit 1; }
echo "  nsA sees k1=v1; nsB does not"

echo "== CONTROL: the ordinary tenant path is untouched"
cli_ok $A SET tkey tval
[ "$($A GET tkey)" = "tval" ] || { echo "FAIL (control): tenant SET/GET broke"; exit 1; }
echo "  tenant SET/GET still works"

echo "PASS: a channel serves exactly its data-command budget then closes; PING"
echo "      is free; its writes are namespace-scoped; the tenant path is intact."
