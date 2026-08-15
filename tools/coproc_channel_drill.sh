#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0010 D2 step 2: the PROXYCHAN channel token.
#
# The proxy mints a single-use channel token bound to (namespace, budget,
# deadline); a co-processor opens a channel with `PROXYCHAN <token>` and then
# speaks ordinary data commands, which the proxy scopes and routes with the
# SAME machinery as a tenant. Step 2 is "a channel that nothing yet opens" —
# there are no families (step 3) — so an admin `PROXYCHANMINT` stands in for the
# proxy's internal mint until then.
#
# The claims (drill 2's remaining steps, plus step 2's own):
#   - only an admin may mint a channel token
#   - a valid token opens a channel; its data commands land in the GRANTED
#     namespace and nowhere else (the scoping proof)
#   - `FLINTNS` on a channel is refused — for free, by the #151 edge guard, not
#     a channel-specific check (this is why the opener could not stay FLINT*)
#   - a token is single-use (reopen refused), unknown tokens refused, expired
#     tokens refused at open, and a live channel is closed once past its deadline
#   - a channel cannot be layered onto an already-authed tenant session
#   - the ordinary tenant path is untouched (control)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-chan 6696 6697
fleet_guard
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=$FLINT_DRILL_ROOT/flint-chan; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() { fleet_kill server; fleet_kill proxy; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy --features flint-server/rocks

ADMIN="admin-secret-token"
$B --port 6696 --engine mem 2>"$D/node.log" &
fleet_wait_listen 6696
fleet_wait_ping 6696
$PX --port 6697 --pairs "127.0.0.1:6696" --tenants "tokA=nsA,tokB=nsB" \
    --admin-token "$ADMIN" 2>"$D/proxy.log" &
fleet_wait_listen 6697
# With tenants configured an unauth PING answers -NOAUTH, which itself proves
# the proxy is serving (not a fixed sleep — wait for the answer).
for _ in $(seq 1 100); do
  case "$(valkey-cli -p 6697 PING 2>&1)" in *NOAUTH*|PONG) break ;; esac
  sleep 0.1
done

A="valkey-cli -p 6697 -a tokA --no-auth-warning"
BEE="valkey-cli -p 6697 -a tokB --no-auth-warning"
ADM="valkey-cli -p 6697 -a $ADMIN --no-auth-warning"
# chan <printf-fmt> <args…> — one raw connection carrying several commands
# (inline framing, the proxy accepts it), CRs stripped for grepping.
chan() { local f="$1"; shift; printf "$f" "$@" | nc -w 2 127.0.0.1 6697 | tr -d '\r'; }

echo "== only an admin may mint a channel token"
UNAUTH=$(valkey-cli -p 6697 PROXYCHANMINT nsA 100 60000 2>&1)
echo "$UNAUTH" | grep -qi "NOAUTH" || { echo "FAIL: unauth could mint ($UNAUTH)"; exit 1; }
TEN=$($A PROXYCHANMINT nsA 100 60000 2>&1)
echo "$TEN" | grep -qi "NOAUTH" || { echo "FAIL: a tenant could mint ($TEN)"; exit 1; }
echo "  unauth and tenant both refused: NOAUTH"

echo "== admin mints a token bound to nsA"
T=$($ADM PROXYCHANMINT nsA 100 60000)
[ "${#T}" = 64 ] || { echo "FAIL: mint did not return a 64-hex token (got '${T}')"; exit 1; }
echo "  minted: ${T:0:12}…"

echo "== PROXYCHAN opens; the channel's data commands flow"
OUT=$(chan 'PROXYCHAN %s\nSET ckey cval\nGET ckey\n' "$T")
echo "$OUT" | grep -q "^+OK$"   || { echo "FAIL: PROXYCHAN did not open (+OK missing): $OUT"; exit 1; }
echo "$OUT" | grep -q "^cval$"  || { echo "FAIL: channel GET did not return the value: $OUT"; exit 1; }
echo "  channel opened and did SET/GET"

echo "== the channel's write landed in nsA — and ONLY nsA (scoping proof)"
[ "$($A GET ckey)" = "cval" ] || { echo "FAIL: nsA tenant cannot see the channel's write"; exit 1; }
[ -z "$($BEE GET ckey)" ]     || { echo "FAIL: the channel's write leaked into nsB"; exit 1; }
echo "  nsA sees ckey; nsB does not"

echo "== FLINTNS on a channel is refused (the #151 edge guard, for free)"
T2=$($ADM PROXYCHANMINT nsA 100 60000)
NS=$(chan 'PROXYCHAN %s\nFLINTNS nsB\nGET bsecret\n' "$T2")
echo "$NS" | grep -qi "admin commands are not available" \
  || { echo "FAIL: FLINTNS was not refused on the channel: $NS"; exit 1; }
echo "  FLINTNS refused: $(echo "$NS" | grep -i 'admin commands' | head -1)"

echo "== a single-use token cannot be reopened"
T3=$($ADM PROXYCHANMINT nsA 100 60000)
chan 'PROXYCHAN %s\nPING\n' "$T3" >/dev/null       # consume it
RE=$(chan 'PROXYCHAN %s\nPING\n' "$T3")
echo "$RE" | grep -qi "already used" || { echo "FAIL: a used token reopened: $RE"; exit 1; }
echo "  reopen refused: already used"

echo "== an unknown token is refused"
BAD=$(chan 'PROXYCHAN %s\nPING\n' "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
echo "$BAD" | grep -qi "invalid channel token" || { echo "FAIL: an unknown token opened: $BAD"; exit 1; }
echo "  unknown token refused: invalid"

echo "== an expired token is refused at open"
TE=$($ADM PROXYCHANMINT nsA 100 1)   # 1ms deadline
sleep 0.2
EXP=$(chan 'PROXYCHAN %s\nPING\n' "$TE")
echo "$EXP" | grep -qi "expired" || { echo "FAIL: an expired token opened: $EXP"; exit 1; }
echo "  expired token refused at open"

echo "== a live channel is CLOSED once it outlives its deadline"
# Open with a 500ms deadline, prove one command works, then let the deadline
# pass and prove the next is refused and the connection dropped.
TD=$($ADM PROXYCHANMINT nsA 100 500)
DL=$({ printf 'PROXYCHAN %s\nGET ckey\n' "$TD"; sleep 0.9; printf 'GET ckey\n'; } \
     | nc -w 2 127.0.0.1 6697 | tr -d '\r')
echo "$DL" | grep -q "^cval$"          || { echo "FAIL: the in-deadline command did not work: $DL"; exit 1; }
echo "$DL" | grep -qi "deadline exceeded" || { echo "FAIL: the channel was not closed past its deadline: $DL"; exit 1; }
echo "  in-deadline command served; past-deadline command refused + closed"

echo "== a channel cannot be layered onto an authed tenant session"
TL=$($ADM PROXYCHANMINT nsA 100 60000)
LAY=$(chan 'AUTH tokA\nPROXYCHAN %s\n' "$TL")
echo "$LAY" | grep -qi "fresh connection" || { echo "FAIL: PROXYCHAN layered onto a tenant session: $LAY"; exit 1; }
echo "  refused: must open on a fresh connection"

echo "== CONTROL: the ordinary tenant path is untouched"
cli_ok $A SET tkey tval
[ "$($A GET tkey)" = "tval" ] || { echo "FAIL (control): tenant SET/GET broke"; exit 1; }
echo "  tenant SET/GET still works"

echo "PASS: PROXYCHAN admits a single-use, namespace-scoped, deadline-bounded"
echo "      channel; mint is admin-only; FLINTNS/reuse/unknown/expired/layering"
echo "      all refused; the tenant path is unchanged."
