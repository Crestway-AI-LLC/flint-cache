#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A tenant must never be able to name another tenant's namespace.
#
# The proxy pins each backend connection with `FLINTNS <ns>`; the data port
# TRUSTS its callers about that, because the proxy is the tenant boundary.
# So the whole isolation guarantee reduces to one property: a `FLINT*`
# command from a client must never reach a backend.
#
# WHY THIS DRILL EXISTS. That refusal used to live in a per-command match
# that a transaction never reached. `transaction_step` relays the raw bytes
# and returns first, so inside a MULTI the command went straight through:
#
#   AUTH tokA / MULTI / FLINTNS nsbravo / GET bsecret / EXEC
#     -> "tenant-B-private"
#
# Full cross-tenant read AND write, needing only the other tenant's
# namespace name. Refused outside a transaction, allowed inside one — the
# guard was real and simply not on every path. This drill exercises the
# paths, not the guard, so a future refactor that reintroduces a
# forward-before-check ordering fails here.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-nsesc 6851 6852
fleet_guard
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=/tmp/flint-nsesc; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() { fleet_kill server; fleet_kill proxy; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy

$B --port 6851 --engine mem 2>"$D/node.log" &
fleet_wait_listen 6851
fleet_wait_ping 6851
$PX --port 6852 --pairs "127.0.0.1:6851" --tenants "tokA=nsalpha,tokB=nsbravo" 2>"$D/proxy.log" &
fleet_wait_listen 6852
# NOT fleet_wait_ping: with tenants configured, an unauthenticated PING is
# answered -NOAUTH, which is itself proof the proxy is serving. And not a
# fixed sleep either (#110) — wait for the answer, not for a duration.
for _ in $(seq 1 100); do
  case "$(valkey-cli -p 6852 PING 2>&1)" in
    *NOAUTH*|PONG) break ;;
  esac
  sleep 0.1
done
case "$(valkey-cli -p 6852 PING 2>&1)" in
  *NOAUTH*|PONG) ;;
  *) echo "FAIL: proxy never came up"; tail -5 "$D/proxy.log"; exit 1 ;;
esac

A="valkey-cli -p 6852 -a tokA --no-auth-warning"
BEE="valkey-cli -p 6852 -a tokB --no-auth-warning"
SECRET="tenant-B-private"

# Speak raw RESP for the attack cases: valkey-cli will not send an unknown
# command inside MULTI the way a hostile client would.
attack() {
  { printf '*2\r\n$4\r\nAUTH\r\n$4\r\ntokA\r\n'
    printf '*1\r\n$5\r\nMULTI\r\n'
    printf '%b' "$1"
    printf '*2\r\n$3\r\nGET\r\n$7\r\nbsecret\r\n'
    printf '*1\r\n$4\r\nEXEC\r\n'
    sleep 0.6; } | nc -w 3 127.0.0.1 6852 | tr -d '\r'
}

echo "== tenant B stores a secret"
cli_ok $BEE SET bsecret "$SECRET"

# POSITIVE CONTROL, and it is not decoration. If B could not read its own
# secret, every "A cannot read it" below would pass for the wrong reason —
# the drill would be asserting that a key nobody can read is unreadable.
GOT=$($BEE GET bsecret)
[ "$GOT" = "$SECRET" ] || {
  echo "FAIL (control): tenant B cannot read its OWN secret ($GOT)."
  echo "      Every isolation check below would pass vacuously."
  exit 1
}
echo "  control: B reads its own secret, so the checks below mean something"

echo "== tenant A cannot reach nsbravo, by any route"
# Each entry is a raw RESP frame injected between MULTI and the GET.
try() {
  local label="$1" frame="$2" out
  out=$(attack "$frame")
  if echo "$out" | grep -qF "$SECRET"; then
    echo "FAIL: [$label] LEAKED tenant B's secret across the namespace boundary"
    echo "$out" | sed 's/^/        /'
    exit 1
  fi
  echo "  refused: $label"
}

try "FLINTNS inside MULTI"      '*2\r\n$7\r\nFLINTNS\r\n$7\r\nnsbravo\r\n'
try "lowercase flintns in MULTI" '*2\r\n$7\r\nflintns\r\n$7\r\nnsbravo\r\n'
try "mixed-case FlInTnS in MULTI" '*2\r\n$7\r\nFlInTnS\r\n$7\r\nnsbravo\r\n'
try "FLINTNSBYTES in MULTI"     '*2\r\n$12\r\nFLINTNSBYTES\r\n$7\r\nnsbravo\r\n'
try "no MULTI, plain FLINTNS"   ''

# Outside a transaction too, on its own connection.
OUT=$($A FLINTNS nsbravo 2>&1)
echo "$OUT" | grep -q "not available through the proxy" \
  || { echo "FAIL: plain FLINTNS was not refused: $OUT"; exit 1; }
echo "  refused: FLINTNS on a plain connection"

echo "== tenant A cannot WRITE into nsbravo either"
{ printf '*2\r\n$4\r\nAUTH\r\n$4\r\ntokA\r\n'
  printf '*1\r\n$5\r\nMULTI\r\n'
  printf '*2\r\n$7\r\nFLINTNS\r\n$7\r\nnsbravo\r\n'
  printf '*3\r\n$3\r\nSET\r\n$7\r\nplanted\r\n$3\r\nbad\r\n'
  printf '*1\r\n$4\r\nEXEC\r\n'
  sleep 0.6; } | nc -w 3 127.0.0.1 6852 >/dev/null
PLANTED=$($BEE GET planted)
[ -z "$PLANTED" ] || {
  echo "FAIL: tenant A planted a key in nsbravo (planted=$PLANTED)"; exit 1; }
echo "  nsbravo holds no key written by A"

echo "== CONTROL: ordinary transactions still work for A"
# The cheapest way to "fix" this bug would be to break MULTI. Prove we did
# not: a normal two-command transaction must still queue and execute.
OUT=$({ printf '*2\r\n$4\r\nAUTH\r\n$4\r\ntokA\r\n'
        printf '*1\r\n$5\r\nMULTI\r\n'
        printf '*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$4\r\nmine\r\n'
        printf '*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n'
        printf '*1\r\n$4\r\nEXEC\r\n'
        sleep 0.6; } | nc -w 3 127.0.0.1 6852 | tr -d '\r')
echo "$OUT" | grep -q "^mine$" || {
  echo "FAIL (control): a normal MULTI no longer works — the guard broke transactions"
  echo "$OUT" | sed 's/^/        /'
  exit 1
}
echo "  a normal MULTI still queues and executes"

echo "== and A's own key is in A's namespace, not B's"
[ "$($A GET mykey)" = "mine" ] || { echo "FAIL: A lost its own key"; exit 1; }
[ -z "$($BEE GET mykey)" ] || { echo "FAIL: A's key is visible to B"; exit 1; }
echo "  mykey: readable by A, invisible to B"

echo "PASS: no route lets a tenant name another tenant's namespace"
echo "      (FLINT* refused pre-auth and pre-transaction; MULTI still works)"
