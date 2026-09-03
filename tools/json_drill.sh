#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# JSON documents end to end through the PROXY against a TWO-pair cluster,
# on the rocks engine. The conformance corpus proves the command semantics
# against a single node; this proves the parts a corpus run cannot reach:
#   - both reply dialects survive the proxy hop intact, including the RESP
#     arrays with nil ELEMENTS that JSONPath misses produce (a re-encoding
#     bug there would silently flatten them);
#   - documents route by slot like any other key, so two documents land on
#     different pairs and both stay readable through one connection;
#   - hash tags co-locate related documents;
#   - a document survives a WARM RESTART with its TTL (it is one metadata
#     row, so this also proves the row round-trips through the LSM);
#   - tenant isolation holds for the JSON keyspace.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-json-state 7311 7312 7313 7314 7681 7722
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-json-state; INV=$FLINT_DRILL_ROOT/flint-json.flint
fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane
  rm -rf "$STATE" "$INV"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
tls on
cp 127.0.0.1:7722
pair 127.0.0.1:7311,127.0.0.1:7312
pair 127.0.0.1:7313,127.0.0.1:7314
proxy 127.0.0.1:7681
EOF
A="valkey-cli -p 7681 -a tok-acme --no-auth-warning"
B="valkey-cli -p 7681 -a tok-beta --no-auth-warning"

echo "== bootstrap 2 pairs + two tenants"
./target/release/flintctl -f "$INV" bootstrap >"$STATE-boot.log" 2>&1 || {
  # Capture it and STOP. This discarded bootstrap's output and
  # ignored its exit status, so a failed bootstrap ran on into the
  # assertions below and was reported as whichever one broke first
  # -- a product fault asserted for what was really "bootstrap
  # failed and nobody looked" (BUG-0064).
  echo "FAIL: bootstrap"; tail -25 "$STATE-boot.log"; exit 1; }
./target/release/flintctl -f "$INV" tenant add acme tok-acme acme 1 >/dev/null 2>&1
./target/release/flintctl -f "$INV" tenant add beta tok-beta beta 1 >/dev/null 2>&1

echo "== documents route by slot: many keys, spread across both pairs"
for i in $(seq 0 39); do
  $A JSON.SET "doc:$i" '$' "{\"n\":$i,\"tags\":[\"a\"]}" >/dev/null
done
[ "$($A DBSIZE)" = "40" ] || { echo "FAIL: seeded $($A DBSIZE) of 40"; exit 1; }
# Each master's own count must be a strict share — that is what proves the
# documents really span pairs rather than piling onto one.
share() {
  python3 - "$1" <<'PY'
import socket, ssl, sys, os
d=os.environ.get("FLINT_DRILL_ROOT","/tmp")+"/flint-json-state/certs"
ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); ctx.load_verify_locations(f"{d}/ca.crt")
ctx.load_cert_chain(f"{d}/int.crt", f"{d}/int.key"); ctx.check_hostname=False
s=ctx.wrap_socket(socket.create_connection(("127.0.0.1",int(sys.argv[1])),timeout=5),
                  server_hostname="flint-internal")
def cmd(*a):
    s.sendall(b"*%d\r\n"%len(a)+b"".join(b"$%d\r\n%s\r\n"%(len(x),x) for x in a)); return s.recv(65536)
cmd(b"FLINTNS",b"acme")
print(cmd(b"DBSIZE").decode(errors="replace").strip().lstrip(":"))
PY
}
S1=$(share 7311); S2=$(share 7313)
[ "$((S1 + S2))" = "40" ] || { echo "FAIL: shares $S1 + $S2 != 40"; exit 1; }
[ "$S1" -gt 0 ] && [ "$S2" -gt 0 ] || { echo "FAIL: one pair holds everything ($S1/$S2)"; exit 1; }
echo "  $S1 + $S2 = 40 documents, both pairs serving"

echo "== every document is readable through the ONE proxy connection"
BAD=0
for i in $(seq 0 39); do
  [ "$($A JSON.GET "doc:$i" .n)" = "$i" ] || BAD=$((BAD + 1))
done
[ "$BAD" = "0" ] || { echo "FAIL: $BAD documents unreadable through the proxy"; exit 1; }
echo "  40/40 read back correctly regardless of which pair holds them"

echo "== hash tags co-locate related documents"
$A JSON.SET '{u1}:profile' '$' '{"name":"a"}' >/dev/null
$A JSON.SET '{u1}:prefs' '$' '{"dark":true}' >/dev/null
[ "$($A JSON.GET '{u1}:profile' .name)" = '"a"' ] || { echo "FAIL: tagged doc 1"; exit 1; }
[ "$($A JSON.GET '{u1}:prefs' .dark)" = "true" ] || { echo "FAIL: tagged doc 2"; exit 1; }
echo "  {u1}:profile and {u1}:prefs both served"

echo "== both dialects survive the proxy hop, nil ELEMENTS included"
$A JSON.SET shape '$' '{"n":1,"a":[1,2],"o":{}}' >/dev/null
# `$` -> containers. valkey-cli renders a 1-element array as "1) x", so we
# assert on the raw framing via a line count + content, not a flat string.
[ "$($A JSON.GET shape '$.n')" = "[1]" ] || { echo "FAIL: \$ GET not wrapped: $($A JSON.GET shape '$.n')"; exit 1; }
[ "$($A JSON.GET shape .n)" = "1" ] || { echo "FAIL: legacy GET wrapped"; exit 1; }
[ "$($A JSON.GET shape '$.gone')" = "[]" ] || { echo "FAIL: \$ miss not empty container"; exit 1; }
$A JSON.GET shape .gone 2>&1 | grep -qi 'does not exist' || { echo "FAIL: legacy miss not an error"; exit 1; }
# The nil-element case: ARRLEN on an object under `$` is a one-element
# array whose element is nil. redis-cli prints that element as empty.
OUT=$($A JSON.ARRLEN shape '$.o')
[ "$(printf '%s' "$OUT" | wc -l | tr -d ' ')" = "0" ] && [ -z "$(printf '%s' "$OUT" | tr -d ' ')" ] \
  || { echo "FAIL: nil element did not survive re-encoding: [$OUT]"; exit 1; }
$A JSON.ARRLEN shape .o 2>&1 | grep -qi 'array' || { echo "FAIL: legacy wrong-shape not an error"; exit 1; }
[ "$($A JSON.ARRLEN shape '$.a')" = "2" ] || { echo "FAIL: \$ ARRLEN value"; exit 1; }
echo "  \$ -> containers (incl. empty and nil-element), legacy -> bare/errors"

echo "== TTL rides along with the document, and survives a WARM RESTART"
$A JSON.SET tdoc '$' '{"v":1}' >/dev/null
$A EXPIRE tdoc 600 >/dev/null
$A JSON.SET tdoc '$' '{"v":2}' >/dev/null   # root replacement keeps the TTL
T1=$($A TTL tdoc)
[ "$T1" -gt 500 ] 2>/dev/null || { echo "FAIL: root write cleared the TTL ($T1)"; exit 1; }
./target/release/flintctl -f "$INV" stop >/dev/null 2>&1
sleep 1
./target/release/flintctl -f "$INV" start >/dev/null 2>&1
for _ in $(seq 1 40); do $A PING >/dev/null 2>&1 && break; sleep 0.5; done
[ "$($A JSON.GET tdoc .v)" = "2" ] || { echo "FAIL: document lost across restart"; exit 1; }
T2=$($A TTL tdoc)
[ "$T2" -gt 500 ] 2>/dev/null || { echo "FAIL: TTL lost across restart ($T2)"; exit 1; }
[ "$($A JSON.GET 'doc:7' .n)" = "7" ] || { echo "FAIL: seeded documents lost across restart"; exit 1; }
echo "  document + TTL intact after stop/start (ttl $T1 -> $T2)"

echo "== tenant isolation holds for documents"
$B JSON.SET mine '$' '{"who":"beta"}' >/dev/null
[ "$($B JSON.GET mine .who)" = '"beta"' ] || { echo "FAIL: beta cannot read its own doc"; exit 1; }
[ "$($A JSON.GET mine)" = "" ] || { echo "FAIL: acme sees beta's document"; exit 1; }
[ "$($B JSON.GET 'doc:7')" = "" ] || { echo "FAIL: beta sees acme's document"; exit 1; }
[ "$($B DBSIZE)" = "1" ] || { echo "FAIL: beta DBSIZE $($B DBSIZE)"; exit 1; }
echo "  neither tenant can see the other's documents"

# The cluster must also AGREE WITH ITSELF, not merely pass the one
# path this drill exercises — the gap two shipped bugs lived in.
echo "== integrity: every view of the cluster reconciles"
./target/release/flintctl -f "$INV" verify --probe acme:tok-acme >/dev/null \
  || { echo "FAIL: cluster does not reconcile (run: flintctl -f $INV verify --probe acme:tok-acme)"; exit 1; }
echo "  verified"

echo "PASS: JSON through the proxy — documents shard by slot, both reply dialects survive the hop, TTLs persist across restart, tenants stay isolated"
