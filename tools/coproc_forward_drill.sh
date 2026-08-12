#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0010 D1/D6 step 3 part 2: a family command REACHES a co-processor.
#
# Drill 1's positive half, end to end. A registered family command (VEC.SET) is
# charged once, minted a single-use channel token, and forwarded as
# `FLINTFAM <token> <callback> <ns> VEC.SET k v` to a co-processor; the co-processor
# opens `PROXYCHAN` back to the callback and stores in the GRANTED namespace;
# its reply relays to the client. Then the properties that matter:
#   - the storage landed in that tenant's namespace and NO other (scoping, now
#     through the whole two-hop path, not just the channel)
#   - a co-processor's OWN `-ERR` relays as-is (its logic said no)
#   - a DOWN co-processor answers `-COPROCUNAVAIL` (a transport failure, kept
#     distinct from the co-processor's own error)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-fwd 6684 6685 6686
fleet_guard
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=/tmp/flint-fwd; rm -rf "$D"; mkdir -p "$D"
COPROC_PID=""
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() {
  [ -n "$COPROC_PID" ] && kill -9 "$COPROC_PID" 2>/dev/null
  fleet_kill server; fleet_kill proxy; rm -rf "$D"
}
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy --features flint-server/rocks

# --- the stand-in co-processor (the D6 contract, plaintext for the drill) -----
# On each FLINTFAM frame it opens PROXYCHAN <token> back to the callback and,
# for VEC.SET k v, stores coproc:k=v in the channel's namespace, then replies
# +FAMOK. Anything else it -ERRs (to prove that path is relayed, not masked).
# It loops on the connection, because the proxy POOLS and reuses it (D6).
cat > "$D/coproc.py" <<'PY'
import socket, sys, threading
LISTEN = int(sys.argv[1])
def read_array(f):
    line = f.readline()
    if not line or line[:1] != b'*': return None
    n = int(line[1:]); out = []
    for _ in range(n):
        h = f.readline(); ln = int(h[1:]); d = f.read(ln); f.read(2); out.append(d)
    return out
def cmd(*parts):
    o = b'*%d\r\n' % len(parts)
    for p in parts:
        if isinstance(p, str): p = p.encode()
        o += b'$%d\r\n%s\r\n' % (len(p), p)
    return o
def handle(conn):
    f = conn.makefile('rb')
    while True:
        a = read_array(f)
        if a is None: break
        if a[0].upper() != b'FLINTFAM':
            conn.sendall(b'-ERR expected FLINTFAM\r\n'); continue
        # FLINTFAM <token> <callback> <ns> <original command...> (ADR-0017 D2:
        # ns is a per-tenant index-selection label, not a credential).
        token, callback, ns, orig = a[1], a[2].decode(), a[3], a[4:]
        if len(orig) >= 3 and orig[0].upper() == b'VEC.SET':
            host, port = callback.split(':')
            ch = socket.create_connection((host, int(port)), timeout=3); cf = ch.makefile('rb')
            ch.sendall(cmd(b'PROXYCHAN', token)); cf.readline()             # +OK
            ch.sendall(cmd(b'SET', b'coproc:' + orig[1], orig[2])); cf.readline()  # +OK
            ch.close()
            conn.sendall(b'+FAMOK\r\n')
        else:
            conn.sendall(b'-ERR unknown family command\r\n')
srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", LISTEN)); srv.listen(8)
while True:
    c, _ = srv.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
PY
python3 "$D/coproc.py" 6686 &
COPROC_PID=$!
sleep 0.5

$B --port 6684 --engine mem 2>"$D/node.log" &
fleet_wait_listen 6684
fleet_wait_ping 6684
$PX --port 6685 --pairs "127.0.0.1:6684" --tenants "tokA=nsA,tokB=nsB" \
    --families "VEC.=127.0.0.1:6686" --edge-advertise "127.0.0.1:6685" 2>"$D/proxy.log" &
fleet_wait_listen 6685
for _ in $(seq 1 100); do
  case "$(valkey-cli -p 6685 PING 2>&1)" in *NOAUTH*|PONG) break ;; esac
  sleep 0.1
done

A="valkey-cli -p 6685 -a tokA --no-auth-warning"
BEE="valkey-cli -p 6685 -a tokB --no-auth-warning"

echo "== a family command REACHES the co-processor and its reply relays back"
R=$($A VEC.SET k hello 2>&1)
echo "$R" | grep -q "FAMOK" \
  || { echo "FAIL: VEC.SET did not reach the co-processor (got: $R)"; tail -5 "$D/proxy.log"; exit 1; }
echo "  VEC.SET -> $R"

echo "== the co-processor's storage landed in nsA — and ONLY nsA (scoping, end to end)"
[ "$($A GET coproc:k)" = "hello" ] || { echo "FAIL: the co-processor's write is not visible to nsA"; exit 1; }
[ -z "$($BEE GET coproc:k)" ]      || { echo "FAIL: the co-processor's write leaked into nsB"; exit 1; }
echo "  nsA sees coproc:k=hello; nsB does not"

echo "== a co-processor's OWN -ERR relays as-is (NOT turned into COPROCUNAVAIL)"
E=$($A VEC.BOGUS x 2>&1)
echo "$E" | grep -qi "COPROCUNAVAIL" \
  && { echo "FAIL: a co-processor -ERR was turned into COPROCUNAVAIL ($E)"; exit 1; }
echo "$E" | grep -qi "unknown family command" \
  || { echo "FAIL: the co-processor's own -ERR was not relayed ($E)"; exit 1; }
echo "  VEC.BOGUS -> $E"

echo "== a DOWN co-processor answers -COPROCUNAVAIL (transport failure, distinct)"
kill -9 "$COPROC_PID" 2>/dev/null; COPROC_PID=""; sleep 0.4
U=$($A VEC.SET k2 v2 2>&1)
echo "$U" | grep -qi "COPROCUNAVAIL" \
  || { echo "FAIL: a down co-processor did not answer COPROCUNAVAIL ($U)"; exit 1; }
echo "  co-processor down -> $U"

echo "PASS: a registered family command reaches the co-processor, its storage is"
echo "      scoped to the granted namespace end to end, its own -ERR relays"
echo "      as-is, and a down co-processor is -COPROCUNAVAIL."
