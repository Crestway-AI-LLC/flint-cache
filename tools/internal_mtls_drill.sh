#!/usr/bin/env bash
# Internal-hop mutual-TLS drill (mTLS block, increment 2: proxy↔backend).
#   - a shared internal CA signs one internal cert (SAN flint-internal, both
#     server+client EKU); the server and the proxy both use that triple
#   - flint-server requires mTLS on its data port; the proxy dials it as a
#     mutual-TLS client -> SET/GET flow end to end over the encrypted hop
#   - a PLAINTEXT client on the server's data port is REJECTED (mTLS enforced)
#   - a proxy WITHOUT the internal creds cannot reach the TLS backend (no
#     master discovered -> writes error out, not silently plaintext)
#   - the replication hijack (FLINTSYNC) handshakes over TLS (its duplex is
#     single-threaded now; end-to-end replication parity over TLS is proven
#     in node_tls_drill.sh)
#   - with no --internal-* flags, server+proxy are plaintext, unchanged
set -u
cd "$(dirname "$0")/.."
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=/tmp/flint-mtls; rm -rf "$D"; mkdir -p "$D"
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null; sleep 0.4
cleanup() { pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null; rm -rf "$D"; }
trap cleanup EXIT

echo "== mint an internal CA and one internal cert (SAN flint-internal, server+client EKU)"
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$D/ca.key" -out "$D/ca.crt" \
  -days 1 -subj "/CN=flint-internal-ca" -addext "basicConstraints=critical,CA:TRUE" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$D/int.key" -out "$D/int.csr" \
  -subj "/CN=flint-internal" >/dev/null 2>&1
openssl x509 -req -in "$D/int.csr" -CA "$D/ca.crt" -CAkey "$D/ca.key" -CAcreateserial \
  -out "$D/int.crt" -days 1 \
  -extfile <(printf "subjectAltName=DNS:flint-internal\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE") \
  >/dev/null 2>&1
[ -s "$D/int.crt" ] || { echo "FAIL: cert generation"; exit 1; }
echo "  CA + internal cert generated"
INT="--internal-ca $D/ca.crt --internal-cert $D/int.crt --internal-key $D/int.key"

echo "== flint-server with internal mTLS on the data port"
$B --port 6770 --engine rocks --data-dir "$D/data" $INT 2>"$D/srv.log" &
sleep 0.8
grep -q "internal mTLS" "$D/srv.log" || { echo "FAIL: server not in mTLS mode"; cat "$D/srv.log"; exit 1; }
echo "  server up in internal-mTLS mode"

echo "== proxy dials the backend as a mutual-TLS client (frontend stays plaintext)"
$PX --port 7770 --pairs "127.0.0.1:6770" $INT 2>"$D/px.log" &
sleep 1.0
[ "$(valkey-cli -p 7770 SET ik hello)" = "OK" ] || { echo "FAIL: SET through mTLS backend"; cat "$D/px.log"; exit 1; }
[ "$(valkey-cli -p 7770 GET ik)" = "hello" ] || { echo "FAIL: GET through mTLS backend"; exit 1; }
echo "  SET/GET flowed proxy -> backend over mTLS"

echo "== a PLAINTEXT client on the server's data port is REJECTED"
P=$(python3 - <<'PY'
import socket
try:
    with socket.create_connection(("127.0.0.1", 6770), timeout=3) as s:
        s.sendall(b"*1\r\n$4\r\nPING\r\n")
        s.settimeout(3)
        try: print(repr(s.recv(4096)))
        except socket.timeout: print("<no reply>")
except Exception as e:
    print(f"<error: {e}>")
PY
)
echo "$P" | grep -q "PONG" && { echo "FAIL: plaintext client got PONG on mTLS server port ($P)"; exit 1; }
echo "  plaintext rejected at server port: $P"

echo "== a proxy WITHOUT internal creds cannot reach the TLS backend"
# Plaintext proxy dials the TLS server: the FLINTINFO probe fails the TLS
# handshake, so no master is discovered and writes have nowhere to land.
$PX --port 7771 --pairs "127.0.0.1:6770" 2>"$D/px_noc.log" &
sleep 1.0
R=$(valkey-cli -p 7771 SET nc v 2>&1)
[ "$R" = "OK" ] && { echo "FAIL: credential-less proxy wrote to TLS backend ($R)"; exit 1; }
echo "  credential-less proxy could not reach backend: ${R:0:60}"
pkill -9 -f "flint-proxy --port 7771" 2>/dev/null

echo "== replication hijack (FLINTSYNC) WORKS over TLS (single-threaded duplex)"
X=$(python3 - "$D" <<'PY'
import socket, ssl, sys
d = sys.argv[1]
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
ctx.load_cert_chain(certfile=f"{d}/int.crt", keyfile=f"{d}/int.key")  # present client cert (mutual)
with socket.create_connection(("127.0.0.1", 6770), timeout=3) as raw:
    with ctx.wrap_socket(raw, server_hostname="flint-internal") as s:
        s.sendall(b"*2\r\n$9\r\nFLINTSYNC\r\n$1\r\n0\r\n")
        s.settimeout(3)
        try: print(s.recv(4096).decode("latin1"))
        except socket.timeout: print("<no reply>")
PY
)
echo "$X" | grep -q "FLINTSYNC-OK" || { echo "FAIL: FLINTSYNC handshake failed over TLS (got: $X)"; exit 1; }
echo "  FLINTSYNC over TLS handshakes: ${X%%$'\r'*} (full parity in node_tls_drill)"

echo "== no --internal-* flags: server+proxy plaintext, unchanged"
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null; sleep 0.4
$B --port 6771 --engine rocks --data-dir "$D/data2" 2>"$D/srv2.log" &
sleep 0.6
grep -q "plaintext" "$D/srv2.log" || { echo "FAIL: server not plaintext without flags"; exit 1; }
$PX --port 7772 --pairs "127.0.0.1:6771" 2>"$D/px2.log" &
sleep 0.8
[ "$(valkey-cli -p 7772 SET pk world)" = "OK" ] || { echo "FAIL: plaintext path broken"; exit 1; }
[ "$(valkey-cli -p 7772 GET pk)" = "world" ] || { echo "FAIL: plaintext GET"; exit 1; }
echo "  plaintext mode unchanged"

echo "PASS: internal mTLS proxy<->backend — mutual auth enforced, plaintext & credential-less rejected, FLINTSYNC handshakes over TLS, plaintext mode intact"
