#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Proxy TLS-termination drill (mTLS block, increment 1: the client-facing hop).
#   - the proxy accepts TLS on its client port and terminates it; the existing
#     RESP path runs over the encrypted stream (PING/SET/GET all work)
#   - a PLAINTEXT client hitting the TLS port is REJECTED (handshake fails,
#     no RESP is ever processed) — TLS is enforced, not optional
#   - the same proxy binary with no --tls-* flags still serves plaintext
#     byte-identically (no silent downgrade, no behavior change)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-tls 6760 7760 7761
fleet_guard
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=/tmp/flint-tls; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() { fleet_kill server; fleet_kill proxy; rm -rf "$D"; }
trap cleanup EXIT

# One backend; the proxy runs open-mode (no tenants) with a single static pair.
$B --port 6760 --engine rocks --data-dir "$D/data" 2>/dev/null &
sleep 0.6

echo "== generate a self-signed server cert (dev only)"
# -addext (SAN) forces an X.509 v3 cert — rustls rejects v1.
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$D/key.pem" -out "$D/cert.pem" -days 1 \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1
[ -s "$D/cert.pem" ] && [ -s "$D/key.pem" ] || { echo "FAIL: cert generation"; exit 1; }
echo "  cert + key generated"

# RESP helper over TLS via python3 (fully bounded — openssl s_client busy-loops
# on stdin EOF). Sends one frame, reads the reply with a hard timeout, prints
# it. Self-signed cert: verification disabled (we test transport, not PKI).
tls_send() {  # $1 = raw RESP frame (python bytes-escape form)
  python3 - "$1" <<'PY'
import socket, ssl, sys
frame = sys.argv[1].encode().decode('unicode_escape').encode('latin1')
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
with socket.create_connection(("127.0.0.1", 7760), timeout=3) as raw:
    with ctx.wrap_socket(raw, server_hostname="localhost") as s:
        s.sendall(frame)
        s.settimeout(3)
        try:
            sys.stdout.buffer.write(s.recv(4096))
        except socket.timeout:
            pass
PY
}

echo "== proxy with TLS termination"
$PX --port 7760 --pairs "127.0.0.1:6760" \
    --tls-cert "$D/cert.pem" --tls-key "$D/key.pem" 2>"$D/px.log" &
sleep 1.0
grep -q "TLS" "$D/px.log" || { echo "FAIL: proxy did not report TLS mode"; cat "$D/px.log"; exit 1; }
echo "  proxy up in TLS mode"

echo "== RESP over TLS: PING -> PONG"
R=$(tls_send '*1\r\n$4\r\nPING\r\n')
echo "$R" | grep -q "PONG" || { echo "FAIL: no PONG over TLS (got: $R)"; exit 1; }
echo "  PONG"

echo "== RESP over TLS: SET then GET round-trips through the encrypted stream"
tls_send '*3\r\n$3\r\nSET\r\n$2\r\nhk\r\n$5\r\nhello\r\n' >/dev/null
R=$(tls_send '*2\r\n$3\r\nGET\r\n$2\r\nhk\r\n')
echo "$R" | grep -q "hello" || { echo "FAIL: GET over TLS (got: $R)"; exit 1; }
echo "  SET/GET over TLS OK"

echo "== a PLAINTEXT client hitting the TLS port is REJECTED"
# A raw plaintext RESP frame against a TLS listener: the server waits for a
# ClientHello, never a RESP reply. Bounded read; must NOT contain PONG.
P=$(python3 - <<'PY'
import socket
try:
    with socket.create_connection(("127.0.0.1", 7760), timeout=3) as s:
        s.sendall(b"*1\r\n$4\r\nPING\r\n")
        s.settimeout(3)
        try:
            print(repr(s.recv(4096)))
        except socket.timeout:
            print("<no reply: timed out>")
except Exception as e:
    print(f"<connect/io error: {e}>")
PY
)
echo "$P" | grep -q "PONG" && { echo "FAIL: plaintext client got PONG on TLS port (got: $P)"; exit 1; }
echo "  plaintext rejected (no PONG): $P"

echo "== same binary, NO --tls-* flags: plaintext still works (no downgrade path)"
fleet_kill proxy; sleep 0.4
$PX --port 7761 --pairs "127.0.0.1:6760" 2>"$D/px2.log" &
sleep 0.8
grep -q "plaintext" "$D/px2.log" || { echo "FAIL: proxy did not report plaintext mode"; cat "$D/px2.log"; exit 1; }
[ "$(valkey-cli -p 7761 PING)" = "PONG" ] || { echo "FAIL: plaintext mode broken"; exit 1; }
[ "$(valkey-cli -p 7761 SET pk world; valkey-cli -p 7761 GET pk)" = "OK
world" ] || { echo "FAIL: plaintext SET/GET"; exit 1; }
echo "  plaintext mode unchanged"

echo "PASS: proxy TLS termination — RESP over TLS, plaintext rejected on TLS port, plaintext mode intact"
