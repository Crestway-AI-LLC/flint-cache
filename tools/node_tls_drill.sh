#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Node↔node mTLS drill (mTLS block, increment 3: the data plane's own hops).
#   - a fresh replica bootstraps from an mTLS master: checkpoint full sync
#     (FLINTFULLSYNC) AND the live tail (FLINTSYNC) both over mutual TLS
#   - ACKs flow back over the same TLS connection (the merged single-threaded
#     duplex): the master reports live_replicas:1 — liveness/lag metrics work
#   - writes after the bootstrap reach the replica through the TLS tail
#   - a slot move between two mTLS nodes (FLINTMIGRATEIN -> FLINTMIGRATEOUT
#     source dial + bulk + tail) completes over mutual TLS
# Every check runs over mutual TLS: the data ports accept nothing else.
set -u
cd "$(dirname "$0")/.."
B=./target/release/flint-server
D=/tmp/flint-ntls; rm -rf "$D"; mkdir -p "$D"
pkill -9 -f flint-server 2>/dev/null; sleep 0.4
cleanup() { pkill -9 -f flint-server 2>/dev/null; rm -rf "$D"; }
trap cleanup EXIT

echo "== mint internal CA + cert (SAN flint-internal, server+client EKU)"
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$D/ca.key" -out "$D/ca.crt" \
  -days 1 -subj "/CN=flint-internal-ca" -addext "basicConstraints=critical,CA:TRUE" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$D/int.key" -out "$D/int.csr" \
  -subj "/CN=flint-internal" >/dev/null 2>&1
openssl x509 -req -in "$D/int.csr" -CA "$D/ca.crt" -CAkey "$D/ca.key" -CAcreateserial \
  -out "$D/int.crt" -days 1 \
  -extfile <(printf "subjectAltName=DNS:flint-internal\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE") \
  >/dev/null 2>&1
[ -s "$D/int.crt" ] || { echo "FAIL: cert generation"; exit 1; }
INT="--internal-ca $D/ca.crt --internal-cert $D/int.crt --internal-key $D/int.key"
echo "  minted"

# Mutual-TLS RESP helper: send command(s) on one TLS connection, print the
# raw reply bytes. Args: port, then commands (RESP-encoded here), read once
# with a generous timeout (FLINTMIGRATEIN blocks until the move completes).
resp() {  # $1=port $2=read-timeout-s, rest: command words (one command)
  python3 - "$@" <<'PY'
import socket, ssl, sys
port, tmo, words = int(sys.argv[1]), float(sys.argv[2]), sys.argv[3:]
frame = f"*{len(words)}\r\n".encode() + b"".join(
    f"${len(w)}\r\n{w}\r\n".encode() for w in words)
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
ctx.load_cert_chain(certfile="/tmp/flint-ntls/int.crt", keyfile="/tmp/flint-ntls/int.key")
with socket.create_connection(("127.0.0.1", port), timeout=5) as raw:
    with ctx.wrap_socket(raw, server_hostname="flint-internal") as s:
        s.sendall(frame)
        s.settimeout(tmo)
        try:
            sys.stdout.buffer.write(s.recv(65536))
        except socket.timeout:
            sys.stdout.write("<timeout>")
PY
}

echo "== master (mTLS) up; seed keys over mutual TLS"
$B --port 6790 --engine rocks --data-dir "$D/m" $INT 2>"$D/m.log" &
sleep 0.8
# Seed: 2000 {mover} keys pipelined on one TLS connection.
N=$(python3 - <<'PY'
import socket, ssl
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
ctx.load_cert_chain("/tmp/flint-ntls/int.crt", "/tmp/flint-ntls/int.key")
buf = bytearray()
for i in range(2000):
    k, v = f"{{mover}}:key{i:06d}", f"base-{i:06d}"
    buf += f"*3\r\n$3\r\nSET\r\n${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n".encode()
with socket.create_connection(("127.0.0.1", 6790), timeout=5) as raw:
    with ctx.wrap_socket(raw, server_hostname="flint-internal") as s:
        s.sendall(bytes(buf))
        s.settimeout(5)
        got = b""
        while got.count(b"+OK\r\n") < 2000:
            got += s.recv(65536)
        print(got.count(b"+OK\r\n"))
PY
)
[ "$N" = "2000" ] || { echo "FAIL: seeded $N/2000"; exit 1; }
echo "  2000 keys seeded over mTLS"

echo "== fresh replica bootstraps over mTLS (full sync + tail)"
$B --port 6791 --engine rocks --data-dir "$D/r" --replica-of 127.0.0.1:6790 $INT 2>"$D/r.log" &
sleep 3
R=$(resp 6791 3 GET "{mover}:key000123")
echo "$R" | grep -q "base-000123" || { echo "FAIL: full-sync key missing on replica (got: $R)"; cat "$D/r.log" | tail -5; exit 1; }
echo "  checkpoint full-sync key present on replica"

echo "== ACKs flow over TLS: master sees a LIVE replica"
LIVE=0
for i in $(seq 1 20); do
  resp 6790 3 FLINTINFO | grep -q "live_replicas:1" && { LIVE=1; break; }
  sleep 0.5
done
[ "$LIVE" = "1" ] || { echo "FAIL: master never saw a live replica (ACKs not flowing over TLS)"; exit 1; }
echo "  live_replicas:1 — ACK/liveness works through the TLS duplex"

echo "== post-bootstrap write reaches the replica through the TLS tail"
resp 6790 3 SET "{mover}:tailkey" tail-hello >/dev/null
OK=0
for i in $(seq 1 20); do
  resp 6791 2 GET "{mover}:tailkey" | grep -q "tail-hello" && { OK=1; break; }
  sleep 0.3
done
[ "$OK" = "1" ] || { echo "FAIL: tail write did not reach replica over TLS"; exit 1; }
echo "  tail write replicated over TLS"

echo "== slot move between two mTLS nodes (migrate-in dials the source over mTLS)"
$B --port 6792 --engine rocks --data-dir "$D/d" $INT 2>"$D/d.log" &
sleep 0.8
SLOT=$(python3 - <<'PY'
def crc16(d):
    poly=0x1021; crc=0
    for b in d:
        crc^=b<<8
        for _ in range(8):
            crc=((crc<<1)^poly)&0xffff if crc&0x8000 else (crc<<1)&0xffff
    return crc
print(crc16(b"mover")%16384)
PY
)
M=$(resp 6792 30 FLINTMIGRATEIN "127.0.0.1:6790" "$SLOT")
echo "  result: ${M%%$'\r'*}"
echo "$M" | grep -q "MIGRATEIN-OK" || { echo "FAIL: migration over mTLS failed: $M"; tail -5 "$D/d.log"; exit 1; }
DGET=$(resp 6792 3 GET "{mover}:key000123")
echo "$DGET" | grep -q "base-000123" || { echo "FAIL: migrated key absent on destination (got: $DGET)"; exit 1; }
echo "  slot $SLOT moved over mutual TLS; destination serves the data"

echo "PASS: node<->node over mutual TLS — full sync, live tail, ACK liveness, and slot migration all encrypted"
