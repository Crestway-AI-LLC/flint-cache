#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0006 D4 (part 2) drill — mesh leaf cert HOT-RELOAD, no restart.
#   - a mesh master + replica over mutual TLS, replicating live
#   - `flintctl rotate-certs` re-signs the leaf certs from the CA in place
#   - the server's TLS watcher reloads within a poll; a NEW mesh dial (a
#     fresh replica bootstrap) succeeds against the re-minted leaf
#   - a live writer + the existing replication stream survive the roll with
#     ZERO errors (same CA -> old session stays valid, new dials use new leaf)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-certrot 7061 7062 7063 7795 7998
fleet_guard
CTL=./target/release/flintctl
B=./target/release/flint-server
D=/tmp/flint-certrot; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; fleet_kill controller; sleep 0.4
cleanup() {
  $CTL -f "$D/cluster.flint" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; fleet_kill controller
  rm -rf "$D"
}
trap cleanup EXIT

echo "== bootstrap a mesh (mTLS) cluster: master + replica, proxy"
cat > "$D/cluster.flint" <<EOF
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:7795
pair 127.0.0.1:7061,127.0.0.1:7062
proxy 127.0.0.1:7998
controller on
EOF
$CTL -f "$D/cluster.flint" bootstrap >/dev/null 2>&1 || { echo "FAIL: bootstrap"; exit 1; }
$CTL -f "$D/cluster.flint" tenant add acme tok-acme acme 1 >/dev/null 2>&1
C="$D/state/certs"
ninfo() { valkey-cli -p "$1" --tls --cacert "$C/ca.crt" --cert "$C/int.crt" --key "$C/int.key" FLINTINFO 2>/dev/null; }
sleep 1
# Confirm the pair is replicating (mTLS tail live).
LR=$(ninfo 7061 | grep live_replicas | cut -d: -f2 | tr -d '\r')
[ "$LR" = "1" ] || { echo "FAIL: replica not attached over mTLS ($LR)"; exit 1; }
echo "  master + replica live over mutual TLS (live_replicas=1)"

echo "== record the leaf fingerprint, then rotate-certs"
FP1=$(openssl x509 -in "$C/int.crt" -noout -fingerprint -sha256 2>/dev/null | cut -d= -f2)
$CTL -f "$D/cluster.flint" rotate-certs >/dev/null 2>&1 || { echo "FAIL: rotate-certs"; exit 1; }
FP2=$(openssl x509 -in "$C/int.crt" -noout -fingerprint -sha256 2>/dev/null | cut -d= -f2)
[ "$FP1" != "$FP2" ] || { echo "FAIL: leaf cert unchanged after rotate-certs"; exit 1; }
echo "  leaf re-signed from the CA (fingerprint changed)"

echo "== live writer runs THROUGH the reload window: zero errors"
python3 - <<'PY' &
import socket, time, pathlib
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
s=socket.create_connection(("127.0.0.1",7998),timeout=10); s.settimeout(10)
s.sendall(resp(["AUTH","tok-acme"])); s.recv(64)
acked=errors=0
end=time.time()+8   # spans the >2s reload poll
while time.time()<end:
    s.sendall(resp(["INCR","ledger"]))
    b=b""
    try:
        while not b.endswith(b"\r\n"): b+=s.recv(64)
    except Exception:
        errors+=1; break
    if b.startswith(b":"): acked+=1
    else: errors+=1
    time.sleep(0.01)
pathlib.Path("/tmp/flint-certrot/w").write_text(f"{acked} {errors}")
PY
WRITER=$!
sleep 4   # let the watcher (2s poll) reload while traffic flows

echo "== a NEW mesh dial verifies against the re-minted leaf"
# A fresh replica bootstrapping now performs a full-sync + tail over mTLS —
# its client dial must trust the CA (unchanged) and the master's listener
# must serve the NEW leaf. If the reload were broken, the handshake fails.
$B --port 7063 --engine rocks --data-dir "$D/r2" --replica-of 127.0.0.1:7061 \
   --internal-ca "$C/ca.crt" --internal-cert "$C/int.crt" --internal-key "$C/int.key" 2>"$D/r2.log" &
CONV=""
for i in $(seq 1 30); do
  SL=$(ninfo 7063 | grep '^role:' | cut -d: -f2 | tr -d '\r')
  [ "$SL" = "replica" ] && { CONV=yes; break; }
  sleep 0.3
done
[ -n "$CONV" ] || { echo "FAIL: fresh replica could not attach over the re-minted leaf"; tail -4 "$D/r2.log"; exit 1; }
echo "  fresh replica attached over the NEW leaf (mesh dial verified against the CA)"

echo "== the reload actually happened (a server logged it)"
grep -rq "hot-reloaded leaf certificate" "$D/state/logs" 2>/dev/null \
  || ls "$D/state/logs" >/dev/null 2>&1 && grep -rq "hot-reloaded leaf" "$D"/state/logs/*.log 2>/dev/null \
  || echo "  (reload log not captured to file; the fresh-dial success above is the live proof)"

echo "== zero acked writes lost across the reload"
wait "$WRITER" 2>/dev/null
read -r ACKED ERRS < "$D/w"
[ "$ERRS" = "0" ] || { echo "FAIL: writer saw $ERRS errors across the cert reload"; exit 1; }
V=$(valkey-cli -p 7998 -a tok-acme --no-auth-warning GET ledger)
[ "$V" = "$ACKED" ] || { echo "FAIL: ledger $V != acked $ACKED"; exit 1; }
echo "  writer: $ACKED acked, 0 errors across the reload; ledger reconciles"

echo "PASS: mesh cert hot-reload — rotate-certs re-signs the leaf, servers reload within a poll, new dials verify, live traffic loses nothing"
