#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0010 step 1: the co-processor credential cannot dial a node.
#
# A co-processor holds a SERVER-only leaf (serverAuth, no clientAuth): the proxy
# can dial IT, but it cannot dial the mesh as a member, because a node's
# mutual-TLS verifier refuses a serverAuth-only leaf as a CLIENT certificate.
# That absence is the entire isolation argument of D2 — and three drafts of the
# ADR got it wrong on paper (a distinct CA branch, a namespace it "couldn't
# express"), each a policy dressed as a capability. This proves the capability
# on the wire, against the leaf the PRODUCT mint actually produces (flintctl
# rotate-certs), not a hand-rolled one.
#
# Two negatives must both fail to reach a node — the co-processor leaf, and
# NOTHING — and one positive control (the mesh leaf) must succeed, or the
# negatives would be passing for the wrong reason (a broken port, say).
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-coproc 6693 6694 6695
fleet_guard
B=./target/release/flint-server
CTL=./target/release/flintctl
D=$FLINT_DRILL_ROOT/flint-coproc; INV=$FLINT_DRILL_ROOT/flint-coproc.flint
rm -rf "$D"; mkdir -p "$D/certs"
fleet_kill server; sleep 0.3
cleanup() { fleet_kill server; rm -rf "$D" "$INV"; }
trap cleanup EXIT

# --features flint-server/rocks even though the node runs --engine mem: the
# build output is SHARED, and omitting it downgrades ./target/release/flint-server
# to a mem-only binary that breaks every later drill wanting rocks. gates.sh lints
# for this.
cargo build --release -q -p flint-server -p flint-ctl --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

# --- mint the REAL cert set through flintctl (the path the product runs) -------
# Place a CA by hand, then let `rotate-certs` mint int + edge + coproc leaves AND
# assert their EKUs. If the coproc leaf came out with clientAuth, the Rust assert
# in resign_leaves aborts rotate-certs HERE, at the mint, before any handshake —
# which is the point of asserting the produced cert rather than the recipe.
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$D/certs/ca.key" -out "$D/certs/ca.crt" \
  -days 1 -subj "/CN=flint-internal-ca" -addext "basicConstraints=critical,CA:TRUE" >/dev/null 2>&1
cat > "$INV" <<EOF
disposable on
statedir $D
bins ./target/release
tls on
cp 127.0.0.1:6695
pair 127.0.0.1:6693,127.0.0.1:6694
EOF

echo "== flintctl rotate-certs mints the leaves and asserts their EKUs"
if ! $CTL -f "$INV" rotate-certs >"$D/mint.log" 2>&1; then
  echo "FAIL: rotate-certs aborted — the mint-time EKU assert fired:"
  sed 's/^/    /' "$D/mint.log"
  exit 1
fi
for f in ca.crt int.crt int.key coproc.crt coproc.key; do
  [ -s "$D/certs/$f" ] || { echo "FAIL: rotate-certs did not produce $f"; exit 1; }
done
echo "  minted int + edge + coproc leaves"

# --- independent cross-check of the leaf the Rust assert just passed -----------
# The mint asserted this in Rust (x509-parser); re-check from the shell with a
# different parser (openssl -text, portable across LibreSSL and OpenSSL) so a
# bug in ONE reader cannot let a bad leaf through both. serverAuth yes,
# clientAuth no.
CT=$(openssl x509 -in "$D/certs/coproc.crt" -text -noout)
echo "$CT" | grep -A2 "Extended Key Usage" | grep -q "Server Authentication" \
  || { echo "FAIL: coproc leaf is missing serverAuth"; exit 1; }
if echo "$CT" | grep -A2 "Extended Key Usage" | grep -q "Client Authentication"; then
  echo "FAIL: coproc leaf carries clientAuth — it must be serverAuth-ONLY"
  exit 1
fi
echo "  coproc leaf is serverAuth-only (openssl agrees with the mint assert)"

# --- the node whose mesh port is the dial target ------------------------------
$B --port 6693 --engine mem \
  --internal-ca "$D/certs/ca.crt" --internal-cert "$D/certs/int.crt" --internal-key "$D/certs/int.key" \
  2>"$D/srv.log" &
fleet_wait_listen 6693
fleet_wait_log "$D/srv.log" "internal mTLS"   # not a sleep: wait for the line the grep below asserts
grep -q "internal mTLS" "$D/srv.log" || { echo "FAIL: server not in mTLS mode"; sed 's/^/    /' "$D/srv.log"; exit 1; }
echo "== node up on 6693 in internal-mTLS mode"

# dial <cert-or-empty> <key-or-empty> — attempt one mTLS mesh dial + PING.
# Prints "REPLY ..." on a completed handshake that answered, "HANDSHAKE_REFUSED
# ..." when the node rejected the client cert (or its absence), or "ERROR ...".
dial() {
  python3 - "$1" "$2" <<'PY'
import socket, ssl, sys
cert, key = sys.argv[1], sys.argv[2]
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE            # we test the SERVER's verdict on US
if cert:
    ctx.load_cert_chain(certfile=cert, keyfile=key)
try:
    with socket.create_connection(("127.0.0.1", 6693), timeout=3) as raw:
        with ctx.wrap_socket(raw, server_hostname="flint-internal") as s:
            s.sendall(b"*1\r\n$4\r\nPING\r\n")
            s.settimeout(3)
            try:
                print("REPLY " + repr(s.recv(256)))
            except socket.timeout:
                print("NOREPLY")
except ssl.SSLError as e:
    print("HANDSHAKE_REFUSED " + str(e).splitlines()[0][:70])
except Exception as e:
    print("ERROR " + str(e)[:70])
PY
}

echo "== CONTROL: the MESH leaf (serverAuth+clientAuth) is accepted and answers"
C=$(dial "$D/certs/int.crt" "$D/certs/int.key")
echo "$C" | grep -q "REPLY.*PONG" \
  || { echo "FAIL (control): the mesh leaf could not dial the node — the negatives below would prove nothing ($C)"; exit 1; }
echo "  mesh leaf dialed the node: $C"

echo "== the CO-PROCESSOR leaf (serverAuth ONLY) is REFUSED as a client cert"
X=$(dial "$D/certs/coproc.crt" "$D/certs/coproc.key")
echo "$X" | grep -q "REPLY.*PONG" \
  && { echo "FAIL: the co-processor leaf DIALED a node — the D2 isolation guarantee is broken ($X)"; exit 1; }
echo "  refused: $X"

echo "== NO client cert is REFUSED"
N=$(dial "" "")
echo "$N" | grep -q "REPLY.*PONG" \
  && { echo "FAIL: a credential-less client dialed a node ($N)"; exit 1; }
echo "  refused: $N"

echo "PASS: the co-processor credential — and no credential — cannot dial a node;"
echo "      the mesh leaf can. The serverAuth-only leaf is enforced on the wire,"
echo "      and the mint refuses to produce one that is not (asserted in Rust,"
echo "      cross-checked by openssl)."
