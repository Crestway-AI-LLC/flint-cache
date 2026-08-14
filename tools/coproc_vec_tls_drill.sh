#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0010 D5 / ADR-0017: the REAL flint-vec co-processor under mesh mTLS.
#
# coproc_vec_drill runs flint-vec plaintext; coproc_cred proves the coproc LEAF
# is serverAuth-only on the wire. This joins them: the actual flint-vec binary,
# configured with --internal-* (presenting coproc.crt, the serverAuth-only leaf),
# behind a proxy that dials it over backend mTLS. Two things must hold:
#   - POSITIVE: VEC.* works end to end over the encrypted hop. A successful
#     VEC.SEARCH is itself the proof the mutual handshake completed — if the
#     co-processor did not accept mTLS, the proxy's backend_tls dial would fail
#     and every VEC.* would answer -COPROCUNAVAIL.
#   - NEGATIVE: a PLAINTEXT dialer to the co-processor's FLINTFAM port is refused
#     at the transport (the TLS server rejects the non-TLS bytes), so the
#     encrypted listener is not silently also accepting cleartext.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-vectls 6702 6703 6704
fleet_guard
B=./target/release/flint-server
PX=./target/release/flint-proxy
VEC=./target/release/flint-vec
CTL=./target/release/flintctl
D=/tmp/flint-vectls; INV=/tmp/flint-vectls.flint
rm -rf "$D" "$INV"; mkdir -p "$D/certs"
COPROC_PID=""
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() {
  [ -n "$COPROC_PID" ] && kill -9 "$COPROC_PID" 2>/dev/null
  fleet_kill server; fleet_kill proxy; rm -rf "$D" "$INV"
}
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy -p flint-vec -p flint-ctl --features flint-server/rocks

# --- mint the REAL cert set through flintctl (same path the product runs) ------
# A CA by hand, then rotate-certs mints int + edge + coproc leaves AND asserts
# their EKUs (the coproc leaf is serverAuth-only or the mint aborts).
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$D/certs/ca.key" -out "$D/certs/ca.crt" \
  -days 1 -subj "/CN=flint-internal-ca" -addext "basicConstraints=critical,CA:TRUE" >/dev/null 2>&1
cat > "$INV" <<EOF
disposable on
statedir $D
bins ./target/release
tls on
cp 127.0.0.1:6702
pair 127.0.0.1:6703,127.0.0.1:6704
EOF
$CTL -f "$INV" rotate-certs >"$D/mint.log" 2>&1 \
  || { echo "FAIL: rotate-certs aborted (mint-time EKU assert?):"; sed 's/^/    /' "$D/mint.log"; exit 1; }
for f in ca.crt int.crt int.key coproc.crt coproc.key; do
  [ -s "$D/certs/$f" ] || { echo "FAIL: rotate-certs did not produce $f"; exit 1; }
done
CA="$D/certs/ca.crt"
echo "== minted ca + mesh(int) + coproc leaves"

# --- the mTLS fleet: node + proxy on the mesh leaf, flint-vec on the coproc leaf
$B --port 6702 --engine mem \
   --internal-ca "$CA" --internal-cert "$D/certs/int.crt" --internal-key "$D/certs/int.key" \
   2>"$D/node.log" &
fleet_wait_listen 6702
sleep 0.6   # not fleet_wait_ping: a plaintext PING at a TLS port never answers
grep -q "internal mTLS" "$D/node.log" || { echo "FAIL: node not in mTLS mode"; sed 's/^/    /' "$D/node.log"; exit 1; }

# flint-vec presents the serverAuth-only coproc leaf and verifies the proxy.
$VEC --port 6704 \
   --internal-ca "$CA" --internal-cert "$D/certs/coproc.crt" --internal-key "$D/certs/coproc.key" \
   2>"$D/vec.log" &
COPROC_PID=$!
fleet_wait_listen 6704
grep -q "mesh mTLS" "$D/vec.log" || { echo "FAIL: flint-vec not in mesh-mTLS mode"; sed 's/^/    /' "$D/vec.log"; exit 1; }

# The proxy dials the co-processor over backend mTLS; its edge stays plaintext
# (tenants authenticate with a token), so the PROXYCHAN dial-back is plaintext.
$PX --port 6703 --pairs "127.0.0.1:6702" --tenants "tok=ns" \
    --families "VEC.=127.0.0.1:6704" --edge-advertise "127.0.0.1:6703" \
    --internal-ca "$CA" --internal-cert "$D/certs/int.crt" --internal-key "$D/certs/int.key" \
    2>"$D/px.log" &
fleet_wait_listen 6703
for _ in $(seq 1 100); do case "$(valkey-cli -p 6703 PING 2>&1)" in *NOAUTH*|PONG) break;; esac; sleep 0.1; done
echo "== fleet up: node(mTLS) + flint-vec(mesh mTLS, coproc leaf) + proxy(backend mTLS)"

A="valkey-cli -p 6703 -a tok --no-auth-warning"
vexec() {
  local i out
  for i in $(seq 1 50); do
    out="$($A "$@" 2>&1 | tr -d '\r' | tr '\n' ' ')"
    case "$out" in *LOADING*) sleep 0.15 ;; *) echo "$out"; return 0 ;; esac
  done
  echo "$out"; return 1
}

echo "== POSITIVE: VEC.* works end to end over the encrypted proxy<->coproc hop"
[ "$(vexec VEC.CREATE docs DIM 3 METRIC l2)" = "OK " ] || { echo "FAIL: VEC.CREATE over mTLS"; tail -5 "$D/px.log"; exit 1; }
for kv in "a 1,0,0" "b 0,1,0" "c 0,0,1"; do
  set -- $kv
  [ "$(vexec VEC.SET docs "$1" "$2")" = "OK " ] || { echo "FAIL: VEC.SET $1 over mTLS"; exit 1; }
done
S="$(vexec VEC.SEARCH docs 0.9,0.1,0 2)"
case "$S" in *a*b*) : ;; *) echo "FAIL: SEARCH order over mTLS, got: $S"; exit 1 ;; esac
case "$(vexec VEC.GET docs a)" in *"1,0,0"*) : ;; *) echo "FAIL: VEC.GET over mTLS"; exit 1 ;; esac
# The durable rows landed in KV through the (plaintext-edge, mTLS-backend) channel.
[ "$($A DBSIZE 2>&1 | tr -d '\r')" = "4" ] || { echo "FAIL: expected 4 durable keys, got $($A DBSIZE)"; exit 1; }
echo "  VEC.SEARCH -> $S  (mutual handshake completed, or this would be COPROCUNAVAIL)"

echo "== NEGATIVE: a PLAINTEXT dialer to the co-processor's FLINTFAM port is refused"
R=$(python3 - <<'PY'
import socket
try:
    s = socket.create_connection(("127.0.0.1", 6704), timeout=3)
    s.sendall(b"PING\r\n")          # cleartext bytes to a TLS listener
    s.settimeout(3)
    data = s.recv(64)
    # A co-processor speaking plaintext would answer RESP (+ - : $ *). A TLS
    # server rejects the bogus ClientHello: closed (b'') or a TLS alert record.
    print("PLAINTEXT_ACCEPTED " + repr(data[:24]) if data[:1] in (b'+', b'-', b':', b'$', b'*') else "REFUSED")
except Exception as e:
    print("REFUSED " + str(e)[:40])
PY
)
case "$R" in REFUSED*) : ;; *) echo "FAIL: the mTLS co-processor accepted a plaintext dialer ($R)"; exit 1 ;; esac
echo "  plaintext FLINTFAM -> $R"

# The co-processor still serves the encrypted path after the rejected plaintext
# attempt (one bad connection does not wedge the listener).
case "$(vexec VEC.SEARCH docs 0.9,0.1,0 1)" in *a*) : ;; *) echo "FAIL: mTLS path broke after the plaintext attempt"; exit 1 ;; esac

echo "PASS: the real flint-vec serves VEC.* end to end under mesh mTLS (presenting the"
echo "      serverAuth-only coproc leaf, proxy dialing over backend mTLS), and its"
echo "      encrypted FLINTFAM listener refuses a plaintext dialer at the transport."
