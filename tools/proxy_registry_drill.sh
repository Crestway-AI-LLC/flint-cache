#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The CP's proxy registry is append-only, and tenant placement shuffle-shards
# across it. So a fleet whose proxy identity ever changed — bind address one
# bootstrap, DNS name the next — carries BOTH rows forever with nothing
# marking which one is live, and a new tenant can be placed on the dead name.
#
# The failure that produces is genuinely nasty: the tenant exists, its token
# digest in the CP matches the token byte for byte, and the edge still answers
# -WRONGPASS. Nothing logs a reason. Diagnosing it on the playground took
# comparing sha256 of the token against the CP state file by hand.
#
# This drill reproduces that end to end, then proves the three things that
# make it survivable: verify NAMES the stray, retire-proxy removes it, and the
# subset sentinels say what they did.
set -u
cd "$(dirname "$0")/.."
D=/tmp/flint-pxreg; INV=$D/cluster.flint
CTL=./target/release/flintctl
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
pkill -9 -f flint-controlplane 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null
sleep 0.4
cleanup() {
  $CTL -f "$INV" stop 2>/dev/null
  pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
  pkill -9 -f flint-controlplane 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null
  rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks

cat > "$INV" <<EOF
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:7733
pair 127.0.0.1:7351,127.0.0.1:7352
proxy 127.0.0.1:7691
controller on
EOF

echo "== bootstrap"
$CTL -f "$INV" bootstrap >/dev/null 2>&1 || { echo "FAIL: bootstrap"; exit 1; }

cp_cmd() {
  python3 - "$@" <<'PY'
import socket, ssl, sys
d = "/tmp/flint-pxreg/state/certs"
c = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); c.load_verify_locations(f"{d}/ca.crt")
c.load_cert_chain(f"{d}/int.crt", f"{d}/int.key"); c.check_hostname = False
s = c.wrap_socket(socket.create_connection(("127.0.0.1", 7733), timeout=5),
                  server_hostname="flint-internal")
out = b"*%d\r\n" % len(sys.argv[1:])
for a in sys.argv[1:]:
    out += b"$%d\r\n%s\r\n" % (len(a.encode()), a.encode())
s.sendall(out)
r = s.recv(16384).decode(errors="replace")
print(r.split("\r\n")[1] if r[:1] == "$" else r.strip())
PY
}

echo "== a stale registration appears (what a re-bootstrap under a new identity leaves)"
cp_cmd CPADDPROXY "127.0.0.1:9999" >/dev/null
REG=$(cp_cmd CPPROXIES)
echo "  registry now: $REG"
case "$REG" in *9999*) ;; *) echo "FAIL: stale row not registered"; exit 1;; esac

echo "== verify NAMES it (this is the whole point: the failure is silent otherwise)"
OUT=$($CTL -f "$INV" verify 2>&1)
echo "$OUT" | grep -q "FAIL.*stray" || { echo "FAIL: verify did not flag the stray registration"; echo "$OUT"; exit 1; }
echo "$OUT" | grep -q "9999" || { echo "FAIL: verify did not name the offending address"; exit 1; }
echo "$OUT" | grep -q "retire-proxy" || { echo "FAIL: verify did not say how to fix it"; exit 1; }
$CTL -f "$INV" verify >/dev/null 2>&1 && { echo "FAIL: verify exited 0 with a stray registration"; exit 1; }
echo "  verify fails, names 127.0.0.1:9999, and points at retire-proxy"

echo "== the trap itself: a tenant placed on the dead name gets -WRONGPASS with a GOOD token"
cp_cmd CPADDTENANT trap tok-trap trap 1 >/dev/null
cp_cmd CPSETSUBSET trap "127.0.0.1:9999" >/dev/null
sleep 1
R=$(valkey-cli -p 7691 -a tok-trap --no-auth-warning PING 2>&1)
case "$R" in
  *WRONGPASS*|*NOAUTH*) echo "  reproduced: correct token, edge says [$R]" ;;
  *) echo "FAIL: expected the placement trap, got [$R]"; exit 1 ;;
esac

echo "== retire-proxy removes the row AND drops it from every tenant subset"
$CTL -f "$INV" retire-proxy "127.0.0.1:9999" 2>&1 | sed 's/^/  /'
REG=$(cp_cmd CPPROXIES)
case "$REG" in *9999*) echo "FAIL: registry still holds the stray: $REG"; exit 1;; esac
echo "  registry now: $REG"

echo "== refusing to retire a LIVE proxy is part of the contract"
$CTL -f "$INV" retire-proxy "127.0.0.1:7691" >/dev/null 2>&1 \
  && { echo "FAIL: retired a proxy the inventory declares"; exit 1; }
echo "  declared proxies cannot be retired by accident"

echo "== the subset sentinels say what they did"
# `-` reads like "all" and means NONE. That misreading is what put a tenant
# nowhere on the playground, so the reply has to be unambiguous.
OUT=$(cp_cmd CPSETSUBSET trap "-")
echo "  '-' -> $OUT"
case "$OUT" in *DRAINED*) ;; *) echo "FAIL: '-' did not warn that it serves nowhere"; exit 1;; esac
OUT=$(cp_cmd CPSETSUBSET trap "*")
echo "  '*' -> $OUT"
case "$OUT" in *"1 proxy"*) ;; *) echo "FAIL: '*' did not place on every registered proxy"; exit 1;; esac
sleep 1
R=$(valkey-cli -p 7691 -a tok-trap --no-auth-warning PING 2>&1)
[ "$R" = "PONG" ] || { echo "FAIL: tenant not served after '*' placement: [$R]"; exit 1; }
echo "  after '*' the same token serves: PONG"

echo "== and the cluster verifies clean again"
$CTL -f "$INV" verify --probe trap:tok-trap >/dev/null 2>&1 \
  || { echo "FAIL: verify still unhappy after cleanup"; exit 1; }
echo "  verified"

echo "PASS: proxy registry — a stray registration is named by verify, retired by retire-proxy, cannot silently strand a tenant, and the subset sentinels state their effect"
