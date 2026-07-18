#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0006 D1 drill — tokens hashed at rest, verified by digest.
#   - CPADDTENANT stores a DIGEST: the CP state file on disk contains no
#     plaintext token; the snapshot frame pushed to proxies carries none
#   - AUTH with the plaintext still works (the proxy hashes and compares)
#   - a WRONG token still fails (hashing did not weaken verification)
#   - CP restart from the digest-only file preserves auth
#   - MIGRATION: a plaintext-era state file is digested on load, once —
#     auth works and the rewritten file holds no plaintext
#   - rotation drain counters key by digest but accept plaintext lookups
set -u
cd "$(dirname "$0")/.."
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
D=/tmp/flint-tokhash; rm -rf "$D"; mkdir -p "$D"
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
pkill -9 -f flint-controlplane 2>/dev/null; sleep 0.4
cleanup() {
  pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
  pkill -9 -f flint-controlplane 2>/dev/null; rm -rf "$D"
}
trap cleanup EXIT

echo "== cluster; tenant added with plaintext token 'super-secret-token'"
$CP --port 7770 --state "$D/cp" 2>/dev/null &
for i in $(seq 1 30); do [ "$(valkey-cli -p 7770 PING 2>/dev/null)" = "PONG" ] && break; sleep 0.2; done
valkey-cli -p 7770 CPADDPROXY 127.0.0.1:7991 >/dev/null
valkey-cli -p 7770 CPADDPAIR 127.0.0.1:7031 >/dev/null
valkey-cli -p 7770 CPADDTENANT acme super-secret-token acme 1 >/dev/null
$B --port 7031 --engine rocks --data-dir "$D/m" 2>/dev/null &
sleep 0.7
$PX --port 7991 --control-plane 127.0.0.1:7770 --advertise 127.0.0.1:7991 2>/dev/null &
sleep 1.2

echo "== the plaintext exists NOWHERE server-side"
grep -q "super-secret-token" "$D/cp" && { echo "FAIL: plaintext token in the CP state file"; exit 1; }
DIGEST=$(printf 'super-secret-token' | shasum -a 256 | cut -d' ' -f1)
grep -q "$DIGEST" "$D/cp" || { echo "FAIL: digest not in the state file"; exit 1; }
SNAP=$(valkey-cli -p 7770 CPSNAPSHOT 127.0.0.1:7991)
echo "$SNAP" | grep -q "super-secret-token" && { echo "FAIL: plaintext in the snapshot push"; exit 1; }
echo "$SNAP" | grep -q "$DIGEST" || { echo "FAIL: digest not in the snapshot"; exit 1; }
echo "  state file + snapshot frame carry the digest only"

echo "== AUTH with the plaintext works; a wrong token still fails"
V=$(valkey-cli -p 7991 -a super-secret-token --no-auth-warning SET k sealed 2>&1)
[ "$V" = "OK" ] || { echo "FAIL: legitimate AUTH broken: $V"; exit 1; }
W=$(valkey-cli -p 7991 -a wrong-token --no-auth-warning PING 2>&1 | head -1)
echo "$W" | grep -qi "PONG" && { echo "FAIL: wrong token authenticated"; exit 1; }
# The digest itself must NOT authenticate (it is stored, not secret).
DW=$(valkey-cli -p 7991 -a "$DIGEST" --no-auth-warning PING 2>&1 | head -1)
echo "$DW" | grep -qi "PONG" && { echo "FAIL: the stored digest authenticated (hash-shucking)"; exit 1; }
echo "  plaintext AUTHs; wrong token and the RAW DIGEST both rejected"

echo "== CP restart from the digest-only file preserves auth"
pkill -9 -f flint-controlplane; sleep 0.3
$CP --port 7770 --state "$D/cp" 2>/dev/null &
sleep 1.5   # proxy re-subscribes and gets a fresh push
V=$(valkey-cli -p 7991 -a super-secret-token --no-auth-warning GET k)
[ "$V" = "sealed" ] || { echo "FAIL: auth broken after CP restart: $V"; exit 1; }
echo "  restart + repush: auth intact, data intact"

echo "== MIGRATION: a plaintext-era state file digests on first load"
pkill -9 -f flint-controlplane; sleep 0.3
# Hand-write an old-format file with a PLAINTEXT token column.
# A version AHEAD of what the proxy has seen: push suppression would keep
# a same-version snapshot from reaching the already-subscribed proxy.
VER=$(( $(grep '^version' "$D/cp" | awk '{print $2}') + 10 ))
cat > "$D/cp-old" <<EOF
version $VER
proxy 127.0.0.1:7991
pair 127.0.0.1:7031 0-16383
tenant legacy legacy-plaintext-tok legacy 127.0.0.1:7991 - 0 0 0 0 0
EOF
$CP --port 7770 --state "$D/cp-old" 2>/dev/null &
sleep 1.5
V=$(valkey-cli -p 7991 -a legacy-plaintext-tok --no-auth-warning PING 2>&1 | head -1)
echo "$V" | grep -qi "PONG" || { echo "FAIL: legacy tenant cannot auth after migration: $V"; exit 1; }
# Any commit rewrites the file; force one and check the plaintext is gone.
valkey-cli -p 7770 CPTENANTQUOTA legacy 0 0 >/dev/null
grep -q "legacy-plaintext-tok" "$D/cp-old" && { echo "FAIL: plaintext survived the rewrite"; exit 1; }
echo "  plaintext-era token hashed on load; auth works; rewrite holds digest only"

echo "== drain counters: digest-keyed, plaintext lookups still answered"
for i in 1 2 3; do valkey-cli -p 7991 -a legacy-plaintext-tok --no-auth-warning PING >/dev/null; done
C=$(valkey-cli -p 7991 PROXYAUTHCOUNT legacy-plaintext-tok)
[ "$C" -ge 3 ] || { echo "FAIL: plaintext drain lookup ($C)"; exit 1; }
LDIG=$(printf 'legacy-plaintext-tok' | shasum -a 256 | cut -d' ' -f1)
C2=$(valkey-cli -p 7991 PROXYAUTHCOUNT "$LDIG")
[ "$C2" -ge 3 ] || { echo "FAIL: digest drain lookup ($C2)"; exit 1; }
echo "  PROXYAUTHCOUNT answers for plaintext ($C) and digest ($C2) alike"

echo "PASS: tokens hashed at rest — no plaintext server-side, digest cannot auth, restart + migration clean"
