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
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-tokhash 7031 6322 7991
fleet_guard
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
D=$FLINT_DRILL_ROOT/flint-tokhash; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; sleep 0.4
cleanup() {
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; rm -rf "$D"
}
trap cleanup EXIT

echo "== cluster; tenant added with plaintext token 'super-secret-token'"
$CP --port 6322 --state "$D/cp" 2>/dev/null &
fleet_wait_ping 6322
fleet_cp 6322 CPADDPROXY 127.0.0.1:7991
fleet_cp 6322 CPADDPAIR 127.0.0.1:7031
fleet_cp 6322 CPADDTENANT acme super-secret-token acme 1
$B --port 7031 --engine rocks --data-dir "$D/m" 2>/dev/null &
fleet_wait_listen 7031
sleep 0.7
$PX --port 7991 --control-plane 127.0.0.1:6322 --advertise 127.0.0.1:7991 2>/dev/null &
fleet_wait_listen 7991
sleep 1.2

echo "== the plaintext exists NOWHERE server-side"
grep -q "super-secret-token" "$D/cp" && { echo "FAIL: plaintext token in the CP state file"; exit 1; }
DIGEST=$(printf 'super-secret-token' | shasum -a 256 | cut -d' ' -f1)
grep -q "$DIGEST" "$D/cp" || { echo "FAIL: digest not in the state file"; exit 1; }
SNAP=$(valkey-cli -p 6322 CPSNAPSHOT 127.0.0.1:7991)
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
fleet_kill controlplane; sleep 0.3
$CP --port 6322 --state "$D/cp" 2>/dev/null &
sleep 1.5   # proxy re-subscribes and gets a fresh push
V=$(valkey-cli -p 7991 -a super-secret-token --no-auth-warning GET k)
[ "$V" = "sealed" ] || { echo "FAIL: auth broken after CP restart: $V"; exit 1; }
echo "  restart + repush: auth intact, data intact"

echo "== MIGRATION: a plaintext-era state file digests on first load"
fleet_kill controlplane; sleep 0.3
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
$CP --port 6322 --state "$D/cp-old" 2>/dev/null &
fleet_wait_listen 6322
sleep 1.5
V=$(valkey-cli -p 7991 -a legacy-plaintext-tok --no-auth-warning PING 2>&1 | head -1)
echo "$V" | grep -qi "PONG" || { echo "FAIL: legacy tenant cannot auth after migration: $V"; exit 1; }
# Any commit rewrites the file; force one and check the plaintext is gone.
valkey-cli -p 6322 CPTENANTQUOTA legacy 0 0 >/dev/null
sha256hex() {
  if command -v sha256sum >/dev/null 2>&1; then printf '%s' "$1" | sha256sum | cut -d' ' -f1
  elif command -v shasum   >/dev/null 2>&1; then printf '%s' "$1" | shasum -a 256 | cut -d' ' -f1
  else python3 -c 'import hashlib,sys;print(hashlib.sha256(sys.argv[1].encode()).hexdigest())' "$1"
  fi
}

# PAIRED POSITIVE CONTROL (ADR-0028 obligation 3).
#
# The absence check below certifies that the plaintext token did not survive the
# rewrite. It certifies that EQUALLY WELL against a cp-old that is empty,
# missing, or truncated -- a matcher that finds nothing agrees with everything.
#
# The first half of this drill already does this correctly: every plaintext
# absence check is paired with a digest PRESENCE check on the same file, so the
# file is proven readable and greppable before its silence is trusted. This one
# was not paired, and it is the security assertion of the two -- "plaintext
# survived the rewrite" is the thing a reader would most want to be true.
LEGACY_DIG=$(sha256hex 'legacy-plaintext-tok')
[ ${#LEGACY_DIG} -eq 64 ] || { echo "FAIL: could not compute a sha256 digest here (got '${LEGACY_DIG}')"; exit 1; }
grep -q "$LEGACY_DIG" "$D/cp-old" || {
  echo "FAIL: the rewritten state file carries no digest for the legacy tenant."
  echo "      The plaintext-absence check below would then pass against a file"
  echo "      it never successfully read (ADR-0028 obligation 3)."
  exit 1; }
grep -q "legacy-plaintext-tok" "$D/cp-old" && { echo "FAIL: plaintext survived the rewrite"; exit 1; }
echo "  plaintext-era token hashed on load; auth works; rewrite holds digest only"

echo "== drain counters: digest-keyed, plaintext lookups still answered"
for i in 1 2 3; do valkey-cli -p 7991 -a legacy-plaintext-tok --no-auth-warning PING >/dev/null; done
C=$(valkey-cli -p 7991 PROXYAUTHCOUNT legacy-plaintext-tok)
[ "$C" -ge 3 ] || { echo "FAIL: plaintext drain lookup ($C)"; exit 1; }
# PORTABLE sha256. `shasum` is a Perl script and is not guaranteed present —
# on the AL2023 gate box it is not, so LDIG came out EMPTY, PROXYAUTHCOUNT ""
# answered 0, and the drill reported "digest drain lookup (0)" as though the
# digest-keyed counter were broken. The plaintext assertion three lines above
# passed, which is the tell: the server was fine and the drill could not
# compute the key it was asking about.
LDIG=$(sha256hex 'legacy-plaintext-tok')
[ ${#LDIG} -eq 64 ] || { echo "FAIL: could not compute a sha256 digest here (got '${LDIG}')"; exit 1; }
C2=$(valkey-cli -p 7991 PROXYAUTHCOUNT "$LDIG")
[ "$C2" -ge 3 ] || { echo "FAIL: digest drain lookup ($C2)"; exit 1; }
echo "  PROXYAUTHCOUNT answers for plaintext ($C) and digest ($C2) alike"

echo "PASS: tokens hashed at rest — no plaintext server-side, digest cannot auth, restart + migration clean"
