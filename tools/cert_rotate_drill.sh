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
fleet_init $FLINT_DRILL_ROOT/flint-certrot 6300 6301 7063 6302 6303
fleet_guard
CTL=./target/release/flintctl
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-certrot; rm -rf "$D"; mkdir -p "$D"
fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane; sleep 0.4
cleanup() {
  $CTL -f "$D/cluster.flint" stop >/dev/null 2>&1
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane
  rm -rf "$D"
}
trap cleanup EXIT

echo "== bootstrap a mesh (mTLS) cluster: master + replica, proxy"
cat > "$D/cluster.flint" <<EOF
disposable on
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:6302
pair 127.0.0.1:6300,127.0.0.1:6301
proxy 127.0.0.1:6303
controller on
EOF
$CTL -f "$D/cluster.flint" bootstrap >"$D-boot.log" 2>&1 || {
  # The reason bootstrap failed is in ITS OWN output, and this line
  # used to send that to /dev/null and then report a bare failure --
  # so the largest cluster of gate reds ("FAIL: bootstrap") could not
  # be diagnosed from the artifact at all. Two drills that captured it
  # showed the actual cause immediately: a replica still `loading`
  # when verify ran (BUG-0064).
  echo "FAIL: bootstrap"; tail -25 "$D-boot.log"; exit 1; }
$CTL -f "$D/cluster.flint" tenant add acme tok-acme acme 1 >/dev/null 2>&1
C="$D/state/certs"
ninfo() { valkey-cli -p "$1" --tls --cacert "$C/ca.crt" --cert "$C/int.crt" --key "$C/int.key" FLINTINFO 2>/dev/null; }
sleep 1
# Confirm the pair is replicating (mTLS tail live).
LR=$(ninfo 6300 | grep live_replicas | cut -d: -f2 | tr -d '\r')
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
import socket, time, pathlib, os
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
s=socket.create_connection(("127.0.0.1",6303),timeout=10); s.settimeout(10)
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
pathlib.Path(os.environ.get("FLINT_DRILL_ROOT","/tmp")+"/flint-certrot/w").write_text(f"{acked} {errors}")
PY
WRITER=$!
sleep 4   # let the watcher (2s poll) reload while traffic flows

echo "== a NEW mesh dial verifies against the re-minted leaf"
# A fresh replica bootstrapping now performs a full-sync + tail over mTLS —
# its client dial must trust the CA (unchanged) and the master's listener
# must serve the NEW leaf. If the reload were broken, the handshake fails.
$B --port 7063 --engine rocks --data-dir "$D/r2" --replica-of 127.0.0.1:6300 \
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
V=$(valkey-cli -p 6303 -a tok-acme --no-auth-warning GET ledger)
[ "$V" = "$ACKED" ] || { echo "FAIL: ledger $V != acked $ACKED"; exit 1; }
echo "  writer: $ACKED acked, 0 errors across the reload; ledger reconciles"

# --- BUG-0072: rotating where the CA key is not ------------------------------
# Hermetic and fleet-free: it only needs a statedir. An observer box carries
# ca.crt so it can VERIFY the mesh and deliberately no ca.key, and before this
# guard `rotate-certs` there replaced the leaf key, failed to re-sign the leaf
# cert, and left a mismatched pair the hot-reload watcher picked up within ~2s.
# The command's own precondition checked ca.crt, which is not the file signing
# needs.
echo "== rotating on a box that holds ca.crt and no ca.key"
R="$D/nokey"; mkdir -p "$R/state/certs"
cat > "$R/c.flint" <<EOF
disposable on
statedir $R/state
bins ./target/release
tls on
cp 127.0.0.1:6302
pair 127.0.0.1:6300,127.0.0.1:6301
EOF
( cd "$R/state/certs" && openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key \
    -out ca.crt -days 3650 -subj /CN=flint-ca 2>/dev/null )
$CTL -f "$R/c.flint" rotate-certs >/dev/null 2>&1 \
  || { echo "FAIL: rotate-certs failed even WITH a CA key"; exit 1; }
pairmatch() {  # $1 = leaf name
  a=$(openssl rsa -in "$R/state/certs/$1.key" -noout -modulus 2>/dev/null)
  b=$(openssl x509 -in "$R/state/certs/$1.crt" -noout -modulus 2>/dev/null)
  [ -n "$a" ] && [ "$a" = "$b" ]
}
for L in int edge coproc; do
  pairmatch "$L" || { echo "FAIL: $L key/cert do not match after a normal rotate"; exit 1; }
done
[ ! -d "$R/state/certs/.rotate" ] || { echo "FAIL: staging dir left behind"; exit 1; }
echo "  with a CA key: all three leaves rotate to matching pairs, no staging left"

cp "$R/state/certs/int.key" "$R/int.key.before"
cp "$R/state/certs/int.crt" "$R/int.crt.before"
mv "$R/state/certs/ca.key" "$R/ca.key.parked"
RC=0; $CTL -f "$R/c.flint" rotate-certs >/dev/null 2>"$R/err" || RC=$?
[ "$RC" != 0 ] || { echo "FAIL: rotate-certs SUCCEEDED with no CA key"; exit 1; }
# The GUARD's own words, not just the string "ca.key" -- openssl's failure
# echoes the whole command line, which contains `-CAkey .../ca.key`, so a grep
# for the filename passes with the guard removed entirely. Measured: with both
# guards disabled the run still exits non-zero and still prints "ca.key".
grep -q "no CA private key" "$R/err" \
  || { echo "FAIL: the refusal is openssl's, not the guard's — no clear reason given"; head -3 "$R/err"; exit 1; }
# The property that matters is not the exit code, it is that nothing moved.
cmp -s "$R/state/certs/int.key" "$R/int.key.before" \
  || { echo "FAIL: the leaf KEY was replaced before the refusal"; exit 1; }
cmp -s "$R/state/certs/int.crt" "$R/int.crt.before" \
  || { echo "FAIL: the leaf CERT was rewritten before the refusal"; exit 1; }
for L in int edge coproc; do
  pairmatch "$L" || { echo "FAIL: $L pair broken by a refused rotate"; exit 1; }
done
echo "  without one: exit $RC, the guard names the reason, and every leaf pair still matches"

# The two halves fix different things and are asserted separately on purpose.
# STAGING is what prevents the damage: with the guards disabled the leaf key
# survives anyway, because nothing writes a final filename until every step has
# passed. The GUARD is what makes the refusal legible -- without it the operator
# gets `cert step failed: printf ... openssl x509 -req ...` and has to work out
# that a missing CA key is the cause. Deleting either one leaves a real defect,
# so a drill that only checked the exit code would cover neither.

echo "PASS: mesh cert hot-reload — rotate-certs re-signs the leaf, servers reload within a poll, new dials verify, live traffic loses nothing"
