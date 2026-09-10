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
#
# It closes with the other declared-vs-actual reconciliation of the same
# shape: the inventory's `capacity` against the disk the nodes report.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-pxreg 7351 7352 7691 7733 9998 9999
fleet_guard
D=$FLINT_DRILL_ROOT/flint-pxreg; INV=$D/cluster.flint
CTL=./target/release/flintctl
fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  $CTL -f "$INV" stop 2>/dev/null
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane
  rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:7733
pair 127.0.0.1:7351,127.0.0.1:7352
proxy 127.0.0.1:7691
controller on
EOF

echo "== bootstrap"
$CTL -f "$INV" bootstrap >"$D-boot.log" 2>&1 || {
  # The reason bootstrap failed is in ITS OWN output, and this line
  # used to send that to /dev/null and then report a bare failure --
  # so the largest cluster of gate reds ("FAIL: bootstrap") could not
  # be diagnosed from the artifact at all. Two drills that captured it
  # showed the actual cause immediately: a replica still `loading`
  # when verify ran (BUG-0064).
  echo "FAIL: bootstrap"; tail -25 "$D-boot.log"; exit 1; }

cp_cmd() {
  python3 - "$@" <<'PY'
import socket, ssl, sys, os
d = os.environ.get("FLINT_DRILL_ROOT","/tmp")+"/flint-pxreg/state/certs"
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

# CPSUBSETS is the READ-BACK this pair of writes never had, and the reason it
# exists is the arithmetic below: registered-minus-union is idle capacity.
# Asserted here rather than only in a unit test, because a renderer that
# nothing dispatches to is the failure mode this suite keeps finding.
OUT=$(cp_cmd CPSUBSETS)
echo "  CPSUBSETS after '-': $(echo "$OUT" | tr -d '\r' | tr '\n' ' ')"
echo "$OUT" | tr -d '\r' | grep -qx "trap -" \
  || { echo "FAIL: CPSUBSETS did not render the drained tenant as 'trap -'"; echo "$OUT"; exit 1; }

OUT=$(cp_cmd CPSETSUBSET trap "*")
echo "  '*' -> $OUT"
case "$OUT" in *"1 proxy"*) ;; *) echo "FAIL: '*' did not place on every registered proxy"; exit 1;; esac
sleep 1
R=$(valkey-cli -p 7691 -a tok-trap --no-auth-warning PING 2>&1)
[ "$R" = "PONG" ] || { echo "FAIL: tenant not served after '*' placement: [$R]"; exit 1; }
echo "  after '*' the same token serves: PONG"

echo "== CPSUBSETS names the idle capacity that used to need an audit"
# The whole point of the command (roadmap M4, proxy scale-out): a proxy in
# the registry and in nobody's subset is invisible today.
union() { cp_cmd CPSUBSETS | tr -d '\r' | awk 'NF>1 && $2!="-" {print $2}' | tr ',' '\n' | sort -u; }
idle() { comm -23 <(cp_cmd CPPROXIES | tr ',' '\n' | sort -u) <(union); }
N=$(idle | grep -c . || true)
[ "$N" = "0" ] || { echo "FAIL: after '*' every registered proxy should be in a subset; idle: $(idle | tr '\n' ' ')"; exit 1; }
echo "  after '*': 0 registered proxies are in nobody's subset"

# POSITIVE CONTROL. Without one, "0 idle" above is what a broken union looks
# like too -- an empty union subtracts nothing and every proxy reads as busy,
# or a union of everything and every proxy reads as idle. Register a proxy no
# tenant names and the count must move to exactly it.
cp_cmd CPADDPROXY "127.0.0.1:9998" >/dev/null
IDLE=$(idle | tr '\n' ' ')
echo "  after registering an unused proxy, idle: $IDLE"
case "$IDLE" in *9998*) ;; *) echo "FAIL: an unplaced proxy did not show as idle: [$IDLE]"; exit 1;; esac
[ "$(idle | grep -c .)" = "1" ] || { echo "FAIL: exactly one proxy should be idle, got: [$IDLE]"; exit 1; }
$CTL -f "$INV" retire-proxy "127.0.0.1:9998" >/dev/null 2>&1 || true
[ "$(idle | grep -c . || true)" = "0" ] || { echo "FAIL: retiring the unused proxy did not clear the idle count"; exit 1; }
echo "  retiring it clears the count again"

echo "== and the cluster verifies clean again"
$CTL -f "$INV" verify --probe trap:tok-trap >/dev/null 2>&1 \
  || { echo "FAIL: verify still unhappy after cleanup"; exit 1; }
echo "  verified"

echo "== inventory capacity is checked against the disk the nodes actually have"
# Over-declaring is the failure that matters: the capacity model sizes
# expansion off this number, so a fleet claiming more disk than it has stays
# quiet through the pressure that should have triggered ExpandCluster. The
# playground declared 1.6 TB on a 436 GB disk for weeks — a wrong constant
# looks exactly like a right one.
cp "$INV" "$INV.bak"
echo "capacity 999999999999999" >> "$INV"        # 1 PB, on a laptop
OUT=$($CTL -f "$INV" verify 2>&1)
echo "$OUT" | grep -q "FAIL.*declared capacity" \
  || { echo "FAIL: verify accepted a capacity larger than the disk"; echo "$OUT"; exit 1; }
echo "$OUT" | grep -q "capacity pressure will fire late or never" \
  || { echo "FAIL: verify did not say what the consequence is"; exit 1; }
$CTL -f "$INV" verify >/dev/null 2>&1 && { echo "FAIL: verify exited 0 with an impossible capacity"; exit 1; }
echo "  over-declared capacity is caught and named"
# A capacity that FITS is fine, and holding headroom back is deliberate, not
# an error — so it reports rather than fails.
cp "$INV.bak" "$INV"; echo "capacity 1000000000" >> "$INV"
$CTL -f "$INV" verify 2>&1 | grep -q "ok   declared capacity fits the disk" \
  || { echo "FAIL: an under-declared capacity should pass"; exit 1; }
$CTL -f "$INV" verify >/dev/null 2>&1 || { echo "FAIL: verify should pass with a capacity that fits"; exit 1; }
echo "  a capacity that fits passes, headroom noted not failed"
cp "$INV.bak" "$INV"

echo "PASS: proxy registry — a stray registration is named by verify, retired by retire-proxy, cannot silently strand a tenant, the subset sentinels state their effect, CPSUBSETS reads the placement back so a proxy in nobody's subset is countable, and a declared capacity larger than the disk is refused"
