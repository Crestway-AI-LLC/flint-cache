#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0010 D1: a co-processor's channel I/O is EXEMPT from the tenant's ops/s
# quota — end to end, under a REAL rate. The mechanism is unit-tested
# (channel_data_is_quota_exempt) and its code path runs in coproc_forward, but
# neither pressures it with an actual quota: static --tenants carries no rate
# (that is a control-plane fact), so this drill is necessarily CP-driven.
#
# The setup: a namespace throttled to 2 ops/s. A single family command
# (VEC.SET) is charged ONCE at admission (D1), and its co-processor then fans
# out 50 channel writes into that same namespace. If channel I/O paid the
# tenant's rate, it would be -THROTTLED after ~2 writes; because it is exempt,
# all 50 land. The control proves the rate is real: the tenant doing 50 writes
# DIRECTLY is throttled on all but a couple.
#
# The claims:
#   - one family command's co-processor completes ALL its channel writes into a
#     2-ops/s namespace (exempt: 50 >> 2)
#   - the SAME namespace throttles its TENANT's direct 50-write burst (the rate
#     is genuinely binding — the positive control that makes the exemption mean
#     something)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-exempt 6672 6673 6674 6675
fleet_guard
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
D=/tmp/flint-exempt; rm -rf "$D"; mkdir -p "$D"
COPROC_PID=""
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; sleep 0.4
cleanup() {
  [ -n "$COPROC_PID" ] && kill -9 "$COPROC_PID" 2>/dev/null
  fleet_kill server; fleet_kill proxy; fleet_kill controlplane; rm -rf "$D"
}
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-controlplane -p flint-proxy --features flint-server/rocks

# The stand-in co-processor: on each FLINTFAM it opens PROXYCHAN back and does
# WRITES=50 channel SETs into the granted namespace, stopping early if any is
# refused (a -THROTTLED would end the loop). It reports "+FAMOK <done>", so
# done==50 is the exemption and done<50 is the quota leaking into channel I/O.
WRITES=50
cat > "$D/coproc.py" <<PY
import socket, sys, threading
LISTEN = int(sys.argv[1]); WRITES = $WRITES
def read_array(f):
    line = f.readline()
    if not line or line[:1] != b'*': return None
    n = int(line[1:]); out = []
    for _ in range(n):
        h = f.readline(); ln = int(h[1:]); d = f.read(ln); f.read(2); out.append(d)
    return out
def cmd(*parts):
    o = b'*%d\r\n' % len(parts)
    for p in parts:
        if isinstance(p, str): p = p.encode()
        o += b'\$%d\r\n%s\r\n' % (len(p), p)
    return o
def handle(conn):
    f = conn.makefile('rb')
    while True:
        a = read_array(f)
        if a is None: break
        if not a or a[0].upper() != b'FLINTFAM':
            conn.sendall(b'-ERR expected FLINTFAM\r\n'); continue
        token, callback = a[1], a[2].decode()
        host, port = callback.split(':')
        ch = socket.create_connection((host, int(port)), timeout=3); cf = ch.makefile('rb')
        ch.sendall(cmd(b'PROXYCHAN', token)); cf.readline()          # +OK
        done = 0
        for i in range(WRITES):
            ch.sendall(cmd(b'SET', b'ci:%d' % i, b'v'))
            r = cf.readline()
            if not r or r[:1] == b'-': break                        # throttled/closed -> stop
            done += 1
        ch.close()
        conn.sendall(b'+FAMOK %d\r\n' % done)
srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", LISTEN)); srv.listen(8)
while True:
    c, _ = srv.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
PY
python3 "$D/coproc.py" 6675 &
COPROC_PID=$!
sleep 0.5

echo "== cluster: CP + master + CP-driven proxy; nsX throttled to 2 ops/s; VEC.=coproc"
$CP --port 6674 --state "$D/cp" 2>/dev/null &
fleet_wait_ping 6674
fleet_cp 6674 CPADDPROXY 127.0.0.1:6673
fleet_cp 6674 CPADDPAIR 127.0.0.1:6672
fleet_cp 6674 CPADDTENANT tenX tokX nsX 1          # subset 1 -> the proxy sees the full rate
# fleet_cp, not `valkey-cli ... || exit`: valkey-cli exits 0 on a -ERR reply, so
# a rejected CPTENANTQUOTA/CPFAMILY would slip past `||` and mislead the failure
# into "exemption broke" when the real cause was a rejected bootstrap command.
fleet_cp 6674 CPTENANTQUOTA tenX 2 0
fleet_cp 6674 CPFAMILY VEC. 127.0.0.1:6675
$B --port 6672 --engine rocks --data-dir "$D/m" 2>/dev/null &
fleet_wait_listen 6672
# --edge-advertise is what a co-processor dials back on for the channel; it is
# a proxy-local fact even in CP mode (the CP supplies pairs/tenants/families).
$PX --port 6673 --control-plane 127.0.0.1:6674 --advertise 127.0.0.1:6673 \
    --edge-advertise 127.0.0.1:6673 2>"$D/px.log" &
fleet_wait_listen 6673

A="valkey-cli -p 6673 -a tokX --no-auth-warning"
# Wait until the tenant AND the family route have propagated from the CP.
for _ in $(seq 1 100); do
  [ "$($A SET __probe__ 1 2>&1)" = "OK" ] || { sleep 0.1; continue; }
  case "$($A VEC.SET __p__ v 2>&1 | tr -d '\r')" in *FAMOK*|*COPROCUNAVAIL*) break ;; esac
  sleep 0.1
done
sleep 1.2   # let the token bucket refill to full (2) after the probes above

echo "== EXEMPT: one family command's co-processor completes all $WRITES channel writes into a 2-ops/s namespace"
R=$($A VEC.SET k v 2>&1 | tr -d '\r')
echo "  VEC.SET -> $R"
DONE=$(printf '%s' "$R" | sed -n 's/.*FAMOK \([0-9]*\).*/\1/p')
[ "$DONE" = "$WRITES" ] \
  || { echo "FAIL: co-processor completed only ${DONE:-0}/$WRITES channel writes — the tenant quota leaked into channel I/O"; tail -5 "$D/px.log"; exit 1; }
echo "  co-processor did $DONE/$WRITES channel writes under a 2-ops/s namespace — EXEMPT (D1)"

sleep 1.2   # refill the bucket before the control

echo "== CONTROL: the SAME namespace throttles its TENANT's direct $WRITES-write burst"
THR=$({ printf 'AUTH tokX\n'; for i in $(seq 1 "$WRITES"); do printf 'SET t%d v\n' "$i"; done; } \
      | nc -w 2 127.0.0.1 6673 | tr -d '\r' | grep -c 'THROTTLED')
echo "  tenant's $WRITES-write burst -> $THR throttled"
[ "$THR" -ge 40 ] \
  || { echo "FAIL: only $THR/$WRITES throttled — the 2-ops/s rate is not binding, so the exemption proves nothing"; exit 1; }
echo "  the rate is real: $THR/$WRITES tenant writes shed -THROTTLED"

echo "PASS: a co-processor's channel writes are exempt from the tenant ops/s quota"
echo "      ($WRITES writes into a 2-ops/s namespace, all served), while the same"
echo "      namespace throttles its tenant's direct burst ($THR/$WRITES shed) — D1,"
echo "      end to end, under a control-plane rate."
