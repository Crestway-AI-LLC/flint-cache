#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Control plane v1 drill:
#   - CP owns the tenant registry + topology; two proxies subscribe (CPWATCH)
#     and are fed ONLY their assigned tenants (shuffle-shard sub-groups)
#   - tenants added AT RUNTIME appear on their proxies within a push cycle,
#     no restarts; AUTH on a non-assigned proxy is refused (sub-group
#     enforcement = blast-radius/connection bounding)
#   - CPSETSUBSET re-assigns live (whale isolation / drain)
#   - CP state survives restart; CP OUTAGE does not touch the data path
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-cp-drill-state 6730 6740 7241 7601 7602
fleet_guard
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; sleep 0.4
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
STATE=$FLINT_DRILL_ROOT/flint-cp-drill-state
cleanup() {
  # Was `pkill -9 -f "flint-server --port 673"`, which covers 6730 and NOT
  # 6740 — so this drill passed while leaking its second node on every run.
  # On 2026-08-10 that orphan made fleet_guard refuse the next 23 drills and
  # the gate reported 24 failures, exactly one of which was real.
  # fleet_kill is scoped to this drill's fleet_init ports, all of them.
  fleet_kill server 2>/dev/null
  fleet_kill proxy
  fleet_kill controlplane
  rm -rf $FLINT_DRILL_ROOT/flint-cpd-* "$STATE" "$STATE.tmp"
}
trap cleanup EXIT
rm -f "$STATE"

# Its own build, as every drill has. Without it this drill ran whatever
# binaries target/ held: in the gate the prebuild's, but run alone a stale
# build, which is how a run of it against a fixed tree reported the unfixed
# control plane's behaviour (BUG-0202).
cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

echo "== data plane: two single-master pairs"
for p in 6730 6740; do
  d="$FLINT_DRILL_ROOT/flint-cpd-$p"; rm -rf "$d"
  $B --port $p --engine rocks --data-dir "$d" 2>"${FLEET_SCOPE}server.log" &
done
sleep 0.8

echo "== control plane + registrations"
$CP --port 7241 --state "$STATE" 2>$FLINT_DRILL_ROOT/flint-cpd-cp.log &
fleet_wait_listen 7241
sleep 0.5
fleet_cp 7241 CPADDPROXY 127.0.0.1:7601
fleet_cp 7241 CPADDPROXY 127.0.0.1:7602
fleet_cp 7241 CPADDPAIR 127.0.0.1:6730
fleet_cp 7241 CPADDPAIR 127.0.0.1:6740

echo "== two proxies in control-plane mode (no --pairs/--tenants flags)"
$PX --port 7601 --control-plane 127.0.0.1:7241 --advertise 127.0.0.1:7601 2>$FLINT_DRILL_ROOT/flint-cpd-p1.log &
$PX --port 7602 --control-plane 127.0.0.1:7241 --advertise 127.0.0.1:7602 2>$FLINT_DRILL_ROOT/flint-cpd-p2.log &
fleet_wait_listen 7601 7602
sleep 1.5

echo "== add tenant with k=1: assigned to exactly one proxy (shuffle shard)"
R=$(valkey-cli -p 7241 CPADDTENANT acme tok-acme acme 1)
echo "  $R"
SUB=$(echo "$R" | grep -oE '\[[^]]*\]' | tr -d '[]')
[ -n "$SUB" ] || { echo "FAIL: no subset in reply: $R"; exit 1; }
APORT=${SUB##*:}
OPORT=$([ "$APORT" = "7601" ] && echo 7602 || echo 7601)
sleep 1.5   # push cycle

echo "== sub-group enforcement: AUTH works ONLY on the assigned proxy"
W=$(valkey-cli -p "$APORT" -a tok-acme --no-auth-warning SET hello world 2>&1)
[ "$W" = "OK" ] || { echo "FAIL: assigned proxy :$APORT rejected tenant: $W"; tail -4 $FLINT_DRILL_ROOT/flint-cpd-p1.log $FLINT_DRILL_ROOT/flint-cpd-p2.log; exit 1; }
G=$(valkey-cli -p "$APORT" -a tok-acme --no-auth-warning GET hello)
[ "$G" = "world" ] || { echo "FAIL: data path via CP-fed proxy: '$G'"; exit 1; }
X=$(valkey-cli -p "$OPORT" -a tok-acme --no-auth-warning GET hello 2>&1)
echo "$X" | grep -q "WRONGPASS" || { echo "FAIL: non-assigned proxy :$OPORT accepted the token: $X"; exit 1; }
echo "  assigned :$APORT serves; other :$OPORT refuses (WRONGPASS) — blast radius bounded"

echo "== runtime add with k=2: appears on BOTH proxies, no restarts"
fleet_cp 7241 CPADDTENANT globex tok-glx globex 2
sleep 1.5
for p in 7601 7602; do
  W=$(valkey-cli -p $p -a tok-glx --no-auth-warning SET g 1 2>&1)
  [ "$W" = "OK" ] || { echo "FAIL: globex not live on :$p: $W"; exit 1; }
done
echo "  globex live on both proxies within one push cycle"

echo "== CPSETSUBSET: drain globex to :$APORT only (live re-assignment)"
fleet_cp 7241 CPSETSUBSET globex "127.0.0.1:$APORT"
sleep 1.5
X=$(valkey-cli -p "$OPORT" -a tok-glx --no-auth-warning SET g 2 2>&1)
echo "$X" | grep -q "WRONGPASS" || { echo "FAIL: drained proxy :$OPORT still accepts globex: $X"; exit 1; }
W=$(valkey-cli -p "$APORT" -a tok-glx --no-auth-warning SET g 2 2>&1)
[ "$W" = "OK" ] || { echo "FAIL: retained proxy :$APORT lost globex: $W"; exit 1; }
echo "  drained live: :$OPORT refuses, :$APORT serves"

echo "== CP outage: data path unaffected; restart restores durable state"
V_BEFORE=$(valkey-cli -p 7241 CPINFO | tr '\r' '\n' | grep "^version" | cut -d: -f2)
fleet_kill controlplane; sleep 0.5
W=$(valkey-cli -p "$APORT" -a tok-acme --no-auth-warning SET during-outage ok 2>&1)
[ "$W" = "OK" ] || { echo "FAIL: data path depends on CP being up: $W"; exit 1; }
[ "$(valkey-cli -p "$APORT" -a tok-acme --no-auth-warning GET during-outage)" = "ok" ] || { echo "FAIL: read during outage"; exit 1; }
$CP --port 7241 --state "$STATE" 2>>$FLINT_DRILL_ROOT/flint-cpd-cp.log &
fleet_wait_listen 7241
sleep 1
V_AFTER=$(valkey-cli -p 7241 CPINFO | tr '\r' '\n' | grep "^version" | cut -d: -f2)
[ "$V_AFTER" = "$V_BEFORE" ] || { echo "FAIL: state lost across restart ($V_BEFORE -> $V_AFTER)"; exit 1; }
echo "  wrote+read during CP outage; restart restored version $V_AFTER"

echo "== tenant added after CP restart still propagates (watch reconnected)"
fleet_cp 7241 CPADDTENANT initech tok-ini initech 2
OK=0
for i in $(seq 1 10); do
  [ "$(valkey-cli -p 7601 -a tok-ini --no-auth-warning SET i 1 2>&1)" = "OK" ] && { OK=1; break; }
  sleep 0.5
done
[ "$OK" = "1" ] || { echo "FAIL: post-restart tenant never propagated"; exit 1; }
echo "  initech live after CP restart — subscriptions self-heal"

echo "== BUG-0202: an idle watch keeps alive, and a watch whose proxy has gone ends"
# A watch sent nothing while there was nothing to push. A proxy could not
# tell a quiet seat from a dead one and abandoned it every ~5 minutes, and
# the CP kept each abandoned watch's thread and socket until the next version
# bump; on the playground, 534 threads in 40 hours. Twenty subscribers each
# take their snapshot and ACK it; nineteen hang up at once, and one stays,
# idle, for 25 s. No version moves in that time.
CPPID=$(fleet_pids controlplane | head -1)
[ -n "$CPPID" ] || { echo "FAIL: no control-plane process"; exit 1; }
threads() {
  if [ -r "/proc/$1/status" ]; then awk '/^Threads:/ {print $2}' "/proc/$1/status"
  else ps -M -p "$1" | tail -n +2 | wc -l | tr -d ' '; fi
}
T0=$(threads "$CPPID")
cat > "$FLINT_DRILL_ROOT/flint-cpd-watch.py" <<'PY'
import re, socket, sys, time
def resp(a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in (s.encode() for s in a))
port, n, idle = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
subs = []
for i in range(n):
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.sendall(resp(["CPWATCH", "127.0.0.1:%d" % (7700 + i), "0"]))
    buf = b""
    while not re.search(rb"SNAPSHOT\r\n:(\d+)\r\n", buf):
        c = s.recv(65536)
        if not c:
            sys.exit("watch %d closed before its snapshot" % i)
        buf += c
    s.sendall(resp(["ACK", re.search(rb"SNAPSHOT\r\n:(\d+)\r\n", buf).group(1).decode()]))
    subs.append(s)
for s in subs[1:]:
    s.close()
stay, got, t0 = subs[0], b"", time.time()
stay.settimeout(1)
while time.time() - t0 < idle:
    try:
        c = stay.recv(4096)
    except socket.timeout:
        continue
    if not c:
        break
    got += c
print(got.count(b"+KEEPALIVE\r\n"))
PY
K=$(python3 "$FLINT_DRILL_ROOT/flint-cpd-watch.py" 7241 20 25)
T1=$(threads "$CPPID")
echo "  keepalives to the idle watch in 25 s: $K; CP threads $T0 before, $T1 after"
[ "${K:-0}" -ge 2 ] 2>/dev/null || { echo "FAIL (BUG-0202): an idle watch got ${K:-no} keepalives in 25 s, not 2 or more"; exit 1; }
[ "$T1" -le $((T0 + 2)) ] || { echo "FAIL (BUG-0202): the CP holds $T1 threads, $((T1 - T0)) more than before twenty watches came and went"; exit 1; }

echo "PASS: control plane v1 — durable registry, shuffle-shard sub-groups enforced, live pushes, CP outage off the data path, and watches keep alive and end with their proxies (BUG-0202)"
