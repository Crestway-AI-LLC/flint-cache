#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0057 D7: consumer groups across a failover, through the proxy.
#
# A producer XADDs and a consumer reads with XREADGROUP `>` and XACKs, while
# the pair fails over twice: a planned failover with both running, and a
# master killed with -9 once its replica has caught up. Those are the two
# shapes in which Flint promises that no acknowledged write is lost
# (failover.md); an unplanned kill mid-stream may lose the async tail, for a
# group's rows as for any key, within the RPO.
#
# WHAT IS ASSERTED, over everything the clients were told:
#   1. every acknowledged XADD is still in the stream;
#   2. every one was delivered: the group's last delivered ID covers it;
#   3. every one is acknowledged or still pending, never neither (an XACK
#      whose reply was lost counts as either);
#   4. no acknowledged XACK is pending again;
#   5. `>` never delivered one entry twice;
#   6. a consumer blocked with `BLOCK 0` across the planned failover takes
#      the entry added after it.
#
# Requires: a release build with --features rocks, valkey-cli, python3.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"

fleet_init "$FLINT_DRILL_ROOT/flint-sgfail" 7936 7937 7938 7939
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-sgfail
INV=$FLINT_DRILL_ROOT/flint-sgfail.flint
W=$STATE/work
fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
sleep 0.4
WORK_PID=""
WAIT_PID=""
cleanup() {
  [ -n "$WORK_PID" ] && kill "$WORK_PID" 2>/dev/null
  [ -n "$WAIT_PID" ] && kill "$WAIT_PID" 2>/dev/null
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill controller; fleet_kill server; fleet_kill proxy; fleet_kill controlplane
  rm -rf "$STATE" "$INV"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
cp 127.0.0.1:7939
pair 127.0.0.1:7936,127.0.0.1:7937
proxy 127.0.0.1:7938
controller on
streams on
stream-groups on
EOF
CTL="./target/release/flintctl -f $INV"
status() { $CTL status 2>/dev/null; }
master() { status | awk '/master/{print $3; exit}'; }
info() { valkey-cli -p "${1##*:}" FLINTINFO 2>/dev/null | tr -d '\r' | sed -n "s/^$2://p"; }
A="valkey-cli -p 7938 -a tok-g --no-auth-warning"

echo "== bootstrap a master+replica pair behind a proxy + controller"
$CTL bootstrap >"$STATE-boot.log" 2>&1 || {
  echo "FAIL: bootstrap"; tail -25 "$STATE-boot.log"; exit 1; }
$CTL tenant add g tok-g g 1 >/dev/null 2>&1
for _ in $(seq 1 30); do [ "$($A PING 2>/dev/null)" = "PONG" ] && break; sleep 0.3; done
[ "$($A XGROUP CREATE s g '$' MKSTREAM 2>&1)" = "OK" ] || {
  echo "FAIL: XGROUP CREATE through the proxy: $($A XGROUP CREATE s g '$' MKSTREAM 2>&1)"; exit 1; }
mkdir -p "$W"
[ "$($A XGROUP CREATE s2 g2 '$' MKSTREAM 2>&1)" = "OK" ] || { echo "FAIL: XGROUP CREATE s2"; exit 1; }

# The workload: one producer, one consumer, plain RESP over TCP. Each records
# only what the server acknowledged; a reply lost to the failover is retried
# or written down as unknown, never as done.
cat > "$W/load.py" <<'PY'
import json, os, socket, sys, threading, time
PORT, W = int(sys.argv[1]), sys.argv[2]
STOP, PAUSE = os.path.join(W, "stop"), os.path.join(W, "pause")

def enc(*parts):
    out = [b"*%d\r\n" % len(parts)]
    for p in parts:
        p = p if isinstance(p, bytes) else str(p).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(p), p))
    return b"".join(out)

class Conn:
    def __init__(self):
        self.s = socket.create_connection(("127.0.0.1", PORT), timeout=5)
        self.buf = b""
        if self.call("AUTH", "tok-g") != b"OK":
            raise OSError("auth")
    def line(self):
        while b"\r\n" not in self.buf:
            d = self.s.recv(65536)
            if not d:
                raise OSError("closed")
            self.buf += d
        l, self.buf = self.buf.split(b"\r\n", 1)
        return l
    def read(self):
        l = self.line()
        t, rest = l[:1], l[1:]
        if t == b"+":
            return rest
        if t == b"-":
            return Exception(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                d = self.s.recv(65536)
                if not d:
                    raise OSError("closed")
                self.buf += d
            v, self.buf = self.buf[:n], self.buf[n + 2:]
            return v
        if t == b"*":
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        raise OSError("bad reply %r" % l)
    def call(self, *a):
        self.s.sendall(enc(*a))
        return self.read()

res = {"adds": [], "delivered": [], "dups": [], "acked": [], "ack_unknown": [], "errors": 0}
lock = threading.Lock()

def run(step):
    c = None
    while True:
        if os.path.exists(PAUSE) and not os.path.exists(STOP):
            time.sleep(0.05)
            continue
        try:
            c = c or Conn()
            if step(c) is False:
                return
        except Exception:
            with lock:
                res["errors"] += 1
            c = None
            time.sleep(0.1)

n = [0]
def produce(c):
    if os.path.exists(STOP):
        return False
    n[0] += 1
    r = c.call("XADD", "s", "*", "n", n[0])
    if isinstance(r, bytes):
        with lock:
            res["adds"].append(r.decode())
    time.sleep(0.002)

seen = set()
empty = [0]
def consume(c):
    r = c.call("XREADGROUP", "GROUP", "g", "c1", "COUNT", 10, "STREAMS", "s", ">")
    if isinstance(r, Exception):
        raise r
    ids = [e[0].decode() for e in r[0][1]] if r else []
    if not ids:
        if os.path.exists(STOP) and not producer.is_alive():
            empty[0] += 1
            if empty[0] >= 3:
                return False
        time.sleep(0.02)
        return
    empty[0] = 0
    with lock:
        for i in ids:
            (res["dups"] if i in seen else res["delivered"]).append(i)
            seen.add(i)
    try:
        a = c.call("XACK", "s", "g", *ids)
    except Exception:
        with lock:
            res["ack_unknown"].extend(ids)
        raise
    with lock:
        if isinstance(a, int):
            res["acked"].extend(ids)
        else:
            res["ack_unknown"].extend(ids)

producer = threading.Thread(target=run, args=(produce,), daemon=True)
consumer = threading.Thread(target=run, args=(consume,), daemon=True)
producer.start(); consumer.start()
producer.join(); consumer.join(timeout=60)
json.dump(res, open(os.path.join(W, "result.json"), "w"))
print("load: %d adds, %d delivered, %d acked, %d ack replies lost, %d errors retried"
      % (len(res["adds"]), len(res["delivered"]), len(res["acked"]), len(res["ack_unknown"]),
         res["errors"]))
PY

# A consumer that waits with BLOCK 0 through the failover: it must wake
# for the entry added after it, wherever the master went meanwhile.
cat > "$W/wait.py" <<'PY'
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])))
def send(*a):
    s.sendall(b"".join([b"*%d\r\n" % len(a)] + [b"$%d\r\n%s\r\n" % (len(x), x) for x in a]))
send(b"AUTH", b"tok-g")
send(b"XREADGROUP", b"GROUP", b"g2", b"waiter", b"BLOCK", b"0", b"STREAMS", b"s2", b">")
buf = b""
while True:
    d = s.recv(65536)
    if not d:
        break
    buf += d
    open(sys.argv[2], "wb").write(buf)
PY
python3 -I "$W/wait.py" 7938 "$W/waited.out" & WAIT_PID=$!

echo "== workload on: one producer, one consumer, through the proxy"
python3 -I "$W/load.py" 7938 "$W" & WORK_PID=$!
sleep 2

M0=$(master); echo "== planned FAILOVER of the master ($M0), with both running"
[ -n "$M0" ] || { echo "FAIL: no master"; exit 1; }
$CTL failover "$M0" 2>&1 | grep -E 'demoted|complete' | sed 's/^/  /'
M1=""
for _ in $(seq 1 60); do
  M1=$(master); [ -n "$M1" ] && [ "$M1" != "$M0" ] && break; sleep 0.5
done
[ -n "$M1" ] && [ "$M1" != "$M0" ] || { echo "FAIL: master still '$M1' 30s after failover of $M0"; status | sed 's/^/    /'; exit 1; }
echo "  new master $M1"
LATE=$($A XADD s2 '*' late 1 2>&1)
case "$LATE" in
  [0-9]*-[0-9]*) ;;
  *) echo "FAIL: XADD s2 after the failover answered '$LATE'"; exit 1 ;;
esac
for _ in $(seq 1 40); do grep -q -- "$LATE" "$W/waited.out" 2>/dev/null && break; sleep 0.25; done
grep -q -- "$LATE" "$W/waited.out" 2>/dev/null || {
  echo "FAIL: 6. the consumer blocked across the failover did not take $LATE; it has:"
  tr -d '\r' < "$W/waited.out" 2>/dev/null | head -5 | sed 's/^/    /'; exit 1; }
kill "$WAIT_PID" 2>/dev/null; WAIT_PID=""
echo "  the consumer blocked with BLOCK 0 across it took $LATE"
sleep 2

echo "== the master killed with -9 once its replica has caught up"
touch "$W/pause"; sleep 0.5
R1=$(status | awk '/replica/{print $3; exit}')
[ -n "$R1" ] || { echo "FAIL: no replica to promote after the planned failover"; status | sed 's/^/    /'; exit 1; }
CAUGHT=0
for _ in $(seq 1 60); do
  L=$(info "$M1" latest_seq); P=$(info "$R1" last_applied)
  [ -n "$L" ] && [ -n "$P" ] && [ "$P" -ge "$L" ] && { CAUGHT=1; break; }
  sleep 0.5
done
[ "$CAUGHT" = 1 ] || { echo "FAIL: replica $R1 at $P never reached the master's $L"; exit 1; }
fleet_signal_port "${M1##*:}" -9
M2=""
for _ in $(seq 1 120); do
  M2=$(master); [ -n "$M2" ] && [ "$M2" != "$M1" ] && break; sleep 0.5
done
[ -n "$M2" ] && [ "$M2" != "$M1" ] || { echo "FAIL: no promotion 60s after $M1 was killed"; status | sed 's/^/    /'; exit 1; }
echo "  $R1 caught up, $M1 killed, $M2 promoted"
rm -f "$W/pause"; sleep 2

echo "== stop the producer; the consumer reads to the end"
touch "$W/stop"
for _ in $(seq 1 120); do kill -0 "$WORK_PID" 2>/dev/null || break; sleep 0.5; done
kill -0 "$WORK_PID" 2>/dev/null && { echo "FAIL: the workload did not finish within 60s"; exit 1; }
wait "$WORK_PID"; WORK_PID=""
[ -s "$W/result.json" ] || { echo "FAIL (HARNESS): the workload wrote no result"; exit 1; }

echo "== the invariants"
$A XRANGE s - + 2>/dev/null | awk 'NR%3==1' > "$W/stream.txt"
$A XPENDING s g - + 1000000 2>/dev/null | awk 'NR%4==1' > "$W/pending.txt"
LAST=$($A XINFO GROUPS s 2>/dev/null | awk 'prev=="last-delivered-id"{print; exit} {prev=$0}')
python3 -I - "$W" "$LAST" <<'PY'
import json, os, sys
W, last = sys.argv[1], sys.argv[2]
res = json.load(open(os.path.join(W, "result.json")))
stream = set(open(os.path.join(W, "stream.txt")).read().split())
pending = set(open(os.path.join(W, "pending.txt")).read().split())
key = lambda i: tuple(int(x) for x in i.split("-"))
adds, acked, unknown = res["adds"], set(res["acked"]), set(res["ack_unknown"])
fails = []
if len(adds) < 500 or len(acked) < 500:
    fails.append("too little load to mean anything: %d adds, %d acked" % (len(adds), len(acked)))
lost = [i for i in adds if i not in stream]
if lost:
    fails.append("1. %d acknowledged XADDs are not in the stream, first %s" % (len(lost), lost[:3]))
undelivered = [i for i in adds if not last or key(i) > key(last)]
if undelivered:
    fails.append("2. %d acknowledged XADDs were never delivered (last-delivered-id %s), first %s"
                 % (len(undelivered), last, undelivered[:3]))
neither = [i for i in adds if i not in acked and i not in unknown and i not in pending]
if neither:
    fails.append("3. %d acknowledged XADDs are neither acknowledged nor pending, first %s"
                 % (len(neither), neither[:3]))
again = sorted(acked & pending, key=key)
if again:
    fails.append("4. %d acknowledged XACKs are pending again, first %s" % (len(again), again[:3]))
if res["dups"]:
    fails.append("5. `>` delivered %d entries twice, first %s" % (len(res["dups"]), res["dups"][:3]))
for f in fails:
    print("FAIL:", f)
if fails:
    sys.exit(1)
print("  %d acknowledged XADDs: all in the stream, all delivered, each acknowledged or pending;"
      % len(adds))
print("  %d acknowledged XACKs: none pending again; no entry delivered twice by `>`"
      % len(acked))
PY
[ $? -eq 0 ] || exit 1

echo "PASS: stream_groups_failover — across a planned failover and a -9 kill of a caught-up master, every acknowledged XADD is in the stream and was delivered, every acknowledged XACK stays acknowledged, \`>\` delivered nothing twice, and a consumer blocked across the failover woke for the entry after it"
