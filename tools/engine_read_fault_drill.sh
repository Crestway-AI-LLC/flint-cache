#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0284: a key the storage engine could not read was served as absent,
# and writes that read first wrote over it. `RocksKv::get` turned RocksDB's
# error into `None`, which no caller can tell from a missing key: a key in a
# block whose checksum failed answered GET with nil, INCR stored 1 over its
# value, SET NX stored over it, DBSIZE stopped counting at the block, and the
# GC deleted a hash's fields as orphans because it could not read the hash.
# Each of those writes rides the WAL to every replica, so one node's bad
# block became lost data everywhere.
#
# One seat, its data flushed to an SST, eight bytes of the SST overwritten in
# a data block. Asserts:
#   - the control: every key reads back before the SST is touched
#   - no key answers nil; a key in the bad block answers an error, and at
#     least one does (else this run proved nothing)
#   - no INCR answers 1 and no SET NX answers OK: neither writes over a key
#     it could not read
#   - DBSIZE answers an error, not a short count
#   - the GC, sweeping every second, deletes the four orphans of a hash
#     deleted on purpose (the positive control) and nothing else
#   - with the SST put back as it was, every key holds exactly what the
#     answered writes made it, and every hash all its fields
#
# BUG-0286: a seat whose role row was in the bad block took it for no role,
# started again at epoch (0,1) with the role its flags named, and then let a
# stale FLINTPROMOTE through its fence. Asserts, on a second seat at epoch
# (0,7) whose first data block (the role key sorts first) is overwritten:
#   - started with a stale --replica-of, it refuses to start, naming BUG-0286
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-rfault- 6618
fleet_guard
fleet_kill server; sleep 0.4
B=./target/release/flint-server
D=$(mktemp -d $FLINT_DRILL_ROOT/flint-rfault-d.XXXXXX)
cleanup() {
  fleet_kill server
  rm -rf "$D"
}
trap cleanup EXIT
P=6618
N=2000   # strings n:i = 1000 + i
H=500    # hashes h:i, four fields each
[ -x "$B" ] || { echo "FAIL: $B not built (cargo build --release --features flint-server/rocks)"; exit 1; }

start() {
  $B --port $P --engine rocks --data-dir "$D/db" 2>>"$D/s.log" &
  fleet_wait_listen $P; fleet_wait_ping $P
}

# One RESP connection per call. Modes print one line per key, and a summary.
rp() {
  python3 - "$P" "$N" "$H" "$@" <<'PY'
import socket, sys

port, n, h, mode = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
s = socket.create_connection(("127.0.0.1", port))
f = s.makefile("rb")


def reply():
    line = f.readline()
    t, rest = line[:1], line[1:].rstrip(b"\r\n")
    if t == b"$":
        k = int(rest)
        return None if k < 0 else f.read(k + 2)[:k].decode()
    if t == b"*":
        k = int(rest)
        return None if k < 0 else [reply() for _ in range(k)]
    return (t + rest).decode(errors="replace")


def call(*args):
    b = b"*%d\r\n" % len(args)
    for a in args:
        a = str(a).encode()
        b += b"$%d\r\n%s\r\n" % (len(a), a)
    s.sendall(b)
    return reply()


def err(r):
    return isinstance(r, str) and r.startswith("-")


if mode == "get":
    nil = bad = wrong = 0
    for i in range(n):
        r = call("GET", "n:%d" % i)
        if r is None:
            nil += 1
        elif err(r):
            bad += 1
            last = r
        elif r != str(1000 + i):
            wrong += 1
    print("nil %d errors %d wrong %d%s" % (nil, bad, wrong, (" e.g. " + last[:80]) if bad else ""))
elif mode == "fields":
    short = bad = 0
    for i in range(h):
        r = call("HGETALL", "h:%d" % i)
        if err(r):
            bad += 1
        elif len(r) != 8 and i != int(sys.argv[5]):
            short += 1
    print("short %d errors %d" % (short, bad))
elif mode == "incr":
    for i in range(0, n, 2):
        print("n:%d %s" % (i, call("INCR", "n:%d" % i)))
elif mode == "setnx":
    for i in range(1, n, 2):
        print("n:%d %s" % (i, call("SET", "n:%d" % i, "over", "NX")))
elif mode == "cmd":
    print(call(*sys.argv[5:]))
elif mode == "final":
    told = {}
    for path, verb in ((sys.argv[5], "INCR"), (sys.argv[6], "SET NX")):
        for l in open(path).read().splitlines():
            k, r = l.split(" ", 1)
            told[k] = (verb, r)
    wrong = 0
    for i in range(n):
        verb, r = told["n:%d" % i]
        # An INCR answered with a number added one; anything refused, and a
        # SET NX answered nil, left the value as it was.
        want = str(1001 + i) if verb == "INCR" and r.startswith(":") else str(1000 + i)
        got = call("GET", "n:%d" % i)
        if got != want:
            wrong += 1
            if wrong <= 4:
                print("WRONG n:%d is %r; %s was answered %s, so it should be %s" % (i, got, verb, r, want))
    print("wrong %d" % wrong)
PY
}

echo "== $N strings and $H hashes of four fields, flushed to an SST by a restart"
start
{
  for i in $(seq 0 $((N - 1))); do printf 'SET n:%d %d\r\n' $i $((1000 + i)); done
  for i in $(seq 0 $((H - 1))); do printf 'HSET h:%d f1 one-%d f2 two-%d f3 three-%d f4 four-%d\r\n' $i $i $i $i $i; done
} | valkey-cli -p $P --pipe >/dev/null
fleet_kill server; sleep 0.3; start
c=$(rp get); [ "$c" = "nil 0 errors 0 wrong 0" ] || { echo "FAIL: the control does not read back: $c"; exit 1; }
c=$(rp fields -1); [ "$c" = "short 0 errors 0" ] || { echo "FAIL: the control's hashes: $c"; exit 1; }
echo "  control: every string and every hash reads back"
fleet_kill server; sleep 0.3
SST=$(ls "$D"/db/*.sst 2>/dev/null)
[ "$(echo "$SST" | wc -w | tr -d ' ')" = 1 ] || { echo "FAIL: expected one SST, found: $SST"; exit 1; }
cp "$SST" "$D/orig.sst"
printf '\377\377\377\377\377\377\377\377' | dd of="$SST" bs=1 seek=3000 conv=notrunc 2>/dev/null
cmp -s "$SST" "$D/orig.sst" && { echo "FAIL: the SST did not change"; exit 1; }

echo "== eight bytes of a data block overwritten: reads"
start
g=$(rp get); echo "  GET of $N strings: $g"
case "$g" in
  "nil 0 errors 0 "*) echo "FAIL: no key met the bad block, so this run proved nothing"; exit 1 ;;
  "nil 0 errors "*" wrong 0"*) ;;
  "nil 0 "*) echo "FAIL: a key read back another value"; exit 1 ;;
  *) echo "FAIL (BUG-0284): a key the engine could not read answered nil, as if it were not there"; exit 1 ;;
esac
hf=$(rp fields -1); echo "  HGETALL of $H hashes: $hf"
case "$hf" in
  "short 0 errors 0") echo "FAIL: no hash met the bad block, so the GC check below would prove nothing"; exit 1 ;;
  "short 0 errors "*) ;;
  *) echo "FAIL (BUG-0284): a hash the engine could not read answered as empty"; exit 1 ;;
esac
d=$(rp cmd DBSIZE); echo "  DBSIZE: ${d:0:90}"
case "$d" in -*) ;; *) echo "FAIL (BUG-0284): DBSIZE answered a count that stops at the bad block"; exit 1 ;; esac

echo "== writes that read first"
rp incr >"$D/incr"
ones=$(grep -c ' :1$' "$D/incr"); refused=$(grep -c ' -' "$D/incr")
echo "  INCR of the even keys: $refused refused, $ones answered 1"
[ "$ones" = 0 ] || { echo "FAIL (BUG-0284): INCR stored 1 over $ones values it could not read"; exit 1; }
[ "$refused" -gt 0 ] || { echo "FAIL: no INCR was refused, yet GET was"; exit 1; }
rp setnx >"$D/setnx"
oks=$(grep -c ' +OK$' "$D/setnx")
echo "  SET NX of the odd keys: $(grep -c ' -' "$D/setnx") refused, $oks answered OK"
[ "$oks" = 0 ] || { echo "FAIL (BUG-0284): SET NX stored over $oks keys it could not read"; exit 1; }
[ "$(grep -c ' -' "$D/setnx")" -gt 0 ] || { echo "FAIL: no SET NX was refused, yet GET was"; exit 1; }

echo "== the GC, every second, with one hash deleted on purpose"
DEL_I=""
for i in $(seq 0 $((H - 1))); do
  r=$(rp cmd HLEN h:$i); [ "$r" = ":4" ] && { DEL_I=$i; break; }
done
[ -n "$DEL_I" ] || { echo "FAIL: no hash readable to delete"; exit 1; }
[ "$(rp cmd DEL h:$DEL_I)" = ":1" ] || { echo "FAIL: DEL h:$DEL_I"; exit 1; }
rp cmd FLINTCONFIG gc-sweep-ms 1000 >/dev/null
orph() { valkey-cli -p $P FLINTINFO | tr -d '\r' | sed -n 's/^gc_swept_orphans://p'; }
for _ in $(seq 1 40); do [ "$(orph)" -ge 4 ] 2>/dev/null && break; sleep 0.25; done
sleep 2.5
o=$(orph); echo "  gc_swept_orphans: $o (the deleted hash's fields are 4)"
[ "$o" = 4 ] || { echo "FAIL (BUG-0284): the GC deleted $o rows; 4 were orphans, the rest are fields of hashes it could not read"; exit 1; }

echo "== the SST put back as it was: every key is what its answered writes made it"
fleet_kill server; sleep 0.3
cp "$D/orig.sst" "$SST"
start
fin=$(rp final "$D/incr" "$D/setnx"); echo "  strings: $(tail -1 <<<"$fin")"
[ "$(tail -1 <<<"$fin")" = "wrong 0" ] || { echo "FAIL (BUG-0284): with the block repaired, keys do not hold what their answered writes made them:"; head -4 <<<"$fin" | sed 's/^/  /'; exit 1; }
fl=$(rp fields "$DEL_I"); echo "  hashes: $fl"
[ "$fl" = "short 0 errors 0" ] || { echo "FAIL (BUG-0284): the GC deleted fields of hashes that were there"; exit 1; }

echo "== a seat at epoch (0,7) whose role row it cannot read (BUG-0286)"
fleet_kill server; sleep 0.3
R="$D/role"
role_start() {
  $B --port $P --engine rocks --data-dir "$R" "$@" 2>>"$D/r.log" &
  SEAT=$!
}
role_start; fleet_wait_listen $P; fleet_wait_ping $P
for i in $(seq 0 $((N - 1))); do echo "SET n:$i $((1000 + i))"; done | valkey-cli -p $P >/dev/null
[ "$(rp cmd FLINTDEMOTE 0 5)" = "+OK demoted at (0,5)" ] && [ "$(rp cmd FLINTPROMOTE 0 7)" = "+OK promoted at (0,7)" ] \
  || { echo "FAIL: could not take the seat to epoch (0,7)"; tail -3 "$D/r.log"; exit 1; }
fleet_kill server; sleep 0.3
role_start; fleet_wait_listen $P; fleet_wait_ping $P
fleet_kill server; sleep 0.3
RSST=$(ls "$R"/*.sst 2>/dev/null)
[ "$(echo "$RSST" | wc -w | tr -d ' ')" = 1 ] || { echo "FAIL: expected one SST, found: $RSST"; exit 1; }
printf '\377\377\377\377\377\377\377\377' | dd of="$RSST" bs=1 seek=100 conv=notrunc 2>/dev/null
role_start --replica-of 127.0.0.1:1
for _ in $(seq 1 100); do kill -0 "$SEAT" 2>/dev/null || break; sleep 0.1; done
if kill -0 "$SEAT" 2>/dev/null; then
  fleet_wait_listen $P
  echo "FAIL (BUG-0286): the seat started without its role: $(valkey-cli -p $P FLINTINFO | tr -d '\r' | grep -E '^role(_epoch)?:' | tr '\n' ' ')"
  echo "  and a stale FLINTPROMOTE 0 2 answers: $(rp cmd FLINTPROMOTE 0 2)"
  exit 1
fi
wait "$SEAT"; rc=$?
grep -q 'BUG-0286' "$D/r.log" || { echo "FAIL: the seat exited $rc without saying why"; tail -3 "$D/r.log"; exit 1; }
echo "  refused to start, status $rc: $(grep 'BUG-0286' "$D/r.log" | head -1 | cut -c1-110)"

echo "PASS: a key the engine cannot read answers an error, never nil; no write lands over it, the GC"
echo "      deletes only real orphans, and with the block repaired every key is intact (BUG-0284);"
echo "      a seat that cannot read its role row does not start (BUG-0286)."
