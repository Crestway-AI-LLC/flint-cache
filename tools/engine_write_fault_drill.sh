#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0283: a write RocksDB refused was answered as if it had landed. Only a
# three-token `SET k v` commits through `apply_writes`, whose error is the
# reply. Every other write reaches the engine row by row through `Kv::put`
# and `Kv::delete`, which discarded RocksDB's error, so on a volume that
# filled, HSET answered :1 and APPEND its new length for writes that were
# never stored, and the keys read back as absent.
#
# A file-size limit stands in for the full volume, which a drill cannot make
# without root: the seat runs under `ulimit -f` with SIGXFSZ ignored, so the
# write that would take its WAL past the limit fails (EFBIG) and RocksDB
# refuses it, and every write after it, as it does on a full disk.
#
# BUG-0285: RocksDB then refuses every write until the database is opened
# again, and the seat went on as master refusing every write until someone
# restarted it. It now ends itself, status 70, and its supervisor starts it.
#
# Asserts:
#   - the control: plain SET, the batched path, is refused, so the fault is
#     real
#   - HSET, APPEND and SET .. EX are each refused at least once
#   - the seat then exits by itself with status 70, saying why (BUG-0285)
#   - every write answered as a success reads back whole after a restart
#     without the limit, and writes land again
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-wfault- 6617
fleet_guard
fleet_kill server; sleep 0.4
B=./target/release/flint-server
D=$(mktemp -d $FLINT_DRILL_ROOT/flint-wfault-d.XXXXXX)
cleanup() {
  fleet_kill server
  rm -rf "$D"
}
trap cleanup EXIT
P=6617
[ -x "$B" ] || { echo "FAIL: $B not built (cargo build --release --features flint-server/rocks)"; exit 1; }

# 16 MiB a file, in bash's 1024-byte units. The memtable is RocksDB's 64 MiB
# default, set here so an inherited FLINT_WRITE_BUFFER_MB cannot roll the WAL
# before it reaches the limit.
LIMIT_KB=16384
ROUNDS=40
VALUE=262144

# Writes ROUNDS rounds of four kinds at VALUE bytes, one at a time, and
# prints "kind round reply" for each; or, with `check`, reads every key the
# first pass was told it wrote and prints "LOST kind round" for each that is
# not there whole.
rw() {
  python3 - "$P" "$ROUNDS" "$VALUE" "$@" <<'PY'
import socket, sys

port, rounds, size, mode = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]


def resp(*args):
    b = b"*%d\r\n" % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        b += b"$%d\r\n%s\r\n" % (len(a), a)
    return b


def reply(f):
    line = f.readline()
    t, rest = line[:1], line[1:].rstrip(b"\r\n")
    if t == b"$":
        n = int(rest)
        if n < 0:
            return None
        return f.read(n + 2)[:n]
    return (t + rest).decode(errors="replace")


s = socket.create_connection(("127.0.0.1", port))
f = s.makefile("rb")


def call(*args):
    s.sendall(resp(*args))
    return reply(f)


if mode == "write":
    # The seat ends itself soon after its engine stops (BUG-0285). A write
    # whose connection closed got no answer, so it is printed as refused.
    gone = False
    for i in range(rounds):
        v = (b"%08d" % i) * (size // 8)
        for kind, args in (
            ("SET", ("SET", "s:%d" % i, v)),
            ("HSET", ("HSET", "h:%d" % i, "f", v)),
            ("APPEND", ("APPEND", "a:%d" % i, v)),
            ("SETEX", ("SET", "e:%d" % i, v, "EX", "3600")),
        ):
            r = None
            if not gone:
                try:
                    r = call(*args)
                except OSError:
                    r = None
            if not r:
                gone = True
                r = "-GONE the connection closed"
            print(kind, i, r[:100], flush=True)
else:
    told = {}
    for line in open(sys.argv[5]):
        kind, i, r = line.rstrip("\n").split(" ", 2)
        if not r.startswith("-"):
            told[(kind, int(i))] = True
    for kind, i in sorted(told):
        v = (b"%08d" % i) * (size // 8)
        key = {"SET": "s", "HSET": "h", "APPEND": "a", "SETEX": "e"}[kind] + ":%d" % i
        got = call("HGET", key, "f") if kind == "HSET" else call("GET", key)
        if got != v:
            print("LOST", kind, i, "read back", "nil" if got is None else "%d bytes" % len(got))
PY
}

echo "== a seat whose files may not grow past $((LIMIT_KB / 1024)) MiB"
bash -c "trap '' XFSZ; ulimit -f $LIMIT_KB; FLINT_WRITE_BUFFER_MB=64 exec $B --port $P --engine rocks --data-dir $D/db" 2>"$D/s1.log" &
SEAT=$!
fleet_wait_listen $P; fleet_wait_ping $P

echo "== $ROUNDS rounds of four writes of $((VALUE / 1024)) KiB: SET, HSET, APPEND, SET .. EX"
rw write >"$D/replies"
for k in SET HSET APPEND SETEX; do
  ok=$(awk -v k=$k '$1 == k && $3 !~ /^-/' "$D/replies" | wc -l | tr -d ' ')
  no=$(awk -v k=$k '$1 == k && $3 ~ /^-/' "$D/replies" | wc -l | tr -d ' ')
  printf '  %-6s %2d answered as written, %2d refused\n' "$k" "$ok" "$no"
  eval "NO_$k=$no"
done
if [ "$NO_SET" = 0 ]; then
  echo "FAIL: no write was refused, so the limit did not bite and this run proved nothing"
  tail -3 "$D/s1.log"; exit 1
fi
echo "  the control, plain SET: $(awk '$1 == "SET" && $3 ~ /^-ERR/ {$1 = $2 = ""; print; exit}' "$D/replies" | cut -c3-90)"
for k in HSET APPEND SETEX; do
  n=$(awk -v k=$k '$1 == k && $3 ~ /^-ERR/' "$D/replies" | wc -l | tr -d ' ')
  [ "$n" -gt 0 ] || { echo "FAIL (BUG-0283): $k was never refused after plain SET was; its refusal is not reaching the client"; exit 1; }
done
echo "  each kind refused at least once: $(awk '$1 == "HSET" && $3 ~ /^-ERR/ {$1 = $2 = ""; print; exit}' "$D/replies" | cut -c3-90)"

echo "== the seat, whose engine now refuses every write, ends itself"
for _ in $(seq 1 150); do kill -0 "$SEAT" 2>/dev/null || break; sleep 0.1; done
if kill -0 "$SEAT" 2>/dev/null; then
  echo "FAIL (BUG-0285): 15 s after its engine refused a write the seat is still up, refusing every"
  echo "  write: HSET now answers '$(valkey-cli -p $P HSET late f v 2>&1 | head -1 | cut -c1-80)'"
  exit 1
fi
wait "$SEAT"; rc=$?
[ "$rc" = 70 ] || { echo "FAIL: the seat exited with status $rc, not 70"; tail -3 "$D/s1.log"; exit 1; }
grep -q 'BUG-0285' "$D/s1.log" || { echo "FAIL: the seat exited 70 without saying why"; tail -3 "$D/s1.log"; exit 1; }
echo "  status 70: $(grep 'BUG-0285' "$D/s1.log" | head -1 | cut -c1-110)"

echo "== after a restart without the limit, every write answered as a success is there, and new ones land"
fleet_kill server; sleep 0.4
$B --port $P --engine rocks --data-dir "$D/db" 2>"$D/s2.log" &
fleet_wait_listen $P; fleet_wait_ping $P
rw check "$D/replies" >"$D/lost2"
if [ -s "$D/lost2" ]; then
  echo "FAIL (BUG-0283): $(wc -l <"$D/lost2" | tr -d ' ') writes answered as a success are not there:"
  head -6 "$D/lost2" | sed 's/^/  /'
  echo "  e.g. $(awk '$1 == "HSET" && $3 !~ /^-/' "$D/replies" | tail -1)"
  exit 1
fi
[ "$(valkey-cli -p $P HSET after f v | tr -d '\r')" = 1 ] && [ "$(valkey-cli -p $P HGET after f | tr -d '\r')" = v ] \
  || { echo "FAIL: the restarted seat does not take writes"; tail -3 "$D/s2.log"; exit 1; }
echo "  all there; HSET after the restart: written and read back"

echo "PASS: a write the engine refused is answered as an error, whatever the command (BUG-0283); the"
echo "      seat then ends itself, status 70 (BUG-0285); every write answered as a success is there."
