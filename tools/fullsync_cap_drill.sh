#!/usr/bin/env bash
# Full-sync admission-control drill: the master bounds concurrent full-syncs
# so a replica herd can't saturate its disk/network or pin SSTs against
# compaction (found while wiring D7's multi-replica pairs).
#   - master with --max-fullsync 1; seed a non-trivial dataset
#   - launch a HERD of fresh replicas at once -> only 1 full-syncs at a time;
#     the rest get -THROTTLED and RETRY with backoff (they do NOT crash)
#   - FLINTINFO fullsync_active never exceeds the cap while the herd drains
#   - every replica eventually seeds and converges
set -u
cd "$(dirname "$0")/.."
B=./target/release/flint-server
D=/tmp/flint-fscap; rm -rf "$D"; mkdir -p "$D"
pkill -9 -f flint-server 2>/dev/null; sleep 0.4
cleanup() { pkill -9 -f flint-server 2>/dev/null; rm -rf "$D"; }
trap cleanup EXIT

echo "== master (--max-fullsync 1) + a dataset worth streaming"
$B --port 6990 --engine rocks --data-dir "$D/m" --max-fullsync 1 2>"$D/m.log" &
for i in $(seq 1 30); do [ "$(valkey-cli -p 6990 PING 2>/dev/null)" = "PONG" ] && break; sleep 0.2; done
# ~8MB across the SSTs so a full sync takes long enough for the herd to overlap.
python3 - <<'PY'
import socket, os
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
s=socket.create_connection(("127.0.0.1",6990),timeout=10); s.settimeout(10)
v=os.urandom(2048).hex()
for i in range(2000):
    s.sendall(resp(["SET",f"k:{i}",v])); s.recv(64)
s.sendall(resp(["FLINTSNAPSHOT","/tmp/flint-fscap/snap"])); s.recv(256)  # flush to SSTs
PY
echo "  master seeded"

echo "== launch a herd of 6 fresh replicas simultaneously"
for n in 1 2 3 4 5 6; do
  $B --port $((6990+n)) --engine rocks --data-dir "$D/r$n" --replica-of 127.0.0.1:6990 2>"$D/r$n.log" &
done

# Sample fullsync_active on the master while the herd drains; it must never
# exceed the cap of 1.
MAXSEEN=0
for i in $(seq 1 120); do
  A=$(valkey-cli -p 6990 FLINTINFO 2>/dev/null | tr '\r' '\n' | grep '^fullsync_active:' | cut -d: -f2)
  [ -n "$A" ] && [ "$A" -gt "$MAXSEEN" ] 2>/dev/null && MAXSEEN="$A"
  UP=0
  for n in 1 2 3 4 5 6; do [ "$(valkey-cli -p $((6990+n)) PING 2>/dev/null)" = "PONG" ] && UP=$((UP+1)); done
  [ "$UP" = "6" ] && break
  sleep 0.25
done
echo "  peak fullsync_active seen: $MAXSEEN (cap 1); replicas up: $UP/6"
[ "$MAXSEEN" -le 1 ] || { echo "FAIL: concurrent full-syncs exceeded the cap ($MAXSEEN)"; exit 1; }
[ "$UP" = "6" ] || { echo "FAIL: not all replicas seeded ($UP/6)"; for n in 1 2 3 4 5 6; do echo "-- r$n:"; tail -2 "$D/r$n.log"; done; exit 1; }

echo "== throttled replicas retried (not crashed) and then seeded"
THROTTLED=$(grep -l "throttled by master" "$D"/r*.log 2>/dev/null | wc -l | tr -d ' ')
echo "  $THROTTLED of 6 replicas hit -THROTTLED and retried"
[ "$THROTTLED" -ge 1 ] || { echo "FAIL: no replica was throttled — herd didn't overlap (raise the dataset size)"; exit 1; }

echo "== all replicas converged to the master's data"
for n in 1 2 3 4 5 6; do
  [ "$(valkey-cli -p $((6990+n)) GET k:1000)" = "$(valkey-cli -p 6990 GET k:1000)" ] || { echo "FAIL: replica r$n did not converge"; exit 1; }
done
echo "  all 6 replicas serve the master's data"

echo "PASS: full-sync admission control — concurrent full-syncs bounded to the cap, over-cap replicas throttled+retried (never crashed), whole herd seeded"
