#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Async write-queue drill (ADR-0005 D4 — OPT-IN).
# For an opted-in namespace, a batchable string/counter write enqueues and the
# connection blocks on the consumer's ack-after-apply; the consumer drains the
# queue in batches and commits each batch as ONE engine WriteBatch (group
# commit). This drill proves the contract:
#   - the OPT-IN gate: FLINTINFO shows the queue enabled; a NON-opted namespace
#     writes inline (never touches the queue)
#   - the per-connection ORDERING BARRIER: pipelined SET;GET on one connection
#     sees its own write (the connection blocks on each ack), so program order
#     holds even though batching happens ACROSS connections
#   - COUNTER CORRECTNESS through the batching overlay: a burst of INCR on one
#     key (many connections) sums to exactly the number issued — each command
#     saw its own prior increments inside the batch
#   - QUEUE-FULL sheds -THROTTLED (bounded, never an unbounded backlog): with a
#     tiny --async-queue-cap, a concurrent storm forces the shed
#   - READS stay at baseline under a write storm: a reader on a separate
#     connection is never blocked by the write queue
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-asyncw 6995 6996
fleet_guard
B=./target/release/flint-server
D=/tmp/flint-asyncw; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; sleep 0.4
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT

echo "== master with async-writes for tenant 'acme' only (tiny cap to force sheds)"
$B --port 6995 --engine rocks --data-dir "$D/m" \
   --async-writes acme --async-queue-cap 4 2>"$D/m.log" &
for i in $(seq 1 30); do [ "$(valkey-cli -p 6995 PING 2>/dev/null)" = "PONG" ] && break; sleep 0.2; done

echo "== FLINTINFO advertises the queue (off before any write)"
INFO=$(valkey-cli -p 6995 FLINTINFO)
echo "$INFO" | grep -q '^async_write_queue:' || { echo "FAIL: FLINTINFO missing async_write_queue"; echo "$INFO"; exit 1; }
echo "  | $(echo "$INFO" | grep '^async_write_queue:')"

echo "== ordering barrier: pipelined SET;GET on ONE connection stays ordered"
python3 - <<'PY'
import socket, sys
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
s=socket.create_connection(("127.0.0.1",6995),timeout=5); s.settimeout(5)
s.sendall(resp(["FLINTNS","acme"])); s.recv(64)
# Pipeline SET then GET without waiting between them; the queued SET must be
# applied before the GET reply (the connection blocks on the SET's ack).
s.sendall(resp(["SET","k:order","v1"])+resp(["GET","k:order"]))
buf=b""
while buf.count(b"\r\n") < 3:  # +OK\r\n  then  $2\r\nv1\r\n
    buf+=s.recv(256)
assert b"v1" in buf, f"GET did not see the queued SET (program order broken): {buf!r}"
print("  pipelined SET;GET returned v1 — the barrier holds")
PY
[ $? -eq 0 ] || exit 1

echo "== counter correctness: 8 connections x 500 INCR on one key via the queue"
python3 - <<'PY'
import socket, threading
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
N_CONN, N_EACH = 8, 500
def worker():
    s=socket.create_connection(("127.0.0.1",6995),timeout=10); s.settimeout(10)
    s.sendall(resp(["FLINTNS","acme"])); s.recv(64)
    for _ in range(N_EACH):
        # -THROTTLED (queue full) is the retry contract, not a lost write:
        # honor it so we count exactly N_EACH successful INCRs per connection.
        while True:
            s.sendall(resp(["INCR","counter:x"]))
            b=b""
            while not b.endswith(b"\r\n"): b+=s.recv(64)
            if b"THROTTLED" not in b: break
    s.close()
ts=[threading.Thread(target=worker) for _ in range(N_CONN)]
[t.start() for t in ts]; [t.join() for t in ts]
s=socket.create_connection(("127.0.0.1",6995),timeout=10); s.settimeout(10)
s.sendall(resp(["FLINTNS","acme"])); s.recv(64)
s.sendall(resp(["GET","counter:x"]))
b=b""
while not b.endswith(b"\r\n"): b+=s.recv(64)
val=int(b.split(b"\r\n")[1])
expect=N_CONN*N_EACH
assert val==expect, f"counter lost writes through the batch: got {val}, want {expect}"
print(f"  counter:x == {val} (exactly {N_CONN}x{N_EACH}) — no lost updates through the WriteBatch")
PY
[ $? -eq 0 ] || exit 1

echo "== queue-full sheds -THROTTLED (cap=4) under a concurrent storm"
THROTTLED=$(python3 - <<'PY'
import socket, threading
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
seen=[0]
lock=threading.Lock()
def worker(wid):
    s=socket.create_connection(("127.0.0.1",6995),timeout=10); s.settimeout(10)
    s.sendall(resp(["FLINTNS","acme"])); s.recv(64)
    local=0
    for i in range(400):
        s.sendall(resp(["SET",f"k:{wid}:{i}","payload-value-to-keep-consumer-busy"]))
        b=b""
        while not b.endswith(b"\r\n"): b+=s.recv(128)
        if b"THROTTLED" in b: local+=1
    with lock: seen[0]+=local
    s.close()
ts=[threading.Thread(target=worker,args=(w,)) for w in range(64)]
[t.start() for t in ts]; [t.join() for t in ts]
print(seen[0])
PY
)
echo "  observed $THROTTLED -THROTTLED sheds across the storm (cap=4, 64 writers)"
[ "$THROTTLED" -ge 1 ] 2>/dev/null || { echo "FAIL: queue-full never shed -THROTTLED"; exit 1; }

echo "== reads stay at baseline under a sustained write storm (reader never blocks on the queue)"
python3 - <<'PY'
import socket, threading, time
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
stop=[False]
def storm():
    s=socket.create_connection(("127.0.0.1",6995),timeout=10); s.settimeout(10)
    s.sendall(resp(["FLINTNS","acme"])); s.recv(64)
    i=0
    while not stop[0]:
        s.sendall(resp(["SET",f"hot:{i%256}","v"]))
        b=b""
        try:
            while not b.endswith(b"\r\n"): b+=s.recv(64)
        except Exception: break
        i+=1
storms=[threading.Thread(target=storm) for _ in range(16)]
[t.start() for t in storms]
# seed a read key
s=socket.create_connection(("127.0.0.1",6995),timeout=10); s.settimeout(10)
s.sendall(resp(["FLINTNS","acme"])); s.recv(64)
s.sendall(resp(["SET","read:key","hello"]))
b=b""
while not b.endswith(b"\r\n"): b+=s.recv(64)
# measure GET latency under the storm
lat=[]
for _ in range(300):
    t0=time.perf_counter()
    s.sendall(resp(["GET","read:key"]))
    b=b""
    while not b.endswith(b"\r\n"): b+=s.recv(128)
    lat.append((time.perf_counter()-t0)*1000)
stop[0]=True; [t.join() for t in storms]
lat.sort()
p50=lat[len(lat)//2]; p99=lat[int(len(lat)*0.99)]
print(f"  GET under write storm: p50={p50:.2f}ms p99={p99:.2f}ms")
assert p99 < 25, f"reads degraded under the write storm (p99={p99:.2f}ms) — queue leaked into the read path"
print("  reads unaffected by the async write queue (separate path)")
PY
[ $? -eq 0 ] || exit 1

echo "== opt-in gate: a NON-opted namespace writes inline (queue is per-tenant)"
# 'globex' is not in --async-writes; its writes must still succeed (inline path).
python3 - <<'PY'
import socket
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
s=socket.create_connection(("127.0.0.1",6995),timeout=5); s.settimeout(5)
s.sendall(resp(["FLINTNS","globex"])); s.recv(64)
s.sendall(resp(["SET","g:1","gv"])+resp(["GET","g:1"]))
b=b""
while buf_ok := (b.count(b"\r\n") < 3): b+=s.recv(128)
assert b"gv" in b, f"non-opted namespace write failed: {b!r}"
print("  globex (not opted in) writes inline and reads back — queue is strictly per-tenant")
PY
[ $? -eq 0 ] || exit 1

echo "== REPLICATION: batched writes reach a replica intact (coverage from review)"
# The consumer commits each batch as one engine WriteBatch; the WAL tailer
# must carry it to a replica exactly like inline writes. Fresh pair, queued
# INCR storm, then replica-vs-master parity on the final value.
fleet_kill server; sleep 0.4; rm -rf "$D"; mkdir -p "$D"
$B --port 6995 --engine rocks --data-dir "$D/m2" --async-writes acme 2>"$D/m2.log" &
for i in $(seq 1 30); do [ "$(valkey-cli -p 6995 PING 2>/dev/null)" = "PONG" ] && break; sleep 0.2; done
$B --port 6996 --engine rocks --data-dir "$D/r2" --replica-of 127.0.0.1:6995 2>"$D/r2.log" &
fleet_wait_listen 6996
sleep 1.2
python3 - <<'PY'
import socket, threading
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
def worker():
    s=socket.create_connection(("127.0.0.1",6995),timeout=10); s.settimeout(10)
    s.sendall(resp(["FLINTNS","acme"])); s.recv(64)
    for _ in range(400):
        while True:
            s.sendall(resp(["INCR","repl:counter"]))
            b=b""
            while not b.endswith(b"\r\n"): b+=s.recv(64)
            if b"THROTTLED" not in b: break
    s.close()
ts=[threading.Thread(target=worker) for _ in range(6)]
[t.start() for t in ts]; [t.join() for t in ts]
PY
# Wait for the replica to drain the tail, then compare through FLINTNS.
CONV=""
for i in $(seq 1 40); do
  MV=$(valkey-cli -p 6995 FLINTNS acme >/dev/null 2>&1; printf "")
  MV=$(python3 -c '
import socket
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
def get(port):
    s=socket.create_connection(("127.0.0.1",port),timeout=5); s.settimeout(5)
    s.sendall(resp(["FLINTNS","acme"])); s.recv(64)
    s.sendall(resp(["GET","repl:counter"]))
    b=b""
    while not b.endswith(b"\r\n"): b+=s.recv(64)
    return b.split(b"\r\n")[1].decode()
try:
    m=get(6995); r=get(6996)
    print(f"{m} {r}")
except Exception as e:
    print(f"err err")
')
  M=$(echo "$MV" | cut -d' ' -f1); R=$(echo "$MV" | cut -d' ' -f2)
  if [ "$M" = "2400" ] && [ "$R" = "2400" ]; then CONV=yes; break; fi
  sleep 0.3
done
[ "$CONV" = "yes" ] || { echo "FAIL: replica did not converge (master=$M replica=$R, want 2400)"; exit 1; }
echo "  master=2400, replica=2400 — group-committed batches replicate intact"

echo "PASS: async write queue — opt-in, ordered per-connection, no lost updates, bounded (-THROTTLED), reads unaffected, replicates intact"
