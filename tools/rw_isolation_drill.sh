#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0005 D1 drill: read/write path independence ACROSS clients, pinned.
#
# The architecture already guarantees this structurally (per-client backend
# connections at the proxy, thread-per-connection at the server, LSM reads
# that never wait behind writes) — this drill exists so a future refactor
# (shared pools, async proxy) can never silently regress it.
#   - client A pipelines a sustained large-value WRITE STORM at the node
#   - client B concurrently samples GET latency on the same node
#   - B's read latency must stay flat: reads are never queued behind
#     another client's writes anywhere in the path
#   - the shared classifier's traffic split (PROXYSTATS
#     commands_read/write_total) counts both sides correctly
set -u
cd "$(dirname "$0")/.."
B=./target/release/flint-server
PX=./target/release/flint-proxy
D=/tmp/flint-rwiso; rm -rf "$D"; mkdir -p "$D"
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null; sleep 0.4
cleanup() { pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null; rm -rf "$D"; }
trap cleanup EXIT

$B --port 6940 --engine rocks --data-dir "$D/m" 2>/dev/null &
sleep 0.7
$PX --port 7861 --pairs "127.0.0.1:6940" 2>/dev/null &
sleep 1.0
valkey-cli -p 7861 SET readkey readval >/dev/null

python3 - <<'PY'
import json, socket, statistics, threading, time, os, sys

def resp(args):
    out = f"*{len(args)}\r\n".encode()
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        out += b"$" + str(len(a)).encode() + b"\r\n" + a + b"\r\n"
    return out

def read_reply(s, buf=b""):
    while True:
        i = buf.find(b"\r\n")
        if i >= 0:
            head = buf[:i]
            if head.startswith(b"$") and head != b"$-1":
                need = int(head[1:]) + i + 4
                while len(buf) < need:
                    buf += s.recv(65536)
                return buf[need:]
            return buf[i + 2:]
        buf += s.recv(65536)

def conn():
    s = socket.create_connection(("127.0.0.1", 7861), timeout=5)
    s.settimeout(5)
    return s

def sample_reads(n, out):
    s = conn()
    buf = b""
    lat = []
    for _ in range(n):
        t0 = time.perf_counter()
        s.sendall(resp(["GET", "readkey"]))
        buf = read_reply(s, buf)
        lat.append((time.perf_counter() - t0) * 1000.0)
    s.close()
    out.extend(lat)

# Baseline read latency, quiet node.
base = []
sample_reads(300, base)
base_p50 = statistics.median(base)
base_p99 = statistics.quantiles(base, n=100)[98]
print(f"baseline read: p50 {base_p50:.3f}ms p99 {base_p99:.3f}ms")

# Write storm: client A pipelines 4KB high-entropy SETs flat-out.
stop = threading.Event()
storm_count = [0]
def storm():
    s = conn()
    buf = b""
    v = os.urandom(2048).hex()
    while not stop.is_set():
        # Pipeline 32 SETs, then drain replies.
        for i in range(32):
            s.sendall(resp(["SET", f"storm:{storm_count[0] + i}", v]))
        for _ in range(32):
            buf = read_reply(s, buf)
        storm_count[0] += 32
    s.close()

t = threading.Thread(target=storm)
t.start()
time.sleep(0.5)  # storm warmed up

under = []
sample_reads(600, under)
stop.set()
t.join()

u_p50 = statistics.median(under)
u_p99 = statistics.quantiles(under, n=100)[98]
print(f"under {storm_count[0]} storm writes: read p50 {u_p50:.3f}ms p99 {u_p99:.3f}ms")

# The invariant: reads from another client stay flat under the storm.
# Bounds are generous for CI noise but far below what serialized
# read-behind-writes would produce (tens to hundreds of ms).
assert u_p50 < 5.0, f"read p50 degraded under a foreign write storm: {u_p50:.3f}ms"
assert u_p99 < 25.0, f"read p99 degraded under a foreign write storm: {u_p99:.3f}ms"
print("isolation holds: another client's write storm did not queue our reads")

with open("/tmp/flint-rwiso/counts", "w") as f:
    json.dump({"reads": 900, "writes": storm_count[0]}, f)
PY
[ $? -eq 0 ] || exit 1

echo "== the shared classifier's traffic split counted both sides"
STATS=$(valkey-cli -p 7861 PROXYSTATS)
READS=$(echo "$STATS" | tr '\r' '\n' | grep "^commands_read_total:" | cut -d: -f2)
WRITES=$(echo "$STATS" | tr '\r' '\n' | grep "^commands_write_total:" | cut -d: -f2)
echo "  commands_read_total=$READS commands_write_total=$WRITES"
[ "$READS" -ge 900 ] || { echo "FAIL: read counter did not track GETs"; exit 1; }
[ "$WRITES" -ge 500 ] || { echo "FAIL: write counter did not track the storm"; exit 1; }

echo "PASS: cross-client read/write isolation pinned (storm-proof reads) + shared-classifier traffic split live"
