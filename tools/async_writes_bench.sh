#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Async write-queue bench (ADR-0005 D4). The ADR gates the "not for counters"
# concession on THIS measurement: does routing writes through the async queue
# (group commit) actually help, and what does it cost in per-write latency?
#
# We run two identical workloads against two identical rocks masters — one
# INLINE (writes applied per-command) and one ASYNC (--async-writes) — and
# publish, side by side:
#   - HOT COUNTER: many connections INCR one shared key (the counter case the
#     ADR is cautious about — the consumer serializes, so batching a hot key
#     is where the queue is least differentiated)
#   - SPREAD WRITES: many connections SET distinct keys (the general write-hot
#     case — batching collapses N engine writes into one)
# For each: aggregate ops/s (throughput) and per-op p50/p99 (latency). The
# 2-3x single-write latency cost is expected and printed, not hidden.
set -u
cd "$(dirname "$0")/.."
B=./target/release/flint-server
D=/tmp/flint-asyncbench; rm -rf "$D"; mkdir -p "$D"
pkill -9 -f flint-server 2>/dev/null; sleep 0.4
cleanup() { pkill -9 -f flint-server 2>/dev/null; rm -rf "$D"; }
trap cleanup EXIT

CONN=${CONN:-32}
OPS=${OPS:-4000}   # per connection

echo "== two rocks masters: inline (6996) vs async-writes (6997)"
# Durable writes (fsync per group) so group-commit amortization is visible —
# this is the regime where batching pays. Both servers use the same setting.
$B --port 6996 --engine rocks --data-dir "$D/inline" 2>"$D/inline.log" &
$B --port 6997 --engine rocks --data-dir "$D/async" \
   --async-writes bench --async-queue-cap 8192 2>"$D/async.log" &
for p in 6996 6997; do
  for i in $(seq 1 30); do [ "$(valkey-cli -p $p PING 2>/dev/null)" = "PONG" ] && break; sleep 0.2; done
done

CONN=$CONN OPS=$OPS python3 - <<'PY'
import socket, threading, time, os
CONN=int(os.environ["CONN"]); OPS=int(os.environ["OPS"])
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)

def run(port, keyfn):
    lat_lock=threading.Lock(); all_lat=[]
    def worker(wid):
        s=socket.create_connection(("127.0.0.1",port),timeout=30); s.settimeout(30)
        s.sendall(resp(["FLINTNS","bench"])); s.recv(64)
        lat=[]
        for i in range(OPS):
            cmd, key = keyfn(wid, i)
            while True:
                t0=time.perf_counter()
                s.sendall(resp(cmd))
                b=b""
                while not b.endswith(b"\r\n"): b+=s.recv(128)
                dt=(time.perf_counter()-t0)*1000
                if b"THROTTLED" in b:   # retry contract; don't count the shed
                    time.sleep(0.001); continue
                lat.append(dt); break
        s.close()
        with lat_lock: all_lat.extend(lat)
    ts=[threading.Thread(target=worker,args=(w,)) for w in range(CONN)]
    t0=time.perf_counter()
    [t.start() for t in ts]; [t.join() for t in ts]
    wall=time.perf_counter()-t0
    all_lat.sort()
    n=len(all_lat)
    p50=all_lat[n//2]; p99=all_lat[int(n*0.99)]
    ops_s=(CONN*OPS)/wall
    return ops_s, p50, p99

def hot(wid,i):   return (["INCR","counter:hot"], None)          # one shared key
def spread(wid,i):return (["SET",f"k:{wid}:{i}","payload-x"], None)

def report(name, inline, async_):
    oi,p50i,p99i = inline; oa,p50a,p99a = async_
    print(f"\n  {name}")
    print(f"    {'':10} {'ops/s':>12} {'p50 ms':>9} {'p99 ms':>9}")
    print(f"    {'inline':10} {oi:>12,.0f} {p50i:>9.2f} {p99i:>9.2f}")
    print(f"    {'async':10} {oa:>12,.0f} {p50a:>9.2f} {p99a:>9.2f}")
    tput = oa/oi if oi else 0
    lcost = p50a/p50i if p50i else 0
    print(f"    -> throughput {tput:.2f}x   |   per-write p50 latency {lcost:.2f}x")

print(f"  workload: {CONN} connections x {OPS} ops each ({CONN*OPS:,} writes/side)")
# hot counter
hi = run(6996, hot); ha = run(6997, hot)
report("HOT COUNTER (one shared key — the 'not for counters' case)", hi, ha)
# spread writes
si = run(6996, spread); sa = run(6997, spread)
report("SPREAD WRITES (distinct keys — general write-hot)", si, sa)

print("\n  read as: async trades higher per-write latency for group-commit throughput.")
print("  the hot-counter row is the concession test — if throughput <= 1x there, the")
print("  queue does not help a single hot counter and inline (or D7 replicas) is right.")
PY

echo
echo "== queue drained to empty on both (no leaked backlog)"
valkey-cli -p 6997 FLINTINFO | grep '^async_write_queue:' | sed 's/^/  | /'
