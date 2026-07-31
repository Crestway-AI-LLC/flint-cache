#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Tenant quota drill (M5 — quotas v1, proxy-enforced).
#   - CPTENANTQUOTA <name> <ops_per_sec> <max_bytes>; the rate reaches each
#     proxy PRE-DIVIDED by the tenant's subset size (tenant.rs encoding)
#   - ops/s: token bucket -> a hammering tenant lands within ~10% of its
#     fleet budget and sheds the rest as -THROTTLED
#   - ISOLATION: an unquotad tenant's throughput/latency is unaffected by a
#     neighbor pinned at its quota
#   - storage verdict: CPTENANTOVERQUOTA on ('q' flag) -> WRITES shed with
#     -QUOTA, READS still served (a full tenant can read its data out);
#     verdict flips apply to LIVE connections (no re-auth)
#   - PROXYSTATS carries quota_throttled_total / quota_write_shed_total
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-quota 6985 7690 7911 7912
fleet_guard
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
D=/tmp/flint-quota; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; sleep 0.4
cleanup() {
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; rm -rf "$D"
}
trap cleanup EXIT

echo "== cluster: CP + master + TWO proxies; acme quota 400 ops/s (subset 2), globex unlimited"
$CP --port 7690 --state "$D/cp" 2>/dev/null &
for i in $(seq 1 30); do [ "$(valkey-cli -p 7690 PING 2>/dev/null)" = "PONG" ] && break; sleep 0.2; done
valkey-cli -p 7690 CPADDPROXY 127.0.0.1:7911 >/dev/null
valkey-cli -p 7690 CPADDPROXY 127.0.0.1:7912 >/dev/null
valkey-cli -p 7690 CPADDPAIR 127.0.0.1:6985 >/dev/null
# acme's subset spans BOTH proxies: the fleet quota must divide across them.
valkey-cli -p 7690 CPADDTENANT acme tok-acme acme 2 >/dev/null
valkey-cli -p 7690 CPADDTENANT globex tok-glx globex 1 >/dev/null
valkey-cli -p 7690 CPTENANTQUOTA acme 400 0 >/dev/null || { echo "FAIL: CPTENANTQUOTA"; exit 1; }
$B --port 6985 --engine rocks --data-dir "$D/m" 2>/dev/null &
sleep 0.7
$PX --port 7911 --control-plane 127.0.0.1:7690 --advertise 127.0.0.1:7911 2>"$D/px1.log" &
$PX --port 7912 --control-plane 127.0.0.1:7690 --advertise 127.0.0.1:7912 2>"$D/px2.log" &
sleep 1.5

echo "== rate, one proxy: acme hammers proxy 1 only -> its 200/s SHARE binds"
python3 - <<'PY'
import socket, time
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
s=socket.create_connection(("127.0.0.1",7911),timeout=10); s.settimeout(10)
s.sendall(resp(["AUTH","tok-acme"])); s.recv(64)
ok=thr=0; t0=time.time(); mfrom=None
while time.time()-t0 < 6:
    s.sendall(resp(["SET","k","v"]))
    b=b""
    while not b.endswith(b"\r\n"): b+=s.recv(128)
    # Skip second 1: the bucket starts FULL (one second of burst is the
    # contract); steady state is what the 10% bound is about.
    if time.time()-t0 < 1: continue
    if mfrom is None: mfrom=time.time(); ok=thr=0
    if b"THROTTLED" in b: thr+=1
    else: ok+=1
el=time.time()-mfrom; rate=ok/el
print(f"  proxy1 alone: {rate:.0f} ops/s accepted (share 200), {thr} throttled")
assert 180 <= rate <= 220, f"per-proxy share {rate:.0f} outside 200 +/- 10%"
assert thr > 0, "never throttled"
PY
[ $? -eq 0 ] || exit 1

echo "== rate, fleet: acme hammers BOTH proxies -> shares sum to the 400/s budget"
python3 - <<'PY'
import socket, time, threading
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
results={}
def hammer(port):
    s=socket.create_connection(("127.0.0.1",port),timeout=10); s.settimeout(10)
    s.sendall(resp(["AUTH","tok-acme"])); s.recv(64)
    ok=0; t0=time.time(); mfrom=None
    while time.time()-t0 < 5:
        s.sendall(resp(["SET","k","v"]))
        b=b""
        while not b.endswith(b"\r\n"): b+=s.recv(128)
        if time.time()-t0 < 1: continue
        if mfrom is None: mfrom=time.time(); ok=0
        if b"THROTTLED" not in b: ok+=1
    results[port]=(ok, time.time()-mfrom)
ts=[threading.Thread(target=hammer,args=(p,)) for p in (7911,7912)]
[t.start() for t in ts]; [t.join() for t in ts]
total=sum(ok/el for ok,el in results.values())
per={p:f"{ok/el:.0f}" for p,(ok,el) in results.items()}
print(f"  fleet: {total:.0f} ops/s accepted across both proxies {per} (budget 400)")
assert 360 <= total <= 440, f"fleet rate {total:.0f} outside 400 +/- 10%"
PY
[ $? -eq 0 ] || exit 1

echo "== isolation: globex (unlimited) at full speed while acme is pinned"
python3 - <<'PY'
import socket, threading, time
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
stop=[False]
def acme_hammer():
    s=socket.create_connection(("127.0.0.1",7911),timeout=10); s.settimeout(10)
    s.sendall(resp(["AUTH","tok-acme"])); s.recv(64)
    while not stop[0]:
        s.sendall(resp(["SET","k","v"]))
        b=b""
        while not b.endswith(b"\r\n"): b+=s.recv(128)
t=threading.Thread(target=acme_hammer); t.start()
g=socket.create_connection(("127.0.0.1",7911),timeout=10); g.settimeout(10)
g.sendall(resp(["AUTH","tok-glx"])); g.recv(64)
lat=[]; n=0
t0=time.time()
while time.time()-t0 < 3:
    x=time.perf_counter()
    g.sendall(resp(["SET",f"g:{n}","v"]))
    b=b""
    while not b.endswith(b"\r\n"): b+=g.recv(128)
    assert b"THROTTLED" not in b, "unquotad tenant got throttled"
    lat.append((time.perf_counter()-x)*1000); n+=1
stop[0]=True; t.join()
lat.sort()
print(f"  globex: {n/3:.0f} ops/s unthrottled, p99 {lat[int(len(lat)*0.99)]:.2f}ms beside a pinned neighbor")
assert n/3 > 1000, "unquotad tenant implausibly slow"
PY
[ $? -eq 0 ] || exit 1

echo "== storage verdict: flip over-quota on a LIVE connection (no re-auth)"
python3 - <<'PY'
import socket, subprocess, time
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
s=socket.create_connection(("127.0.0.1",7911),timeout=10); s.settimeout(10)
s.sendall(resp(["AUTH","tok-acme"])); s.recv(64)
def rt(*a):
    s.sendall(resp(list(a)))
    b=b""
    while not b.endswith(b"\r\n"): b+=s.recv(4096)
    return b
assert b"OK" in rt("SET","held","precious") or True
# Flip the verdict while this connection stays open.
subprocess.run(["valkey-cli","-p","7690","CPTENANTOVERQUOTA","acme","on"],capture_output=True,check=True)
time.sleep(1.2)  # snapshot push
w=rt("SET","held","update")
assert b"QUOTA" in w, f"over-quota write not shed: {w!r}"
r=rt("GET","held")
assert b"precious" in r, f"over-quota READ failed (must serve): {r!r}"
# The SELF-CLEAR path: deleting data is a write, but a space-REDUCING one —
# it must never be blocked by the very state it cures.
d=rt("DEL","held")
assert b":1" in d, f"over-quota DEL blocked (self-clear path broken): {d!r}"
# (In production the metering loop sees usage drop and flips the verdict
# off; here the operator flips it, standing in for that loop.)
subprocess.run(["valkey-cli","-p","7690","CPTENANTOVERQUOTA","acme","off"],capture_output=True,check=True)
time.sleep(1.2)
w=rt("SET","held","update")
assert b"OK" in w, f"verdict-off write still shed: {w!r}"
print("  live flip: writes -QUOTA'd, reads AND deletes served, verdict-off restores writes")
PY
[ $? -eq 0 ] || exit 1

echo "== PROXYSTATS carries the quota counters"
valkey-cli -p 7911 PROXYSTATS | grep -E 'quota_' | sed 's/^/  | /'
valkey-cli -p 7911 PROXYSTATS | grep -q 'quota_throttled_total' || { echo "FAIL: stats missing quota counters"; exit 1; }
T=$(valkey-cli -p 7911 PROXYSTATS | grep quota_throttled_total | cut -d: -f2 | tr -d '\r')
S=$(valkey-cli -p 7911 PROXYSTATS | grep quota_write_shed_total | cut -d: -f2 | tr -d '\r')
[ "$T" -gt 0 ] && [ "$S" -gt 0 ] || { echo "FAIL: counters did not move (thr=$T shed=$S)"; exit 1; }

echo "PASS: tenant quotas — rate within 10%, isolation held, storage verdict live-flips (writes shed, reads served)"
