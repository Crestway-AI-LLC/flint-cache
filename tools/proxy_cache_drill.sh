#!/usr/bin/env bash
# Proxy near-cache drill (ADR-0005 D6, revised — proxy-local, opt-in).
#   - tenant consent gate: acme opts in (CPTENANTCACHE/#c); globex does not
#   - operator runtime knobs: PROXYCACHE <ttl_ms> <max_bytes> on a LIVE proxy
#   - a repeated GET serves from the cache (hits counted; and the acid test:
#     it still answers after the MASTER DIES, within TTL)
#   - staleness bound, BOTH edges: a write straight to the node (bypassing
#     the proxy) is invisible within TTL, visible after it lapses
#   - read-your-own-writes: a write through the SAME proxy invalidates
#   - byte budget honored under key spray
#   - ttl 0 disables AND clears at runtime
set -u
cd "$(dirname "$0")/.."
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
D=/tmp/flint-pcache; rm -rf "$D"; mkdir -p "$D"
pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
pkill -9 -f flint-controlplane 2>/dev/null; sleep 0.4
cleanup() {
  pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
  pkill -9 -f flint-controlplane 2>/dev/null; rm -rf "$D"
}
trap cleanup EXIT

echo "== cluster: CP + master + proxy; acme consents to the cache, globex does not"
$CP --port 7660 --state "$D/cp" 2>/dev/null &
for i in $(seq 1 30); do [ "$(valkey-cli -p 7660 PING 2>/dev/null)" = "PONG" ] && break; sleep 0.2; done
valkey-cli -p 7660 CPADDPROXY 127.0.0.1:7881 >/dev/null
valkey-cli -p 7660 CPADDPAIR 127.0.0.1:6970 >/dev/null
valkey-cli -p 7660 CPADDTENANT acme tok-acme acme 1 >/dev/null
valkey-cli -p 7660 CPADDTENANT globex tok-glx globex 1 >/dev/null
valkey-cli -p 7660 CPTENANTCACHE acme on >/dev/null || { echo "FAIL: CPTENANTCACHE"; exit 1; }
$B --port 6970 --engine rocks --data-dir "$D/m" 2>/dev/null &
sleep 0.7
$PX --port 7881 --control-plane 127.0.0.1:7660 --advertise 127.0.0.1:7881 2>"$D/px.log" &
sleep 1.5

echo "== cache starts OFF; operator enables at RUNTIME: PROXYCACHE 1500 65536"
valkey-cli -p 7881 PROXYCACHE | grep -q 'ttl_ms:0' || { echo "FAIL: cache not off at boot"; exit 1; }
[ "$(valkey-cli -p 7881 PROXYCACHE 1500 65536)" = "OK" ] || { echo "FAIL: PROXYCACHE set"; exit 1; }
valkey-cli -p 7881 PROXYCACHE | grep -q 'ttl_ms:1500' || { echo "FAIL: runtime ttl not applied"; exit 1; }
echo "  ttl=1500ms max_bytes=65536, applied to the live proxy"

A="valkey-cli -p 7881 -a tok-acme --no-auth-warning"
G="valkey-cli -p 7881 -a tok-glx --no-auth-warning"

echo "== populate + repeated GET -> hits accrue (opted-in tenant only)"
$A SET k1 v1 >/dev/null
$A GET k1 >/dev/null   # miss, fills
H0=$(valkey-cli -p 7881 PROXYCACHE | grep hits_total | cut -d: -f2 | tr -d '\r')
for i in 1 2 3 4 5; do $A GET k1 >/dev/null; done
H1=$(valkey-cli -p 7881 PROXYCACHE | grep hits_total | cut -d: -f2 | tr -d '\r')
[ "$H1" -ge $((H0+5)) ] || { echo "FAIL: hits did not accrue ($H0 -> $H1)"; exit 1; }
echo "  hits: $H0 -> $H1"

echo "== consent gate: globex (no CPTENANTCACHE) never touches the cache"
$G SET g1 gv >/dev/null; $G GET g1 >/dev/null; $G GET g1 >/dev/null
M0=$(valkey-cli -p 7881 PROXYCACHE | grep -E 'hits_total|misses_total' | awk -F: '{s+=$2} END{print s}')
$G GET g1 >/dev/null; $G GET g1 >/dev/null
M1=$(valkey-cli -p 7881 PROXYCACHE | grep -E 'hits_total|misses_total' | awk -F: '{s+=$2} END{print s}')
[ "$M1" -eq "$M0" ] || { echo "FAIL: non-consented tenant moved cache counters"; exit 1; }
echo "  globex GETs pass straight through (no hit, no miss recorded)"

echo "== read-your-own-writes: a write through THIS proxy invalidates"
$A SET k1 v2 >/dev/null
V=$($A GET k1)
[ "$V" = "v2" ] || { echo "FAIL: same-proxy write did not invalidate (got $V)"; exit 1; }
echo "  SET k1 v2 via proxy -> immediate GET sees v2"

echo "== staleness bound, edge 1: a write BEHIND the proxy is invisible within TTL"
$A GET k1 >/dev/null  # re-fill cache with v2
# Write v3 straight to the node (FLINTNS + SET on the data port) — no proxy.
python3 - <<'PY'
import socket
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
s=socket.create_connection(("127.0.0.1",6970),timeout=5); s.settimeout(5)
s.sendall(resp(["FLINTNS","acme"])); s.recv(64)
s.sendall(resp(["SET","k1","v3"])); s.recv(64)
PY
V=$($A GET k1)
[ "$V" = "v2" ] || { echo "FAIL: expected the CACHED v2 within TTL (got $V)"; exit 1; }
echo "  node holds v3, proxy still serves cached v2 (the tenant-accepted window)"

echo "== staleness bound, edge 2: after TTL lapses the fresh value appears"
sleep 1.7
V=$($A GET k1)
[ "$V" = "v3" ] || { echo "FAIL: staleness outlived the TTL (got $V)"; exit 1; }
echo "  TTL lapsed -> GET k1 = v3 (bound holds on both edges)"

echo "== the acid test: cached GET answers with the MASTER DEAD"
$A SET k9 alive >/dev/null; $A GET k9 >/dev/null   # fill
pkill -9 -f "flint-server --port 6970"; sleep 0.3
V=$($A GET k9)   # a HIT answers locally and instantly; a miss would error after the retry budget
[ "$V" = "alive" ] || { echo "FAIL: cache did not serve with backend down (got '$V')"; exit 1; }
echo "  GET k9 = alive, served with zero backends up — locally, provably from the cache"
$B --port 6970 --engine rocks --data-dir "$D/m" 2>/dev/null &   # restore for the rest
sleep 0.9

echo "== byte budget honored under key spray"
python3 - <<'PY'
import socket
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
s=socket.create_connection(("127.0.0.1",7881),timeout=10); s.settimeout(10)
s.sendall(resp(["AUTH","tok-acme"])); s.recv(64)
def rt(*a):
    s.sendall(resp(list(a)))
    b=b""
    while not b.endswith(b"\r\n"): b+=s.recv(4096)
for i in range(300):
    rt("SET",f"spray:{i}","x"*400)
    rt("GET",f"spray:{i}")   # each fills ~400B into a 64KiB budget
PY
BYTES=$(valkey-cli -p 7881 PROXYCACHE | grep '^bytes' | cut -d: -f2 | tr -d '\r')
[ "$BYTES" -le 65536 ] || { echo "FAIL: budget exceeded ($BYTES > 65536)"; exit 1; }
echo "  300 x ~400B fills, resident bytes = $BYTES <= 65536"

echo "== runtime disable: PROXYCACHE 0 0 clears and stops serving stale"
python3 - <<'PY'
import socket
def resp(a):
    return f"*{len(a)}\r\n".encode()+b"".join(f"${len(x)}\r\n{x}\r\n".encode() for x in a)
s=socket.create_connection(("127.0.0.1",6970),timeout=5); s.settimeout(5)
s.sendall(resp(["FLINTNS","acme"])); s.recv(64)
s.sendall(resp(["SET","k1","v4"])); s.recv(64)   # behind the proxy again
PY
$A GET k1 >/dev/null   # may refresh cache with v4 or hold v3; either way:
[ "$(valkey-cli -p 7881 PROXYCACHE 0 0)" = "OK" ] || { echo "FAIL: disable"; exit 1; }
ST=$(valkey-cli -p 7881 PROXYCACHE)
echo "$ST" | grep -q 'entries:0' || { echo "FAIL: disable did not clear"; exit 1; }
V=$($A GET k1)
[ "$V" = "v4" ] || { echo "FAIL: disabled cache still interfering (got $V)"; exit 1; }
echo "  disabled + cleared; reads are straight pass-through again (k1 = v4)"

echo "== PROXYSTATS carries the cache series for the exporter"
valkey-cli -p 7881 PROXYSTATS | grep -E 'cache_(hits|misses|bytes)' | sed 's/^/  | /'
valkey-cli -p 7881 PROXYSTATS | grep -q 'cache_hits_total' || { echo "FAIL: stats missing cache fields"; exit 1; }

echo "PASS: proxy near-cache — consent-gated, TTL-bounded on both edges, budget-bounded, runtime-tunable"
