#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
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
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-pcache 6970 6971 7660 7881
fleet_guard
B=./target/release/flint-server
CP=./target/release/flint-controlplane
PX=./target/release/flint-proxy
D=$FLINT_DRILL_ROOT/flint-pcache; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; fleet_kill proxy
fleet_kill controlplane; sleep 0.4
cleanup() {
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; rm -rf "$D"
}
trap cleanup EXIT

echo "== cluster: CP + master + proxy; acme consents to the cache, globex does not"
$CP --port 7660 --state "$D/cp" 2>"${FLEET_SCOPE}cp.log" &
fleet_wait_ping 7660
fleet_cp 7660 CPADDPROXY 127.0.0.1:7881
# Both pair members registered so the proxy can chase a failover to 6971
# (the FAILOVER section below promotes it).
fleet_cp 7660 CPADDPAIR 127.0.0.1:6970,127.0.0.1:6971
fleet_cp 7660 CPADDTENANT acme tok-acme acme 1
fleet_cp 7660 CPADDTENANT globex tok-glx globex 1
valkey-cli -p 7660 CPTENANTCACHE acme on >/dev/null || { echo "FAIL: CPTENANTCACHE"; exit 1; }
$B --port 6970 --engine rocks --data-dir "$D/m" 2>"${FLEET_SCOPE}server.log" &
fleet_wait_listen 6970
sleep 0.7
$PX --port 7881 --control-plane 127.0.0.1:7660 --advertise 127.0.0.1:7881 2>"$D/px.log" &
fleet_wait_listen 7881
sleep 1.5

echo "== cache defaults ON (ttl 300 ms, 256 MB); operator retunes at RUNTIME: PROXYCACHE 1500 65536"
valkey-cli -p 7881 PROXYCACHE | grep -q 'ttl_ms:300' || { echo "FAIL: default ttl not 300"; exit 1; }
valkey-cli -p 7881 PROXYCACHE | grep -q 'max_bytes:268435456' || { echo "FAIL: default budget not 256MB"; exit 1; }
[ "$(valkey-cli -p 7881 PROXYCACHE 1500 65536)" = "OK" ] || { echo "FAIL: PROXYCACHE set"; exit 1; }
valkey-cli -p 7881 PROXYCACHE | grep -q 'ttl_ms:1500' || { echo "FAIL: runtime ttl not applied"; exit 1; }
echo "  defaults verified, then ttl=1500ms max_bytes=65536 applied to the live proxy"

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

echo "== read-your-own-writes holds for EVERY key of a multi-key write (MSET)"
# Regression for the review-found bug: only MSET's FIRST key was invalidated,
# so a cached later key kept serving its old value through the same proxy.
#
# COLOCATED UNDER A HASH TAG (BUG-0053). Bare `ma`/`mb` land in slots 497 and
# 12690, and MSET now refuses cross-slot rather than writing a key onto a node
# that does not own it. That refusal made this step fail with an empty GET,
# which reads as "the cache was not invalidated" and is really "the write
# never happened". What the step is FOR — every key of a multi-key write being
# dropped from the cache, not just the first — needs two keys in ONE write,
# not two keys in two slots, so the tag costs the assertion nothing.
$A MSET '{m}a' old-a '{m}b' old-b >/dev/null
$A GET '{m}a' >/dev/null; $A GET '{m}b' >/dev/null   # cache both
$A MSET '{m}a' new-a '{m}b' new-b >/dev/null         # rewrite both via THIS proxy
VA=$($A GET '{m}a'); VB=$($A GET '{m}b')
[ "$VA" = "new-a" ] || { echo "FAIL: MSET did not invalidate first key (got $VA)"; exit 1; }
[ "$VB" = "new-b" ] || { echo "FAIL: MSET did not invalidate later key (got $VB)"; exit 1; }
echo "  MSET {m}a {m}b -> immediate GETs see new-a/new-b (all written keys dropped)"

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

echo "== FAILOVER: invalidation keeps working through a promoted master (coverage from review)"
# Attach a replica, kill the master, promote — the proxy chases the new
# master; a write through the proxy must invalidate and the next GET must
# come from the NEW master (fresh), not the cache (stale).
$B --port 6971 --engine rocks --data-dir "$D/r" --replica-of 127.0.0.1:6970 2>"${FLEET_SCOPE}server2.log" &
fleet_wait_listen 6971
sleep 1.2
$A SET f1 before >/dev/null; $A GET f1 >/dev/null   # cache 'before'
sleep 0.5                                            # let the write replicate
pkill -9 -f "flint-server --port 6970"; sleep 0.3
valkey-cli -p 6971 FLINTPROMOTE 1 99 >/dev/null 2>&1 || valkey-cli -p 6971 FLINTPROMOTE 2 1 >/dev/null
V=$($A GET f1)   # within TTL: served from cache even during the failover window
[ "$V" = "before" ] || { echo "FAIL: cache did not carry through failover (got $V)"; exit 1; }
$A SET f1 after >/dev/null   # write via proxy -> chases to the promoted master + invalidates
V=$($A GET f1)
[ "$V" = "after" ] || { echo "FAIL: post-failover invalidation broken (got $V)"; exit 1; }
echo "  cache served through the failover window; post-promotion write invalidated cleanly"
pkill -9 -f "flint-server --port 6971" 2>/dev/null
$B --port 6970 --engine rocks --data-dir "$D/m" 2>/dev/null &   # restore original master for the rest
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
