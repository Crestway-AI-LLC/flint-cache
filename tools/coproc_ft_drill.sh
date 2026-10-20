#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0060: RediSearch's FT.* surface through the proxy, served by flint-vec
# with --ft, over HASH and JSON documents the tenant writes with ordinary
# commands, on two pairs. Asserts:
#   - FT.CREATE registers the index's prefixes on EVERY pair (amendment 2):
#     a write under one that does not come over the co-processor's channel
#     is refused by the node with -INDEXED, on each pair, while a write
#     elsewhere is served (the control)
#   - through the proxy the same writes are served: the proxy learns the
#     prefix from -INDEXED and sends them through the co-processor, which
#     writes them and indexes what it reads back
#   - FT.SEARCH answers a filtered KNN in nearest order with its distance,
#     a filter-only search, RETURN and LIMIT; FT.INFO counts the documents
#   - DEL, EXPIRE and HSET of a document move the index with it
#   - an index made over keys that already exist backfills them
#   - a co-processor restart answers -LOADING, rebuilds every index from the
#     durable rows and the documents, registers the prefixes again, and
#     answers the same
#   - FT.DROPINDEX takes the prefixes off every pair: the same direct write
#     is served again
#   - without --ft, FT.CREATE is refused (the first of two releases)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-ftd 6540 6541 6542 6543 6544
fleet_guard
B=./target/release/flint-server
PX=./target/release/flint-proxy
VEC=./target/release/flint-vec
D=$FLINT_DRILL_ROOT/flint-ftd; rm -rf "$D"; mkdir -p "$D"
COPROC_PID=""
fleet_kill server; fleet_kill proxy; sleep 0.4
cleanup() {
  [ -n "$COPROC_PID" ] && kill -9 "$COPROC_PID" 2>/dev/null
  fleet_kill server; fleet_kill proxy; rm -rf "$D"
}
trap cleanup EXIT

cargo build --release -q -p flint-server -p flint-proxy -p flint-vec --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

start_vec() {
  $VEC --port 6543 "$@" 2>>"$D/vec.log" & COPROC_PID=$!
  fleet_wait_listen 6543
}

echo "== two pairs (mem), a proxy routing VEC. and FT. to flint-vec --ft"
$B --port 6540 --engine mem 2>"$D/n1.log" & fleet_wait_listen 6540; fleet_wait_ping 6540
$B --port 6541 --engine mem 2>"$D/n2.log" & fleet_wait_listen 6541; fleet_wait_ping 6541
start_vec --ft
$PX --port 6542 --workers 2 --pairs "127.0.0.1:6540;127.0.0.1:6541" --tenants "tok=ns" \
    --families "VEC.=127.0.0.1:6543;FT.=127.0.0.1:6543" --edge-advertise 127.0.0.1:6542 \
    2>"$D/px.log" & fleet_wait_listen 6542
for _ in $(seq 1 60); do case "$(valkey-cli -p 6542 PING 2>&1)" in *NOAUTH*|PONG) break;; esac; sleep 0.1; done

# The client: a RESP connection, binary-safe, retrying -LOADING as the
# contract for a warming index asks. `step NAME` runs one section of the
# script below against the proxy (and the nodes, directly, as a channel-less
# writer would reach them).
step() {
  python3 - "$1" <<'PY'
import socket, struct, sys, time

def conn(port, auth=None, ns=None):
    s = socket.create_connection(("127.0.0.1", port))
    f = s.makefile("rb")
    c = (s, f)
    if auth:
        assert cmd(c, "AUTH", auth) == "OK"
    if ns:
        assert cmd(c, "FLINTNS", ns) == "OK"
    return c

def enc(a):
    return a if isinstance(a, bytes) else str(a).encode()

def read(f):
    line = f.readline()
    t, rest = line[:1], line[1:-2]
    if t == b"+":
        return rest.decode()
    if t == b"-":
        return Err(rest.decode())
    if t == b":":
        return int(rest)
    if t == b"$":
        n = int(rest)
        if n < 0:
            return None
        b = f.read(n + 2)[:-2]
        return b
    if t == b"*":
        n = int(rest)
        return None if n < 0 else [read(f) for _ in range(n)]
    raise SystemExit("FAIL: bad reply %r" % line)

class Err(str):
    pass

def raw(c, *args):
    s, f = c
    a = [enc(x) for x in args]
    out = b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)
    s.sendall(out)
    return read(f)

def cmd(c, *args):
    for _ in range(60):
        r = raw(c, *args)
        if isinstance(r, Err) and r.startswith("LOADING"):
            time.sleep(0.15)
            continue
        return r
    return r

def vec(*xs):
    return struct.pack("<%df" % len(xs), *xs)

def want(got, exp, what):
    if got != exp:
        raise SystemExit("FAIL: %s: got %r, expected %r" % (what, got, exp))

def refused(r, prefix, what):
    if not (isinstance(r, Err) and r.startswith(prefix)):
        raise SystemExit("FAIL: %s: got %r, expected an error starting %r" % (what, r, prefix))

def keys(reply):
    # [total, key, fields, key, fields, ...]
    return [k.decode() for k in reply[1::2]]

px = conn(6542, auth="tok")
nodes = [conn(6540, ns="ns"), conn(6541, ns="ns")]
which = sys.argv[1]

if which == "create":
    want(cmd(px, "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
             "user", "TAG", "price", "NUMERIC", "SORTABLE", "body", "TEXT",
             "v", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2"),
         "OK", "FT.CREATE")
    refused(cmd(px, "FT.CREATE", "idx", "PREFIX", "1", "x:", "SCHEMA", "a", "TAG"),
            "ERR Index already exists", "a second FT.CREATE of one name")
    want(cmd(px, "FT._LIST"), [b"idx"], "FT._LIST")
    # Every pair refuses a write under the prefix that is not a channel's,
    # and serves one elsewhere.
    for i, n in enumerate(nodes):
        refused(raw(n, "HSET", "doc:direct", "user", "x"), "INDEXED 646f633a",
                "pair %d: a direct HSET under doc:" % i)
        refused(raw(n, "EVAL", "return redis.call('SET', KEYS[1], 'x')", "1", "doc:viascript"),
                "INDEXED 646f633a", "pair %d: a script's declared key under doc:" % i)
        # A key the script builds, in its declared key's slot: refused at
        # the row it writes, since no argument names it.
        r = raw(n, "EVAL", "return redis.call('SET', 'doc:{t}built', 'x')", "1", "{t}declared")
        if not (isinstance(r, Err) and "INDEXED" in r):
            raise SystemExit("FAIL: pair %d: a script's built key under doc: got %r" % (i, r))
        want(raw(n, "SET", "plain:%d" % i, "doc:value"), "OK", "pair %d: a write elsewhere" % i)
    print("  every pair refuses a direct write under doc: (and a script's), and serves others")

if which == "write":
    docs = [("doc:1", "alice", 5, "the quick brown fox", (0.0, 0.0)),
            ("doc:2", "bob", 20, "a lazy dog", (1.0, 0.0)),
            ("doc:3", "alice,carol", 30, "quick thinking", (2.0, 0.0)),
            ("doc:4", "ALICE", 40, "brown bread", (3.0, 0.0))]
    for k, u, p, body, v in docs:
        want(cmd(px, "HSET", k, "user", u, "price", p, "body", body, "v", vec(*v)), 4, "HSET " + k)
    want(cmd(px, "SET", "plain", "x"), "OK", "a write under no prefix")
    r = cmd(px, "FT.SEARCH", "idx", "(@user:{alice})=>[KNN 2 @v $q AS d]",
            "PARAMS", "2", "q", vec(2.9, 0.0), "SORTBY", "d", "RETURN", "2", "user", "d",
            "DIALECT", "2")
    want(r[0], 2, "a filtered KNN's total")
    want(keys(r), ["doc:4", "doc:3"], "a filtered KNN's order")
    f = dict(zip(r[2][::2], r[2][1::2]))
    want(f[b"user"], b"ALICE", "RETURN user")
    if abs(float(f[b"d"]) - 0.01) > 1e-4:
        raise SystemExit("FAIL: squared L2 distance: %r" % f[b"d"])
    r = cmd(px, "FT.SEARCH", "idx", "@price:[10 30]", "SORTBY", "price", "DESC", "NOCONTENT")
    want(r, [2, b"doc:3", b"doc:2"], "a range, sorted, NOCONTENT")
    r = cmd(px, "FT.SEARCH", "idx", '@body:("quick brown")', "NOCONTENT")
    want(r, [1, b"doc:1"], "an exact phrase")
    r = cmd(px, "FT.SEARCH", "idx", "*", "LIMIT", "1", "2", "NOCONTENT")
    want(r, [4, b"doc:2", b"doc:3"], "LIMIT")
    info = cmd(px, "FT.INFO", "idx")
    want(info[info.index(b"num_docs") + 1], 4, "FT.INFO num_docs")
    # The index moves with its documents.
    want(cmd(px, "DEL", "doc:4"), 1, "DEL doc:4")
    want(cmd(px, "HSET", "doc:2", "user", "alice"), 0, "HSET doc:2 user")
    want(cmd(px, "PEXPIRE", "doc:1", "300"), 1, "PEXPIRE doc:1")
    time.sleep(0.6)
    r = cmd(px, "FT.SEARCH", "idx", "@user:{alice}", "NOCONTENT")
    want(r, [2, b"doc:2", b"doc:3"], "after DEL, HSET and an expiry")
    print("  HSET, DEL, PEXPIRE through the proxy; KNN, range, phrase, LIMIT and FT.INFO answer")

if which == "json":
    # Six, so both pairs hold some; written before the index exists, and so
    # by the proxy straight to their pairs.
    for i in range(6):
        want(cmd(px, "JSON.SET", "j:pre%d" % i, "$", '{"tag":"red","n":%d,"v":[%d,0]}' % (i, i)),
             "OK", "a JSON document written before its index")
    want(cmd(px, "FT.CREATE", "jx", "ON", "JSON", "PREFIX", "1", "j:", "SCHEMA",
             "$.tag", "AS", "tag", "TAG", "$.n", "AS", "n", "NUMERIC",
             "$.v", "AS", "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
             "DISTANCE_METRIC", "L2"), "OK", "FT.CREATE ON JSON")
    r = cmd(px, "FT.SEARCH", "jx", "@tag:{red}", "SORTBY", "n", "NOCONTENT")
    want(r, [6] + [b"j:pre%d" % i for i in range(6)], "the backfill found every pair's documents")
    want(cmd(px, "JSON.SET", "j:new", "$", '{"tag":"red","n":9,"v":[9,0]}'), "OK", "JSON.SET")
    r = cmd(px, "FT.SEARCH", "jx", "*=>[KNN 1 @v $q]", "PARAMS", "2", "q", vec(8.0, 0.0),
            "RETURN", "1", "tag")
    want(keys(r), ["j:new"], "a KNN over JSON")
    want(r[2], [b"tag", b"red"], "RETURN of a JSON attribute")
    print("  an index over existing JSON backfills from every pair, then serves new writes")

if which == "same":
    r = cmd(px, "FT.SEARCH", "idx", "(@user:{alice})=>[KNN 3 @v $q AS d]",
            "PARAMS", "2", "q", vec(2.9, 0.0), "SORTBY", "d", "NOCONTENT", "DIALECT", "2")
    want(r, [2, b"doc:3", b"doc:2"], "after the restart, the same KNN")
    r = cmd(px, "FT.SEARCH", "jx", "@tag:{red}", "SORTBY", "n", "NOCONTENT")
    want(r, [7] + [b"j:pre%d" % i for i in range(6)] + [b"j:new"], "after the restart, the JSON index")
    for i, n in enumerate(nodes):
        refused(raw(n, "HSET", "doc:direct", "user", "x"), "INDEXED",
                "pair %d after the restart" % i)
    print("  the rebuilt indexes answer the same, and every pair still guards their prefixes")

if which == "drop":
    want(cmd(px, "FT.DROPINDEX", "idx"), "OK", "FT.DROPINDEX")
    refused(cmd(px, "FT.SEARCH", "idx", "*"), "ERR idx: no such index", "a search of a dropped index")
    for i, n in enumerate(nodes):
        want(raw(n, "HSET", "doc:direct", "user", "x"), 1, "pair %d: doc: is no longer guarded" % i)
        refused(raw(n, "JSON.SET", "j:direct", "$", "{}"), "INDEXED 6a3a",
                "pair %d: j: still is" % i)
    want(cmd(px, "FT.DROPINDEX", "jx", "DD"), "OK", "FT.DROPINDEX DD")
    want(cmd(px, "EXISTS", *(["j:pre%d" % i for i in range(6)] + ["j:new"])), 0,
         "DD deleted the documents")
    want(cmd(px, "GET", "plain"), b"x", "the control key is untouched")
    print("  a dropped index's prefixes are written directly again; DD deleted its documents")

if which == "off":
    refused(cmd(px, "FT.CREATE", "z", "PREFIX", "1", "z:", "SCHEMA", "a", "TAG"),
            "ERR FT.* is not enabled", "FT.CREATE without --ft")
    print("  without --ft, FT.CREATE is refused")
PY
}

echo "== FT.CREATE registers the prefix on every pair"
step create || exit 1
echo "== documents written through the proxy are indexed as they are written"
step write || exit 1
echo "== an index over JSON documents that already exist on both pairs"
step json || exit 1

echo "== a co-processor restart rebuilds every index and registers its prefixes again"
kill -9 "$COPROC_PID"; wait "$COPROC_PID" 2>/dev/null
start_vec --ft
step same || exit 1
grep -q "FT\.\* on" "$D/vec.log" || { echo "FAIL: flint-vec did not say FT.* is on"; exit 1; }

echo "== FT.DROPINDEX takes the prefixes off every pair"
step drop || exit 1

echo "== the first of two releases: without --ft, FT.* is refused"
kill -9 "$COPROC_PID"; wait "$COPROC_PID" 2>/dev/null
start_vec
step off || exit 1

echo "PASS: FT.* serves through the proxy over HASH and JSON documents on two pairs, every pair refuses a write around the co-processor, a restart rebuilds the same answers, a drop releases its prefixes, and --ft gates it (ADR-0060)"
