#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Real client libraries against a real Flint cluster, through the proxy.
#
# The conformance corpus proves the WIRE is right. It cannot prove that a
# client library is happy, because a library does far more than send the
# commands you asked for: it opens with a handshake of its own choosing,
# picks a protocol, and post-processes replies according to what it thinks
# the server is. redis-py 8 defaults to RESP3 and folds credentials into
# `HELLO 3 AUTH ...`; before that was supported, every corpus run was green
# and yet no modern Python client could connect at all. This drill is what
# closes that gap.
#
# Requires a Python with `redis` installed. Point FLINT_COMPAT_PY at it, or
# let the script build a throwaway venv with the newest python3 it finds.
set -u
cd "$(dirname "$0")/.."
STATE=/tmp/flint-compat-state; INV=/tmp/flint-compat.flint
PORT=7683

pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
pkill -9 -f flint-controlplane 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  pkill -9 -f flint-server 2>/dev/null; pkill -9 -f flint-proxy 2>/dev/null
  pkill -9 -f flint-controlplane 2>/dev/null; pkill -9 -f flint-controller 2>/dev/null
  rm -rf "$STATE" "$INV"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV"

# Find a python that has redis-py, or make one.
PY=${FLINT_COMPAT_PY:-}
if [ -z "$PY" ]; then
  for cand in python3.14 python3.13 python3.12 python3.11 python3; do
    command -v "$cand" >/dev/null || continue
    if "$cand" -c 'import redis' 2>/dev/null; then PY=$(command -v "$cand"); break; fi
  done
fi
if [ -z "$PY" ]; then
  VENV=/tmp/flint-compat-venv
  BASE=""
  for cand in python3.14 python3.13 python3.12 python3.11 python3; do
    command -v "$cand" >/dev/null && { BASE=$(command -v "$cand"); break; }
  done
  [ -n "$BASE" ] || { echo "SKIP: no python3 available"; exit 0; }
  [ -x "$VENV/bin/python" ] || "$BASE" -m venv "$VENV" >/dev/null 2>&1
  "$VENV/bin/pip" install -q redis >/dev/null 2>&1 || {
    echo "SKIP: could not install redis-py (offline?)"; exit 0; }
  PY="$VENV/bin/python"
fi
"$PY" -c 'import redis' 2>/dev/null || { echo "SKIP: redis-py unavailable"; exit 0; }
echo "== client: $("$PY" -c 'import redis;print("redis-py", redis.__version__)')"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks

cat > "$INV" <<EOF
statedir $STATE
bins ./target/release
tls on
cp 127.0.0.1:7724
pair 127.0.0.1:7321,127.0.0.1:7322
pair 127.0.0.1:7323,127.0.0.1:7324
proxy 127.0.0.1:$PORT
EOF

echo "== bootstrap 2 pairs + tenant"
./target/release/flintctl -f "$INV" bootstrap >/dev/null 2>&1
./target/release/flintctl -f "$INV" tenant add acme tok-acme acme 1 >/dev/null 2>&1
for _ in $(seq 1 30); do
  [ "$(valkey-cli -p $PORT -a tok-acme --no-auth-warning PING 2>/dev/null)" = "PONG" ] && break
  sleep 0.3
done

PORT=$PORT "$PY" - <<'PY'
import asyncio, os, sys
import redis

PORT = int(os.environ["PORT"]); PW = "tok-acme"
fails = []
def check(name, fn, expect_unsupported=False):
    try:
        fn()
        ok = not expect_unsupported
        note = "" if ok else "expected to be unsupported but SUCCEEDED"
    except redis.ResponseError as e:
        ok = expect_unsupported
        note = "" if ok else f"ResponseError: {e}"
    except Exception as e:
        ok = False
        note = f"{type(e).__name__}: {e}"
    print(f"  {'ok ' if ok else 'FAIL'} {name}{'  ' + note if note else ''}")
    if not ok:
        fails.append(name)

# The default constructor, with NOTHING special set: this is the line every
# tutorial and framework uses, and it is what was broken.
r = redis.Redis(host="127.0.0.1", port=PORT, password=PW, decode_responses=True)
print("== the default client connects and serves")
check("connect (default RESP3 + inline HELLO AUTH)", lambda: r.ping())
check("protocol really is RESP3",
      lambda: (_ for _ in ()).throw(AssertionError("not RESP3"))
      if r.execute_command("HELLO").get("proto") != 3 else None)
check("set/get", lambda: (r.set("k", "v"), r.get("k") == "v" or _fail()))

print("== typed replies arrive as the client's OWN types")
def hashes():
    r.delete("h"); r.hset("h", mapping={"f1": "v1", "f2": "v2"})
    got = r.hgetall("h")
    assert got == {"f1": "v1", "f2": "v2"}, f"hgetall -> {got!r} (a dict is the point)"
check("HGETALL is a dict", hashes)
def zsets():
    r.delete("z"); r.zadd("z", {"a": 1, "b": 2.5})
    got = r.zrange("z", 0, -1, withscores=True)
    assert got == [("a", 1.0), ("b", 2.5)], f"zrange -> {got!r}"
    assert r.zscore("z", "b") == 2.5, r.zscore("z", "b")
check("ZRANGE WITHSCORES pairs, ZSCORE is a float", zsets)
def sets():
    r.delete("s"); r.sadd("s", "x", "y")
    assert set(r.smembers("s")) == {"x", "y"}
check("SMEMBERS", sets)
check("nil is None", lambda: r.get("definitely-missing") is None or _fail())

print("== the shapes clients iterate with")
def scan_iter():
    for i in range(50):
        r.set(f"si:{i:03d}", "v")
    assert len({k for k in r.scan_iter(match="si:*", count=10)}) == 50
check("scan_iter across both shards", scan_iter)
def hscan_iter():
    r.delete("bh"); r.hset("bh", mapping={f"f{i}": str(i) for i in range(30)})
    assert len({k for k, _ in r.hscan_iter("bh")}) == 30
check("hscan_iter", hscan_iter)
def pipeline():
    p = r.pipeline(transaction=False)
    p.set("p1", "1"); p.get("p1")
    assert p.execute()[-1] == "1"
check("pipeline (transaction=False)", pipeline)
def pool():
    pool = redis.ConnectionPool(host="127.0.0.1", port=PORT, password=PW,
                                decode_responses=True, max_connections=8)
    cs = [redis.Redis(connection_pool=pool) for _ in range(8)]
    for i, c in enumerate(cs):
        c.set(f"pool:{i}", str(i))
    assert all(cs[i].get(f"pool:{i}") == str(i) for i in range(8))
check("ConnectionPool (8 connections)", pool)

print("== the JSON client, in the dialect its docs use")
def json_client():
    j = r.json()
    j.set("doc", "$", {"name": "flint", "tags": ["a", "b"], "n": 1})
    assert j.get("doc", "$.name") == ["flint"], j.get("doc", "$.name")
    assert j.get("doc", ".name") == "flint"
    assert j.get("doc")["name"] == "flint"
    assert j.type("doc", "$.tags") == ["array"], j.type("doc", "$.tags")
    assert j.arrlen("doc", "$.tags") == [2]
    assert j.arrappend("doc", "$.tags", "c") == [3]
    assert j.numincrby("doc", "$.n", 5) == [6]
    assert j.get("doc", "$.gone") == []
check("redis-py JSON client", json_client)

print("== asyncio, the path every AI framework actually takes")
async def _async():
    import redis.asyncio as aredis
    ar = aredis.Redis(host="127.0.0.1", port=PORT, password=PW, decode_responses=True)
    assert await ar.ping()
    await ar.set("async:k", "v")
    assert await ar.get("async:k") == "v"
    await ar.hset("async:h", mapping={"a": "1"})
    assert await ar.hgetall("async:h") == {"a": "1"}
    assert "async:k" in [k async for k in ar.scan_iter(match="async:*")]
    await ar.aclose()
check("asyncio client", lambda: asyncio.run(_async()))

print("== commands we exclude by design still fail HONESTLY")
check("MULTI/EXEC", lambda: r.pipeline(transaction=True).set("t", "1").execute(),
      expect_unsupported=True)
check("SUBSCRIBE", lambda: r.pubsub().subscribe("c") or r.execute_command("SUBSCRIBE", "c"),
      expect_unsupported=True)
check("BLPOP", lambda: r.blpop("nolist", timeout=1), expect_unsupported=True)
check("KEYS", lambda: r.keys("*"), expect_unsupported=True)

def _fail():
    raise AssertionError("unexpected value")

if fails:
    print(f"\nFAIL: {len(fails)} client-visible problem(s): {', '.join(fails)}")
    sys.exit(1)
print("\nall client checks passed")
PY
RC=$?
[ $RC -eq 0 ] || { echo "FAIL: client compatibility"; exit 1; }

echo "PASS: client compatibility — the default redis-py client (RESP3, credentials inside HELLO) connects, serves, and gets its own native types back; sync, async, pooled, pipelined, and JSON; excluded commands fail honestly"
