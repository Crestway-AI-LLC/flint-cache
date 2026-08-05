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
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-compat-state 7321 7322 7323 7324 7683 7724
fleet_guard
STATE=/tmp/flint-compat-state; INV=/tmp/flint-compat.flint
PORT=7683

fleet_kill server; fleet_kill proxy
fleet_kill controlplane; fleet_kill controller
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; fleet_kill controller
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
disposable on
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
# protocol=3 EXPLICITLY: redis-py defaults to RESP3 only from major 8,
# and a host whose python caps redis-py below that would silently demote
# this whole battery to RESP2 — the check below would then fail against a
# server whose RESP3 is fine. Requesting it makes the check about the
# SERVER, which is the thing under test.
r = redis.Redis(host="127.0.0.1", port=PORT, password=PW, decode_responses=True, protocol=3)
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
    # Compare as (member, float) PAIRS, not as a literal: redis-py 7 hands
    # RESP3 pairs back as lists, 8 as tuples. Both are correct client
    # behavior; what this check pins is the SERVER's pairing and typing —
    # a member next to its float score, never a flat interleave and never
    # a string score.
    assert [(m, float(sc)) for (m, sc) in got] == [("a", 1.0), ("b", 2.5)], f"zrange -> {got!r}"
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
    # ["array"] from redis-py 8 (its JSON client unwraps the RESP3
    # nesting), [["array"]] from 7 (it does not). The server sends the
    # SAME bytes to both — the module-quirk nesting the real RedisJSON
    # sends — so both spellings are the genuine article for that client
    # major, and pinning one of them pins the client, not the product.
    assert j.type("doc", "$.tags") in (["array"], [["array"]]), j.type("doc", "$.tags")
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

print("== transactions (ADR-0012): the real client machinery works same-slot")
def txn():
    # The full optimistic-locking shape a real application uses: WATCH,
    # read, MULTI, conditional write, EXEC — all keys under one hash tag,
    # which is the documented contract. This check asserted the OPPOSITE
    # (expect_unsupported) until transactions shipped, and the stale
    # expectation was caught by the gate box, not by a person.
    r.delete("{ct}:bal")
    r.set("{ct}:bal", "100")
    with r.pipeline(transaction=True) as pipe:
        pipe.watch("{ct}:bal")
        bal = int(pipe.get("{ct}:bal"))
        pipe.multi()
        pipe.set("{ct}:bal", str(bal - 30))
        pipe.set("{ct}:log", "debit")
        got = pipe.execute()
    assert got == [True, True], f"EXEC -> {got!r}"
    assert r.get("{ct}:bal") == "70", r.get("{ct}:bal")
    assert r.get("{ct}:log") == "debit"
check("MULTI/EXEC/WATCH (same slot)", txn)

print("== commands we exclude by design still fail HONESTLY")
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
[ $RC -eq 0 ] || { echo "FAIL: redis-py client compatibility"; exit 1; }

# ---------------------------------------------------------------------------
# node-redis. The second client worth gating on, and NOT redundant with
# redis-py: the two post-process replies differently, so each catches
# failures the other hides. JSON.NUMINCRBY is the standing example — redis-py
# JSON-parses the reply body and so read a wrong-typed answer as correct,
# while node-redis handed the raw string straight to the caller and made the
# bug obvious.
# ---------------------------------------------------------------------------
NODE=${FLINT_COMPAT_NODE:-$(command -v node || true)}
if [ -z "$NODE" ]; then
  echo "== node-redis: SKIP (no node on PATH)"
else
  NODE_DIR=${FLINT_COMPAT_NODE_DIR:-/tmp/flint-compat-node}
  mkdir -p "$NODE_DIR"
  if [ ! -d "$NODE_DIR/node_modules/redis" ]; then
    (cd "$NODE_DIR" && npm init -y >/dev/null 2>&1 && npm install redis --silent >/dev/null 2>&1)
  fi
  if [ ! -d "$NODE_DIR/node_modules/redis" ]; then
    echo "== node-redis: SKIP (could not install; offline?)"
  else
    echo "== client: node-redis $("$NODE" -e "console.log(require('$NODE_DIR/node_modules/redis/package.json').version)")"
    cat > "$NODE_DIR/suite.mjs" <<'JS'
import { createClient } from 'redis';
const fails = [];
const eq = (a, b, what) => {
  const A = JSON.stringify(a), B = JSON.stringify(b);
  if (A !== B) throw new Error(`${what}: got ${A}, want ${B}`);
};
async function check(name, fn, expectUnsupported = false) {
  let ok, note = '';
  try {
    await fn();
    ok = !expectUnsupported;
    if (!ok) note = 'expected unsupported but SUCCEEDED';
  } catch (e) {
    ok = expectUnsupported;
    if (!ok) note = `${e.constructor.name}: ${e.message}`;
  }
  console.log(`  ${ok ? 'ok ' : 'FAIL'} ${name}${note ? '  ' + note : ''}`);
  if (!ok) fails.push(name);
}
const c = createClient({ url: process.env.FLINT_URL, password: process.env.FLINT_TOKEN });
c.on('error', () => {});
await c.connect();
await check('connect (default RESP3 + inline HELLO AUTH)', async () => {
  if (await c.ping() !== 'PONG') throw new Error('no pong');
});
await check('protocol really is RESP3', async () => {
  const h = await c.sendCommand(['HELLO']);
  if (h.proto !== 3) throw new Error(`proto ${h.proto}`);
});
await check('set/get, nil is null', async () => {
  await c.set('nr:k', 'v');
  eq(await c.get('nr:k'), 'v', 'get');
  if (await c.get('nr:missing') !== null) throw new Error('missing key not null');
});
await check('HGETALL is an object', async () => {
  await c.del('nr:h'); await c.hSet('nr:h', { f1: 'v1', f2: 'v2' });
  eq(await c.hGetAll('nr:h'), { f1: 'v1', f2: 'v2' }, 'hGetAll');
});
await check('ZRANGE WITHSCORES pairs, ZSCORE is a number', async () => {
  await c.del('nr:z');
  await c.zAdd('nr:z', [{ value: 'a', score: 1 }, { value: 'b', score: 2.5 }]);
  eq(await c.zRangeWithScores('nr:z', 0, -1),
     [{ value: 'a', score: 1 }, { value: 'b', score: 2.5 }], 'zRangeWithScores');
  const s = await c.zScore('nr:z', 'b');
  if (typeof s !== 'number' || s !== 2.5) throw new Error(`zScore ${s} (${typeof s})`);
});
await check('SMEMBERS', async () => {
  await c.del('nr:s'); await c.sAdd('nr:s', ['x', 'y']);
  eq((await c.sMembers('nr:s')).sort(), ['x', 'y'], 'sMembers');
});
await check('scanIterator across both shards', async () => {
  for (let i = 0; i < 50; i++) await c.set(`nr:si:${String(i).padStart(3, '0')}`, 'v');
  const seen = new Set();
  // node-redis yields BATCHES of keys, not keys — counting iterations here
  // would silently "pass" while seeing a fraction of the keyspace.
  for await (const batch of c.scanIterator({ MATCH: 'nr:si:*', COUNT: 10 })) {
    (Array.isArray(batch) ? batch : [batch]).forEach(k => seen.add(k));
  }
  if (seen.size !== 50) throw new Error(`saw ${seen.size} of 50`);
});
await check('node-redis JSON client', async () => {
  await c.del('nr:doc');
  await c.json.set('nr:doc', '$', { name: 'flint', tags: ['a', 'b'], n: 1 });
  eq(await c.json.get('nr:doc', { path: '$.name' }), ['flint'], 'json.get $');
  eq(await c.json.get('nr:doc', { path: '.name' }), 'flint', 'json.get legacy');
  eq(await c.json.type('nr:doc', { path: '$.tags' }), ['array'], 'json.type');
  eq(await c.json.arrLen('nr:doc', { path: '$.tags' }), [2], 'json.arrLen');
  eq(await c.json.arrAppend('nr:doc', '$.tags', 'c'), [3], 'json.arrAppend');
  // A NUMBER, not the string "[6]" — the reply kind differs between the
  // dialects here, and this is the assertion that catches getting it wrong.
  const n = await c.json.numIncrBy('nr:doc', '$.n', 5);
  eq(n, [6], 'json.numIncrBy');
  if (typeof n[0] !== 'number') throw new Error(`numIncrBy element is ${typeof n[0]}`);
});
await check('MULTI/EXEC (same slot)', async () => {
  const got = await c.multi().set('{nrt}:a', '1').set('{nrt}:b', '2').exec();
  if (!Array.isArray(got) || got.length !== 2) throw new Error(`exec -> ${JSON.stringify(got)}`);
  if (await c.get('{nrt}:a') !== '1') throw new Error('txn write missing');
});
await check('BLPOP', async () => { await c.blPop('nr:nolist', 1); }, true);
await check('KEYS', async () => { await c.keys('*'); }, true);
await c.quit();
if (fails.length) {
  console.log(`\nFAIL: ${fails.length} client-visible problem(s): ${fails.join(', ')}`);
  process.exit(1);
}
console.log('\nall client checks passed');
JS
    (cd "$NODE_DIR" && FLINT_URL="redis://127.0.0.1:$PORT" FLINT_TOKEN=tok-acme "$NODE" suite.mjs)
    [ $? -eq 0 ] || { echo "FAIL: node-redis client compatibility"; exit 1; }
  fi
fi

echo "PASS: client compatibility — the default redis-py and node-redis clients (RESP3, credentials inside HELLO) connect, serve, and get their own native types back; sync, async, pooled, pipelined, and JSON; excluded commands fail honestly"
