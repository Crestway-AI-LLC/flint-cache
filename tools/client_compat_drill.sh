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
fleet_init $FLINT_DRILL_ROOT/flint-compat-state 7321 7322 7323 7324 7683 7724
fleet_guard
STATE=$FLINT_DRILL_ROOT/flint-compat-state; INV=$FLINT_DRILL_ROOT/flint-compat.flint
PORT=7683

fleet_kill controller; fleet_kill server
fleet_kill proxy; fleet_kill controlplane
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill controller; fleet_kill server
  fleet_kill proxy; fleet_kill controlplane
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
  VENV=$FLINT_DRILL_ROOT/flint-compat-venv
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
  -p flint-controller -p flint-ctl --features flint-server/rocks || { echo "FAIL: build"; exit 1; }

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
./target/release/flintctl -f "$INV" bootstrap >"$STATE-boot.log" 2>&1 || {
  # Capture it and STOP. This discarded bootstrap's output and
  # ignored its exit status, so a failed bootstrap ran on into the
  # assertions below and was reported as whichever one broke first
  # -- a product fault asserted for what was really "bootstrap
  # failed and nobody looked" (BUG-0064).
  echo "FAIL: bootstrap"; tail -25 "$STATE-boot.log"; exit 1; }
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

print("== multi-key deletes across the two pairs (BUG-0179)")
# `a` is slot 15495 and `b` slot 3300: the two pairs' halves of the range. A
# DEL or EXISTS naming both used to be forwarded whole to `a`'s pair, which
# answered for `a` alone -- DEL said 1 and `b` survived.
def cross_pair_del():
    r.set("a", "1"); r.set("b", "1")
    assert r.exists("a", "b") == 2, f"EXISTS a b -> {r.exists('a', 'b')}"
    assert r.delete("a", "b") == 2, "DEL a b did not count both"
    assert r.get("b") is None, "DEL a b left b behind"
    assert r.exists("a", "b") == 0
check("DEL and EXISTS count every key, across pairs", cross_pair_del)
def cross_slot_txn():
    r.set("a", "1"); r.set("b", "1")
    p = r.pipeline(transaction=True)
    p.delete("a", "b")
    try:
        p.execute()
    except redis.ResponseError as e:
        # redis-py turns the CROSSSLOT prefix into ClusterCrossSlotError and
        # drops it from the message, so match the server's own words.
        assert "don't hash to the same slot" in str(e), f"refused, but not as CROSSSLOT: {e}"
    else:
        raise AssertionError("a transaction deleting keys in two slots was not refused")
    assert r.exists("a", "b") == 2, "the refused transaction deleted something"
check("a transaction refuses a cross-slot DEL at queue time", cross_slot_txn)
def cross_slot_mset_txn():
    # BUG-0181: MSET checks its own slots, but only at EXEC, where a refusal
    # is one element of the reply and everything queued beside it applies.
    # It must be refused when QUEUED, so the SET before it never lands.
    r.delete("a")
    p = r.pipeline(transaction=True)
    p.set("a", "1")
    p.mset({"a": "2", "b": "3"})
    try:
        p.execute()
    except redis.ResponseError as e:
        assert "don't hash to the same slot" in str(e), f"refused, but not as CROSSSLOT: {e}"
    else:
        raise AssertionError("a transaction with a cross-slot MSET was not refused")
    assert r.get("a") is None, "the SET queued beside a refused MSET applied"
check("a cross-slot MSET poisons its whole transaction", cross_slot_mset_txn)

print("== MGET across slots (ADR-0048)")
# The proxy splits an MGET per slot and puts the values back in order. `a`
# (15495) is on one pair, `b` (3300) and `c` (7365) on the other, in two
# slots. Before ADR-0048 this was refused with CROSSSLOT, and Rails'
# read_multi swallowed the refusal and read every key as a miss.
def cross_slot_mget():
    r.delete("mg:none")
    r.set("a", "va"); r.set("b", "vb"); r.set("c", "vc")
    got = r.mget("a", "mg:none", "b", "c", "a")
    assert got == ["va", None, "vb", "vc", "va"], f"MGET -> {got!r}"
    # Many slots on both pairs: every value in its own position.
    ks = [f"mg:{i}" for i in range(200)]
    for k in ks:
        r.set(k, k.upper())
    got = r.mget(ks)
    assert got == [k.upper() for k in ks], "a 200-key MGET came back out of order"
check("MGET answers every key in order, across slots and pairs", cross_slot_mget)
def mget_in_a_pipeline():
    # The GET ahead of the MGET may be staged (prefetched); the MGET ends
    # that run and is split. Each reply must still be the right one.
    r.set("a", "va"); r.set("b", "vb")
    p = r.pipeline(transaction=False)
    p.get("a"); p.mget("a", "b"); p.get("b")
    got = p.execute()
    assert got == ["va", ["va", "vb"], "vb"], f"pipeline -> {got!r}"
check("MGET across slots inside a pipeline", mget_in_a_pipeline)
def still_refused():
    # What ADR-0048 does NOT change: MSET stays atomic, so it is refused
    # across slots rather than split; and a transaction is not split either.
    r.set("a", "va"); r.set("b", "vb")
    for name, attempt in (
        ("MSET", lambda: r.mset({"a": "x", "b": "x"})),
        ("MGET in MULTI", lambda: r.pipeline(transaction=True).mget("a", "b").execute()),
    ):
        try:
            attempt()
        except redis.ResponseError as e:
            assert "don't hash to the same slot" in str(e), f"{name} refused, but not as CROSSSLOT: {e}"
        else:
            raise AssertionError(f"{name} across slots was not refused")
    assert r.mget("a", "b") == ["va", "vb"], "the refused MSET wrote something"
check("MSET and a transaction are still refused across slots", still_refused)

print("== the cache-store clear() path")
# BUG-0178: FLUSHDB was unknown, so Django's cache.clear() raised and Rails'
# RedisCacheStore#clear swallowed the error and cleared nothing.
def flushdb():
    r.set("fdb:a", "v"); r.set("fdb:b", "v")
    assert r.flushdb() is True, "flushdb did not answer OK"
    assert r.dbsize() == 0, f"{r.dbsize()} keys survived FLUSHDB"
check("FLUSHDB empties the tenant's keyspace", flushdb)

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
# What RAN, for the verdict line. A client that skipped must not appear in a
# sentence saying it connected (BUG-0176: the first version of that line named
# go-redis on a box that had no Go).
RAN="redis-py"; SKIPPED=""

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
  SKIPPED="$SKIPPED node-redis ioredis"
else
  NODE_DIR=${FLINT_COMPAT_NODE_DIR:-$FLINT_DRILL_ROOT/flint-compat-node}
  mkdir -p "$NODE_DIR"
  if [ ! -d "$NODE_DIR/node_modules/redis" ] || [ ! -d "$NODE_DIR/node_modules/ioredis" ]; then
    (cd "$NODE_DIR" && npm init -y >/dev/null 2>&1 && npm install redis ioredis --silent >/dev/null 2>&1)
  fi
  if [ ! -d "$NODE_DIR/node_modules/redis" ]; then
    echo "== node-redis: SKIP (could not install; offline?)"
    SKIPPED="$SKIPPED node-redis"
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
// RESP: 3 EXPLICITLY — node-redis defaults to RESP3 only from major 6,
// and an npm that resolves 5.x would silently demote this battery to
// RESP2, failing the protocol check against a server whose RESP3 is fine
// (the same trap redis-py < 8 sets, fixed the same way).
const c = createClient({ url: process.env.FLINT_URL, password: process.env.FLINT_TOKEN, RESP: 3 });
c.on('error', () => {});
await c.connect();
await check('connect (default RESP3 + inline HELLO AUTH)', async () => {
  if (await c.ping() !== 'PONG') throw new Error('no pong');
});
await check('protocol really is RESP3', async () => {
  // node-redis majors disagree about a map reply's spelling: 6 hands back
  // a plain object, 5 an array of alternating field/value entries, and a
  // Map is plausible under RESP3 options. Read proto out of whichever
  // arrived — the check pins the SERVER's negotiated protocol, not the
  // client major's decoding of it.
  const h = await c.sendCommand(['HELLO']);
  let proto;
  if (Array.isArray(h)) {
    const i = h.findIndex((x) => String(x) === 'proto');
    proto = i >= 0 ? Number(h[i + 1]) : undefined;
  } else if (h instanceof Map) {
    proto = Number(h.get('proto'));
  } else if (h && typeof h === 'object') {
    proto = Number(h.proto);
  }
  if (proto !== 3) throw new Error(`proto ${proto} (reply shape: ${h?.constructor?.name})`);
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
await check('MGET across slots (ADR-0048)', async () => {
  await c.set('a', 'va'); await c.set('b', 'vb'); await c.del('nr:none');
  eq(await c.mGet(['a', 'nr:none', 'b']), ['va', null, 'vb'], 'mGet');
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
    RAN="$RAN, node-redis"
  fi

  # -------------------------------------------------------------------------
  # ioredis, CONSTRUCTED EXACTLY AS docs/tenant-guide.md SHOWS IT (BUG-0176).
  # Its default `enableReadyCheck` sends INFO before the first command and
  # treats any error but NOPERM as fatal: disconnect, retry, forever. INFO was
  # unknown everywhere, so the guide's own sample never connected, and nothing
  # ran it. Defaults are the point here; do not add options to make it pass.
  # -------------------------------------------------------------------------
  if [ ! -d "$NODE_DIR/node_modules/ioredis" ]; then
    echo "== ioredis: SKIP (could not install; offline?)"
    SKIPPED="$SKIPPED ioredis"
  else
    echo "== client: ioredis $("$NODE" -e "console.log(require('$NODE_DIR/node_modules/ioredis/package.json').version)")"
    cat > "$NODE_DIR/ioredis.js" <<'JS'
const Redis = require("ioredis");
const port = Number(process.env.FLINT_PORT);
const fails = [];
const say = (ok, name, note) => {
  console.log(`  ${ok ? "ok " : "FAIL"} ${name}${note ? "  " + note : ""}`);
  if (!ok) fails.push(name);
};
// The guide's constructor, minus `tls: {}` (this cluster's edge is plaintext).
const r = new Redis({ host: "127.0.0.1", port, password: process.env.FLINT_TOKEN });
let lastError = "";
r.on("error", (e) => { lastError = e.message; });
(async () => {
  const ready = await Promise.race([
    new Promise((res) => r.once("ready", () => res(true))),
    new Promise((res) => setTimeout(() => res(false), 10000)),
  ]);
  say(ready, "becomes ready with the default ready check (INFO)",
      ready ? "" : `status ${r.status}; last error: ${lastError || "none"}`);
  if (!ready) { r.disconnect(); process.exit(1); }
  try {
    await r.set("io:k", "hello");
    const got = await r.get("io:k");
    say(got === "hello", "set/get", got === "hello" ? "" : `got ${JSON.stringify(got)}`);
  } catch (e) { say(false, "set/get", e.message); }
  try {
    const info = await r.info();
    say(/\r\nloading:0\r\n/.test(info), "INFO carries loading:0", JSON.stringify(info.slice(0, 80)));
  } catch (e) { say(false, "INFO carries loading:0", e.message); }
  try {
    await r.del("io:h"); await r.hset("io:h", { f1: "v1", f2: "v2" });
    const h = await r.hgetall("io:h");
    const ok = h.f1 === "v1" && h.f2 === "v2" && Object.keys(h).length === 2;
    say(ok, "HGETALL is an object", ok ? "" : JSON.stringify(h));
  } catch (e) { say(false, "HGETALL is an object", e.message); }
  r.disconnect();
  if (fails.length) {
    console.log(`\nFAIL: ${fails.length} client-visible problem(s): ${fails.join(", ")}`);
    process.exit(1);
  }
  console.log("\nall ioredis checks passed");
})();
JS
    (cd "$NODE_DIR" && FLINT_PORT=$PORT FLINT_TOKEN=tok-acme "$NODE" ioredis.js)
    [ $? -eq 0 ] || { echo "FAIL: ioredis client compatibility (the tenant guide's sample)"; exit 1; }
    RAN="$RAN, ioredis (default ready check)"
  fi
fi

# ---------------------------------------------------------------------------
# go-redis v9, the guide's third sample, with its default options: RESP3 via
# HELLO, then CLIENT SETINFO, which Flint does not implement and go-redis is
# expected to tolerate. Measured working on 2026-09-24 (v9.22.0); gated so it
# stays that way.
# ---------------------------------------------------------------------------
GO=${FLINT_COMPAT_GO:-$(command -v go || true)}
if [ -z "$GO" ]; then
  echo "== go-redis: SKIP (no go on PATH)"
  SKIPPED="$SKIPPED go-redis"
else
  GO_DIR=${FLINT_COMPAT_GO_DIR:-$FLINT_DRILL_ROOT/flint-compat-go}
  mkdir -p "$GO_DIR"
  cat > "$GO_DIR/main.go" <<'GOSRC'
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("FLINT_ADDR"), Password: os.Getenv("FLINT_TOKEN")})
	fails := 0
	say := func(ok bool, name string, err error) {
		mark, note := "ok ", ""
		if !ok {
			mark, fails = "FAIL", fails+1
			if err != nil {
				note = "  " + err.Error()
			}
		}
		fmt.Printf("  %s %s%s\n", mark, name, note)
	}
	err := rdb.Set(ctx, "go:k", "hello", 0).Err()
	say(err == nil, "connect + set (default options)", err)
	v, err := rdb.Get(ctx, "go:k").Result()
	say(err == nil && v == "hello", "get", err)
	rdb.Del(ctx, "go:h")
	rdb.HSet(ctx, "go:h", "f1", "v1", "f2", "v2")
	h, err := rdb.HGetAll(ctx, "go:h").Result()
	say(err == nil && len(h) == 2 && h["f1"] == "v1" && h["f2"] == "v2", "HGETALL is a map", err)
	if fails > 0 {
		fmt.Printf("\nFAIL: %d client-visible problem(s)\n", fails)
		os.Exit(1)
	}
	fmt.Println("\nall go-redis checks passed")
}
GOSRC
  if [ ! -f "$GO_DIR/go.sum" ]; then
    (cd "$GO_DIR" && { [ -f go.mod ] || "$GO" mod init flintcompat >/dev/null 2>&1; } \
      && "$GO" get github.com/redis/go-redis/v9 >/dev/null 2>&1)
  fi
  if [ ! -f "$GO_DIR/go.sum" ]; then
    echo "== go-redis: SKIP (could not fetch the module; offline?)"
    SKIPPED="$SKIPPED go-redis"
  else
    echo "== client: go-redis $(sed -n 's/.*go-redis\/v9 \(v[0-9.]*\).*/\1/p' "$GO_DIR/go.mod" | head -1)"
    (cd "$GO_DIR" && FLINT_ADDR=127.0.0.1:$PORT FLINT_TOKEN=tok-acme "$GO" run .)
    [ $? -eq 0 ] || { echo "FAIL: go-redis client compatibility"; exit 1; }
    RAN="$RAN, go-redis"
  fi
fi

# ---------------------------------------------------------------------------
# Rails' RedisCacheStore (ADR-0048). A framework store rather than a client
# library, and gated for one reason: its read_multi sends MGET over whatever
# slots the app's keys fall in, and its error handler turned the CROSSSLOT
# refusal into {}. Every multi-read was a miss, and nothing said so. The
# handler here only RECORDS what the store swallows, so that failure shape is
# visible: anything swallowed is a FAIL. Default options otherwise.
# ---------------------------------------------------------------------------
RUBY=${FLINT_COMPAT_RUBY:-$(command -v ruby || true)}
if [ -z "$RUBY" ]; then
  echo "== rails cache store: SKIP (no ruby on PATH)"
  SKIPPED="$SKIPPED rails-cache-store"
else
  RB_HOME=${FLINT_COMPAT_GEM_HOME:-$FLINT_DRILL_ROOT/flint-compat-ruby}
  # GEM_HOME only. Setting GEM_PATH too hides the system gems, and gem install
  # does not copy a dependency the system already satisfies (the distro's
  # bigdecimal, say), so the store would install and then fail to load.
  rb_ready() { GEM_HOME="$RB_HOME" "$RUBY" -e 'require "active_support"; require "redis"' >/dev/null 2>&1; }
  if ! rb_ready; then
    GEM_HOME="$RB_HOME" "$(dirname "$RUBY")/gem" install --no-document --silent activesupport redis >/dev/null 2>&1
  fi
  if ! rb_ready; then
    echo "== rails cache store: SKIP (could not install activesupport and redis; offline?)"
    SKIPPED="$SKIPPED rails-cache-store"
  else
    mkdir -p "$RB_HOME"
    cat > "$RB_HOME/store.rb" <<'RUBYSRC'
require "active_support"
require "active_support/cache"
require "redis"

puts "== client: activesupport #{ActiveSupport.version} redis-rb #{Redis::VERSION}"
fails = []
check = lambda do |name, ok, note = ""|
  puts "  #{ok ? 'ok  ' : 'FAIL'} #{name}#{ok ? '' : "  #{note}"}"
  fails << name unless ok
end
# x and y are two slots on one pair; a and b are on the two pairs.
keys = %w[x y a b c]
[nil, "app"].each do |namespace|
  swallowed = []
  store = ActiveSupport::Cache::RedisCacheStore.new(
    url: "redis://127.0.0.1:#{ENV.fetch('FLINT_PORT')}",
    password: ENV.fetch("FLINT_TOKEN"),
    namespace: namespace,
    error_handler: ->(method:, returning:, exception:) { swallowed << "#{method}: #{exception.message}" }
  )
  label = namespace ? "namespace #{namespace}" : "no namespace"
  data = keys.to_h { |k| [k, "v-#{k}"] }
  store.delete("rb:missing")
  store.write_multi(data)
  got = store.read_multi(*keys)
  check.("read_multi across slots (#{label})", got == data, got.inspect)
  got = store.fetch_multi(*keys, "rb:missing") { |k| "computed-#{k}" }
  want = data.merge("rb:missing" => "computed-rb:missing")
  check.("fetch_multi computes only the miss (#{label})", got == want, got.inspect)
  check.("nothing swallowed by the error handler (#{label})", swallowed.empty?, swallowed.join("; "))
end
if fails.any?
  puts "\nFAIL: #{fails.size} client-visible problem(s): #{fails.join(', ')}"
  exit 1
end
puts "\nall rails cache store checks passed"
RUBYSRC
    GEM_HOME="$RB_HOME" FLINT_PORT=$PORT FLINT_TOKEN=tok-acme "$RUBY" "$RB_HOME/store.rb"
    [ $? -eq 0 ] || { echo "FAIL: rails cache store compatibility"; exit 1; }
    RAN="$RAN, rails-cache-store"
  fi
fi

[ -z "$SKIPPED" ] || echo "SKIP: client(s) not checked:$SKIPPED"
echo "PASS: client compatibility — $RAN, each with its default options, connect, serve, and get their own native types back; excluded commands fail honestly"
