# Flint — Tenant Guide

Everything a tenant needs to use Flint well: how to connect, what the
error vocabulary means, which opt-ins exist and exactly what each one
trades away, how quotas behave, and where to see your own numbers.

## What you receive

When your namespace is provisioned you get exactly what you need to
connect — and nothing you have to manage:

1. **Endpoint** — one `host:port`, TLS. The single address your client
   dials; the proxy hides every node, failover, and migration behind it.
2. **Token** — your credential. You present it with `AUTH`. Treat it like
   a password (it is stored only as a hash on our side).
3. **CA certificate** — a small `flint-ca.crt` file your client uses to
   verify the TLS connection to the endpoint.
4. **Your limits** — a rate quota (**ops/second**) and a **storage cap**
   (bytes), both visible live in the console. Typical starting grant:
   e.g. *50,000 ops/s, 50 GB* — your actual numbers are on your welcome
   page.

**About your namespace.** Your keys live in their own isolated keyspace —
invisible to every other tenant, by construction — but you never type the
namespace. Your token maps to it automatically at the proxy, so you use
ordinary key names (`user:42`, `session:abc`) and they are transparently
scoped to you. There is nothing to prefix and no namespace argument.

## Connecting

You get **one endpoint** and **one token** — no cluster topology, no node
addresses, no client-side sharding. Any standard Redis/Valkey client
works; `AUTH <token>` (or `AUTH <user> <token>`, the user is ignored).
During a token rotation both your old and new tokens authenticate until
the rotation completes; swap at your leisure inside that window.

**RESP2 and RESP3 both work, and you do not have to choose.** Clients that
open with `HELLO 3` — which is the default in redis-py 8 and current
node-redis — get RESP3, including credentials passed inside the handshake;
older clients stay on RESP2. Under RESP3 your client hands back its own
native types (`HGETALL` → `dict`, `SMEMBERS` → `set`, `ZSCORE` → `float`)
because Flint sends the typed replies those clients expect. Nothing to
configure either way.

### Sample code

**redis-cli / valkey-cli**

```sh
redis-cli -h <endpoint> -p <port> --tls --cacert flint-ca.crt -a <token>
> SET user:42 "hello"
OK
> GET user:42
"hello"
```

**Python (`redis-py`)**

```python
import redis

r = redis.Redis(
    host="<endpoint>", port=<port>,
    password="<token>",
    ssl=True, ssl_ca_certs="flint-ca.crt",
)
r.set("user:42", "hello")          # in the write-ahead log before this returns
print(r.get("user:42"))            # b"hello"
```

**Node.js (`ioredis`)**

```js
const Redis = require("ioredis");
const fs = require("fs");

const r = new Redis({
  host: "<endpoint>", port: <port>,
  password: "<token>",
  tls: { ca: fs.readFileSync("flint-ca.crt") },
});
await r.set("user:42", "hello");
console.log(await r.get("user:42")); // "hello"
```

**Go (`go-redis/v9`)**

```go
import (
    "context"; "crypto/tls"; "crypto/x509"; "os"
    "github.com/redis/go-redis/v9"
)

ca, _ := os.ReadFile("flint-ca.crt")
pool := x509.NewCertPool(); pool.AppendCertsFromPEM(ca)
rdb := redis.NewClient(&redis.Options{
    Addr:      "<endpoint>:<port>",
    Password:  "<token>",
    TLSConfig: &tls.Config{RootCAs: pool},
})
rdb.Set(context.Background(), "user:42", "hello", 0)
```

Everything after connecting is ordinary Redis. Failovers are absorbed: if
a storage node dies mid-request the proxy retries against the promoted
successor within its budget — you see a latency spike, not an error, and
never reconfigure anything (promotion in seconds, bounded loss).

## The error vocabulary

Flint tells you to back off or stop in exactly three ways. Everything
else a well-behaved client never sees (`-MOVED` and `-TRYAGAIN` are
absorbed by the proxy).

| error | meaning | what to do |
|---|---|---|
| `-THROTTLED ...` | back-pressure: your ops/s quota, a loss-protection guard, or admission control | retry with backoff — the condition is transient |
| `-QUOTA ...` | your storage cap is exceeded; **writes** are rejected | reads still work, and **deletes always work** — free space (DEL/UNLINK/FLUSHALL/EXPIRE) and the verdict clears itself within a sweep |
| `-QUOTA server is low on disk space ...` | the **server**, not your cap, is short of room | same contract: reads served, deletes work, and it clears itself once space returns. Your usage may be well under quota — this one is ours to fix, and it pages us |
| `-WRONGPASS` / `-NOAUTH` | bad or missing token | check the token; re-AUTH |

Two invariants worth knowing:

- **You can always read your data out.** No quota state ever blocks a
  read.
- **The self-clear path is never blocked.** Space-reducing commands are
  exempt from the storage shed precisely so a full tenant can cure the
  condition that gated it.

## Quotas

Two limits, both visible in the console:

- **ops/s** (fleet-wide): enforced as token buckets at the proxies with a
  one-second burst allowance. Over it, requests shed `-THROTTLED`.
- **storage bytes**: metered as *resident* bytes (engine-level, includes
  storage overhead; recently written data may register with a short
  delay). Crossing the cap flips writes to `-QUOTA`; dropping back under
  ~90% clears it automatically. No human is involved in either direction.

## Opt-ins and their staleness contracts

Everything below is **off by default** and changes read semantics only
for you, only when you ask. Each trades a bounded, stated staleness
window for performance:

| opt-in | what it does | the window you accept |
|---|---|---|
| replica reads | your reads fan across the pair's replicas; writes stay on the master | replication lag, bounded by the cluster's lag cap |
| proxy near-cache | repeated GETs answer from a short-TTL cache at the proxy | the cache TTL; a write through the *same* proxy invalidates immediately |
| async writes | writes group-commit in batches (specialist workloads) | cross-client read freshness = queue depth; your *own* reads are never stale |

With replica reads **and** the near-cache, the windows add: worst-case
staleness = cache TTL + replica lag. Both were your choice; the sum is
the real bound. Your own writes through one proxy connection always read
back fresh regardless of any opt-in.

## Seeing your numbers

Two ways, same data. The **console** (a browser portal) is part of the
Crestway managed service; on a self-hosted fleet the same scoped views
answer on your normal connection as `PROXYLATENCY` (your latency lanes)
and `PROXYHOTKEYS` (your hot keys) — no portal required.

### The console (managed service)

Point a browser at your console endpoint and paste your token. You get:

- **Latency** (read and write lanes: count, avg, p50, p99) — measured
  *at the proxy*, one hop from your client. Your client-side numbers
  minus these ≈ your network. This is the number to bring to a support
  conversation: it decomposes "is it me or is it Flint" with data.
- **Storage** — resident bytes vs your cap, and the live quota verdict.
- **Hot keys** — your top keys by sampled traffic. A dominating key here
  is your first candidate for client-side caching (or the replica-reads
  opt-in if it is read-hot).

The console authenticates with **your token**. It has no privileged
access: it can see exactly what you can see through the API, nothing
else — key names and latency shapes of other tenants are invisible to
it by construction.

## Command surface

Strings (SET/GET/SETNX/SETEX/MSET/INCR/DECR/INCRBY/DECRBY/APPEND),
hashes, sets, sorted sets, lists, TTLs (EXPIRE/PEXPIRE/TTL/PERSIST),
DEL/UNLINK/EXISTS/TYPE, DBSIZE/FLUSHALL (scoped to your namespace).
Conformance is validated against Valkey continuously. Not in v0:
pub/sub, streams, Lua, MULTI/EXEC, blocking commands, cross-slot
multi-key operations (multi-key commands route by their first key).
