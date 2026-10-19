# ADR-0060: RediSearch's `FT.*` vector surface, for the frameworks that speak only it

Status: **ACCEPTED 2026-10-19, option A** (Jeff: "go with your
recommendation"), with the first release's scope and the two releases as
recommended. The downloads its verification needs, to install and run the
three clients' own tests, are still Jeff's to approve when it is built.
Nothing is built yet. An amendment, the guard on the data node rather than on
every proxy knowing, is PROPOSED below for Jeff before it is built.

Proposed 2026-10-19.

## Context

ADR-0059's survey read `redisvl` 0.28.0, `langchain-redis` 0.2.6 and
`llama-index-vector-stores-redis` 0.9.0. All three reach a server only
through RediSearch's `FT.*` commands, over documents in ordinary `HASH` or
JSON keys. None sends `VEC.*`, so a tenant on LangChain or LlamaIndex cannot
use Flint's vector search today, with or without ADR-0059's filters.

**What they send:**
- **Writes are plain key writes,** in pipelines without `MULTI`: `HSET key`
  with every field (a vector as its float32 bytes) or `JSON.SET key $ <doc>`,
  then `EXPIRE key ttl` when the store has a TTL. Deletes are `UNLINK`
  (`redisvl`, LangChain) or `DEL` (LlamaIndex).
- **`FT.CREATE idx ON HASH|JSON PREFIX n <prefix> ... SCHEMA ...`**, with
  `TEXT`, `TAG`, `NUMERIC` and `VECTOR FLAT|HNSW` fields (`TYPE`, `DIM`,
  `DISTANCE_METRIC`, and HNSW's `M`, `EF_CONSTRUCTION`, `EF_RUNTIME` and
  `EPSILON`). A JSON field is its path with `AS` and a name.
- **`FT.SEARCH idx "<filter>=>[KNN k @vec $vector AS vector_distance]"
  PARAMS 2 vector <bytes> RETURN n ... SORTBY vector_distance LIMIT 0 k
  DIALECT 2`.** Its filter is ADR-0059's subset. Searches without a vector
  page through matches for deletes by document id and for chat history.
- **`FT.INFO`** (for `num_docs` and the index's definition), **`FT._LIST`**
  and **`FT.DROPINDEX [DD]`**. `redisvl`'s hybrid and full-text queries
  also use `FT.AGGREGATE` and `FT.CURSOR`.
- **LangChain's default schema adds a `TEXT` field, `_index_name`,** and
  ANDs `@_index_name:("<index>")` into every vector query, so it needs an
  exact-phrase match on a `TEXT` field even when the application filters
  nothing.

**What Flint has to build on:**
- The co-processor (ADR-0017) serves a command family beside the data nodes,
  writes durably over its channel before it moves its index, and rebuilds the
  index from durable rows after a restart.
- ADR-0059 B (public 928c8f46) gave it declared fields, a field index and a
  filter parser in RediSearch's syntax.
- **The data nodes keep no index and send no change feed.** ADR-0052
  measured keyspace notifications and kept them absent. `FT.*`'s defining
  property is an index over keys the application writes with ordinary
  commands, so how the index hears of those writes decides the design.

## Options

### A. Writes to an indexed prefix are routed through the co-processor (recommended)

- `FT.CREATE` records the index and its prefixes. The proxy keeps each
  tenant's prefixes and sends every routable write to a key under one of
  them to the co-processor instead of the key's pair: `HSET`, `HDEL`, `DEL`,
  `UNLINK`, `EXPIRE` and `PEXPIRE`, `PERSIST`, `JSON.SET` and `JSON.DEL`.
- The co-processor writes the key over its channel first and moves its index
  only when that lands, as `VEC.SET` does: a search never returns a document
  that is not durable.
- **Every other write to an indexed key is refused** while the index exists:
  `EVAL` and `MULTI` whose declared keys fall under a prefix, `RENAME`,
  `COPY`, `RESTORE`, `HINCRBY`, `JSON.NUMINCRBY` and the rest. A refusal is
  loud; an index that silently misses a write is the outcome this option
  exists to avoid. None of the three clients sends one.
- The co-processor keeps a durable index of the keys it has written, as
  `VEC.*` keeps one of its ids, so a rebuild reads those keys, not the
  tenant's whole keyspace.
- `FT.SEARCH` answers from the co-processor's RAM, then reads its `k`
  documents' `RETURN` fields from the pairs over the channel: one pipelined
  read a search, where `VEC.SEARCH` reads nothing.

Cost: an indexed write takes the co-processor's hop, so indexed writes run
at `VEC.SET`'s rate, not a pair's. The proxy classifies each write's keys
against a prefix table, which is a lookup only for tenants that have an
index.

### B. The proxy writes the pair, then tells the co-processor

Writes go to the pair as today; after the pair answers, the proxy sends the
co-processor the key to re-read. Nothing else changes for writes. But the
index can miss a write (a proxy restart between the two, a co-processor that
is down), and only a periodic reconcile, a walk of every indexed prefix,
repairs it. Between reconciles a search can return a deleted document or
miss a new one.

### C. A change feed from the data nodes

Each master sends the co-processor every write under a subscribed prefix,
from its write path, so scripts, transactions, `RENAME` and expiry are all
seen. It is the most complete, and the largest: a new stream out of every
master, its position kept across failover and resumed on the new master,
and the keyspace-notification machinery ADR-0052 chose not to build.

### D. No index: `FT.SEARCH` scans the prefix

Correct with no feed at all, and linear in the documents under the prefix
for every search. Only for a few thousand documents, and a framework gives
no sign of how many it will write.

## Recommendation

**A.** It is the only option where the index is never ahead of the durable
documents and never silently behind them, with no new machinery in the data
nodes: it reuses the co-processor's write ordering, its rebuild and
ADR-0059's field index and parser. What it refuses, the frameworks do not
send. C would serve everything, and is worth its cost only when a tenant
needs `FT.*` over keys it writes with scripts.

### The first release's scope

- **Served:** `FT.CREATE` (`ON HASH` and `ON JSON`, `PREFIX`; `TAG`,
  `NUMERIC`, `TEXT`, and `VECTOR` `FLAT` or `HNSW` with `FLOAT32`, `L2`,
  `IP` or `COSINE`), `FT.SEARCH` (ADR-0059's filter, `=>[KNN ...]` with
  `EF_RUNTIME`, `PARAMS`, `RETURN`, `SORTBY` on the distance or a
  `NUMERIC` or `TAG` field, `LIMIT`, `NOCONTENT`, `DIALECT 2`), `FT.INFO`,
  `FT._LIST` and `FT.DROPINDEX [DD]`.
- **`TEXT` is matched as an exact phrase only:** `@f:("...")` matches a
  document whose field holds those words in that order, split as
  RediSearch splits them and lowercased, with no stemming or scoring. That
  is what LangChain's `_index_name` needs. Any other full-text term is
  refused by name.
- **Refused by name:** `FT.AGGREGATE`, `FT.CURSOR`, `FT.ALTER`, aliases,
  suggestions and spelling, `GEO`, and the `FT.SEARCH` options none of the
  three clients writes. Each is a later ADR if a tenant needs it.
- **Two releases,** as for every new write path: the release that
  introduces `FT.*` refuses `FT.CREATE` unless the co-processor runs with a
  flag, off by default. A release before it does not route writes under an
  index's prefix, so after a rollback writes would reach the pairs and miss
  the index. The next release turns it on, and a rollback from it lands on a
  release that routes them.

## Amendment, 2026-10-19 (PROPOSED, for Jeff): the guard is on the data node

Option A as written has the proxy route each write under an indexed prefix
to the co-processor. A fleet has several proxies, and nothing confirms that
every one of them has heard of a new index:
- the control plane pushes its registry to each proxy, but keeps no record
  of which version a proxy has applied;
- it skips a push when it judges a proxy's view unchanged;
- a proxy whose watch is broken goes on serving its last table.

A proxy that had not yet heard of an index would send a write straight to
the pair, and the index would miss it with nothing said. That is the failure
A exists to prevent.

Slot migration has the same shape and does not depend on the proxies
agreeing. The data node refuses a write it no longer owns with `-MOVED`, and
a proxy that is behind is redirected. The same works here:

- **The data node keeps each namespace's indexed prefixes.** The
  co-processor sets them over its channel, with `FLINT*` commands a tenant's
  connection cannot send. They are a row in the namespace, replicated with
  its data, so a failover's new master has them.
- **The node refuses a write under an indexed prefix unless it comes over
  the co-processor's channel.** The proxy marks the backend connections it
  opens for a channel in the `FLINTNS` handshake, as it marks async writes
  with its `a` flag. Any other write is refused with `-INDEXED <prefix>`.
- **A proxy that gets `-INDEXED` learns the prefix** and sends the write
  through the co-processor, as a proxy that gets `-MOVED` learns the new
  owner. Proxies converge on their own, and correctness never waits for
  them.
- **Scripts and transactions are checked on the node too,** for every key
  they write, declared or not. That closes the gap A had, which could check
  only the keys a script declares.
- **`FT.CREATE` completes in three steps:**
  1. register the prefixes on every pair, failing if a pair cannot be
     reached;
  2. backfill the index by scanning the prefixes' keys over the channel;
  3. serve.

  A write that reaches a pair between the first two steps is refused, so the
  backfill misses nothing.
- **The same guard closes BUG-0265's open half.** The co-processor's own
  `\x00vec\x00` rows become a prefix only its channel may write, so a tenant
  can no longer plant them.

**Cost:**
- a prefix lookup on each write, on the node, for a namespace that has an
  index or co-processor rows;
- one handshake flag on the proxy's channel connections;
- a refused-and-retried write the first time a stale proxy meets an index.

**Two releases, as before.** The release that brings the guard enforces it
only when the co-processor has set prefixes, which only a co-processor
started with the new flag does. A release before it ignores the prefix row,
so after a rollback writes would reach the pairs unguarded. The rollback
note is the same as the ADR's.

## Verification

- **The clients' own tests against Flint:** `redisvl`'s, and LangChain's and
  LlamaIndex's Redis vector store suites, run unchanged against a Flint
  fleet. That needs those packages installed and run, which is a download
  Jeff approves; the survey only read them.
- **Recall and latency** of `FT.SEARCH` against `VEC.SEARCH` on the same
  vectors, with and without a filter, through the proxy, so the cost of the
  document read is measured, not assumed.
- **The write path:** an indexed `HSET`, `JSON.SET`, `EXPIRE` and `UNLINK`
  at the co-processor's rate against a pair's; a refused write (`RENAME`,
  `EVAL`, `MULTI`) per kind; a co-processor restart rebuilding the same
  answers from the durable key index; a write shed with `-QUOTA` leaving the
  index unmoved.
- **Rollback:** the drill ADR-0059 used, with the release before `FT.*`,
  showing what it does with an indexed prefix, so the flag is shown to be
  needed.

## What this does not decide

- Full-text search with scoring, stemming and stop words.
- `FT.AGGREGATE` and hybrid text-and-vector ranking.
- An index over keys written by scripts or transactions (option C).
- Redis 8's vector sets (`V*` commands).
