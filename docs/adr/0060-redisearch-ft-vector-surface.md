# ADR-0060: RediSearch's `FT.*` vector surface, for the frameworks that speak only it

Status: **ACCEPTED 2026-10-19, option A** (Jeff: "go with your
recommendation"), with the first release's scope and the two releases as
recommended. The downloads its verification needs, to install and run the
three clients' own tests, are still Jeff's to approve when it is built.
Release N is built, on a branch (As built, below). The amendment below, the guard on the
data node rather than on every proxy knowing, was ACCEPTED 2026-10-19 (Jeff:
"Accept the node guard"). Amendment 2, where each node keeps the prefixes,
was ACCEPTED 2026-10-19, option A (Jeff: "A: node row").

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

## Amendment, 2026-10-19 (accepted the same day): the guard is on the data node

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
  opens for a channel with `FLINTCHANNEL` after the `FLINTNS` handshake (not
  a `FLINTNS` flag: `FLINTNS` refuses a flag it does not know, which would
  fail every dial to an older node; built 2026-10-19 for BUG-0265). Any
  other write is refused with `-INDEXED <prefix>`.
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

## Amendment 2, 2026-10-19 (accepted the same day, option A): where each node keeps the prefixes

The first amendment says the prefixes are "a row in the namespace,
replicated with its data". Building it showed that an ordinary row cannot
do that. A key in the namespace lives on the one pair that owns its slot,
but every pair has to refuse writes under the prefixes. The node's own
`\x00flint\x00` rows are no better, because replication drops them by
design: they are a node's identity, not data. Every pair has to hold the
prefixes:
- through a failover, so the new master already has them;
- through a slot move to a pair that never had them, as when the fleet
  scales out;
- through a roll, including a replica that runs the older release.

### A. A node row of its own, outside `\x00flint\x00` (recommended)

- **The row.** `\x00flintidx\x00<ns>` holds the namespace's prefixes, and
  each node keeps it in memory for its write path. The prefix
  `\x00flintidx\x00` is not under `\x00flint\x00`, so replication's filter
  passes it, and a replica on either release applies it as it applies any
  other row.
- **Writing it.** `FLINTINDEXED <prefix>...` writes the row, from a channel
  only. The proxy fans it out to every master, as it does `DBSIZE`, and
  reports any pair it could not reach.
- **Where else it comes from.** A checkpoint full sync copies it with
  everything else. A slot move carries it: the destination asks the source
  for the namespace's row before it takes the slot's first key.
- **Rebuilds.** The co-processor writes the row again on every rebuild, as
  well as at `FT.CREATE` and `FT.DROPINDEX`, so a pair that lost it, such
  as one restored from an older backup, gets it back the next time the
  co-processor starts.
- **The memory copy.** A node loads it at start. It updates the copy when
  `FLINTINDEXED` writes the row or a replication batch applies it, and
  reloads after a full sync and on promotion.

Cost:
- one small row per namespace per node;
- a fan-out at `FT.CREATE` and `FT.DROPINDEX`;
- one extra step at the start of a slot move;
- a namespace lookup on each write, for a namespace that has the row.

### B. One row per slot

The co-processor writes the prefixes once for each of a namespace's 16,384
slots, as ordinary keys whose hash tags pick the slots. Each row then moves
with its slot and replicates with it, and nothing new is needed in the node.
But one `FT.CREATE` costs 16,384 writes, about 64 channels at a channel's
256-command budget, and so does every change of the prefixes.

### C. The control plane pushes the prefixes to every node

This is how per-seat settings such as `--evictable-ns` reach a node today.
But the control plane does not record which version each node has applied.
That is the gap the first amendment exists to close, so C brings it back
for the nodes.

**Recommendation: A.** It is the only one of the three that reaches every
pair through failover, scale-out and a roll without a cost per slot, and
it reuses replication, full sync and the slot move as they are. The
rollback note does not change: a release before the guard ignores the row.

## As built, release N (2026-10-19)

Branch `adr0060-ft`, held by `coproc_ft_drill`. Two pairs, a proxy and
`flint-vec --ft`, over HASH and JSON documents.

- **`--ft` gates `FT.CREATE` only.** An index made while it was on is still
  loaded, searched, written and dropped without it. Otherwise its prefixes
  would stay guarded on every pair with no one to serve their writes, and
  the tenant could not drop the index to free them.
- **`FT.CREATE` takes the amendment's three steps on its own channel:**
  1. `PROXYINDEXED` with the namespace's prefixes, which the proxy fans out
     to every master as `FLINTINDEXED`, failing if one cannot be reached;
  2. the definition rows;
  3. a backfill, a `SCAN` of each prefix over the channel.

  A backfill that outlasts its channel's budget resumes on the channel of
  the next `FT.SEARCH` or `FT.INFO` of that index. Until it ends, the search
  answers `-LOADING`.
- **A routed write takes its keys' locks** across a document-list `SADD`,
  the write itself, and a read-back (`HGETALL` or `JSON.GET key $`, then
  `PTTL`), and indexes what it read. That is up to five channel commands for
  one key. A write that landed but could not be read back takes the key out
  of its indexes and answers an error, so a search never returns what is no
  longer there.
- **A delete needs no read-back.** Under its lock, nothing writes the key
  again.
- **The proxy learns a prefix from `-INDEXED`** and sends writes under it to
  the `FT.` co-processor for 10 minutes, learning it again after that. A
  dropped index's prefix costs the extra hop until then.
- **The first write after a proxy learns a prefix may be split.** If it is a
  multi-key `DEL` across pairs, the pairs without the guarded key delete
  theirs before the co-processor deletes them all, and the reply's count can
  be short.
- **A node finds a command's keys with `flint_commands::written_keys`,** a
  superset by command: `MSET`'s every other argument, `RENAME`'s two, the
  `*STORE` destinations, and so on. A value is never mistaken for a key.
- **Scripts are checked twice:** their declared keys before they run, and
  each row as they write it, so a key a script builds is caught.
- **`FLUSHALL` and `FLUSHDB` are refused in a namespace with an index.**
  RediSearch drops its indexes on a flush, and the clients' test suites flush
  between tests. Serving a flush through the co-processor, which would drop
  the indexes and their prefixes with it, is the obvious next step and is not
  built.
- **Known differences from RediSearch, to check against the clients' own
  tests** (the downloads are still Jeff's to approve):
  - an exact phrase keeps its stop words;
  - replies are RESP2's shape only;
  - L2 distances are squared;
  - the missing-index errors are `Unknown index name` (`FT.INFO`),
    `Unknown Index name` (`FT.DROPINDEX`) and `<index>: no such index`
    (`FT.SEARCH`), RediSearch's wording as recalled, not read from its
    source or the clients'.
- **A rebuild that cannot register its prefixes** on every pair leaves the
  namespace `-LOADING`, `VEC.*` included, until it can. A failed
  registration is never served as if it had worked.
- **Cost:** a namespace's rebuild reads every document twice over the
  channel (its document and its TTL), about 120 documents per channel at the
  default budget of 256, so a large FT namespace rebuilds slower than a
  vector set of the same size.
- **Not yet measured:** recall and latency of `FT.SEARCH` against
  `VEC.SEARCH`, and the indexed write rate against a pair's.

## Verification against redisvl's own suite, 2026-10-20

Downloads approved by Jeff (2026-10-20):
- redisvl 0.28.0's source distribution from PyPI, sha256-checked. It
  carries its tests; langchain-redis's and the LlamaIndex store's do not,
  and theirs are only on GitHub, a source not yet approved.
- redis-py 8.1.0, pytest and pytest-timeout, in a fresh Python 3.14
  virtualenv on the laptop.

The run:
- **The suites:** redisvl's integration suites for indexes, queries,
  results, storage, message history and field options, 18 files and 321
  tests.
- **The fleet:** two `mem` pairs, `flint-vec --ft` and a proxy routing `VEC.`
  and `FT.`.
- **Changed:** only the test harness, so that it reaches that fleet rather
  than starting Redis in Docker.
- **Left out:** the suites that need a model, a service or a cluster
  (vectorizers, rerankers, LLM caches, routers, SQL, migrations, cluster,
  SVS, MCP).

| run | passed | failed | errors |
|---|---|---|---|
| as built, `test_search_index.py` alone | 6 of 49 | 40 | 3 |
| after the fixes below, all 18 files | 202 | 51 | 65 |
| the same, with the tests' `GEO` fields removed, for discovery only | 223 | 78 | 17 |

**No failure is a wrong answer.** Every one is a refusal of what is not
served, or a test that needs `sentence-transformers`. Where Flint answers,
it answers what the tests expect.

**Fixed from the suite:**
- **`FT.CREATE`** takes `SCORE`, which redis-py's index definition sends on
  every index, and `LANGUAGE`, `LANGUAGE_FIELD`, `SCORE_FIELD`,
  `PAYLOAD_FIELD` and a field's `WITHSUFFIXTRIE`. All of these shape
  full-text scoring, stemming or suffix queries, none of which is served.
- **`FT.SEARCH`** takes `SCORER`, which picks the text scoring function.
- **`FT.INFO` answers the schema as declared**, which redisvl reads back to
  rebuild it:
  - each field's `SORTABLE`, `UNF`, `NOSTEM`, `WITHSUFFIXTRIE` and `WEIGHT`;
  - the vector's `M`, `ef_construction` and `epsilon`;
  - `stopwords_list`.
- **RESP3.** redis-py 8 negotiates RESP3 by default, and RediSearch answers
  `FT.SEARCH` and `FT.INFO` in another structure there. The proxy carries
  both shapes, from the co-processor's RESP2 reply.
- **A cosine distance is never below 0.** A vector's distance to itself came
  out at -1.2e-7.
- **A term on a `NOINDEX` field matches no document**, as in RediSearch,
  rather than being refused.
- **A vector range query is refused by name.** Before, it read as a filter
  on an unknown field.

**Not served, by count of failing tests, each a proposal for Jeff:**
- **`WITHSCORES`** (22). Each result's text score. No search here is ranked
  by one.
- **`GEO` fields** (most of the 65 setup errors, and 6 queries).
  redisvl's shared test schema has one.
- **Vector range queries, `VECTOR_RANGE`** (12).
- **`MODULE LIST`** (9). The tests read it to decide what to run; the
  proxy answers it as an unknown command.
- **`FT.AGGREGATE`** (8). redisvl iterates documents by a filter with it,
  `WITHCURSOR`.
- **redisvl's upsert script** (4), an `EVAL` over a document's key. The
  co-processor does not run scripts, and the node refuses one on an indexed
  key.
- **`EXPIRE` inside `MULTI`** (2). redisvl's `expire_keys` sends them as a
  transaction, which the node refuses.
- **A page of results larger than a channel's budget** (2). Each result is
  read back with one command, and a channel has 256.
- **`SORTBY` on a TEXT field** (2).
- **Aliases** (1), and full-text terms (2).
- **The first command after a co-processor restarts** answers `-LOADING`,
  which redis-py does not retry. ADR-0017 D3 makes warming visible on
  purpose, and `coproc_vec_drill` holds it so. A proxy that retried for a few
  seconds would hide it from the frameworks, which do not retry.

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
