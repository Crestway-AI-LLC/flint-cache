# ADR-0060: RediSearch's `FT.*` vector surface, for the frameworks that speak only it

Status: **ACCEPTED 2026-10-19, option A** (Jeff: "go with your
recommendation"), with the first release's scope and the two releases as
recommended. Jeff approved the downloads its verification needs, to install
and run the three clients' own tests, on 2026-10-20 ("Yes, both"). Release
N is built (As built, below) and on public main after v0.1.0-rc.85. The
amendment below, the guard on the data node rather than on every proxy
knowing, was ACCEPTED 2026-10-19 (Jeff:
"Accept the node guard"). Amendment 2, where each node keeps the prefixes,
was ACCEPTED 2026-10-19, option A (Jeff: "A: node row"). Amendment 3,
what the frameworks' suites need next, was ACCEPTED 2026-10-20 as
recommended (Jeff: "As recommended"): parts 1, 3, 4, 5 and 6 are built,
and part 2 is documented only.
Amendment 4, a script and `JSON.MERGE` on an indexed key, was ACCEPTED
2026-10-30, option A (Jeff: "go with your recommendations"), and is built
(its As built, below).
Amendment 5, several VECTOR fields in one index, `FLOAT64` vectors and
several vector ranges in one query, is PROPOSED 2026-10-30, recommended A.

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

## Amendment 3, 2026-10-20 (accepted the same day, as recommended): what the frameworks' own suites need next

Jeff set its scope on 2026-10-20 ("Amend ADR-0060, cheap set"):
- `MODULE LIST`;
- writes inside `MULTI`;
- pages past a channel's budget;
- aliases.

`GEO`, vector range queries, `FT.AGGREGATE`, `WITHSCORES` and full text, and
redisvl's upsert script, are left to ADRs of their own. `-LOADING` stays
visible as ADR-0017 D3 has it. The three suites (Verification, below)
measured each item. Two measurements change what is worth building, and the
suites found a fifth item, the flush. Each part below is its own choice.

### 1. A page of results larger than a channel's budget (recommended)

- **What fails.** `FT.SEARCH` reads each result back over its one channel,
  and refuses a page that does not fit: about 250 results at the default
  `--family-budget` of 256. redisvl's `clear()` and `paginate()` ask for
  pages of 500, so `clear()` fails on any index of more than about 250
  documents.
- **Why not more channels.** The proxy mints one single-use grant per
  family command (ADR-0010 D2), so one `FT.SEARCH` has one channel.
- **The change.** The proxy sizes `FT.SEARCH`'s grant to its page: the
  commands its results need plus a margin, at least `--family-budget`, and
  at most what 10,000 results need. Its deadline grows with it, by the same
  share. A page past 10,000 is refused with today's error. RediSearch's own
  default ceiling is far higher (`MAXSEARCHRESULTS`), but the largest page
  the suites ask for is 500.
- **Rollout.** Proxy only: a co-processor of either release uses the budget
  it is given. A proxy before it refuses as today.

### 2. Writes inside `MULTI` (recommended: not built)

- **What fails.** redisvl's `expire_keys` sends its `EXPIRE`s as one
  transaction, and langchain-redis's chat history `clear()` its `DEL`s. The
  node refuses a write under an indexed prefix at queue time.
- **What building it would fix.** None of the suites' failing transactions.
  Every one names keys in different slots (`rvl:1`, `rvl:2`), which a
  transaction through the proxy refuses with `CROSSSLOT` whether or not the
  keys are indexed (`docs/command-support.md`). Serving indexed writes in a
  one-slot transaction would need the co-processor to run `MULTI` over its
  channel under every key's lock. That is real work for a case the
  frameworks do not send.
- **Instead:** document the case in `docs/command-support.md`, beside
  `CROSSSLOT`.

### 3. `MODULE LIST` (recommended: answer it when `FT.` is served)

- **What reads it.** redisvl reads it only to decide whether SVS-VAMANA
  indexes exist (`search` at 20810 or later). Its test suite reads it to
  skip what an older module lacks. langchain-redis and LlamaIndex do not
  read it.
- **The change.** When the proxy declares an `FT.` family, `MODULE LIST`
  answers one module, `search` at version 20600: the RediSearch whose
  `FT.SEARCH` dialect 2 and vector fields the frameworks require
  (redisvl's `DEFAULT_REQUIRED_MODULES`). It is below 20810, so redisvl does
  not try SVS-VAMANA, which is not served. Without an `FT.` family it stays
  an unknown command.
- **The risk.** A client that reads a version as a promise of every feature
  of that version would try what is not served, such as `GEO`. It is then
  refused by name, as today.

### 4. Aliases (recommended)

- `FT.ALIASADD`, `FT.ALIASUPDATE` and `FT.ALIASDEL`, and an alias accepted
  wherever an index name is (`FT.SEARCH`, `FT.INFO`, `FT.DROPINDEX`).
- An alias is a durable row of the index list's kind, in the namespace, and
  is rebuilt with it. `FT.DROPINDEX` removes its aliases, as RediSearch does.
- One suite test needs it. It is cheap, and an alias is how a schema changes
  without the clients changing: build the new index, then move the alias.

### 5. A flush drops the namespace's indexes (found by the suites; recommended)

- **What fails.** `FLUSHALL` and `FLUSHDB` are refused in a namespace with an
  index (As built, below). RediSearch frees every index when a flush starts
  (its `onFlush`), and langchain-redis's and redisvl's suites flush between
  tests.
- **The change.**
  - The node lets a tenant's flush through when the namespace has
    prefixes, and the flush deletes the namespace's
    `\x00flintidx\x00<ns>` row with its keys. The row is outside the
    namespace, so the flush names it. Its delete replicates as the row's
    writes do.
  - After the flush, the proxy's FLINTNSDROP (BUG-0268) makes the
    co-processor forget the namespace's indexes with its sets.
  - A write under a dropped prefix that the proxy still sends through the
    co-processor, for up to its 10 minutes, is written through
    unindexed.
- **Rollout.** A node before it refuses the flush with `-INDEXED`, as today,
  so a mixed pair refuses rather than half-flushing.

### 6. Results that tie come in write order (found by the suites; recommended)

- **What differs.** RediSearch orders results that tie, on distance or on a
  `SORTBY` value, and results with no order asked for, by its internal
  document ids, which it assigns at each write. Flint orders them by key.
  LlamaIndex's test embeds every text as the same vector (`MockEmbedding`,
  `[0.5] * dim`) and expects the
  first document written. It passes on Flint only when the keys, which are
  random, happen to sort that way.
- **The change.** Each document carries the sequence number of its last
  write, and ties are broken by it, as RediSearch's document ids break
  them. For a KNN, the k-th place among ties goes to the earliest write on a
  `FLAT` index. On `HNSW` the walk decides which tied documents are found at
  all, as in RediSearch.
- **Cost:** 8 bytes per document. A rebuild assigns numbers in the order it
  reads the documents, as RediSearch's reload assigns new ids.

### Amendment 3, as built (2026-10-20, rides with release N)

- **Part 1.** The proxy's `family_grant` gives `FT.SEARCH` a budget of its
  `LIMIT`'s count plus 16, at least `--family-budget`, for at most 10,000
  results. Its deadline grows by 1 ms for each command past the budget,
  about 15 seconds at 10,000. Growing it "by the same share", as part 1
  says, would allow 391 seconds there. The `LIMIT` is read with the
  `PARAMS` and `RETURN` blocks stepped over, so a parameter named `LIMIT` is
  not one.
- **Part 2.** Documented in `docs/command-support.md`, beside `CROSSSLOT`.
- **Part 3.** The proxy answers `MODULE LIST` while an `FT.` family is
  declared: `search` at 20600, keyed with bulk strings as Redis answers it.
- **Part 4.**
  - Aliases are one durable set (`kind = b'a'`). Each member is
    `<alias length>:<alias><index>`, so a rebuild reads every alias with one
    command.
  - `FT.ALIASUPDATE` removes the old member before it adds the new one, so
    a crash between them leaves the alias naming nothing rather than the
    wrong index.
  - The refusals are RediSearch's:
    - `Alias already exists`;
    - `Unknown index name (or name is an alias itself)`;
    - `Alias conflicts with an existing index name`;
    - `Alias does not exist`.
- **Part 5.** Outside `MULTI`, the node lets a tenant's `FLUSHALL` or
  `FLUSHDB` through and deletes the namespace's `\x00flintidx\x00<ns>` row
  first. The proxy's `FLINTNSDROP` after the flush (BUG-0268) has the
  co-processor forget the indexes. Inside `MULTI` the flush is still refused.
- **Part 6.** Each document carries a sequence number, 8 bytes, which raises
  the meter's charge per document from 96 to 104 bytes. A KNN asks its set
  for 16 more neighbours than it answers, and breaks ties by the sequence.
- **`coproc-ft` gates only `FT.CREATE`.** flintctl routes the `FT.` family
  to every `VEC.` seat whether it is on or off. Before, off took the route
  away too, so an index made while it was on stranded its prefixes: every
  data seat refused the writes under them, and no `FT.DROPINDEX` could reach
  the co-processor to release them. A seat of a release before `FT.*`
  answers the family as an unknown command.
- **Held by:**
  - unit tests:
    - `results_that_tie_come_in_write_order`;
    - `an_alias_names_its_index_and_goes_with_it`;
    - `an_ft_search_is_granted_a_channel_for_its_page`;
  - `coproc_ft_drill`'s `amend3`, `aliassame` and `flush` sections.

## Amendment 4, 2026-10-30 (ACCEPTED the same day, option A): a script, and `JSON.MERGE`, on an indexed key

**Why now.** With `FT.AGGREGATE` served (ADR-0064), redisvl's
`update_by_filter` gets past its cursor to its writes, and stops there.
Each write is one of two scripts on one declared key:

```
EVAL "if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
      redis.call('HSET', KEYS[1], unpack(ARGV)) return 1" 1 <key> <field> <value> ...
EVAL "if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
      redis.call('JSON.MERGE', KEYS[1], '$', ARGV[1]) return 1" 1 <key> <json>
```

A node refuses both, as amendment 2 has it: a script that writes under an
index's prefix, and `JSON.MERGE`, which the co-processor does not serve.
11 of redisvl's catalogued tests stop there (ADR-0064's As built), up from
4 before `FT.AGGREGATE`.

**What Flint does today:**
- **The proxy** sends a write a node refused with `-INDEXED` to the `FT.`
  co-processor, and every write under that prefix for 10 minutes after.
  A script is no exception (`data_command`, after amendment 2).
- **The co-processor** serves only the writes `ftstore::routed_write`
  lists: `HSET`, `HMSET`, `HDEL`, `DEL`, `UNLINK`, the `EXPIRE` family,
  `PERSIST`, `JSON.SET`, `JSON.DEL` and `JSON.FORGET`. It refuses a script
  and `JSON.MERGE`.
- **A script runs on its node.** Its writes are buffered and committed as
  one batch when it ends (ADR-0051). Its guard refuses a write under an
  index prefix at the row, so a key it builds and never declares is caught
  too.
  - redisvl's scripts do not `pcall`, so the refusal ends each one with
    nothing written.
  - A script may write any key in its declared keys' slot (ADR-0052 D2).
  - Over a co-processor's channel, no prefix is guarded at all.

### Options

**A. The co-processor runs the script, and the node lets it write only the
keys it declares (recommended).**
- `EVAL` and `EVALSHA` join the co-processor's writes. It runs the script
  on the pair over its channel, under the declared keys' locks, as it runs
  `HSET`. It then reads each declared key under an index's prefix back, and
  indexes it.
- A script that arrives over a channel runs with the namespace's prefixes
  guarded, except for its declared keys. So a write to an undeclared key
  under a prefix is refused there too, as it is from a tenant, and the
  co-processor's read-back holds everything the script wrote under a
  prefix.
- `JSON.MERGE` joins the routed writes, and is read back as `JSON.SET` is.
- **Exact** for every script that declares the indexed keys it writes, as
  redisvl's do and as Redis Cluster asks of every script.
- **Costs:**
  - a node change: the channel's script guard, about the size of
    amendment 2's;
  - two more commands on the co-processor's write path, which already
    locks, writes, reads back and indexes.

**B. A, and a script may also write an undeclared indexed key.** The node
reports every key under a prefix that a script wrote, and the co-processor
reads those back.
- Exact for every script.
- A new reply shape for the one channel command that runs a script, and
  no client of the three needs it.

**C. Keep refusing them.** redisvl's `update_by_filter` stays refused on an
index.

### Recommendation: A

- **Releases:** the node and the co-processor change together. If this
  lands before v0.1.0-rc.86 is cut, it ships with `FT.*`'s first release,
  under the rule that already holds: every seat runs the release before
  `coproc-ft on`. So no new rollback rule.
- **Unchanged:**
  - a script inside a transaction, which is refused as any transaction
    holding an indexed write is (2 of redisvl's tests, `EXPIRE` inside
    `MULTI`);
  - a script that `pcall`s the refusal of an undeclared key, which gets
    the refusal and goes on, as today;
  - `EVAL_RO`, `EVALSHA_RO`, `FUNCTION` and `FCALL`, which Flint does not
    serve.
- **Verification:**
  - unit tests for the channel guard: a declared key under a prefix is
    written, and an undeclared one is refused;
  - unit tests for the co-processor's plan: a script's declared keys are
    read back, and `JSON.MERGE`'s key is;
  - `coproc_ft_drill`: redisvl's two scripts through the proxy, on HASH and
    JSON, with each document's new values found by a search, and an
    undeclared indexed key refused;
  - the suites: the 11 tests pass, and nothing that passed now fails.

### Amendment 4, as built (2026-10-30, rides with v0.1.0-rc.86)

- **The co-processor** serves `EVAL`, `EVALSHA` and `JSON.MERGE` as routed
  writes (`ftstore::routed_write`).
  - A script's keys are the ones it declares, read from its `numkeys`. Each
    one under an index's prefix is read back after the script and indexed,
    as an `HSET`'s key is.
  - A script that declares no keys is refused: `ERR a script that writes a
    key under an FT index's prefix declares it in KEYS (ADR-0060 amendment
    4)`. It reaches the co-processor only after its node refused it with
    `-INDEXED`, so it wrote under a prefix.
  - `JSON.MERGE` is planned and read back as `JSON.SET` is.
- **The node.** An `EVAL` or `EVALSHA` over a co-processor's channel runs
  with its namespace's index prefixes guarded, as a tenant's script does,
  except for its declared keys (`KeyGuard::declared_exempt`). So a key it
  builds under a prefix is refused at the row. A script that does not
  `pcall` the refusal ends with nothing written, its declared keys
  included.
- **The proxy** is unchanged. It already sends a write a node refused with
  `-INDEXED`, outside a transaction, to the `FT.` family.
- **Held by:**
  - unit tests:
    - `a_channel_script_writes_only_its_declared_indexed_keys` (node);
    - the co-processor's plan test, for a script's declared keys, a script
      that declares none, and `JSON.MERGE`;
  - `coproc_ft_drill`'s `scripts` section: redisvl's two scripts and
    `JSON.MERGE` through the proxy on HASH and JSON indexes, each write
    found by a search, a script on a missing key making no document, and a
    script that builds an undeclared indexed key refused whole.

## Amendment 5, 2026-10-30 (PROPOSED): several VECTOR fields in one index, `FLOAT64` vectors, and several vector ranges in one query

**Why now.** ADR-0064 left one redisvl suite file waiting on this ADR:
`test_aggregation.py`, 26 cases (16 tests, some run with several
parameters). Its fixture is one index with three VECTOR fields, each FLAT
and cosine: `user_embedding` (3 dims) and `image_embedding` (5 dims) are
`FLOAT32`, and `audio_embedding` (6 dims) is `FLOAT64`. Flint refuses that
index ("an index has at most one VECTOR field"), so every case stops at the
fixture.

**What redisvl sends** (redisvl 0.28.0, printed from its own builders):

```
FT.AGGREGATE <idx> "(~@description:(a | medical | ...))=>[KNN 10 @user_embedding $vector AS vector_distance]"
  SCORER BM25STD ADDSCORES LOAD 6 user ... DIALECT 2
  APPLY "(2 - @vector_distance)/2" AS vector_similarity APPLY "@__score" AS text_score
  APPLY "0.3*@text_score + 0.7*@vector_similarity" AS hybrid_score SORTBY 2 @hybrid_score DESC MAX 10

FT.AGGREGATE <idx> "@user_embedding:[VECTOR_RANGE 2.0 $vector_0]=>{$YIELD_DISTANCE_AS: distance_0}
                    @image_embedding:[VECTOR_RANGE 2.0 $vector_1]=>{$YIELD_DISTANCE_AS: distance_1}"
  SCORER TFIDF LOAD 6 user ... DIALECT 2
  APPLY "(2 - @distance_0)/2" AS score_0 APPLY "(2 - @distance_1)/2" AS score_1
  APPLY "@score_0 * 1.0 + @score_1 * 1.0" AS combined_score SORTBY 2 @combined_score DESC MAX 10
```

The first is `AggregateHybridQuery`, a KNN on one of the index's VECTOR
fields. The second is `MultiVectorQuery`: one `VECTOR_RANGE` term for each
of two or three fields, ANDed, each yielding its distance for the `APPLY`
steps. A `FLOAT64` field's query vector is 8 bytes a dimension.

**Measured against today's build** (2c2c6281), with the fixture cut to its
one `FLOAT32` VECTOR field so that it loads, 13 cases pass: the 12 hybrid
queries, with their filters, GEO, stop words and weights, and one argument
check. The 13 multi-vector cases stop at "a query takes one vector range
term (ADR-0061)". So the work is three
things and nothing else this file reaches:

1. several VECTOR fields in one index;
2. `FLOAT64` vectors;
3. several `VECTOR_RANGE` terms, ANDed, in one query.

**What Flint does today:**
- `IndexDef` holds at most one `VectorField`, and the index backs it with
  one vector set (ADR-0017's engine) over the index's documents.
- `parse_vector` refuses any `TYPE` but `FLOAT32`, and a document's vector
  must be `DIM` × 4 bytes.
- `take_range` takes one `VECTOR_RANGE` term per query: alone or ANDed
  with a filter. A second, or one under `|` or `-`, or one beside a KNN, is
  refused by name. A range counts at most 10,000 documents (ADR-0061).
- The kernel is float32, with the 8-bit and 1-bit codes of ADR-0049 built
  on it.

### Options

**A. Several VECTOR fields, each its own set; `FLOAT64` read as float32;
several ranges ANDed (recommended).**
- Each VECTOR field gets its own vector set over the index's documents,
  with its own `DIM`, metric and algorithm. A KNN or a range names its
  field, as RediSearch's do; naming a field the index does not have is
  refused as today. An index holds at most 8 VECTOR fields, refused by
  name past that, each a set of its own; the 16-attribute cap of the other
  fields is unchanged, so every index that loads today still loads.
- A `FLOAT64` field's vectors, in documents and in queries, are 8 bytes a
  dimension and are rounded to float32 when read. Distances are computed in
  float32, as for every other field, so a `FLOAT64` field's distances agree
  with RediSearch's to about seven significant digits, not to sixteen. The
  document keeps the bytes its client wrote; `FT.INFO` reports `FLOAT64`.
  Documented in `docs/command-support.md`.
- A query may AND several `VECTOR_RANGE` terms, on the same field or on
  different ones. Each term is the set of documents within its radius,
  each counted up to the 10,000 of ADR-0061, and the query's documents are
  those in every term and in the filter. Each term's `$YIELD_DISTANCE_AS`
  names a distance that `FT.SEARCH` returns and `FT.AGGREGATE`'s steps
  read; two terms may not yield the same name. A range under `|` or `-`,
  or beside a KNN, stays refused by name.
- **Exact** for every float32 field. For a `FLOAT64` field, exact up to
  float32's precision, which is past the precision of the embedding models
  the three clients ship with.
- **Costs:**
  - a set for each VECTOR field: an index with three costs what three
    one-field indexes over the same documents cost, each metered;
  - a change to the definition row's encoding. It is written so that a
    release before this amendment refuses to load an index it cannot serve
    whole, rather than serving one of its fields;
  - no kernel change.

**B. As A, but `FLOAT64` kept as float64.** A second kernel, the flat
search and the HNSW graph generic over the element type, and no 8-bit or
1-bit code for a `FLOAT64` field.
- Distances agree with RediSearch's to float64's precision.
- About twice A's work, and twice the RAM for a `FLOAT64` field's vectors,
  for a precision no client of the three uses.

**C. Several VECTOR fields and several ranges, `FLOAT64` still refused.**
A client whose fields are all `FLOAT32` gains, and `test_aggregation.py`'s
fixture is still refused: none of its 26 cases pass.

### Recommendation: A

- **Releases:** the co-processor alone changes; the data node and the
  proxy do not. If this is built before v0.1.0-rc.86 is cut, it ships with
  `FT.*`'s first release. Otherwise it is a co-processor upgrade with one
  rollback rule: **drop every index with more than one VECTOR field, or
  with a `FLOAT64` field, before rolling back** to a release without it.
  The older co-processor refuses to load such an index, so its prefixes
  would stay guarded with no index serving them.
- **Unchanged:** the 16-attribute cap; ADR-0061's 10,000-document range count;
  a KNN and a range in one query, refused; `FLOAT16`, `BFLOAT16`, `INT8`
  and `UINT8`, refused by name (no client of the three sends them).
- **Verification:**
  - unit tests: an index with several VECTOR fields of different `DIM`s
    and metrics; a `FLOAT64` document and query vector and the distance
    they give; two and three range terms, ANDed with a filter, each
    yielding its own distance; the refusals that stay;
  - the definition row: an index of this amendment round-trips, and the
    decoder of today refuses it;
  - `coproc_ft_drill`: redisvl's two queries through the proxy on HASH and
    JSON, before and after a co-processor restart;
  - the suites: `test_aggregation.py`'s 26 cases pass, and nothing that
    passed now fails.

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
  - replies were RESP2's shape only (RESP3's added from redisvl's suite, below);
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

## Verification against langchain-redis's and LlamaIndex's suites, 2026-10-20

Downloads approved by Jeff (2026-10-20): their tests from GitHub, one file at
a time, and their packages and test dependencies from PyPI, each in a fresh
Python 3.14 virtualenv. Both packages declare Python below 3.14, so they
were installed with `--ignore-requires-python`.
- **langchain-redis:** the tests at tag `libs/redis/v0.2.6` (110f379b, 28
  files). The package is 0.2.6, with redisvl 0.28.0 and redis-py 8.1.0, so
  RESP3.
- **LlamaIndex:** `llama-index-vector-stores-redis`'s tests from
  llama_index's main (619d7466, 3 files, unchanged since March). The package
  is 0.9.0, with redisvl 0.4.1 and redis-py 5.3.1, so RESP2.

The run:
- **The fleet:** the one redisvl's suite used.
- **Changed:** only how the tests reach it. langchain-redis reads
  `REDIS_URL`. LlamaIndex's conftest started a Docker container, and now
  connects to the fleet.
- **Left out:** langchain-redis's LangCache file, which needs that service,
  and its unit tests, which mock Redis.

| suite | passed | failed | errors |
|---|---|---|---|
| langchain-redis's integration tests, as built | 124 | 57 | 23 |
| LlamaIndex's | 18 | 1 | 0 |

**One wrong answer, fixed.** A JSON TAG field ignored a declared
`SEPARATOR`. langchain-redis declares `|` and writes `"category": "a|b"`, so
`@category:{a}` matched nothing, in 8 tests. RediSearch gives a JSON TAG no
separator unless one is declared (`TAG_FIELD_DEFAULT_JSON_SEP` is none), and
splits each string, and each element of an array, on a declared one. Flint
now does the same, and `FT.INFO` answers a JSON TAG's absent separator as
`""`, as RediSearch does.

**A fault in released code:** BUG-0269. Each indexed write, like each
`VEC.SET`, dials its own channel, and the co-processor's closes left one
TIME_WAIT socket per write. The suite's pipelined loads ran the laptop out of
ephemeral ports in 20 seconds, and 14 tests failed `COPROCUNAVAIL` or
`Errno 49`.

**The rest:**
- **A cascade, 42 of the chat history's 44 failures, from langchain-redis's
  own parsing.** Its chat history reads `FT.INFO`'s `index_definition` as a
  flat list (`.index("prefixes")`). Under RESP3, RediSearch answers it as a
  map (`RedisModule_ReplyKV_Map` in its `info_command.c`), as Flint does, and
  redis-py 8's default adapter leaves the map in place. So the second
  history on one index fails, against Redis as against Flint, and its test
  leaves the index behind for every later setup to fail on.
  - Over RESP2 (`?protocol=2`) the same file passes 47 of 54. Six of the
    rest need `WITHSCORES` (its `search_messages`), and one a page past the
    channel's budget (amendment 3, part 1).
- **`CROSSSLOT`:** langchain-redis's cache `clear()` deletes its keys in one
  transaction across slots, which the proxy refuses
  (`docs/command-support.md`).
- **A flush is refused** in a namespace with an index. Amendment 3, part 5.
- **Ties come in key order,** not write order: LlamaIndex's one failure,
  flaky between runs. Amendment 3, part 6.
- **Not served,** for later ADRs: vector range queries (8 tests), fuzzy and
  prefix text terms (4), `GEO` (3), `WITHSCORES` (1).

### The three suites again, after amendment 3 (7df3a801, 2026-10-20)

| suite | passed | failed | errors | before |
|---|---|---|---|---|
| redisvl's 18 files | 204 | 40 | 65 | 202 / 51 / 65 |
| langchain-redis, RESP3 | 141 | 40 | 23 | 124 / 57 / 23 |
| langchain-redis, RESP2 (`?protocol=2`) | 179 | 24 | 1 | not run |
| LlamaIndex | 18 | 1 | 0 | 18 / 1 / 0 |

- **redisvl** now skips 12 tests, which read `MODULE LIST` to decide what
  to run.
- **Still no wrong answer.** What is left:
  - the features not served: `GEO`, `WITHSCORES` and full text, vector
    range queries, `FT.AGGREGATE`;
  - langchain-redis's own RESP3 parsing;
  - transactions across slots;
  - one more, below.
- **The first command after a flush answers `-LOADING`.**
  - BUG-0268 has every co-processor forget the namespace after a flush, and
    rebuild it, now empty, at its next touch. That touch answers
    `-LOADING`, as ADR-0017 D3 has warming answer.
  - The frameworks do not retry it, so the first test after each flush in
    the suites fails: LlamaIndex's one failure, and 1 to 3 in the others.
  - Serving a command whose lent channel finishes the rebuild would end
    this. It changes what D3 shows, so it was a question for Jeff, not part
    of this ADR. He accepted it as ADR-0017's D3 amendment (2026-10-20),
    built at 228a1dfc. With it:
    - LlamaIndex passes all 19;
    - redisvl passes 205, with 39 failed and 65 errors;
    - langchain-redis is as above;
    - no suite loses a test to `-LOADING`.

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
