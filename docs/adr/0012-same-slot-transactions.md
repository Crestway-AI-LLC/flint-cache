# ADR-0012: Same-slot MULTI / EXEC / WATCH

Status: accepted — August 2026. D6 was the decision that needed making and
was taken deliberately: ship the three guarantees Flint can honestly give
and document the fourth's absence, rather than reverse readers-take-no-lock.

> Numbering: 0005–0009 are private-plane records, 0010 is reserved for the
> co-processor extension model, 0011 is backup and restore. See
> [README](README.md) for why the sequence is shared across repositories.

## Context

`docs/command-support.md` lists MULTI/EXEC/WATCH under "Excluded by design",
alongside pub/sub, streams and EVAL. That entry has been true but it has
been imprecise in the same way the cross-slot entry was until this week:
what conflicts with slot-sharded multi-tenancy is the **cross-slot** form.
A transaction whose keys all live in one slot lands on exactly one node and
one pair, which is the same shape SINTER, ZUNIONSTORE and COPY already
have.

Transactions are also the single most common reason a team says "we can't
move off Redis". The pattern is nearly always small and local — read a
counter and conditionally write it, move an item between two keys the
application already colocates, claim a job by checking and setting one
field. Those are same-slot by nature, and a hash tag makes them same-slot
by construction.

### What the code already provides

Four things exist that a transaction needs, none of them built for this:

- **Per-connection backend ownership at the proxy.** `Backends` is created
  inside the per-client connection handler (`crates/flint-proxy/src/main.rs`,
  beside `authed_ns` and `replica_reads`), holding one backend connection
  per address for that client alone. Per-connection state on a node can
  therefore survive across a client's commands — the property MULTI needs
  and the one a connection-pooling proxy would destroy.

- **A write-batching overlay.** `BatchingKv`
  (`crates/flint-storage/src/batch.rs`) buffers writes, overlays them on
  reads so each command sees its own predecessors' effects, and commits the
  whole buffer as one engine `WriteBatch` through `RocksKv::apply_writes`.
  It was built for the async write queue (ADR-0005 D4) and is almost
  exactly EXEC's execution model.

- **Writer-writer exclusion.** `write_lock.rs` already has the hierarchical
  scheme, including a `GLOBAL.write()` mode that the async queue consumer
  takes per batch specifically so "queued batches and inline writers can
  never interleave". EXEC wants that same mode.

- **Atomic engine apply.** `apply_writes` puts every op into a single
  `rocksdb::WriteBatch`. One WriteBatch is one WAL sequence group, so a
  replica tailing the WAL applies the whole transaction or none of it. The
  replication story for EXEC is therefore free — provided the transaction
  really does become one batch.

### The four things that are genuinely in the way

**1. The read overlay does not cover scans.** `batch.rs` says so plainly:
`for_each_prefix` delegates to the underlying store, "buffered writes are
invisible to a prefix scan by design — the queue never enqueues a scanning
command". Every collection type reads through a prefix scan. So
`MULTI; SADD k a; SMEMBERS k; EXEC` would report the set *without* `a`,
which is wrong and wrong quietly. This is the single largest piece of work
in the item and it is invisible from the string case.

**2. Readers take no lock.** `write_lock.rs` is explicit that it fixes lost
*writes* and that "Redis-level read-vs-write tearing on multi-part reads
(e.g. HGETALL racing HSET) is a separate, pre-existing caveat". Redis is
single-threaded and so gives a transaction true serial isolation against
everything. Flint can give all-or-nothing *application* of a transaction
and exclusion against other *writers*, but a concurrent reader performing a
multi-part read can still observe a partial view.

**3. The proxy retries transparently.** `RETRY_BUDGET` is five seconds
spanning MOVED chases, TRYAGAIN waits and failover rediscovery, and
recovery drops and re-dials backend connections (`drop_conn`,
`rediscover_for`). A re-dial mid-transaction silently discards the queued
commands on the node while the client's own connection stays up and
believes its MULTI is still open. Transparent retry is correct for single
commands and actively dangerous for a transaction.

**4. Nothing tracks per-key modification.** WATCH needs to answer "was this
key touched between WATCH and EXEC". Strings carry no version, and
`ComplexMeta.version` changes only when a key is recreated, not when it is
mutated. This machinery does not exist in any form.

## Decisions

**D1 — Same slot, inferred, and enforced at the first offending command.**
Every key across every queued command must hash to one slot. The slot is
taken from the first key seen after MULTI; a later command naming a key in
a different slot is refused with `CROSSSLOT` *at queue time*, and — as
Redis does for any queue-time error — the transaction is poisoned so EXEC
returns `EXECABORT`. Refusing at queue time rather than at EXEC means the
client learns which command was wrong while it still has the context to fix
it.

**D2 — The transaction lives on the node, not the proxy.** The proxy stays
a router. It gains only the negative obligations in D7. Putting the queue on
the node keeps one implementation for both direct-dial and proxied clients
and keeps the execution adjacent to the lock and the store, which is where
atomicity is actually available.

**D3 — EXEC runs under `GLOBAL.write()`, through `BatchingKv`, committing
one `apply_writes`.** This reuses the async-queue path wholesale rather
than inventing a second atomicity mechanism. It gives: no interleaved
writer, each command in the transaction observing its predecessors, and one
WAL group so replicas see all-or-nothing.

**D4 — The scan overlay gets built.** `BatchingKv::for_each_prefix` must
merge buffered puts and deletes into the underlying ordered scan rather
than delegating past them. Without it, transactions over collections are
silently wrong, and collections are most of what people put in
transactions. This also removes a latent trap from the async queue, whose
current safety rests on a command-set restriction rather than on the
overlay being correct.

**D5 — WATCH uses striped modification counters, not value fingerprints.**
A fixed-size array of counters, each write bumping the stripe its key
hashes to; WATCH records the current values, EXEC aborts if any changed.
Bounded memory regardless of key count, no write-path allocation, and — the
property that decides it — **no false negatives**. Hash collisions cause
occasional unnecessary aborts, which is a retry; a missed modification is a
lost update, which is data loss. The rejected alternative, fingerprinting
the watched rows and comparing at EXEC, misses the ABA case where a key is
changed and changed back, and costs a read per watched key at EXEC.

**D6 — Isolation is stated, not overstated.** Flint's transaction
guarantees are: **atomic application** (all commands' writes land in one
engine batch, or none do), **no interleaved writer**, and **replicas
observe the transaction whole**. It does *not* guarantee that a concurrent
reader sees a serial history — a multi-part read racing an EXEC may observe
a partial view, exactly as one already may racing a single HSET. This
divergence from Redis goes in `command-support.md` in those words. It is
not acceptable to imply serial isolation we do not implement; readers-take-
no-lock is a deliberate throughput decision that predates this ADR, and
reversing it is a much larger change than transactions.

**D7 — The proxy must fail a transaction rather than repair it.** Inside an
open MULTI, on the affected connection: no transparent retry, no MOVED
chase, no replica routing, no near-cache answer. A backend re-dial, a
failover, or a MOVED aborts the transaction and tells the client so. A
transaction that silently loses its queue is worse than one that fails
loudly, because the client's next EXEC would apply a subset.

**D8 — Fail closed on migration and failover.** A slot entering migration
mid-transaction aborts it; the existing gate already answers TRYAGAIN for
writes to a migrating slot, and a TRYAGAIN discovered halfway through a
batch is not recoverable. A promoted master has no watch state and no
queue, so any transaction spanning a promotion aborts. Aborting is always
available and always correct here.

## Excluded, deliberately

Cross-slot transactions; blocking commands inside MULTI (Flint has none);
nested MULTI; `DISCARD` semantics beyond dropping the queue; Lua. Also
excluded: making EXEC atomic *against readers*, per D6.

## Plan

Phase A is the load-bearing one and is worth landing on its own, since the
async queue benefits from it immediately:

- **A. Scan-aware `BatchingKv`** (D4) — overlay puts/deletes onto ordered
  prefix scans, with the async queue's existing drills as the regression
  suite.
- **B. Node-side MULTI/EXEC/DISCARD** (D1–D3) — per-connection queue, slot
  inference, queue-time errors, EXEC under the global write lock.
- **C. WATCH/UNWATCH** (D5) — striped counters, watch set per connection,
  abort on change.
- **D. Proxy obligations** (D7) — suppress retry/replica/near-cache inside a
  transaction, abort on re-dial.
- **E. Failure semantics** (D8) + a drill that kills a master mid-transaction
  and asserts the client sees an abort rather than a partial apply.

Conformance gets a transaction family compared against real Valkey, as
every other family is. Two behaviours cannot be oracle-compared and need
flint-only assertions, for the reason the CROSSSLOT refusal did: a stock
Valkey has no slots, so it accepts cross-slot transactions, and it gives
serial isolation we explicitly do not (D6).
