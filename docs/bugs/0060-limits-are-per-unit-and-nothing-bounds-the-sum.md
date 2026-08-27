# BUG-0060: every resource limit is per-unit, and nothing bounds the aggregate (OPEN)

Status: OPEN, opened 2026-08-27 as an audit · Severity: unknown until the audit
runs, which is the point.

## The question this exists to answer

What system boundaries does Flint actually have, and where is a limit missing
that would let a node crash rather than refuse? Refusing is a behaviour this
product already has everywhere else — `-QUOTA` on a full disk, `-THROTTLED`
below write quorum, a closed connection on a hostile query buffer. A node that
OOMs instead has skipped all of that machinery.

## What EXISTS today, so the audit starts from an inventory rather than zero

Roughly two dozen named limits across the workspace. The protocol and
connection tier:

| limit | value | scope |
|---|---|---|
| `MAX_CONNS` | 2048 (runtime-adjustable) | per node |
| `MAX_QUERY_BUF` | 1 GiB | **per connection** |
| `OUT_FLUSH_THRESHOLD` | 1 MiB | **per connection** |
| `MAX_INLINE_LEN` | 64 KiB | per command |
| `MAX_BULK_LEN` | 512 MiB | per bulk string |
| `MAX_ARRAY_LEN` | 1 Mi elements | per array |

Plus, elsewhere: `MAX_VALUE_BYTES`, `MAX_KEY_BYTES`, `MAX_FULLSYNC`,
`MAX_MARKS`, `MIN_MARKS_TO_FORCE`, `MIN_FORCED_PASS_INTERVAL_MS`,
`MAX_LOG_FILE_SIZE`, `MAX_PREFETCH`, `MAX_DEPTH`, `MAX_SAMPLES`, per-tenant
quotas, and the disk guard's two thresholds.

This is not a system with no limits. It is a system whose limits were each
added where a specific thing went wrong.

## The gap the inventory makes visible

**Read the scope column.** `MAX_QUERY_BUF` is per connection and `MAX_CONNS`
is 2048, so the aggregate input buffer this node will accept is
2048 x 1 GiB = **2 TiB**, on a seat with 16 GiB of RAM. Nothing anywhere
bounds the sum.

The per-connection comment is correct on its own terms — "a legitimate
connection can never accumulate this much; hitting it means a hostile or broken
client, and the connection is closed". Each limit is defensible. **The product
of two defensible limits is not defended by either of them.**

`OUT_FLUSH_THRESHOLD` is the more likely one to bite: 1 MiB of reply buffer per
connection is entirely ordinary, and 2048 ordinary connections is 2 GiB of
reply buffers on a 16 GiB node, with no single client doing anything wrong.

That is the shape to look for everywhere else, and it is why this is an audit
rather than a fix: the defect is not in any one limit.

## How to run the audit

For each of the request path, the replication path, the admin/introspection
commands, and the background tasks, ask two questions:

1. **What grows here?** With dataset size, with client count, with key count,
   with pipeline depth, with the number of namespaces.
2. **What bounds it, and is that bound per-unit or aggregate?** A per-unit
   bound multiplied by an unbounded count is not a bound.

Known-good pattern to copy: `RocksKv::clear` (FLUSHALL) already chunks its
deletes at 10,000 rather than collecting the keyspace, with a comment naming
it "FLUSHALL's version of the DBSIZE OOM — two dataset-sized allocations". So
this class has bitten before and been fixed once, locally.

Known-good pattern from 2026-08-26: `MAX_MARKS` bounds the eviction mark set at
100k precisely because marks are full keys in memory and a node's keyspace does
not fit. That one was bounded before it shipped, because the question was asked.

## Candidates worth checking first

- `scan_prefix` returns a fully materialised `Vec<(Vec<u8>, Vec<u8>)>`. Where
  is it reachable from a user command against a user-sized prefix?
- Pipeline depth: `MAX_ARRAY_LEN` bounds one array; what bounds the number of
  commands in flight on one connection, and their accumulated replies?
- The async write queue's depth against value size.
- Per-namespace state that scales with the NUMBER of namespaces rather than
  with their contents.

## What a fix looks like

Not more per-unit limits. Either a node-level budget that the per-unit limits
draw against, or a per-unit limit derived from the node's memory and the
connection count rather than chosen as a constant. The disk guard is the
existing model for this: it reads the actual device and refuses early, rather
than trusting that each writer is individually reasonable.

## Audit pass 1, 2026-08-27 — candidate 1 resolved: the collection commands

`scan_prefix` returns a fully materialised `Vec<(Vec<u8>, Vec<u8>)>`, and the
question was where a USER command reaches it against a USER-sized prefix.
Answer: three of them, all ordinary tenant verbs, none capped.

| command | path | what it materialises |
|---|---|---|
| `HGETALL` | `hashes.rs:228` | every (field, value) pair in the hash |
| `SMEMBERS` | `sets.rs:172` | every member of the set |
| `ZRANGE` (and friends) | `zsets.rs:301`, `:602` via `all_ordered` | every (member, score) in the zset |

Each does it **twice**: `scan_prefix` builds one `Vec`, `.map(...).collect()`
builds a second, both live simultaneously. Then the reply is serialised on top.
Peak is roughly 2x the collection plus the encoded reply, per in-flight
command, with `MAX_CONNS` 2048 of them permitted concurrently.

**Nothing caps collection cardinality.** Grepped for `MAX_MEMBERS`,
`MAX_FIELDS`, `MAX_CARDINALITY` and the lowercase forms across the workspace:
no such limit exists. A tenant may build a hash of arbitrary size with ordinary
`HSET` calls and then ask for all of it.

### The comment that is the bug in miniature

`zsets.rs:300`, immediately above the call:

    /// zset lives in ONE key, so this is bounded by the zset's cardinality.

That is true, and it is exactly the reasoning this audit exists to reject.
"Bounded by cardinality" is not a bound when cardinality is what the user
chooses. It is the per-unit sentence from the inventory table written as
reassurance — the same shape as `MAX_QUERY_BUF`'s "a legitimate connection can
never accumulate this much", which is also true per connection and false at
2048 of them.

### Severity, MEASURED 2026-08-27 (this section's earlier text said "not yet measured") What is established is
reachability and the absence of a bound; what is NOT established is the size a
real tenant reaches, or whether the RESP encoder's own buffering fails first
and more gracefully. A 10M-member set at ~20 bytes per member is ~200 MB
materialised twice on a 16 GiB seat — survivable once, not at concurrency.

### Why this is not "add MAX_MEMBERS"

Per the fix section above: another per-unit constant is the same mistake one
layer down. The two shapes worth considering, neither chosen here:

- **Stream the reply** rather than materialising — the collection commands are
  the natural cursor candidates, and `SSCAN`/`HSCAN`/`ZSCAN` already exist as
  the cursor-shaped answer. The gap may be that nothing REFUSES the
  materialising form when it would be large, not that the cursor form is
  missing.
- **A node-level budget** the per-unit limits draw against, as the disk guard
  does with the real device.

Refusing is the behaviour this product already has everywhere else. A
`-TOOLARGE` on `HGETALL` against a collection past a derived threshold, naming
`HSCAN`, would match `-QUOTA` and `-THROTTLED` exactly.

**Remaining candidates unaudited:** pipeline depth and accumulated replies,
the async write queue against value size, per-namespace state scaling with
namespace count.

## The measurement, and the two harnesses that measured nothing

`flint-server --engine mem`, one hash of 2000 fields x 100 KB = ~200 MB, one
`HGETALL`, RSS sampled at 50 ms:

    RSS loaded                 226 MB
    peak RSS during HGETALL    639 MB
    delta                    + 412 MB     (~2x the collection)
    reply on the wire          181 MB

**~2x the collection, transient, per in-flight command**, on a node that
permits `MAX_CONNS` 2048 of them. Sixteen concurrent clients each doing this
to a 200 MB collection is 6.5 GB of transient allocation on a 16 GiB seat,
none of it refused, none of it accounted anywhere. That is the aggregate this
bug is named for, with a number on it.

The 2x is not a surprise once measured — it is `scan_prefix`'s `Vec` plus the
`.map(...).collect()`, both live, and then the encoded reply on top.
`OUT_FLUSH_THRESHOLD` does not help: it flushes BETWEEN replies in a pipeline,
and a single reply is appended whole before the check.

### Two earlier harnesses reported no problem, and both were broken

Recorded because a wrong negative here would have closed the candidate:

1. **50k-100k fields x 32 B**, sampled at 100 ms: delta 0 MB. The collection
   was ~4 MB and the operation finished inside one sampling interval. Reading
   "0 MB" as "no problem" would have been reading the sampler, not the server.
2. **2000 fields x 100 KB via `valkey-cli --pipe`**: RSS 6 MB after a
   nominally 200 MB load. `--pipe` silently landed NOTHING — `HLEN` 0 — while a
   single `HSET` of the same 100 KB value succeeded. The load failed, not the
   server, and the giveaway was RSS 6 MB where 200 MB was expected.

Both produced a clean-looking result from a harness that never exercised the
thing. The working harness speaks RESP over a socket directly; `--pipe` is not
usable for large values.

### What this does not establish

That any tenant does this. The measurement shows the cost is real and
unbounded, not that it has been paid. `MAX_CONNS` 2048 is the multiplier that
makes it a node-level question rather than a slow query.

## Audit pass 2, 2026-08-27 — candidate 2 (pipeline depth) is CLEAN, measured

Recorded as a negative because an audit that reports only findings cannot be
trusted about what it cleared.

**Question:** `MAX_ARRAY_LEN` bounds one array; what bounds the number of
commands in flight on one connection and their accumulated replies?

**Answer, from the serve loop:** commands are decoded and executed ONE at a
time, and `OUT_FLUSH_THRESHOLD` is checked after each reply is appended. So
accumulated replies are bounded at ~1 MiB plus one reply, per connection —
not by pipeline depth.

**Measured**, against a deliberately hostile reader (300 pipelined `GET`s of
1 MB values, sent in one burst, replies drained in 64 KB reads with sleeps to
force server-side buffering):

    RSS loaded                303 MB
    peak RSS during drain     305 MB
    delta                     + 2 MB      for 300 MB of replies in flight
    reply bytes read          286 MB

Two megabytes. The flush works, and the slow-reader case — the one that would
expose it — does not accumulate.

**What this contrast buys.** Candidate 1 measured +412 MB for a single
`HGETALL`; candidate 2 measures +2 MB for 300 MB of pipelined replies through
the same encoder on the same node. The defect is therefore isolated precisely:
it is not the reply path, not pipelining, and not the encoder. It is the
single materialised collection, where a reply is built whole before the
threshold is ever consulted.

That also narrows the fix. `OUT_FLUSH_THRESHOLD`'s incremental-flush pattern
already exists and already works; the collection commands are the ones that
cannot use it, because they hand the encoder a finished `Vec`. A streaming
`HGETALL` would fall into the same bounded path everything else already uses.

**Remaining unaudited:** the async write queue against value size, and
per-namespace state scaling with namespace count.

## Audit pass 3, 2026-08-27 — candidate 3 (async write queue): the shape is there, NOT measured

**The bound is entries, not bytes.** `write_queue.rs:56`:

    /// Default bounded queue depth. Full -> the writer gets -THROTTLED (the
    /// existing back-off contract), never an unbounded backlog.
    pub const DEFAULT_QUEUE_CAP: usize = 4096;

and each entry owns its payload — `args: Vec<Vec<u8>>` at `:116`, the full
value bytes, not a reference. So the queue's resident cost is
`4096 x value size`, and nothing counts the bytes.

The comment is the same sentence this audit keeps finding: *"never an
unbounded backlog"* is true of the COUNT and says nothing about the SUM. At an
ordinary 1 MB value that is ~4 GB resident on a 16 GiB seat, held by a
mechanism whose stated purpose is to bound itself. `MAX_BULK_LEN` is 512 MiB,
so the arithmetic ceiling is far worse, though no real client writes that.

**Not measured, and the reason matters.** `--async-writes requires
--engine rocks`, and the checked-in release binary is built without the rocks
feature, so the path cannot be exercised without a feature build. The
measurement that would settle it: `--engine rocks --async-writes all`, submit
~1500 x 1 MB values faster than the consumer drains, sample RSS, and confirm
whether peak tracks `min(inflight, 4096) x value size`.

Filed at this depth deliberately. The two measured results above cost several
harness attempts each, two of which silently measured nothing; asserting a
number here from arithmetic alone would be exactly the per-unit reasoning the
bug is about, one level up.

**If it measures as predicted**, the fix is the same shape as candidate 1's:
not a smaller entry cap, but a byte budget the queue draws against — the
`-THROTTLED` contract already exists and is the right refusal, it is simply
being triggered by the wrong quantity.

**Remaining unaudited:** per-namespace state scaling with namespace count.

## Audit pass 4 — candidate 4 (per-namespace state): a DIFFERENT risk class

Per-namespace state exists: `eviction.rs:95` holds
`policies: Mutex<HashMap<Vec<u8>, S3Fifo>>`, one admission policy per
namespace, and `:112` a `last_forced_ms` map beside it. Nothing caps the
number of namespaces — grepped for `MAX_NS`, `MAX_TENANTS`,
`MAX_NAMESPACE`: none exists.

**But the multiplier is operator-controlled, not client-controlled**, and that
is the distinction the audit's two questions do not by themselves draw:

- Namespaces are provisioned through the control plane (`flintctl tenant add`
  / `CPTENANT*`), not created by data-plane clients.
- Policies are tied to the evictable DECLARATION and cleared when it changes
  (`:207`) — a namespace that stops being evictable stops accumulating state,
  and a re-declared one starts clean.
- Eviction is opt-in, so the default fleet holds none of this at all.

So the growth is real and it tracks operator actions. That is a capacity
question, not a hostile-input question, and it does not belong in the same
bucket as candidate 1 where an ordinary tenant command reaches an unbounded
allocation.

**Refinement this suggests for the audit's method.** The two questions ("what
grows / what bounds it") need a third: **who controls the multiplier?** A
per-unit bound times an OPERATOR-controlled count is a sizing guide. A
per-unit bound times a CLIENT-controlled count is the defect this bug is
about. `MAX_QUERY_BUF` x `MAX_CONNS` and `HGETALL` x cardinality are both the
second kind; per-namespace policy state is the first.

Adjacent, and worth naming as the known-good it is: `SCAN_CURSOR_CAP = 1024`
with a 120s TTL and oldest-eviction bounds the scan-cursor table as an
AGGREGATE, not per connection. That is the pattern candidates 1 and 3 lack,
already written in this codebase, three files away.

## Audit status after pass 4

| candidate | verdict |
|---|---|
| collection commands (`HGETALL`/`SMEMBERS`/`ZRANGE`) | **DEFECT, measured** — +412 MB for a 200 MB collection, client-controlled |
| pipeline depth and accumulated replies | **clean, measured** — +2 MB for 300 MB in flight |
| async write queue vs value size | **defect shape, NOT measured** — bounds entries not bytes; needs a rocks build |
| per-namespace state | **different class** — operator-controlled multiplier |

The audit's original question was "where is a limit missing that would let a
node crash rather than refuse". One answer with a number, one cleared, one
pending a feature build, one reclassified.
