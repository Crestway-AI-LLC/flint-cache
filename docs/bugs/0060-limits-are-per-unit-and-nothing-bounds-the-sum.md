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
| async write queue vs value size | **MEASURED, mechanism corrected** — bounded by concurrent CONNECTIONS, not queue slots; `max-conns` (2048) binds before the 4096 cap. See pass 5 |
| per-namespace state | **different class** — operator-controlled multiplier |

The audit's original question was "where is a limit missing that would let a
node crash rather than refuse". One answer with a number, one cleared, one
pending a feature build, one reclassified.

## Audit pass 5, 2026-08-27 — candidate 3 MEASURED: the mechanism was wrong, the shape survives

Pass 3 filed this as "defect shape, NOT measured" and predicted RSS would
track `min(inflight, 4096) x value size`. Built the rocks binary and measured
it. **The prediction is not what happens, and the reason corrects the audit.**

### What the queue actually is

`main.rs:3819` and the ADR-0005 D4 comment at `:2036` are explicit: a queued
write **blocks the connection on the consumer's ack-after-apply**. This is a
GROUP COMMIT, not a fire-and-forget buffer — it trades "~2-3x write latency
for far fewer engine writes". Nothing is acked early.

So the queue can only ever hold as many entries as there are connections
currently blocked in it. **One client cannot fill it, however hard it
pipelines.** Measured, holding value size and volume constant and varying only
the number of concurrent connections:

| concurrent writers | peak `async_write_queue` |
|---|---|
| 1 | 0 |
| 4 | 1 |
| 16 | 9 |
| 48 | 29 |

Linear in connections at roughly 0.6x, and nowhere near the 4096 cap. A single
connection pipelining 4000 x 1 MB never moved the depth above 0.

Two controls confirm the path was genuinely exercised rather than skipped:
the startup notice `async-writes ENABLED (opt-in write queue): all (cap 4096)`
was asserted, not assumed — `flint-server` IGNORES unrecognised arguments
(docs/bugs/0034), so a mistyped flag would have measured the synchronous path
and reported "no defect" with total confidence. And async ran consistently
SLOWER than the synchronous baseline (242 vs 332 MB/s with a stalled consumer,
538 vs 563 MB/s without), which is the documented latency trade appearing
exactly where the design says it should.

Deliberately slowing the consumer did not change the answer.
`FLINT_BG_JOBS=1` with an 8 MB memtable produced a real stall
(`write_stall_readable=1`, 63 MB pending compaction, throughput halved) and
peak depth went from 6 to 7. A backlog needs blocked *connections*, and
starving the consumer does not create them.

### The shape survives, one level down

`min(inflight, 4096) x value size` was the right form with the wrong variable.
The correct statement is:

    resident queue bytes = min(concurrent writing connections, 4096) x value size

which is still a per-unit limit multiplied by an unbounded count — this bug's
whole thesis — except the count is **connections**, not queue slots. And that
puts the binding constraint somewhere the audit never looked:

| | value | binds? |
|---|---|---|
| `DEFAULT_QUEUE_CAP` | 4096 entries | only if connections exceed it |
| `MAX_CONNS` compiled default (`main.rs:586`) | **2048** | **yes, at the default** |
| `max-conns` as documented in the flintctl inventory header | 10000 | then the queue cap binds |

So **at the compiled default the queue cap is unreachable**: 2048 connections
cap out first, for a 2 GB ceiling at 1 MB values rather than the 4 GB pass 3
predicted. But an operator who sets `max-conns 10000` — the value the
inventory documentation itself shows — moves the binding limit to the queue
cap and restores the 4 GB ceiling. Neither number is written next to the
other, and the interaction is documented nowhere.

That is a better example of this bug than the one originally filed: not one
limit failing to bound a sum, but **two limits whose product is the real
bound, tuned independently, in different files, by different people.**

### What this does and does not establish

- **Establishes:** the queue is group-commit with ack-after-apply; depth is
  bounded by concurrent blocked connections and measured linear in them; a
  single client cannot fill it; and with default `max-conns` the queue's own
  cap never binds.
- **Does not establish:** the ceiling itself. Reaching it needs a 2048- or
  4096-connection fan-out each with a large write in flight, which was not
  run. The extrapolation from 48 connections is arithmetic, and arithmetic
  standing in for a measurement is what pass 3 got wrong.
- **Unchanged:** the fix shape. A byte budget the queue draws against still
  works, and `-THROTTLED` is still the right refusal. What changes is that it
  should be sized against `max-conns`, not chosen independently of it.

### Status

Candidate 3 is **measured, and downgraded**: real, but it needs a connection
fan-out rather than one aggressive client, and the default `max-conns` holds
the ceiling to half what was predicted. Candidate 1 (the collection read at
+412 MB) remains the more urgent of the two.

## Audit pass 6, 2026-08-27 — the three paths the audit never actually swept

Passes 1-5 worked the list under "Candidates worth checking first" and the
status table then read as though the audit were done. It was not. The method
this bug sets out for itself is broader than that list:

> For each of **the request path, the replication path, the admin/introspection
> commands, and the background tasks**, ask two questions...

Passes 1-5 covered the request path (candidates 1 and 2), one internal
mechanism (candidate 3) and one operator-controlled multiplier (candidate 4).
**The replication path, the admin/introspection commands and the background
tasks were never examined**, and the summary line "one answer with a number,
one cleared, one pending, one reclassified" reads as completion over three
unswept areas. Swept now.

### Replication path — clean, on this pattern

`repl.rs` holds eleven full materialisations, more than any other file, and
all of them are inside `#[cfg(test)] mod tests` (from line 461). `scan_all` at
`:655` is a test helper asserting master/replica parity. Nothing in the
production replication path materialises a dataset-sized collection.

### Admin / background — a DEFECT: an unbounded read buffer with no cumulative deadline

`migrate.rs` contains two loops that read a peer's reply into a growing
`Vec`. They are 500 lines apart and only one of them is bounded.

**Bounded (`:~560`, the drain loop).** Checks a real cumulative deadline every
iteration and rolls the migration back when it passes:

    if std::time::Instant::now() > deadline { rollback(); return ... }

**Unbounded (`call_once_with`, `:1095`).** The budget it is given is applied
as `s.set_read_timeout(Some(budget))` — a **per-read** timeout, not a
deadline. Each individual `read()` must return within the budget; nothing
bounds how many reads there are, or how large `buf` becomes:

    let mut buf = Vec::new();
    loop {
        match decode(&buf) {
            Ok(Decoded::Complete(v, _)) => return Ok(v),
            Ok(Decoded::NeedMore) => { let n = s.read(&mut chunk)?; ... 
                                       buf.extend_from_slice(&chunk[..n]); }

A peer that sends *some* bytes inside every timeout window keeps this loop
alive indefinitely. The read timeout never fires, because it is measuring the
wrong thing: it asks "did this read stall", not "has this call taken too
long".

**And the caps that exist are, once again, per-unit.** `decode` does enforce
limits — `MAX_BULK_LEN` 512 MiB at `resp/lib.rs:457`, `MAX_ARRAY_LEN` 1,048,576
at `:483` — so this is not literally unbounded. It is bounded by their
PRODUCT, which is the shape this whole bug is about: a reply of one million
elements is legal, and `buf` must hold all of it before `decode` returns
Complete. At a modest 1 KB per element that is 1 GB of resident buffer from a
single legal reply, on a seat that is also serving.

**Reachability, stated honestly.** The peer is another Flint seat reached
through `internal_connect` (mutual TLS, internal SNI), not a client. So this
needs a malfunctioning or compromised *node*, not a hostile tenant, and it is
strictly less urgent than candidate 1. But "malfunctioning" is the ordinary
case, not the exotic one: a seat that is swapping, mid-compaction, or
half-dead trickles bytes, and trickling is exactly what defeats a per-read
timeout.

**A secondary observation, now bounded.** `decode(&buf)` is called on every
iteration and re-parses the buffer from the start, so filling the buffer is
quadratic in its size. Before the cap that was unbounded in CPU as well as in
memory; with an 8 MiB cap the worst case is bounded but still noticeable — the
regression test that fills the cap takes ~16 s, and that time is the
re-parsing, not the I/O. Not worth restructuring the decode loop for a control
path, but worth knowing before anyone reuses this helper for anything larger.

**The fix is already written 500 lines above it.** `call_once_with` should
take the same cumulative deadline the drain loop uses, and additionally refuse
once `buf` exceeds a byte budget rather than trusting the decoder's per-unit
caps to add up to something survivable.

### What pass 6 did NOT cover

This swept for two specific shapes — full-collection materialisation
(`scan_prefix`/`collect`) and unbounded accumulation in a read loop. It is not
a command-by-command audit of the admin surface. `manifest.rs` (5 hits) and
`lib.rs` (10) were not read; both are storage-internal rather than request-
reachable, which is a reason to rank them lower, not a reason to call them
clean.

### Status after pass 6

| area | verdict |
|---|---|
| request path | **1 defect** (collection commands, +412 MB measured, client-controlled) |
| replication path | **clean** on this pattern — all materialisations are test-only |
| admin / background | **1 defect, FIXED** — `call_once_with` buffered a peer's reply with no byte budget; capped at 8 MiB with a mutation-verified regression test |
| internal mechanisms | async write queue measured; bounded by connections, `max-conns` binds first |
| operator-controlled | per-namespace state, different risk class |

## Audit pass 7, 2026-08-27 — candidate 1: two micro-fixes tried, both reverted

Went after the measured defect and got nowhere useful, which is worth
recording because both dead ends were the kind that look like wins.

### The baseline, re-measured

A 2000-field hash of 100 KB values (205 MB), one `HGETALL`, RSS sampled every
10 ms: **peak +557 MB**. Higher than pass 1's +412 MB, and the difference is
the sampling interval, not the code — a finer sampler catches a peak a coarser
one steps over. Worth noting for anyone comparing the two numbers.

### Attempt 1: drop the redundant copy in `hgetall` — a NO-OP, and the code says why

`hgetall` does `scan_prefix(..).into_iter().map(|(k, v)| (k[prefix.len()..]
.to_vec(), v)).collect()`, which reads like two full copies of the hash.
Replacing it with an in-place `drain` measured **+557 MB — identical**.

The reason is in the expression: `k` is copied, `v` is **moved**. For a hash of
100 KB values and 2-byte field names, the "second copy" was a few kilobytes of
field names. The 205 MB was never duplicated there at all. Reverted.

### Attempt 2: the same fix for `SMEMBERS`, where members ARE the keys — indistinguishable from noise

`smembers` maps `|(k, _)| k[prefix.len()..].to_vec()`, and here the copied `k`
IS the payload, so the same edit should genuinely halve a copy. First
measurement agreed: **+415 MB unfixed vs +315 MB fixed**, a 100 MB saving on a
205 MB set.

Then a third and fourth run:

| | peak delta |
|---|---|
| unfixed | +415 MB, +167 MB, +400 MB |
| fixed | +315 MB, +253 MB |

**An unfixed run came in lower than both fixed runs.** The spread on the
unfixed arm alone is 248 MB, larger than the effect being claimed. Reverted:
the saving is not demonstrated, and landing it on the strength of one pairing
would have been precisely the arithmetic-dressed-as-measurement this bug
already caught once in pass 3.

### The methodological finding, which is the durable part

**Peak-RSS sampling of a single operation cannot resolve an effect smaller
than the dataset on this workload.** Run-to-run variance comes from RocksDB's
memtable and compaction state at the moment of the read, block cache
occupancy, and allocator behaviour — none of which the harness controls. It
was adequate for pass 1 (+412 MB against a ~0 baseline is far outside the
noise) and for pass 5 (a queue that peaks at 7 entries is not a measurement
problem). It is not adequate for "did this change save half a copy".

Anything targeting a sub-dataset improvement here needs a different
instrument: allocator-level accounting, or medians over many runs with the
spread reported, not two runs and a subtraction.

### Where the peak actually comes from — derived from the code, NOT measured

Stated as a hypothesis with its status attached, since pass 7 is a lesson in
not skipping that step. For `HGETALL` on a 205 MB hash:

| stage | dataset-sized? |
|---|---|
| `scan_prefix` → `Vec<(Vec<u8>, Vec<u8>)>` | **yes**, one copy |
| `.map(..).collect()` in `hgetall` | no — values are moved |
| `Value::Map(..)` in `commands.rs` | no — the `Vec<u8>`s are moved into `Value::Bulk` |
| `encode()` into the connection's out-buffer | **yes**, one copy |

Two live dataset-sized allocations ≈ 410 MB, against a measured 557 MB with
RocksDB's own read path making up the rest. Consistent, and consistent is not
confirmed.

**If that hypothesis holds, neither remaining copy is removable by a local
edit** — one is the store materialising the collection, the other is the
encoder serialising it — which is the same conclusion this bug reached at the
top: *"this is an audit rather than a fix; the defect is not in any one
limit."* The answer is to stop materialising, i.e. stream the scan into the
encoder. `RocksKv` already has native iterators (`rocks.rs:581`), and the
`Kv` trait could take an additive `for_each_prefix` with a default that
delegates to `scan_prefix`, so the 25 existing call sites keep working while
the three collection commands move over one at a time.

That is a design change, not a patch, and belongs in an ADR rather than in
this file.

## 2026-09-03 — pass 7's next step was already decided and two-thirds built; zsets was the third

Pass 7 ended: *"the answer is to stop materialising... the `Kv` trait could take
an additive `for_each_prefix` with a default that delegates to `scan_prefix`...
That is a design change, not a patch, and belongs in an ADR rather than in this
file."* Three things about that are now wrong, and the last one is a live gap.

**1. `for_each_prefix` already exists, and the delegation runs the other way.**
It is a required method on the `Kv` trait (`flint-storage/src/lib.rs:128`) with
a native streaming implementation on `RocksKv` (`rocks.rs:749` — *"nothing is
materialized beyond one row at a time"*). `scan_prefix` is the one with the
default body, and it is built **on top of** `for_each_prefix` by collecting.
Implementing pass 7's sentence literally would have inverted a working design.

**2. The ADR it asks for is ADR-0025**, `stream-collection-reads-instead-of-
materialising-them`, and it predates the request rather than following it. It
names the same three commands in scope: *"Three commands reach it — `HGETALL`,
`SMEMBERS`, `ZRANGE`"*.

**3. `ZRANGE` was in that scope and was never converted.** `cd1e3c83`
implemented ADR-0025 and moved `hashes.rs` and `sets.rs` to `for_each_prefix`.
`zsets.rs` kept `scan_prefix` at both sites — `all_ordered` and the
rank/score-range reader. A decision that named three commands shipped for two,
and nothing recorded the difference, which is why pass 7 later re-derived the
whole design from scratch as though none of it existed.

### Fixed: both zset sites now stream

Same edit, and for the same reason as `SMEMBERS` rather than `HGETALL`: a zset's
member lives in the KEY, after the 8-byte score, so `scan_prefix` copies the
entire collection to build its `Vec` and the suffix map copies it again. Hashes
measured identical under this edit only because they MOVE their values; sets and
zsets do not.

No sorting is needed and none was added: the key is
`prefix || score(8B big-endian) || member`, so ascending key order already IS
ascending score order — which the function's own doc comment had said all along.

The doc comment above it is also corrected. It read *"a zset lives in ONE key,
so this is bounded by the zset's cardinality"* — the sentence this file quotes as
"the bug in miniature" — and now says why one key is not a bound.

**Verification:** clippy clean and tests green in BOTH feature configurations
(512 mem, 582 rocks, zero failures).

### What is NOT claimed, given pass 7's own standard

**The memory saving is not measured.** Pass 7 reverted two changes precisely
because their savings were not demonstrated, and it would be inconsistent to
land this one on a number it does not have. It is not inconsistent to land it on
the argument ADR-0025 already accepted: the same edit to `SMEMBERS` was reverted
in pass 7 for want of a measurement and then landed hours later in `cd1e3c83` on
the ADR's reasoning. This completes that decision rather than re-opening the
question.

What the change provably removes is one dataset-sized allocation — the
`Vec<(Vec<u8>, Vec<u8>)>` holding every key — which is a fact about the types,
not an estimate. Pass 7's methodological finding stands untouched: resolving
what that is worth in RSS still needs an instrument nobody has built, and no
such harness is committed to this repo.

### What remains of this bug, unchanged

Streaming the SCAN is half of ADR-0025. The returned `Vec` still owns every
member, so the reply is still O(collection) per in-flight command with
`max-conns` of them permitted — and that is this bug's actual title. The
aggregate is still bounded by nothing.

## 2026-09-04 — the seat can now read its own memory, which both candidate fixes need

This file's "What a fix looks like" offers two shapes: *a node-level budget that
the per-unit limits draw against*, or *a per-unit limit derived from the node's
memory and the connection count rather than chosen as a constant* — and names
the disk guard as the model, because "it reads the actual device and refuses
early".

**Neither was buildable.** The seat reads its disk and has never read its
memory, so a limit "derived from the node's memory" had no number to derive
from and a budget had nothing to draw against.

`flint-storage/src/mem.rs` now samples `/proc/meminfo`, and `FLINTINFO` carries
`mem_avail_bytes`, `mem_total_bytes`, `mem_avail_pct` and `mem_src`.

**It sets no policy and sheds nothing**, deliberately. `disk.rs` earned that
separation the hard way — its own comment records that an unreadable total must
read as *100% free* so a failed measurement can never shed writes by itself —
and the same convention is followed here. What this buys is that the budget can
be argued from a number rather than a guess; what it does not do is decide the
budget.

`mem_src` is the tri-state: `proc` or `unreadable`. Without it a zero would be
indistinguishable from a machine with no memory free, and those are opposite
facts. macOS reports `unreadable` rather than a figure reconstructed from
`vm_stat`, on the same reasoning as the drill guard line: the fleet is Linux,
and a locally invented number would be compared against real ones later and
believed.

**A defect its own test caught before it shipped.** `avail_pct` was
`avail.saturating_mul(100) / total`. Saturating clamps the NUMERATOR and then
divides, so on a machine with more than `u64::MAX/100` bytes the ratio becomes
**1 instead of 100** — a wrong answer wearing an overflow guard. The property
test failed on its first run; the arithmetic is 128-bit now. Unreachable on real
hardware, and the point is that the guard was backwards rather than absent.

**Still open, and unchanged:** the reply `Vec` owns every member, so a
collection read is O(collection) per in-flight command with `max-conns` of them
permitted. That is this bug's title and it is a design change, not a field.

## 2026-09-04 — CORRECTION: collections ARE capped, and the grep that missed it

Pass 1 states: *"Nothing caps collection cardinality. Grepped for
`MAX_MEMBERS`, `MAX_FIELDS`, `MAX_CARDINALITY` and the lowercase forms across
the workspace: no such limit exists. A tenant may build a hash of arbitrary size
with ordinary `HSET` calls and then ask for all of it."*

**The conclusion is wrong and the grep is why.** The cap exists, is enforced, and
is measured in BYTES rather than elements, so none of those three names could
find it: `DEFAULT_MAX_VALUE_BYTES`, **512 MB**, configurable by
`--max-value-bytes` and applied by **all five** collection types — hashes, sets,
zsets, lists and bloom.

Worse for the original reading, its rationale is already written down at
`flint-storage/src/lib.rs:50` and is this bug's own argument:

> On a beyond-RAM engine the cap matters more than in Valkey: a collection can
> exceed physical memory, at which point any read-all command (HGETALL,
> SMEMBERS, LRANGE 0 -1) is an OOM, so writes past the cap are rejected instead.

So "a tenant may build a hash of arbitrary size" is false at the default: the
write is refused with `ValueTooLarge` once the collection would pass 512 MB.
Searching for the name a limit *would* have, rather than for the quantity it
bounds, is what cost this — the same shape as BUG-0086's port scan.

### What survives, and it is this bug's actual title

The **aggregate** is still unbounded, and the numbers to size it are now all in
the tree:

| quantity | value | source |
|---|---|---|
| per-collection cap | **512 MiB** | `DEFAULT_MAX_VALUE_BYTES` = `512 * 1024 * 1024` |
| peak RSS per in-flight read | **3.03x** the collection | measured at the cap on Linux x86_64, 2026-09-04; 3.19x on macOS |
| so one max-size read costs | **~1.6 GB** | measured directly at the cap, not extrapolated |
| concurrent reads permitted | **2048** | `max-conns` |

**Five concurrent max-size collection reads is ~8.1 GB.** That is the defect, and
it is a much narrower and more defensible statement than "nothing caps
collections": each unit is bounded, nothing bounds their sum, and the sum
reaches physical memory at single-digit concurrency.

### What that changes about the fix

A cardinality cap must NOT be built — it would duplicate `max_value_bytes` under
a second name, which is precisely the "second declaration to keep in sync"
BUG-0086 warns about.

What is missing is admission against a node-level budget, and the two pieces it
needs now both exist: **`ComplexMeta` already carries `bytes`**, so a
collection's size is known from one cheap metadata read *before* anything is
materialised, and **`mem_total_bytes`/`mem_avail_bytes` are now in FLINTINFO**
(2026-09-04), so the budget can be a fraction of real memory rather than a
constant. The decision to make is the policy — what fraction, and whether an
over-budget read is refused or queued — not the mechanism.

## 2026-09-04 — the admission design rests on a predictor

> **Superseded the same day.** The measurement below ran every size in ONE
> server process, so each size after the first was measured against a base
> already inflated by the previous read. Its numbers are low and its central
> claim — that the ratio is flat — is an artifact of that contamination.
> The section after it has the corrected measurement. Kept because the
> reasoning about *why* the design needs a predictor is still right, and
> because the shape of the mistake is worth having on the record.

The approved fix refuses a collection read that would push the node past a
memory budget. That requires knowing the cost BEFORE serving it, and the only
thing knowable beforehand is `ComplexMeta.bytes` — one cheap metadata read. So
the whole design rests on `peak = k x bytes` with **k stable enough to admit
against**. If k moved with size, admission from metadata could not bound
anything and the fix would need a different shape.

Measured before building the policy rather than after. Hashes of 100 KB values,
`HGETALL` each, RSS sampled every 5 ms:

| fields | logical | peak delta | ratio |
|---|---|---|---|
| 500 | 50.0 MB | 104.7 MB | **2.09** |
| 1000 | 100.0 MB | 209.1 MB | **2.09** |
| 2000 | 200.0 MB | 441.8 MB | **2.21** |

**Spread 6% across a 4x size range** — and that apparent stability is the
artifact. Measured cleanly the ratio climbs 2.19 -> 3.19 over the same range;
the warm-base bias grows with size, which is precisely the direction that
flattens a climbing curve. `mem_avail_bytes` and `ComplexMeta.bytes` are still
the right two inputs. **2.2x is not** the multiplier to admit against.

### A discrepancy worth stating rather than smoothing

Pass 7 measured **2.72x** (a 205 MB hash peaking at +557 MB). This run gives
2.21x for the same shape. Both are wrong, in the same direction, for reasons
settled below.

## 2026-09-04 (later) — the discrepancy settled: both numbers were LOW

Jeff asked for this to be settled rather than carried. It cost one worktree, one
rebuild and about an hour of measurement, and it changed a design input by 45%.

### 1. The read path is innocent

Built `flint-server` at `0382637` — the commit *before* `cd1e3c83` converted
`hashes.rs` from `scan_prefix` to `for_each_prefix` under ADR-0025 — and ran the
identical measurement against both binaries, back to back on one machine.

| binary | 2000-field fresh read, 3 reps |
|---|---|
| `0382637`, pre-conversion | +564.6 / +564.1 / +564.4 MiB |
| `7d344e7`, current | +563.7 / +563.6 / +564.3 MiB |

Six runs, a 1 MiB spread, no separation between the binaries. The streaming
conversion did **not** cut the ratio, and `cd1e3c83`'s own comment said so in
advance: the value is copied exactly once either way, and what streaming removed
here was the full-key copies and one outer `Vec` — 2000 x ~48 bytes, not 100 MB.
The tidy story was checkable and it was false.

### 2. What actually differed was the harness

Pass 7 read ONE 2000-field hash in a fresh server. The 09-04 run swept
500 -> 1000 -> 2000 in ONE process. Peak is recorded as a delta over base, RSS
does not fall back, so by the time the sweep reached 2000 the allocator and the
RocksDB read path were already warm — inflating `base`, shrinking `delta`. Run
both shapes against both binaries:

| shape | pre-conversion | current |
|---|---|---|
| fresh (2000 only) | 2.96 / 2.96 / 2.96 | 2.96 / 2.96 / 2.96 |
| sweep (500,1000,2000) | 2.50 / 2.60 / 3.02 | 2.50 / 2.77 / 2.51 |

The binary moves nothing. The harness moves everything — and note the sweep is
not merely biased, it is *unstable*, because how much the prior read left behind
varies run to run. Pass 7's arithmetic already contained the clue: it budgeted
two dataset-sized allocations at ~410 MB against a measured 557 MB and assigned
the rest to "RocksDB's own read path". That remainder is exactly what a warm
sweep has already paid for.

### 3. Two more errors, both understating the number

- **Wrong denominator.** Pass 7 divided by 205 MB. `ComplexMeta.bytes` — the
  quantity admission will actually divide by — is `field.len() + value.len()`
  summed, so 2000 x (6 + 100000) = 200,012,000 B. Confirmed by positive control
  rather than by reading the code: the same accumulator drives `ValueTooLarge`,
  so with a 1000-byte cap and 50 B fields + 50 B values, field names being
  counted predicts refusal at entry 11 and values-only predicts entry 21. It
  refused at 11.
- **Mixed units.** `ps -o rss=` reports KiB. Both harnesses divided by 1024 to
  get MiB and then divided by a denominator computed in decimal MB, understating
  every ratio by 4.9%.

Corrected, on the right denominator and in consistent units:

| number | as published | corrected |
|---|---|---|
| pass 7 (+557 MiB) | 2.72x | **2.92x** |
| 09-04 sweep (+441.8 MiB) | 2.21x | 2.32x, and contaminated |
| 2000-field fresh, either binary | — | **2.96x** |

### 4. The predictor is not flat — and this is the finding that matters

The design's premise was `peak = k x bytes` with k stable. Measured with each
size in its OWN fresh server, 3 reps each, reproducible to +/-0.001:

| collection | fields | k |
|---|---|---|
| 50 MB | 500 | 2.191 |
| 100 MB | 1000 | 2.586 |
| 200 MB | 2000 | 2.958 |
| 400 MB | 4000 | 3.135 |
| 512 MiB (the cap) | 5368 | 3.148 |

**k climbs 43% and then saturates near 3.15.** It is not a constant. The design
still works, because k is *increasing*: for any collection at or below the cap,
`peak = k(b) x b <= k(cap) x b`, so k measured at the cap bounds every smaller
read. A mid-range sample does not.

k also moves with field COUNT at equal bytes, so "at the cap" has to mean the
worst corner, not just the largest byte count:

| ~200 MB as | fields | k |
|---|---|---|
| 100 KB values | 2,000 | 2.934 |
| 10 KB values | 20,000 | **3.189** |
| 1 KB values | 200,000 | 3.165 |

| at the 512 MiB cap as | fields | k |
|---|---|---|
| 100 KB values | 5,368 | 3.148 |
| 10 KB values | 53,644 | **3.179** |
| 1 KB values | 532,610 | 3.146 |

Both dimensions saturate near 3.19. **Maximum observed anywhere: 3.189.**

### 5. The instrument has a failure mode, and it hides the peak

Two trials of the same shape disagreed by 40% (3.175 vs 2.462). The cause was
not the server: macOS was compressing, 5.56 GiB held by the compressor against
6.02 GiB free. Compressed pages leave the resident set, so `ps` RSS *under*-
reports demand under pressure. The harness now samples the compressor around
each read and flags a trial that ran under it; the 1 KB-at-cap trials make the
correlation plain — compressor delta +0.37 / +0.26 / +0.02 GiB gave ratios
2.279 / 2.466 / **3.146**, the least-compressed trial reading highest.

This is why the tables above report the **max** across trials, not the median:
an RSS peak is a *lower bound* on true demand, compression only loosens it, so
the largest trial is the best estimate and every figure here is a floor.

### What to use for the policy

**3.5x**, against `ComplexMeta.bytes`. Maximum measured is 3.189, every
measurement is a lower bound, and the curve is approached from below — so the
margin covers the instrument, not just the variance. One max-size read is then
budgeted at ~1.9 GB.

Corrections this run forces on the rest of this file: the summary table's 2.72x
becomes 3.03x on the platform that ships, five concurrent max-size reads is
**~8.1 GB** rather than ~7 GB,
and my note to Jeff that the unresolved discrepancy was "costing 23% of the
admission budget to conservatism" had the sign backwards — 2.72x was not
conservative, it was optimistic by 16%, and the 2.21x alternative by 45%.

### Measured on Linux, which is what ships — and it reads LOWER

Run on the gate box (c7i.4xlarge, AL2023 x86_64, nothing else on it) with
`tools/collection_read_peak.py`, the harness this investigation produced.
**Zero swap-out on all 21 trials**, so unlike every macOS figure above these are
measurements rather than floors — the instrument had nothing to hide behind.

| shape | macOS (16 KiB pages) | Linux x86_64 (4 KiB pages) |
|---|---|---|
| 500 x 100 KB | 2.191 | 1.957 |
| 1000 x 100 KB | 2.586 | 2.346 |
| 2000 x 100 KB | 2.958 | 2.648 |
| 4000 x 100 KB | 3.135 | 2.844 |
| 5368 x 100 KB (at the cap) | 3.162 | 2.857 |
| 53,644 x 10 KB (at the cap) | 3.179 | 2.890 |
| 532,610 x 1 KB (at the cap) | 3.146 | **3.025** |

**The shape reproduces and the magnitude does not.** k climbs with collection
size and again with field count, and saturates — on both platforms, with the
same ordering. But every Linux figure is 5-10% below its macOS counterpart, and
**that is the opposite of what this file predicted.** The prediction was that
Linux would read equal or higher, on the reasoning that macOS numbers are floors.
The reasoning was sound and the conclusion still wrong, because it assumed the
only difference was the instrument. The likelier cause is the page size: macOS
rounds every allocation up to 16 KiB pages against Linux's 4 KiB, so the same
allocations waste more, and the per-field overhead that drives the count term is
larger there. A platform difference in the *quantity*, not in the measurement.

Two caveats that both point the same way and neither changes the answer:

- **The Linux run batched its writes** (variadic `HSET`, ~1 MB of arguments per
  call) because 532,610 single-field writes through `valkey-cli` is hours, not
  minutes. Batching measured ~4% LOW against unbatched on macOS at 500 fields
  (2.11 vs 2.19), presumably a different SST layout under the read. So the Linux
  figures are, if anything, slightly under.
- The macOS cap x small-field corner ran under compression and stays a floor.

### What to use for the policy

**3.5x against `ComplexMeta.bytes`, unchanged by the Linux run.** It clears the
Linux maximum of 3.025 by 16%, clears the macOS maximum of 3.189 by 10%, and
both platforms' curves are approached from below. Production is Linux x86_64, so
3.025 is the figure that governs and 3.5x is a real margin over it rather than a
rounding. One max-size read is then budgeted at ~1.9 GB.

### The remaining design gap: a sample does not bound a sum

Worth stating before the policy is built, because it is this bug's actual title.
Admission that samples `mem_avail_bytes` per read is nearly right — a read that
has already allocated is visible in the next sample — but reads admitted
CONCURRENTLY inside one sampling window all see the same headroom and all pass.
At `max-conns` = 2048 that race is not theoretical.

Bounding the sum needs an in-flight RESERVATION, not just a sample: add the
estimate on admission, subtract it when the reply has been written, refuse when
`in_flight + estimate` exceeds the budget. Note the release point is after the
reply is ENCODED, not after the collection is materialised — the reply buffer is
the second dataset-sized copy and is a live part of the peak this file measures.
That places the reservation in the server, around the whole command, with the
store exposing the collection's size cheaply; it does not fit inside
`hgetall`/`smembers`/`all_ordered`, which cannot see the reply's lifetime.

## 2026-09-04 — the mechanism is BUILT, opt-in and off by default

Jeff's call: ship the mechanism opt-in with refusal off, the way eviction did on
2026-08-26. That settles the second decision (refuse, not queue) and defers the
first (the fraction) to whoever turns it on, which is where it belongs — the
right percentage is a property of a deployment, not of the code.

`--collection-read-budget-pct`, default **0 = disabled**. Nothing changes for an
existing workload until an operator sets it.

### What it does

`flint-storage/src/admission.rs`. A read is costed BEFORE anything is
materialised, from the metadata read that already knows the collection's size,
and refused if it would not fit alongside what is already in flight:

    THROTTLED collection read needs ~1879048192 bytes (536870912 x 3.5 peak),
      in-flight 0, past --collection-read-budget-pct 25 of 2147483648 available
      (536870912), retry with backoff

**The reservation, not the sample, is what bounds the sum.** A per-read check
against `mem_avail_bytes` alone lets every read admitted inside one sampling
window see the same headroom and pass. So admission keeps its own exact count
of what it has admitted and not released, and the guard is dropped only after
the reply has been ENCODED — the materialised reply owns the collection until
then, and releasing at the store's return would admit a second read against
memory the first has not given back.

### Three decisions that would otherwise read as bugs

- **An unreadable budget ADMITS, and is counted.** `mem::sample()` is `None` on
  macOS and on any unreadable `/proc/meminfo`. Refusing everything would take a
  node down over a missing file; admitting silently would let this claim a check
  it never made. So it admits and increments `collection_read_unmeasured`, which
  is in FLINTINFO. That field moving means the bound is NOT in force. Same rule
  BUG-0079 broke, applied to an admission rather than a refusal.
- **The budget double-counts our own in-flight reads**, because `mem_avail`
  already reflects them. That makes admission progressively stricter as load
  rises, which is the conservative direction and what a stampede valve wants.
- **`LRANGE` is admitted on the requested slice** (2026-09-05). It was left out
  at first because `ListStore::lrange` reads only the ranks asked for, so
  costing it against the whole list would refuse `LRANGE key 0 0` on a large
  one — and leaving it out was itself the hole, since `LRANGE key 0 -1` is
  exactly the unbounded read this bug is about. `ListStore::range_bytes`
  normalises the bounds the way `lrange` does and estimates elements-in-range x
  mean element size; a test asserts it agrees with what `lrange` actually
  returns across ten bound pairs including negatives, clamping and inversion,
  because an estimate for a different range than the one served is worse than
  none. `ZRANGE` IS charged the whole zset despite taking bounds, because
  `ZSetStore::zrange` builds the entire ordered set before slicing. Membership
  follows what a command materialises, not what its signature suggests.

### How it is tested, and the part that needed care

The unit tests use a seam (a fixed available-memory figure) so the arithmetic
can be proven on a host with no `/proc/meminfo` — without it every local test
would take the `AdmittedUnmeasured` path and a broken bound would still pass.
Both mutations were run: disabling the refusal fails
`a_read_past_the_budget_is_refused_and_reserves_nothing` and
`the_sum_is_what_is_bounded_not_the_individual_read`; making `release` a no-op
fails the latter and the unmeasured test.

The seam leaves the WIRING untested, so `tools/collection_admission_drill.sh`
runs the real path: the same read with admission off (must SUCCEED) and with a
1% budget (must be REFUSED). The control is the point — a refusal alone proves
only that something went wrong, since a broken build refuses too. It also
asserts a one-field hash still passes, so a bound that refuses EVERYTHING cannot
masquerade as a working one. On macOS it prints `SKIP:` and exits 0, which
`FLINT_GATE_STRICT=1` promotes to a failure; the gate sets STRICT and runs on
Linux, so it must never reach that line.

### Still open

- **The multiplier was measured on hashes only.** Sets and zsets store members
  as keys with empty values, so their per-item overhead differs and k may too.
  `tools/collection_read_peak.py` builds hashes; extending it is the next step
  if the approximation is ever load-bearing.
- **Skew in a list** makes the mean element size an approximation. Bounded by
  how far the requested elements sit from average, and the alternative — reading
  the range to size it — is the work being admitted.
- **The fraction to recommend.** 25% is a starting point in `docs/self-hosting.md`
  and not a measured recommendation.
