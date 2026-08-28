# ADR-0025 — stream collection reads instead of materialising them

**Status:** ACCEPTED 2026-08-27. **Step 1 below was wrong and is corrected at the bottom — read that first.** Comes out of BUG-0060 candidate 1, which is
measured and has now survived two attempts to fix it locally.

## The problem, with a number

One `HGETALL` against a 205 MB hash costs **+557 MB of peak RSS** on the seat
serving it (2000 fields x 100 KB, RSS sampled at 10 ms). Nothing in the
command's path is wrong on its own terms; the cost is that the whole
collection exists twice in memory before a byte reaches the client.

Three commands reach it — `HGETALL`, `SMEMBERS`, `ZRANGE` — all ordinary
tenant verbs, none capped, and the size is chosen by whoever wrote the data.

## Why a local fix does not exist

BUG-0060 pass 7 tried the two obvious ones and reverted both.

Removing the apparent double-copy in `hgetall` measured **identical** (+557 MB
before and after): in `.map(|(k, v)| (k[..].to_vec(), v))` the key is copied
and the value is **moved**, so with 100 KB values and short field names the
"second copy" was kilobytes. The same edit for `SMEMBERS`, where members ARE
the keys and the copy is real, could not be distinguished from noise across
five runs — one unfixed run came in below both fixed runs.

What survives, derived from the code rather than measured, is two allocations
that are genuinely dataset-sized and genuinely necessary *given the current
shape*:

| stage | dataset-sized? |
|---|---|
| `Kv::scan_prefix` → `Vec<(Vec<u8>, Vec<u8>)>` | **yes** |
| `.map(..).collect()` in the store | no — values are moved |
| `Value::Map` / `Value::Array` in `commands.rs` | no — the buffers are moved into `Value::Bulk` |
| `encode()` into the connection's out-buffer | **yes** |

One is the store materialising the collection; the other is the encoder
serialising it. Neither is removable by editing the command. **That is why
this is an ADR and not a patch** — and it is the same conclusion BUG-0060
reached in its opening: *"the defect is not in any one limit."*

## Decision

**Add a streaming read path and move the three collection commands onto it.
Do not add a size cap.**

A cap would be the fourth per-unit limit in a bug about per-unit limits, and
it would convert a working `HGETALL` on a large hash into an error for a
tenant doing nothing wrong. The collection is legitimately that big; the
defect is that we choose to hold all of it.

### 1. An additive trait method

    fn for_each_prefix(&self, prefix: &[u8], f: &mut dyn FnMut(&[u8], &[u8]) -> bool);

with a **default implementation that delegates to `scan_prefix`**. That is the
load-bearing part of the design: the trait has 5 production implementors (plus a test double) and
`scan_prefix` has 25 production call sites, and none of them has to change.
`RocksKv` overrides it with the native iterator it already uses elsewhere
(`rocks.rs:581`), and everything else inherits today's behaviour until it is
moved deliberately.

Returning `bool` so the visitor can stop early — `ZRANGE` takes a range, and a
scan that cannot be cut short would make the streaming version slower than
the thing it replaces for the common small case.

### 2. A streaming reply

The command layer currently returns a `Value`, which is the second
materialisation. The three commands need a way to write directly into the
connection's out-buffer, emitting the array header and then each element as
it arrives from the iterator.

`OUT_FLUSH_THRESHOLD` (1 MiB, `main.rs:2613`) already exists and already
flushes the out-buffer when it fills. **A streaming producer that checks it
between elements turns the reply's resident cost from O(collection) into
O(1 MiB)** — the mechanism is in place; nothing today gives a command a way to
reach it mid-reply.

### 3. Move the three commands, one at a time

`SMEMBERS` first: single-valued elements, simplest encoding, and the case
where the store-side copy is real rather than a move.

## What this does not fix, and must not be claimed to

`OUT_FLUSH_THRESHOLD` is itself a per-unit limit — 1 MiB **per connection**,
and BUG-0060's opening example is precisely that 2048 connections x 1 MiB is
2 GiB of reply buffers with no single client misbehaving. Streaming moves the
collection commands from "unbounded per request" to "1 MiB per connection",
which is a large improvement and *not* an aggregate bound.

The aggregate bound — a node-level budget that the per-connection buffers draw
against, on the model of the disk guard reading the real device rather than
trusting each writer — is a separate decision and should stay separate. This
ADR is worth doing on its own: it removes the case where a single ordinary
command allocates a multiple of the data it was asked for.

## Verification

The harness exists (BUG-0060 pass 7) and its limits are known, which changes
what can honestly be asserted:

- **Peak-RSS sampling cannot resolve sub-dataset effects here.** Run-to-run
  spread on the unfixed arm was 248 MB. It is fine for this change, because
  the predicted effect is *larger* than the dataset (from ~2 copies to ~0),
  not smaller than it. Say so when reporting, rather than reusing the method
  outside the range where it works.
- **The delivery control is mandatory.** A streaming encoder that produces a
  truncated or malformed array is the obvious failure, and it would show up as
  a wonderful memory number. Assert the element count and the array header on
  every run, not just the RSS.
- **A drill, not just a measurement.** Fixed thresholds on RSS would be flaky
  given that spread; assert the *shape* instead — that peak RSS during
  `SMEMBERS` of an N-MB set does not scale with N once streaming is in.

## Alternatives rejected

**Cap the collection size.** Turns a legitimate read into an error, adds a
fourth per-unit limit, and leaves the 1 MiB x connections aggregate untouched.

**Chunk internally and concatenate.** This is `RocksKv::clear`'s pattern and
is right for a *write* that can be split into independent batches. A read has
to produce one RESP array, so chunking without streaming the output just
rebuilds the same buffer in pieces.

**Change `scan_prefix` to return an iterator.** Correct in the abstract and
touches 25 call sites plus 4 implementors in one commit, most of them nowhere
near this bug. The additive method reaches the same end state incrementally,
and leaves `scan_prefix` available for the callers where materialising is
genuinely what is wanted.

---

## Correction, 2026-08-27 — step 1 already exists, and that sharpens the job

Found on the first hour of implementation. **`Kv::for_each_prefix` is not
something to add: it is already a REQUIRED method on the trait**
(`flint-storage/src/lib.rs:128`, no default body), implemented by all five
implementors — `MemKv`, `RocksKv` (`rocks.rs:577`), the batch wrapper, the
watch wrapper — with existing tests for ordered streaming, early stop, and
survival across reentrant deletes. There is a streaming `count_prefix` beside
it.

In fact the relationship runs the other way from what this ADR assumed:
**`scan_prefix` is the DEFAULT implemented in terms of `for_each_prefix`**, and
its doc comment already says what to use when:

> Materializes the whole range — use only where the range is bounded by a
> single value's size (one hash/set/zset, one slot's manifest rows). CF-wide
> ranges (DBSIZE, GC) go through `for_each_prefix` / `count_prefix`; at fleet
> scale a materialized scan OOMs the process.

So the streaming primitive, the trait design, and the "don't materialise
CF-wide ranges" rule were all in place before this ADR was written. What
BUG-0060 candidate 1 actually found is that **the carve-out is wrong**: "one
hash/set/zset" is treated as inherently bounded, and it is not — the measured
case is a single 205 MB hash.

### What that changes

The plan drops from three steps to one, and the one that remains is the hard
one:

- ~~Add an additive `for_each_prefix`~~ — exists, nothing to do.
- ~~Override it in `RocksKv`~~ — exists (`rocks.rs:577`).
- **The reply path is the entire job.**

And it is worth being exact about why switching the commands to
`for_each_prefix` on its own buys nothing measurable. `Hashes::hgetall`
returns `Pairs`, so a streaming scan would fill the same 205 MB `Vec` by a
different route. The measurement in pass 7 already showed this from the other
side: the store-side copy people assume is there is mostly a MOVE, and editing
it changed peak RSS by zero.

The two allocations that are real are the materialised collection and the
encoded reply, and **both are only removable by letting the command write into
the connection's out-buffer as it goes.** Commands today return a `Value`,
`encode_proto` serialises the whole of it into `out`, and only then does
`main.rs:2581` consider flushing. Nothing in that path lets a command emit an
array header, stream elements, and flush between them.

### The revised design

One dispatch change, then the commands follow:

    enum Reply { Value(Value), Streamed }

A command that returns `Streamed` has already written its own bytes and
flushed as needed; everything else is untouched and keeps returning
`Value`. The collection commands get a writer handle carrying `&mut out` plus
the flush the serve loop already performs at `OUT_FLUSH_THRESHOLD`.

`SMEMBERS` first, as before — single-valued elements, simplest encoding.

### Why the ADR is left standing rather than rewritten

Its decision — stream rather than cap — is unchanged and still right, and the
"what this does not fix" section is unchanged: 1 MiB per connection is not an
aggregate bound. Only the mechanism section was wrong, and deleting the error
would hide that this ADR proposed building something the codebase already had.
The lesson is worth more than the tidiness: **the ADR asserted a missing
primitive without grepping for it**, which is the same failure as asserting a
measurement from arithmetic — and this file already criticises that in
BUG-0060 pass 3.

### Two further corrections, from the session that took the implementation

**The implementor count was 4 and is 5 in production** — `MemKv`, `RocksKv`,
`ReadOnlyKv`, `BatchingKv`, `WatchedKv` — plus a `NoMaterializeKv` test
double, which is 6 if you count it and should not be counted as one. The miscount has a dull cause worth naming
because it will recur: the grep behind it was scoped to
`crates/flint-storage/src/*.rs`, and the test double lives in
`flint-server/src/commands.rs:3539`. A count taken from one crate was reported
as a count of the trait.

**And a fact that changes the standing of "what this does not fix".** That
section says 1 MiB per connection times 2048 connections is 2 GiB of reply
buffers, and calls the aggregate bound a separate open decision. Redis has a
shape for exactly this — `client-output-buffer-limit`, per-class soft and hard
thresholds that disconnect a client whose reply buffer runs away. (Worth confirming against
the specific Redis version before it is quoted.)

So the aggregate bound is **not an open design question, it is an unimplemented
known shape**, and until it exists this is a place where the comparison
favours Redis. That is a stronger reason to do it than "unbounded is
untidy", and it means the eventual design has a reference to argue with rather
than a blank page. It stays out of THIS ADR's scope, but it should stop being
described as undecided.

## What the implementation actually achieved, 2026-08-28

Corrected against `cd1e3c8`, which implemented this. **Two claims above are
overstated and are withdrawn here.**

**"~1 MiB per connection" is not the bound.** Drains happen BETWEEN elements,
so a single oversized bulk still lands in the buffer whole. The honest bound is
**`OUT_FLUSH_THRESHOLD` plus the largest single element**.

**And it is not "down to a window".** The reply `Value` still owns every
member, so the resident cost goes from roughly two dataset-sized copies to
roughly **one copy plus a flush window** — not to a window. Removing the last
copy needs the store and the encoder fused, which is a larger change than this
ADR scoped and is not done.

So the accurate summary of the change is: **one of the two dataset-sized
allocations is gone; the other remains.** That is a real improvement and it is
half of what the "Decision" section above implies. Recorded here rather than
edited into that section, because the gap between what an ADR predicts and
what its implementation achieves is worth being able to see.

**A note on the verification, which departed from this ADR's suggestion and
was right to.** This ADR asked for an RSS-shaped drill. The implementation
declined, on this ADR's own rule: the predicted effect is one dataset-sized
copy (~205 MB on the fixture) against a measured run-to-run peak-RSS spread of
248 MB, so peak RSS cannot resolve it — and a drill asserting "peak RSS does
not scale with N" would fail anyway while the reply `Value` still holds the
collection. It asserts deterministically instead: the streamed transcript is
byte-identical to `encode_proto` in both protocols, the array header matches
the elements that decode back out, the buffer's peak stays under
threshold-plus-element while the collection grows 100x, and a flush is
asserted to have occurred so the tests cannot pass with the streaming deleted.

That is the better instrument, and the ADR's suggestion was the worse one.
