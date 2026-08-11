# 16. Bloom filters: RedisBloom's protocol, a blocked filter on disk

Date: 2026-08-10

## Status

Accepted. D2's block size was reasoned when this was written and has since
been **measured** (D2, Verification 4). The chain bound in D5 remains a
choice rather than a measurement, and is marked as such.

## Context

Flint supports no probabilistic structures: no `BF.*`, `CF.*`, `CMS.*` or
`TOPK.*`, and no Bloom filter inside the engine either (#149 — a separate
decision about RocksDB's own read-path filter, which shares a name with
this and nothing else).

The decision taken here is to expose Bloom filters through **the same Redis
protocol** — RedisBloom's `BF.*` command family — so that an existing client
works unchanged and nobody has to learn a Flint dialect to use one.

Checked against the code rather than memory, because most of this design is
determined by what the encoding layer already does:

- **Dotted command names already work end to end.** `JSON.SET` and
  `JSON.GET` sit in the classifier tables (`crates/flint-commands/src/lib.rs:98,159`)
  and parse, route and classify like any other name. `BF.*` needs table
  entries, not parser work.
- **`ValueType` has five variants, `Json = 5`** (`crates/flint-storage/src/encoding.rs:27`),
  and the tag lives in the low 4 bits of the flags byte — so a sixth is free
  and WRONGTYPE falls out of the existing check.
- **`Kv` has no multi-get.** The seam is `get` / `put` / `delete` /
  prefix-scan (`crates/flint-storage/src/lib.rs:79`). *k* scattered bit
  positions in *k* different rows would be *k* separate engine round trips,
  and nothing in the layer can amortize them.
- **The ordinary write path is not atomic across rows.** `hset` does two
  independent `kv.put` calls — the subkey row, then the metadata row
  (`crates/flint-storage/src/hashes.rs:104,112`). Atomic multi-row writes
  exist (`RocksKv::apply_writes`, one `WriteBatch`) but are the batching
  overlay's path, not the per-command path.
- **A type with extra metadata gets its own meta struct, and the tail is a
  trap.** Lists carry head/tail counters past the shared fields, and
  `ComplexMeta::write_version` exists precisely because decode-then-encode
  silently truncates them — "COPY found this the honest way, by disagreeing
  with Valkey" (`crates/flint-storage/src/encoding.rs:236`).
- **A single value is capped at 512 MB** (`DEFAULT_MAX_VALUE_BYTES`,
  `crates/flint-storage/src/lib.rs:52`), because on a beyond-RAM engine a
  read-all command over a larger collection is an OOM.
- **The proxy near-cache is GET-only** (`crates/flint-proxy/src/main.rs:2004`),
  so this family is outside it by construction — nothing to exclude.
- **Conformance has a shape for module-only families.** `flint_only("json")`
  skips the `--reference` run because Valkey cannot serve `JSON.*`, and
  `tools/redisjson_compare.sh` runs the same corpus against the real module
  so the claim is "matches RedisJSON" rather than "matches the contract we
  wrote" (`crates/flint-conformance/src/main.rs:95-109`).

Two properties make this type unlike every one that came before it, and
both shape the decisions below:

1. **Its correctness contract is probabilistic.** A Bloom filter may say yes
   wrongly at a bounded rate; it may never say no wrongly. A false negative
   is not a degraded answer, it is a broken one — and it is invisible to
   every functional test, because "found nothing" is also what an empty
   filter and a working filter mostly return.
2. **Its read cost is set by the internal layout, not by the value size.**
   For a hash, HGET reads one row because the caller named the field. For a
   Bloom filter, how many rows one `BF.EXISTS` reads is entirely our choice
   of representation. That choice is the ADR.

## Decisions

### D1 — Native, not a co-processor

`BF.*` is served by `flint-server` like every other type family.

ADR-0010 D4 already decided this in advance: the co-processor model is for
families "where two network hops are already small against the work", and
the existing type families stay native forever, because "the first 'just
this one command' is how a cache acquires a plugin ABI and a p99 nobody can
explain." `BF.ADD` and `BF.EXISTS` are point operations in GET's latency
class. They would be that first command.

D2 is what makes this affordable rather than merely principled.

### D2 — A blocked filter: one row per block, one block per item

The obvious representation — a flat bitmap chunked across subkey rows,
addressed by bit position — is the one to reject. With *k*≈7 hash
functions at a 1% error rate, each `BF.ADD` would touch up to 7 rows in 7
places, which is 7 point gets and 7 puts against a seam with no multi-get,
and a torn write of any subset is a false negative.

Instead: **an item hashes to exactly one block, and all *k* of its bit
probes land inside that block.** One block is one subkey row, addressed by
block index. So:

| operation | rows touched |
|---|---|
| `BF.EXISTS` | 1 get |
| `BF.ADD` | 1 get + 1 put (+ the metadata row) |
| `BF.MEXISTS` *m* items | ≤ *m* gets, deduplicated and ordered by block |

That is HGET's and HSET's cost, which is what earns D1.

This is the split-block / blocked Bloom filter (Putze–Sanders–Singler),
the same family of technique RocksDB and Parquet use for their own
filters. It is not free: confining an item's probes to one block adds
variance in per-block load, and a more loaded block has a higher error
rate than the average, so a blocked filter needs slightly more space than
a flat one for the same measured FPR.

**How much depends entirely on block size, and at a 4 KiB row it is
small.** At the classic 9.585 bits/item for a 1% target, a 4 KiB block
(32,768 bits) holds ~3,400 items, so per-block occupancy has a standard
deviation of ~1.7% of the mean. The space premium is expected to be well
under a percent. The premium that gives blocked filters their reputation
comes from cache-line-sized (512-bit) blocks, where a block holds ~50
items and the variance is enormous — a size that makes sense when the
motivation is one cache miss, and no sense when it is one disk read.

**MEASURED, 2026-08-10** (`cargo run --release -p flint-storage --example
bloom_blocking_premium`; 1 M items, 4 M absent trials per cell, 1% target,
premium read at 9.75 bits/item):

| layout | measured FPR | premium vs flat |
|---|---|---|
| flat | 0.00933 | — |
| blocked, 64 B blocks | 0.01256 | **6.11%** |
| blocked, 512 B blocks | 0.00972 | 0.84% |
| blocked, **4 KiB** blocks | 0.00926 | **−0.16%** |

At 4 KiB the premium is **at the measurement's resolution floor (0.15% of
bits/item), which means indistinguishable from zero — not measured to be
zero.** The reading is slightly negative, which is scatter: confining
probes to a block cannot improve the rate in expectation.

The 64 B row is the reason to believe the rest of the table. Blocking is
supposed to cost real space at cache-line block sizes, so it doubles as
this measurement's positive control — a harness reporting ~0% everywhere
would be one that cannot detect a premium at all, and would "confirm" this
ADR by being blind. It resolves at 6.11%, and the harness fails loudly if
it ever stops doing so.

Two consequences fall out for free:

- **Small filters need no special case.** A filter whose whole bitmap is
  smaller than one block is one row, which is exactly a plain Bloom filter
  stored inline. The scheme degenerates gracefully instead of needing a
  promotion path and two code paths to test.
- **A 512 MB filter is the cap, and it is a real one.** 512 MB at 9.585
  bits/item is ~448 M items in a single filter. `BF.RESERVE` past that is
  rejected at reserve time with a clean error rather than discovering the
  ceiling on some later write.

### D3 — Blocks materialize lazily; the meter charges for rows, not capacity

A `BF.RESERVE` writes only the metadata row. A block row is created on the
first bit set in it, and an absent block row reads as all-zero — which is
already the correct answer for "nothing here."

So a filter reserved for 100 M items and holding ten of them occupies ten
rows, and `FLINTKEYSIZE` (ADR-0013 D1), `FLINTNSBYTES` and the tenant
quota all see ten rows. **Reserved capacity is not billed; occupied disk
is.** The alternative — charging nominal capacity — would bill for bytes
that do not exist, and the honest number is the one the disk guard also
sees.

### D4 — The hash function is part of the on-disk format, and is version-tagged

Everything about where a bit lives is a pure function of the hash. Change
it and every stored filter silently becomes noise — not corrupt in a way
any check would catch, just a filter that answers wrongly in both
directions. This is the sharpest edge in the whole design and it does not
announce itself.

So: one 128-bit hash of the item, block index from the high half, the *k*
in-block probes derived by double hashing (Kirsch–Mitzenmacher) from the
low half. Implemented in-tree with published test vectors — there is no
hashing crate in the workspace today and this is not the dependency to add
one for, since the requirement is byte-stability forever rather than
speed.

`BloomMeta` carries a **hash-algorithm id and a layout version**. A future
change to either is a new id and a read-path branch, never an in-place
reinterpretation of existing rows. `BloomMeta` is its own struct in the
`ListMeta` mould, appended past `ComplexMeta`'s shared tail — and, per the
COPY bug that produced `write_version`, the Bloom store must never rebuild
its metadata row by decoding to `ComplexMeta` and re-encoding.

### D5 — Scaling filters ship, with a bounded chain and a larger default

RedisBloom's default filter is scalable: when full it chains a new,
larger sub-filter, and a lookup must check every link. Compatibility says
ship it — clients expect it, and a silently non-scaling filter degrades
its error rate past the number the user asked for, which is exactly the
kind of quiet wrongness this type must not have.

But here each link is a **disk read**, not a pointer chase. So two
divergences from RedisBloom's defaults, both about the same fact:

- **The chain is bounded** (proposed: 32 links), and the bound is an error
  when reached, not a degradation. A user whose filter needs a 33rd link
  needs `BF.RESERVE`, and should be told so.
- **The default capacity of an auto-created filter is raised** from
  RedisBloom's 100 to a configurable `--bloom-default-capacity`, proposed
  100,000. RedisBloom's default is tuned for a structure that costs 120
  bytes of RAM to over-provision; ours costs lazily-materialized disk. At
  capacity 100, a filter that ends up holding a million items is ~14 links
  and therefore ~14 disk reads per `BF.EXISTS` — a p99 set by a default
  the user never saw.

`BF.INFO` reports the chain length, so this is diagnosable from a client
rather than only from the operator side.

### D6 — Write ordering makes a torn write a benign one

`BF.ADD` writes the block row and the metadata row (which carries the
inserted-item count behind `BF.CARD`). Two puts, non-atomic, exactly as
`hset` does today.

**Order is load-bearing: the block row is written first.** A crash between
the two then leaves a set bit whose count was never incremented —
`BF.CARD` under-reports, and the filter is still correct. The reverse
order would leave a counted item whose bits were never set, which is a
false negative: the one answer a Bloom filter is not allowed to give.

Stated as a decision because it is one line of code, no test would notice
it, and it is the difference between a benign inconsistency and a broken
guarantee. It is also why D2's one-block rule matters beyond performance —
with an item's bits spread over *k* rows, no ordering makes a tear benign,
and correctness would have to be bought with the atomic batch path.

### D7 — The deliberate divergences from RedisBloom

Named here so they are choices on the record rather than gaps found later,
in the same spirit as JSON's three (docs/command-support.md). Each has a
case to ITSELF in the conformance corpus — see Verification 3 for why that
turned out to be load-bearing rather than tidy.

**All four were confirmed against RedisBloom 2.8.16 on 2026-08-11**, which
is also when the list stopped being three:

1. **`TYPE` returns `bloom`, not `MBbloom--`.** Following the precedent
   already set: Flint's JSON type answers `json` where RedisJSON answers
   `ReJSON-RL` (conformance corpus, `crates/flint-conformance/src/main.rs:2224`).
2. **`BF.SCANDUMP` and `BF.LOADCHUNK` are refused in v1**, with an error
   that says why. Their payload is a serialized filter, and ours is a
   different layout, so implementing them would emit a blob that looks
   portable, is accepted by nothing, and would be discovered at the far
   end of somebody's migration. Refusing is the honest failure. Importing
   a real RedisBloom dump is a genuine future feature and belongs to a
   format reader, not to a command that pretends.
3. **`BF.INFO … SIZE` reports MATERIALISED bytes, not reserved ones.** A
   filter reserved for 5000 items reads 0 here and ~9984 on RedisBloom,
   because blocks are written lazily (D3) where RedisBloom allocates the
   whole filter up front. Both answer "how big is this filter" honestly;
   they answer different questions, and ours is the one that matches what
   the tenant is billed for and what the disk actually holds.
4. **An unknown `BF.RESERVE` option is refused, not ignored** — the one
   divergence where WE are the stricter side. RedisBloom 2.8.16 accepts and
   silently drops trailing tokens it does not recognise: `BF.RESERVE k 0.01
   100 WAT WAT WAT` returns `OK`, and so does `EXPANSION notanum`.
   Matching that would mean a misspelled `NONSCALNG` quietly produces a
   SCALING filter — the caller believes the size is capped and it grows
   instead. An error is recoverable in one line; a filter that silently
   disobeys the flag it was handed is found much later, by capacity.

   Stated as a decision because it has a cost: if RedisBloom later adds an
   option we do not implement, a client using it gets `OK` there and an
   error here. That is still the safer direction — the client learns at
   once instead of getting a filter that ignored the request.

**Defaults also differ** (D5), because a link costs a disk read here. Not
numbered with the above: it changes what you get when you ask for nothing,
not how a given command is answered.

## What we are explicitly NOT building

- **The rest of the probabilistic families.** No Cuckoo filter, Count-Min
  Sketch, Top-K or t-digest. Each is its own layout problem with its own
  disk-resident redesign, and shipping one well is worth more than
  shipping four that were assumed to be similar.
- **A tenant-tunable hash, or tunable block size per filter.** Both are
  on-disk format, and a per-key format knob is a migration surface with no
  demonstrated demand.
- **Cross-implementation dump compatibility** (D7.2).
- **A Bloom filter as a co-processor family** (D1). The `BF.*` surface is
  not the wedge for making the cache pluggable.

## Consequences

- The first type whose contract is statistical rather than exact, which
  the test strategy has to answer for directly — see Verification. Every
  existing family is checked by comparing an answer to a known-correct
  one; this one cannot be.
- The first type where a code change with no test failure can invalidate
  every byte already on disk (D4). The version tag is the mitigation, and
  it only works if it is written from the very first release.
- A large Bloom filter is, structurally, a hot single key: one key, one
  slot, and no way to split it. The per-slot heat counter (#82) will show
  it as a hotspot and be right. Worth stating before someone reports it as
  a bug.
- Replication, slot routing, TTL/DEL/EXISTS, WRONGTYPE, quotas and the
  disk guard all come free — this is a new `ValueType` and a new store over
  the same `Kv`, which is what ADR-0002's layering was for.
- `ComplexMeta.size` is a `u32`; `BloomMeta` carries its own counters, and
  `BF.CARD` should report a `u64` so a 430 M-item filter (D2's cap) is
  nowhere near the counter's ceiling.
- The command surface is implemented from RedisBloom's documented
  behaviour, not from its source; the divergences in D7 are documented in
  docs/command-support.md alongside JSON's.

## Verification

The usual gate (conformance on both engines, a drill, the 3-target run)
applies, plus three things it cannot cover:

1. **A false-positive rate test that can actually fail.** Load *n* items,
   query *m* known-absent ones, assert the observed rate is within
   tolerance of the configured error. Run it at more than one error rate,
   because a filter that ignores the requested rate passes a single-point
   test.
2. **A false-negative test, which is the real one.** Every item ever added
   must still read as present after: a warm restart, a full sync to a
   fresh replica, a scale-out to a new chain link, and a slot migration.
   **Its positive control is a deliberately broken build** — flip D6's
   write order, or perturb one probe — and confirm the test goes red. A
   false-negative test that has never failed is indistinguishable from one
   that asserts nothing, which is this codebase's most expensive recurring
   bug class (docs/field-notes.md §1).

   **DONE 2026-08-11** — `tools/bloom_drill.sh`, 5000 items into a filter
   reserved for 500, so the chain grew to **4 links** and every assertion
   covers items living in older links. `5000/5000` present at all five
   checkpoints (built, warm restart after `kill -9`, replica after full
   sync, destination after slot migration, destination after a further
   add), with `8/2000` never-added items reading present at each — the
   vacuity control, well under the 10% bound, so the no-false-negative
   result is not the trivial one a fully-set filter would produce.

   **The control was run, not just specified.** Sabotaging `add` to set
   `k-1` of its `k` probe bits — so the write path and the read path
   disagree by exactly one bit — took the drill red at the first
   checkpoint: `FAIL [built]: 1546/5000 added items still present — 3454
   FALSE NEGATIVES`. Reverted, rebuilt, re-run green. The drill can fail,
   which is the only thing that makes its passing worth anything.

   Note the shape of the sabotage, because the obvious perturbation does
   NOT work: changing `place()` itself moves the bits for inserts and
   lookups together, so the filter stays self-consistent and the drill
   correctly stays green. A false negative needs the two paths to
   disagree, which is also why D6's write ordering is the other thing
   worth breaking on purpose.
3. **`tools/redisbloom_compare.sh`**, mirroring `redisjson_compare.sh`: the
   same corpus against a real RedisBloom, asserting the only failures are
   D7's divergences. Without it, `flint_only("bloom")` means the corpus
   proves only that Flint matches the contract we wrote.

   **DONE 2026-08-11, against RedisBloom 2.8.16** (`docker run -d -p
   6391:6379 redis/redis-stack-server`, then `REBLOOM_ADDR=127.0.0.1:6391`).
   Result: PASS — every non-divergent step agrees — but only after it found
   two things reasoning had not:

   - **`BF.INFO key FIELD` is a ONE-ELEMENT ARRAY, not a bare value.**
     RedisBloom answers `*1\r\n:5000\r\n`, and its client libraries index
     `[0]`. Flint returned `:5000`, so a real client would have broken on
     the single-field form. This was a genuine compatibility bug written
     from the documented behaviour, which does not state the shape; only
     the wire does. Fixed, with the nil for a NONSCALING filter wrapped
     the same way and a bad section name left as a BARE error, both
     verified on the wire rather than assumed.
   - **D7.4 above** — RedisBloom's lax option parsing, which nothing in
     its documentation mentions.

   Two lessons are now built into the script and the corpus. First, **the
   run must refuse a non-oracle**: `BF.ADD` answering does not prove the
   module is loaded, because Flint answers `BF.ADD` too, so the script
   demands a numeric `bf` version from `MODULE LIST` and fails otherwise.
   Second, **each divergence gets its own case**, because `run_case` stops
   a case at its first failing step: on the first run, `TYPE` failing at
   step 6 of the lifecycle case meant `BF.SCANDUMP` at step 20 was never
   sent, so a divergence this ADR claimed was under test had never once
   been exercised against RedisBloom. A gate that cannot reach half its own
   assertions is not a gate.

And one measurement that gates a constant rather than a behaviour:

4. **The blocking premium, measured before the block size is fixed.**
   **DONE** — `crates/flint-storage/examples/bloom_blocking_premium.rs`,
   results in D2. The premium at 4 KiB is below the measurement's
   resolution floor, and the harness carries its own positive control (the
   64 B row, which must resolve above 5% or the run fails). Re-run it if
   the hash, the probe derivation or the block size ever change — all
   three move where bits land, and none of them would show up as a failure
   anywhere else.

## Implementation order

1. **The hash, its test vectors, and `BloomMeta`** — the parts that are
   permanent. Nothing above them can be built safely first.
2. **The blocked filter over `Kv`**, single link, `BF.RESERVE` / `BF.ADD` /
   `BF.EXISTS` / `BF.CARD` / `BF.INFO`, with the FPR harness and
   verification 4 running against it. This is where the design is proved
   or the block size changes.
3. **The command surface and classifier entries**, the conformance family,
   `flint_only`, and the compare script — the point at which "RedisBloom
   compatible" becomes a claim with evidence behind it.
4. **Scaling chains** (D5) and the multi-item commands, both of which are
   read-amplification work and belong after the single-link cost is known.
