# ADR-0025 — stream collection reads instead of materialising them

**Status:** proposed, 2026-08-27. Comes out of BUG-0060 candidate 1, which is
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
load-bearing part of the design: the trait has 4 implementors and
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
