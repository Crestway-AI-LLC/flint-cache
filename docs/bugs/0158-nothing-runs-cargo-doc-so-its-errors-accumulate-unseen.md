# BUG-0158: nothing runs `cargo doc`, so 99 rustdoc errors accumulated unseen (OPEN)

Status: **OPEN**, found 2026-09-16 while gating an unrelated change · Severity:
**low-medium** — not a product defect and not a release blocker: the bundle is
14 binaries and carries no rustdoc. What it is, is a check this repo does not
have, in a repo whose sibling gates the same check, plus the debt that absence
allowed.

## Measured

```
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
  exit 101, 99 errors
```

| kind | count |
|---|---|
| unclosed HTML tag | 54 |
| unresolved link | 32 |
| could not document | 8 |
| public documentation links to a private item | 3 |
| unknown disambiguator | 2 |

Across **8 crates and 13 source files**: `flint-controlplane` (24 references),
`flint-server` (17), `flint-proxy` (14), `flint-ctl` (14), `flint-journal` (10),
`flint-controller` (6), `flint-vec` (3), `flint-chaos` (3).

## The dominant kind has one cause, and it is our own house style

54 of 99 are `unclosed HTML tag`, and they come from **placeholder syntax in doc
comments**. `state.rs`'s module header documents the durable format the way the
format is written:

```
//! Serialized format (line-based, space-delimited …):
//!   version <n>
//!   proxy <addr>
//!   tenant <name> <token> <ns> …
```

rustdoc renders doc comments as Markdown, so `<n>` and `<addr>` are HTML tags
that are never closed. The convention is right and the markup is wrong: the fix
is backticks, not different prose. That means the bulk of this is mechanical,
which is worth knowing before anyone quotes 99 as a large number.

## The actual defect is that nothing runs it

`tools/gates.sh` has no rustdoc step, in either group:

- `check` — `fmt`, `clippy` (mem), `clippy` (rocks), `test` (mem), `test`
  (rocks), `licences`.
- `docs` / `document_assertions` — greps, awks and python scans over the tree.
  No toolchain at all, deliberately.

So `cargo doc` has never been a check that passes here. It is a check that does
not run, which is a different thing and the reason the count could reach 99
without anybody choosing that. **flint-kv gates the equivalent** and has paid
for it twice: two of its gate runs died at step 4 of ~30 on `unresolved link`,
leaving every later step unverified for the sake of a type name.

## Not a regression from 2026-09-16

Six of the errors are in `crates/flint-controlplane/src/state.rs`, which
ADR-0032's tolerant reader touched today. **All six are at lines 15-31** — the
module header quoted above — while the ~290 lines added today sit from
`load_or_new` downward. Checked by location rather than assumed, because "my
change did not cause this" is exactly the claim that should not be taken on
trust from the person who made the change.

Of the 13 offending files, 6 have changed since `v0.1.0-rc.72` and only that one
today.

## A number I got wrong earlier the same day

I said **"88 unresolved-link errors across seven crates"** twice on 2026-09-16 —
to Jeff, and to the peer session cutting the release. Both halves are wrong, and
the second half is the one that matters.

**The count.** 88 came from an earlier commit and a cruder extraction: it
counted only errors whose location line matched `--> crates/…/`, which misses
every error naming its file elsewhere in the block. The figure on the tree as
this was written is **99 across eight crates**.

**The kind, which is worse.** Calling them all *unresolved-link* is not a
rounding error, it is the wrong diagnosis. Only **32 of 99** are unresolved
links. **54** are `unclosed HTML tag`, and that distinction is the whole
character of this bug: 99 broken doc links would be 99 decisions, while 54
unclosed tags are one convention and a sweep of backticks. I reported the
dominant failure mode as the minority one, to two readers, one of whom was
deciding whether it blocked a release.

Nothing downstream moved on it — "not a cut blocker" was true under either
reading — and the peer caught the discrepancy between my message and the commit
title within the hour. **Their point is the reason this section exists:** a
number that changes between the write-up and the title is something this repo
notices later and cannot reconstruct. Reconstructing it needs the breakdown
above, so the breakdown is above.

## What closing it would involve, in the order that makes sense

1. **Add the step, failing, and see the number.** A gate step that has never run
   is worth less than one that has been seen red once.
2. **Fix the 54 mechanical ones** — backtick the placeholders.
3. **Decide the 3 public-links-to-private-item cases deliberately.** Each is
   either a doc that should not promise a private type, or a type that should be
   public. That is a design answer per site, not a lint sweep.
4. **Then turn it on**, in `check` rather than `docs`, because it needs the
   toolchain and `docs` is defined by not needing one.

Not started. Filed rather than fixed because it was found while gating something
else, and a doc sweep inside an unrelated change is how an unrelated change
stops being reviewable.
