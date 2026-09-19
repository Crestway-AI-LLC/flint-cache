# BUG-0168: the gate's rustdoc step covered only the mem surface, so nineteen errors sat behind a green check (FIXED 2026-09-19)

Status: **FIXED 2026-09-19** · Severity: **low in effect, and the shape is the
point** — nothing shipped wrong; a check that had been green for months was
green about a subset.

## What was wrong

`tools/gates.sh` ran rustdoc, and had for a long time:

    step "doc" doc \
      env RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --keep-going

No rocks features. Every `cfg(feature = "rocks")` doc comment in the workspace
was therefore outside what it compiled, and nineteen errors had accumulated
there on a green main: one genuinely broken intra-doc link
(`rocks.rs`, `[write_stall]` needing `Self::`) and eighteen command signatures
written as prose — `FLINTSNAPSHOT <root>`, `FLINTPROMOTE <generation> <counter>`
and six more — where rustdoc reads `<root>` as an unclosed HTML tag.

**A check that covers a subset reads exactly like a check that covers
everything.** That is the whole finding. Nothing was broken about the step; it
answered a narrower question than anyone reading `PASS doc` would assume, and
there was no way to tell from the output which question it had answered.

## How it was found, which was not by review

While implementing BUG-0163 I ran `cargo doc --features rocks` out of habit,
saw a failure, and assumed the pipeline did not run rustdoc at all — a
conclusion reached by grepping the WRONG repository's `tools/gates.sh`, the ops
one, while standing in it. The public repo has its own and it was there all
along at line 3900. The real gap was one word narrower than the one first
reported: not "no docs step" but "a docs step over one configuration".

## The fix

Two legs, matching what `clippy` and `test` in the same file already do:

    step "doc (mem)"   doc-mem   … cargo doc --workspace --no-deps --keep-going
    step "doc (rocks)" doc-rocks … --features flint-server/rocks,flint-backup/rocks

**The mem leg is not redundant.** `cfg(not(feature = "rocks"))` items exist —
`flintsnapshot`'s no-rocks stub among them — and a rocks-only run would stop
covering them. Replacing one narrow leg with a different narrow leg is this bug
inverted.

CI gets the same pair: the rocks invocation was added to `check-rocks` when the
errors were fixed, and the mem one to `check` here, which had no doc step at
all.

## Verified in both directions

Both legs clean on this tree; re-breaking the `Self::write_stall` link makes the
rocks leg exit 101 naming the item. A step that cannot fail is not a check, and
that is exactly what the single narrow leg had quietly become for the surface it
omitted.
