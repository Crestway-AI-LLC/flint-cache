# BUG-0039 — CI floats on `@stable`, so a Rust release reds the repo on its own (FIXED)

**Status:** FIXED. Occurrence fixed (`6ff8ca8`, `096aa63`, `c9487f3`);
toolchain PINNED and the rocks gap closed (`3f2d701`); **the MSRV half was
closed the same night by `907f5f35`** and this line said otherwise for two
weeks — see "The MSRV half, closed 2026-08-20 and unrecorded until 2026-09-05"
below.

## Symptom

`ci / check` failed on three consecutive commits — `5a937a6`, `310b711`,
`1a7a9e2` — with:

```
error: using `chunks_exact` with a constant chunk size
  --> crates/flint-resp/src/lib.rs:501:26
  = note: `-D clippy::chunks-exact-to-as-chunks` implied by `-D warnings`
```

`gate` was green on all three. Local `cargo clippy --workspace --all-targets
-- -D warnings` was green on all three, and still is.

## The wrong conclusion, drawn twice

The three red commits were mine and consecutive, so the red was read as
something one of them had introduced, and a later commit was described as
having fixed it. Neither was true: **none of the three touched
`crates/flint-resp`**, and the line clippy rejected was written on
2026-07-26 in `74b628e`, three weeks earlier.

The second wrong conclusion was quieter and worth more: local clippy passing
was treated as evidence the workspace was clean. It is not evidence of
anything here. `chunks_exact_to_as_chunks` ships in Rust 1.98; this laptop
has 1.96 and no rustup, so **the local check cannot fail on this lint** —
not "did not", *cannot*. A green local gate and a red CI gate were both
correct, about different compilers.

## Root cause

Three workflow steps in this repo, and two in the ops repo, install the
toolchain with:

```yaml
- uses: dtolnay/rust-toolchain@stable
```

and then run clippy with `-D warnings`. Every new stable Rust adds lints.
So the set of things that fail this repo's CI changes on Rust's release
schedule, with no commit here, and the failure lands on whatever happens to
be at the top of `main` at the time — which is why it read as a regression
in unrelated work.

Two things make it worse than a nuisance:

- **It is invisible before it fires.** Nothing warns that a new stable is
  out or that it added lints; the first signal is a red gate during a
  release cut, which is when it happened.
- **The lint it fired on was not a defect.** `items` is exactly `2 * len`
  elements by construction, so `chunks_exact(2)` could never drop a trailing
  element. An hour went into a red gate that was reporting a style
  preference, at the moment it was most expensive to spend.

There is a third, separate hole in the same area. `Cargo.toml` declares
`rust-version = "1.85"` and **no job builds at 1.85** — `devcontainer.yml`
only asserts the toolchain is *at least* 1.85. So the MSRV claim cannot
fail either. That is why clippy's suggested `as_chunks::<2>()` was not
adopted: it postdates 1.85, and nothing would have caught the violation.

## The fix that landed

`6ff8ca8` removed the only `chunks_exact(<const>)` in the workspace, by
consuming the pairs instead of cloning them — semantically identical, two
fewer clones per map field, and structurally out of the lint's reach. A
grep confirms there is no second occurrence hiding behind the first, which
mattered because clippy aborts at the first crate that fails.

## What is still open

The next stable Rust that adds a lint does this again. Options, in the
order they seem worth considering:

1. **Pin the toolchain** (`rust-toolchain.toml`), so upgrades are a commit
   with a diff and a CI run of their own. Makes local and CI agree, which
   is the property that actually failed here. Wants a decision about
   release binaries first — check whether `sign-release.yml` builds or only
   signs, because pinning would change the compiler that produces shipped
   artifacts.
2. **Add an MSRV job** at 1.85, so the declaration is checked rather than
   decorative.
3. Keep floating but drop `-D warnings` for lints new in the current
   release. Rejected on first look: it needs a lint allowlist that itself
   drifts.

Not done during a release cut deliberately — changing the compiler that
builds shipped binaries is not a thing to fold into unblocking a gate.

## A third hole, found while clearing the second round

`--keep-going` made one run report every remaining lint instead of one per
round, and that surfaced the last two: `result_large_err` on openraft's
`RPCError` and `StorageError` in flint-controlplane. Notable because
**`result_large_err` is not new in 1.98** — it has been in `clippy::perf` for
many releases, and 1.96 does not fire it on these functions. Something about
its detection or threshold changed, which means "new toolchain adds lints" is
too narrow a description: a toolchain can also start firing an OLD lint on
code that has been there for months.

Both were allowed rather than boxed, with the reason at the site: openraft
fixes those signatures through `RaftNetwork` and `RaftStorage`, so boxing in
the private helper is unboxed again at every call site and the large error
crosses the trait boundary regardless.

Separately — and this one turned out to be real after a check that nearly
retracted it: `check-rocks` was gated on `if: github.event_name ==
'pull_request'`. Work lands here by pushing to `main`, so **that job did not
run on any commit in this repo's normal flow**.

The near-retraction is worth keeping. `tools/gates.sh` DOES run both feature
configurations — `clippy (rocks)` and `test (rocks)` are steps in its `check`
stage — which looks like CI coverage and is not. `gate.yml` invokes
`tools/gates.sh conformance drills chaos` and deliberately omits `check`,
because its own header says ci.yml already covers fmt, clippy and tests. So
`check` ran in exactly one place, ci.yml, whose rocks half was PR-gated. Two
workflows each reasonably assuming the other covered it.

FIXED in `3f2d701`: the `if:` is removed and check-rocks runs on push. It had
been running on PRs as recently as 2026-08-19 and takes ~90s with a warm
cache, so the gap bought nothing.

## What now holds it

- **The toolchain is pinned** — `rust-toolchain.toml` at 1.98.0 (`3f2d701`).
  This also pins the RELEASE build, which was the stronger reason:
  `packaging/aws/release-box/run.sh` installs rustup with
  `--default-toolchain stable`, so until now the compiler that produced every
  shipped binary was whichever stable was current that day.
- **The local/CI toolchain gap is now printed** — `report_toolchain_vs_pin` in
  `tools/gates.sh`, before the clippy steps. Deliberately not pass/fail: a
  contributor without rustup is fine, and the mismatch is not an error but a
  fact about what a green clippy is evidence OF. On this laptop (1.96, no
  rustup) it prints the NOTE, which is correct — the pin does not close the
  gap here, it makes it stable and visible instead of moving and silent.
- **check-rocks runs on push** (`3f2d701`).

## The MSRV half, closed 2026-08-20 and unrecorded until 2026-09-05

The section below is what this file said for two weeks. It was already false
when it was written: `907f5f35`, timestamped 23:59 the same night, added
`.github/workflows/msrv.yml` and corrected the declaration.

> **Still open — the MSRV.** `Cargo.toml` declares `rust-version = "1.85"` and
> nothing builds at it — `devcontainer.yml` only asserts the toolchain is *at
> least* 1.85. The claim cannot fail, which is why clippy's `as_chunks::<2>()`
> suggestion was not adopted: it postdates 1.85 and nothing would have caught
> the violation.
>
> Not fixed on 2026-08-20 because adding a blocking job for an unverified
> claim, immediately after a release, risks leaving `main` red overnight on
> something nobody has checked.

**What actually happened, and the scheduling judgement was honoured exactly.**
The workflow was **dispatch-only until the answer was known**. Its first run
said the declaration was false — `hdrhistogram`, `time` and `validit` all
require 1.88, and cargo refuses at RESOLUTION, so our own code had never been
compiled at the version published as the minimum. A second run at 1.88 passed.
*Then* it became blocking and `rust-version` was corrected to 1.88. Adding it
to the blocking path first would have reddened `main` on a claim nobody had
checked, which is the thing the paragraph above was worried about.

It tests the declaration rather than a number: the version comes from
`Cargo.toml` unless a dispatch input overrides it, so raising or lowering the
declaration changes what is verified.

### The gap that was left, and is now closed

`msrv.yml` runs `cargo check --workspace --all-targets` with **default
features**. The durable engine is behind `rocks`, so a rocks-only path using a
newer API would pass CI and fail the self-hoster the declaration exists for.

`tools/gates.sh msrv` (2026-09-05) covers both configs. Opt-in — valid as an
argument, absent from the default run — because it installs a second toolchain
to answer a question that only changes when the declaration or the dependency
set does. Measured on the gate box:

    EVIDENCE: msrv rustc = rustc 1.88.0 (6b00bc388 2025-06-23) (declared 1.88)
    EVIDENCE: msrv default features OK at 1.88 (149s)
    EVIDENCE: msrv rocks features OK at 1.88 (2s)

**So the declaration is true in both configs.** Two things about that output
are worth keeping.

The **2s** is not a leg that failed to run, and the timing is printed so the
question gets asked. `flint-bench` depends on `rocksdb` unconditionally, so
`librocksdb-sys` is in the DEFAULT graph and its C++ is compiled by the first
leg; the second is the incremental re-check of the feature-gated code, which
is the part that can use a newer API. Confirmed with `cargo tree -i
librocksdb-sys` in both configs rather than assumed.

The **EVIDENCE: prefix** was added on the second attempt. The first run passed
in 148s and left nothing behind but the word `PASS`: `run.sh` pulls logs only
on FAILURE, so which compiler answered — the one thing the stage exists to
establish — terminated with the instance. A stage whose entire product is its
finding cannot put that finding where a green run discards it.

The stage also refuses to report at all unless the compiler that answers is the
one asked for. `rust-toolchain.toml` pins this directory to 1.98 and
`RUSTUP_TOOLCHAIN` is the one override that outranks it, so a silently
ineffective override would have certified a claim about 1.88 by compiling with
1.98 — a green run proving nothing, which is this file's own subject.
