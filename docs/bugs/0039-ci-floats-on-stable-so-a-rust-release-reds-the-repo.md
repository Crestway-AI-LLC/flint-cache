# BUG-0039 — CI floats on `@stable`, so a Rust release reds the repo on its own

**Status:** the occurrence is fixed (`6ff8ca8`); the mechanism is OPEN.

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

## The check that now holds it

None. That is the open half, and this file is the record of it.
