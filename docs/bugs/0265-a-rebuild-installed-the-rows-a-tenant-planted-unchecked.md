# BUG-0265: a co-processor rebuild installed durable rows a tenant could plant, unchecked, past both index caps, and one of the wrong length crashed it for every tenant (FIXED 2026-10-19, the cap half open)

**Status:** **The crash is FIXED 2026-10-19** in `flint-vec`. A rebuild
holds each durable vector row to the shape `VEC.SET` would have: a set this
binary loaded, a valid id and the set's `DIM`. A row that fails is left out
and the rebuild says so, by reason, as it does an unreadable row. **The cap
half is open:** planted rows still take a namespace past both caps at a
rebuild, and closing that is a design choice, below.
Found reading the proxy for ADR-0060, 2026-10-19. In every release with the
vector co-processor up to v0.1.0-rc.84; the fix ships in the release after.
**Severity:** high. One tenant, the playground's anonymous ones included,
could take the co-processor down for every tenant on it, and keep it down
across restarts. It needed a co-processor restart after the rows were
planted, which every roll is.
**Area:** `crates/flint-vec/src/main.rs` `install`, and
`crates/flint-vec/src/lib.rs` `Store::check_vector` and
`Store::install_vector`.

## What happened

A test that commits rows as `install` did, in a release build:

```
cap 2000, charged after 200 rebuilt rows: 67290
a 10-float row in a 3-d QUANT bin set: panicked at crates/flint-vec/src/hnsw.rs:106:10
a 1-float row in a 3-d QUANT sq8 set: no panic, a wrong distance
```

## Why

- **The rows are in the tenant's namespace,** under the prefix
  `\x00vec\x00`, and nothing reserves it. The proxy and the server serve a
  tenant's `SET` of such a key like any other. `tools/coproc_vec_drill.sh`
  already plants rows this way, with the tenant's own token, to test the
  rebuild.
- **`VEC.SET` was the only check.** It refuses a vector of the wrong
  length and a write past a cap. A rebuild read the rows back and committed
  each one to the index directly. For the cap that is the design: a rebuild
  loads every durable row (ADR-0017 D3), and a cap governs only new writes,
  so a cap lowered below what a namespace holds leaves its vectors
  searchable. `tools/coproc_vec_drill.sh` restarts a co-processor that way.
- **So a planted row reached the index as it was.** Past the caps, a
  tenant's index was bounded only by its storage quota, and a small vector
  costs more RAM than storage: an HNSW node's 320 B beyond its vector,
  against a text row of a few dozen bytes. And a `QUANT bin` set's codes are a fixed
  width a slot, so a row of another length panicked writing one. `install`
  holds the store's lock while it commits, so the panic poisoned it, and
  every tenant's next `VEC.*` failed. The rows stay, so the next rebuild of
  that namespace did it again.

## The fix

- `Store::check_vector` checks a rebuilt row's set, id and length.
  `install` installs a row only when it passes, and logs each reason once,
  with a count and the first row: "rebuild of ns ... left out N vector
  row(s) whose length is not their set's DIM; first: set ... id ...".
- `VEC.SET`'s two cap checks are one function now, `Store::over_cap`.
- `tools/coproc_vec_drill.sh` plants a 5-float row in a 3-d `QUANT bin` set
  through the proxy, restarts the co-processor, and checks that it serves
  on, gives the same results, and says it left the row out.

## What stays open: the caps, for Jeff to choose

A tenant can still write its own co-processor rows, and a rebuild loads them
past both caps, bounded by its storage quota. Planted rows can no longer
crash the co-processor, but they can still take RAM the caps exist to keep
for other tenants and for the host's data seats. Three ways to close it:

- **A. Reserve the prefix.** Refuse a tenant's write to a `\x00vec\x00` key.
  The proxy routes by a command's first key and has no table of every key a
  command writes, and a script can write keys it never declares, so this
  needs both a key table and a guard in the server's script path.
- **B. Hold a rebuild to the caps.** Simple, but it changes ADR-0017 D3: a
  cap lowered below what a namespace holds, or a meter estimate raised by a
  release, would leave durable vectors unsearchable.
- **C. Tag the co-processor's rows.** Each row carries a MAC under a fleet
  key only the co-processor holds, and a rebuild leaves out rows without
  one. Complete, and a row-format change in two releases, as ADR-0050 D5
  was for packed rows.

Recommended: **A** for the plain commands now, which leaves only scripts, and
C if a script path turns out to matter. Not built: it is design, and Jeff's.
