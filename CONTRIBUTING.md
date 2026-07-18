# Contributing to Flint

Thanks for wanting to improve Flint. Two things keep contributions smooth:

## 1. Contributor License Agreement

By submitting a pull request, you agree that:

- your contribution is your original work (or you have the right to submit
  it), and
- you grant Crestway AI LLC a perpetual, worldwide, irrevocable, royalty-free
  license to use, modify, sublicense, and relicense your contribution as part
  of Flint.

This is what lets the project keep one clear license (Elastic-2.0 today)
rather than a patchwork of per-contributor terms. If your employer owns your
work, make sure you have permission to contribute.

## 2. The gates

Every change must pass, in CI order:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo clippy --workspace --all-targets --features flint-server/rocks -- -D warnings
cargo test --workspace --features flint-server/rocks
```

Rules of the road:

- **New commands need conformance cases.** Any command you add or change gets
  a corpus entry in `flint-conformance`, and the corpus must pass against a
  real Valkey (the oracle), Flint's mem engine, and Flint's rocks engine.
  If Valkey disagrees with your case, your case is wrong.
- **New commands need classification.** Add every command to the shared
  read/write classifier in `flint-commands` — the replica gate and the proxy
  route by it, so a missing entry is a correctness bug, not an oversight.
- **Behavior proofs are drills.** If your change affects a running topology
  (replication, failover, routing, rotation, TLS), extend or add a drill in
  `tools/` that a reviewer can run.
- **License headers.** New source files carry the
  `SPDX-License-Identifier: Elastic-2.0` header line like their neighbors.
- Tests use `.expect("...")`, not `.unwrap()` (workspace lint).
