# BUG-0245: an `if let` match guard in the proxy broke the build at the declared MSRV (FIXED 2026-10-09)

**Status:** **FIXED 2026-10-09**. The pub/sub arm of `serve_client` reads the
subscription command before the match and guards on `is_some()`, so the proxy
builds at Rust 1.89 again.
**Severity:** low for the fleet, since releases build at the pinned 1.98. It
matters for the claim: README and self-hosting.md say 1.89 is enough, and for
one push it was not.

## What happened

Pub/sub (`b2ec90f`) added a match arm guarded by `if !txn.open && let
Some((kind, on)) = ...`. An `if let` guard is not stable at 1.89, though the
pinned 1.98 accepts it. So the local build, clippy, rustdoc, and gate bpubsub
(173 steps) were all green. CI's `msrv` leg failed on the push:

    error[E0658]: `if let` guards are experimental
      --> crates/flint-proxy/src/main.rs:2925:25

The gate leaves `msrv` out of its default run on purpose, because CI runs it
on every push (BUG-0166). This time the leg was read as soon as it finished,
within the hour.

## The fix

The guard reads `subscription.is_some()`, and the arm takes the value with
`let ... else`. The fix was gated with the `msrv` stage added to the default
stages, so the gate itself showed the build at 1.89.

## The lesson

A change that uses syntax the codebase has not used before (here a guard
form) needs `msrv` in its gate. The pinned compiler accepting it says
nothing about 1.89.
