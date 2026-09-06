# BUG-0106 — `QUIT` works while a seat is LOADING and stops once it is READY (OPEN)

**Status: OPEN.** Found 2026-09-05 while writing conformance cases for the
commands `docs/command-support.md` claimed were gated and were not (BUG-0103)
· Severity: low — the client path goes through the proxy, which answers
`QUIT` correctly. What is wrong is a seat's direct behaviour, and the
direction it is wrong in.

## Symptom

Against a READY seat:

    $ valkey-cli -p 6733 QUIT
    ERR unknown command 'QUIT', with args beginning with:

Against the same seat while it is still loading, and against the proxy, the
same command returns `+OK`. So the command works during startup and stops
working once the node is actually serving.

## Root cause: the only arm lives in the loading handler

`serve_loading` (`crates/flint-server/src/main.rs:3269`) answers `PING`,
`HELLO`, `FLINTINFO` and `QUIT`, refusing everything else with
`LOADING_ERR`. The ready path is `serve`, which routes every verb into
`commands.rs`'s dispatcher — and that dispatcher has no `QUIT` arm, so the
verb falls to the unknown-command arm.

`QUIT` *is* in `commands.rs`'s `NO_KEY` list, which is exactly the trap the
matrix already warns about under "`NO_KEY` is a routing table, not a dispatch
table": being listed there says a routing slot cannot be derived from
argument 1, and says nothing about whether anything implements the verb.

## Why it is not a one-line fix

`QUIT`'s contract is *reply, then close*. The dispatcher returns a `Value`
and cannot close a stream, so the arm has to go in `serve`'s loop — the loop
that batches writes and holds `WriteInFlight` guards. Closing there without
first flushing a pending batch would drop writes a client had every reason to
think were in flight, which is a durability bug in exchange for a
compatibility nicety. The fix is real but it is a change to the write path,
and it wants its own review rather than a ride on a docs commit.

## What was done instead

`docs/command-support.md` said `QUIT (minimal, compatibility-shaped)` in the
supported list with no qualification. It now states where the command is
answered and where it is not, and points here. No conformance case asserts
the current seat behaviour on purpose: a case that pins `ERR unknown command`
would make the defect the contract, and the next person to fix this would
have to delete a passing test to do it.
