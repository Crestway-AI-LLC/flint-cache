# BUG-0090 — the write timeout that fixed a hang made the test blame the product

**Status:** FIXED 2026-09-04, found while gating the tree for rc.69 ·
**Area:** `crates/flint-server/src/migrate.rs`, `ctl_reply_cap_tests`

## What happened

Two `gates.sh` runs, back to back, on the identical tree (`c35f70d`).
`test (rocks)` passed the first at 124.7s and **failed the second at 55.2s**:

    ---- migrate::ctl_reply_cap_tests::trickling_peer_is_refused_rather_than_buffered
    assertion `left == right` failed: expected the byte cap, got: closed
      left: UnexpectedEof
     right: InvalidData

The second run was faster because the build was warm, and it failed because it
was faster — more of the machine was doing test work at once.

## The mechanism, proven rather than inferred

`UnexpectedEof` has exactly one source here: the peer thread returning, which
closes the socket. The thread returns on any write error, and BUG-0084 had
given it a reason to have one:

    let _ = s.set_write_timeout(Some(Duration::from_secs(5)));
    ...
    if s.write_all(&blob).is_err() { return; }

That timeout was added so the writer could not park forever on a full socket
buffer — the rc.67 release box, healthy at 0.24% CPU for forty minutes. It
works. But `write_all(..).is_err()` cannot tell **"the client hung up"** from
**"the client is slow"**, and a full socket buffer is what a slow client looks
like. Under contention the write times out, the peer hangs up, the client
reads EOF, and the assertion reports the byte cap failing to fire.

**The fix for one hang manufactured a flake in the test written to catch it.**

Not left as an inference. Shrinking the write timeout to 1ms makes the timeout
path fire on essentially every write, and the test then fails **byte-identically
to the gate**: `expected the byte cap, got: closed`, `UnexpectedEof` where
`InvalidData` belongs. Same output, same line, deterministic.

## Why this is BUG-0064's shape again

The failure text asserts a **product** fault (the cap stopped firing) for a
condition it cannot distinguish from a **timing** one (the runner was busy).
BUG-0064 is the same sentence about `cold_start_roles`, and its `decommission`
half is what reddened the rc.68 release gate. This is the third release
candidate in a row where a *test-harness* timing assumption, not the product,
was the thing at risk of stopping the cut.

The natural rate could NOT be measured: 1 failure observed, then 16 isolated
runs and 4 full-suite runs (two at a time) all green. That is why the 1ms
control matters — the mechanism is established even though the rate is not,
and those are different claims.

## The fix

A write timeout means the reader is slow, which is **the condition this test
exists to survive**. So the peer retries instead of hanging up:

- `write` in a loop tracking the offset, not `write_all`. A partial write
  followed by a whole-blob retry would splice a truncated bulk into the stream
  and `decode()` would then fail for a third, unrelated reason.
- `WouldBlock` / `TimedOut` retry; any other error still ends the thread, which
  is the normal ending once the cap fires and the client refuses.
- A 300s deadline on the peer thread, because a timeout no longer exits on its
  own. This is what keeps BUG-0084's forty-minute park impossible: bounded
  spin, ~60 five-second waits, not an indefinite one.

## The standing check

`a_slow_reader_does_not_look_like_a_missing_cap` is the 1ms control, kept as a
test. It is not a duplicate of its sibling: the sibling asserts the cap fires,
this one asserts **the harness cannot counterfeit the cap's absence**. Against
the old code it fails every run; against the fix it passes.

Verified with a mutation control, so the fix is not just quieting the test:
with `MAX_CTL_REPLY_BYTES` defeated (`usize::MAX`), the suite still fails, at
`a never-completing reply must be refused`. The test still detects the thing it
was written for.
