# BUG-0243: the flint-server unit suite could deadlock in its write_lock tests (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. The server spawners in `serve_tests`
(`spawn_server`, `spawn_rocks_server`, `spawn_watched_rocks_server`) take
`write_lock::test_serial()` and hand the guard back to the test, so a test
that runs a server holds it for its whole length. Before, seven tests did
not take it.
**Severity:** low for users, since production never waits while holding a
guard. High for the gate: a hung suite stops it until its step budget runs
out.

## What happened

On 2026-10-08, one `cargo test -p flint-server` run under load from peer
builds sat for 31 minutes. Five `write_lock` tests had each been "running
for over 60 seconds" and the binary was idle. The suite had passed in 0.3 s
just before and passed eight times in a row afterwards.

The mechanism was reproduced directly by a throwaway test. One thread loops
`lock_all()` while the test holds `lock_key` and a thread it joins takes
`lock_key_pure` on another stripe. It deadlocked on the first iteration.
Two `write_lock` tests have that hold-and-join shape:
`a_pure_write_of_another_key_is_not_blocked_by_an_rmw` and
`two_pure_writes_of_one_key_run_concurrently`. std's RwLock queues a new
reader behind a pending writer. So when a `lock_all()` arrives in that
window, the writer waits for the held read, the joined thread's read waits
behind the writer, and the test waits for its thread.

The `lock_all()` came from a test outside the serial discipline:
`write_lock.rs` says "take this before touching the locks above, from ANY
test module", but the `spawn_server` tests (MSET in a transaction,
pipelined writes, a script reaching past its keys took it by hand) mostly
did not, and their served writes take the global locks. The 2026-08-31
56-minute hang was the same family; serialising one module then left these.

Production is not exposed: the write path takes its guard only after the
queue branch, so a connection never waits on the batch consumer, which
takes `lock_all()`, while holding a stripe (`main.rs`, the comment above
`write_guard`).

## The fix

The spawners take the serial lock themselves and return it, so a test that
starts a server cannot forget it. The tests that took it by hand before
spawning now take it from the spawner; a second `lock()` on the same
non-reentrant mutex would hang.
