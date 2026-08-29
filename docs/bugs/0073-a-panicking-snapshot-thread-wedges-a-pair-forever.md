# BUG-0073 — a panicking snapshot thread stops that pair snapshotting, permanently and silently (FIXED 2026-08-29)

**Found** 2026-08-29, by applying a lesson from the Flint KV session to our own
periodic paths. Their `flush_due` — the job that bounds their log — was disabled
by an error path, and the general form is: **a mitigation that exists but never
runs reads exactly like a mitigation that works.** Their suggested test was
"arrange for one participant to be unable to snapshot and check whether the
others still do". Ours pass that. This is what the reading found instead.

## The defect

`flint-controller/src/main.rs`, snapshot scheduling. A pair starts a snapshot
only by swapping `snapshot_inflight` from false to true, and the spawned thread
cleared it as its LAST STATEMENT:

```rust
std::thread::spawn(move || {
    match call_slow(&addr, &[b"FLINTSNAPSHOT", dir.as_bytes()], Duration::from_secs(30)) {
        Ok(Value::Simple(reply)) => journal_event(...),
        other => eprintln!("[{id}][{label}] snapshot on {addr} failed: {other:?}"),
    }
    inflight.store(false, Ordering::SeqCst);   // <-- skipped on unwind
});
```

A panic anywhere above that line — in `call_slow`'s parse, in `journal_event`'s
I/O, in a poisoned lock — skips it. The thread dies, the flag stays true, and
the scheduler's `swap(true)` never succeeds again. **That pair never snapshots
again for the life of the controller process.**

Note the ERROR path was already handled: a failed snapshot logs and resets, and
retries next interval. It is only the unwind path that leaks, which is why
reading the error handling gives no hint of it.

## Why it matters more than a missing metric

`try_rewind` needs a snapshot at or after the branch point. Without one, a
rejoin cannot rewind and takes the **full re-seed** — BUG-0071's 94.2 s write
blackout at `min-replicas-to-write=1`, against a 10 s budget. So one panic
converts every later failover for that pair into the worst case.

And it is invisible. The symptom is an ABSENCE of `SnapshotTaken` events, which
is indistinguishable from an interval that has not elapsed unless someone is
counting. BUG-0071's own evidence for the timer running is three snapshots an
outage happened to record — that is evidence it ran three times, not that it
runs.

**Not observed in the wild.** No soak has shown a wedged pair, and a panic here
needs an unusual trigger. It is filed and fixed because the failure is
permanent, silent, and lands on the one path whose absence costs 94 seconds.

## Fixed

The reset is now a drop guard, so it runs on unwind:

```rust
struct SnapshotInflightGuard(Arc<AtomicBool>);
impl Drop for SnapshotInflightGuard {
    fn drop(&mut self) { self.0.store(false, Ordering::SeqCst); }
}
```

## Held by, and shown to fail

Three tests. `a_panicking_snapshot_thread_still_clears_the_inflight_flag` models
the scheduler by swapping the flag true first (asserting it started clear, so
the test cannot pass against a flag nothing set), panics inside the guard's
scope, and asserts the flag is clear. `a_normal_snapshot_thread_clears_the_inflight_flag`
is its control — without it, a guard that cleared eagerly and did nothing useful
would pass. `the_guard_is_not_clear_until_it_drops` pins the other direction: a
guard dropped early re-arms the scheduler mid-snapshot, which is the concurrent
storm the flag exists to prevent.

Mutation-checked by making `Drop` a no-op — the exact pre-fix behaviour, where
the reset happened only if the body ran to completion. Both tests fail, the
panicking one on its own message: *"a panicking snapshot thread left inflight
set: this pair never snapshots again"*.

That step is the point, not a formality: the corollary from the Flint KV session
is that **a test asserting a mitigation fires has to be shown to fail**. Theirs
passed against unfixed code because a `HashMap` seed happened to order things
favourably, and only running the control caught it.

## The class, for the next periodic path

Any "at most one in flight" flag cleared by a trailing statement means "at most
one, ever" the first time the body unwinds. The guard shape is the fix; the
question to ask of every such flag is what clears it when the body does not
finish.
