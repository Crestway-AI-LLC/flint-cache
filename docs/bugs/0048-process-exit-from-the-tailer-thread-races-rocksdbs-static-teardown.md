# BUG-0048: `process::exit` from the tailer thread races RocksDB's static teardown

Status: FIXED 2026-08-26, `b899cf7`. Found 2026-08-25 · Severity was MEDIUM —
the diagnosis and the re-seed marker are both CORRECT and already written when
it happens, so no state is lost and the next start does the right thing. What
was lost is the exit code: operators and supervisors saw a segfault where the
design promises a deliberate `exit 3`.

`hard_exit()` flushes stdout and stderr by hand, then `_exit()`, so the C++
handlers never run. `libc` became a direct dependency; it was already in the
tree transitively via flint-storage, so nothing changed in the build.

The audit below is done and its result is asserted, not trusted: five of the
six sites are argument and engine validation on the main thread before any
store exists and keep `process::exit`, which flushes properly and has no
teardown to race. A source invariant pins the production count and refuses any
plain `process::exit` inside `mod replica`, so a seventh site forces a
decision rather than inheriting this.

**On the strength of the evidence, since it matters here.** reseed passes 3/3
idle and 4/4 with every core loaded — but it also passed 3/3 BEFORE the fix.
The race is intermittent and has only ever been observed on the contended CI
runner, so a local green is not evidence the fix works. The argument is
mechanical: the handlers that did the damage no longer run. If 139 is ever
seen again on this path, this file is wrong and the cause is elsewhere.

## What was seen

`reseed` on the drills leg, run 32867220264. 3.8 s, on a branch whose only
change was in `flint-ctl` — a different crate, which cannot alter this binary:

    == the replica must diagnose it and STOP, not retry forever
    reseed_drill.sh: line 56: 61620 Segmentation fault (core dumped) \
      "$BIN" --port $RPORT --engine rocks --data-dir "$RDIR" --replica-of ...
    FAIL: expected exit 3, got 139

**Read what came before it in the same log**, because it changes what this bug
is:

    FATAL: WALGAP cursor 3 is no longer reachable from this WAL (sequence 4 is
    no longer in the WAL (latest is 20003)): full sync required — this link can
    never resume. Marking for re-seed

The replica detected the unclosable gap, said so, and wrote the marker. It
crashed **on the way out, after doing everything right**. This is not a failure
to diagnose; it is a failure to leave.

## The mechanism

`main.rs:4533` calls `std::process::exit(3)` from inside `replica::run`, which
is spawned on its own thread at `main.rs:1431`:

    std::thread::spawn(move || replica::run(&target, &kv, &stop));

`std::process::exit` skips Rust destructors but **runs libc `atexit` /
`__cxa_atexit` handlers**, and RocksDB is C++ with static objects registered
there — caches, env singletons, its background thread pool. Meanwhile every
other thread keeps running: the serving path (whose DB handle the comment
above the exit call explicitly notes is shared), the disk sampler visible in
the same log (`sampling ... every 2s OR SOONER`), and RocksDB's own compaction
threads. One of them touching a static that teardown has already freed is a
SIGSEGV.

That shape explains the intermittency exactly. It needs another thread to be
mid-access in the window between "teardown starts" and "process is gone", so a
contended 4-vCPU runner hits it and an idle 16-core box does not — three local
runs passed, and it passed on main at 8cfedce in 4.5 s.

## Why this is not one call site

There are **six** `process::exit` calls in `flint-server/src/main.rs`. Any of
them reached from a non-main thread while RocksDB is open has the same
exposure; this is simply the one with a drill pointed at it that asserts the
exact code. The audit is part of the fix, not a follow-up — fixing only the
site that was observed is how the `pkill` predicate survived twenty call sites
(see field-notes, 2026-08-25).

## Proposed fix

Do not run C++ static destructors on the way out of a crash path. The standard
remedy is to bypass `atexit` entirely — flush the streams we care about by
hand, then `_exit(3)` — because there is nothing this process needs to tidy: a
re-seed marker is already on disk and the DB is about to be resynced from a
checkpoint regardless.

`libc` is **not** currently a dependency of flint-server or of the workspace,
so this is a dependency decision and not a one-line change. Alternatives worth
weighing before adding one:

- signal the main thread to exit and let it own the shutdown, which costs a
  channel and a wait but keeps a single exit path;
- keep `process::exit` and quiesce the other threads first, which is the
  option the comment at the call site already rejected as disproportionate —
  and it was right when the only concern was tidiness, but the concern now is
  a crash.

Not decided here. What is decided is that the current code promises `exit 3`
and delivers 139 under load.

## For whoever re-runs the gate on this

A re-run is legitimate **because the cause is diagnosed and written down here**
— that is the distinction from the re-run-to-green habit. Record the run that
tripped it, so the occurrence is not lost under a green label: `gh` reports a
re-run as success and attempt 1's evidence survives only as a separate
artifact.

Occurrences so far: run 32867220264 (drills leg, 2026-08-25).
