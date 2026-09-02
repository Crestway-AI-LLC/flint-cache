# BUG-0081 — a benign read timeout rotates the proxy's control-plane seat (defect 1 FIXED 2026-09-02; defect 2 NARROWED to one call 2026-09-02, still unreproduced)

**Found** 2026-09-01, in the tail of an `elasticache-bench --regime a` run. The
benchmark produced every number correctly and then **exited 1**, on this,
repeated once a second:

```
control-plane watch (172.31.73.41:7500): Resource temporarily unavailable (os error 11); trying next seat
```

`os error 11` is `EAGAIN`. On a watch socket that is a READ TIMEOUT: the
control plane had nothing new to push. It is the normal idle state of a
subscription, not a failure of one.

## What the code does with it

`crates/flint-proxy/src/main.rs:3904` rotates on ANY `Err` from
`watch_control_plane`:

```rust
if let Err(e) = watch_control_plane(cp, &advertise, &topo, &mut last_version) {
    eprintln!("control-plane watch ({cp}): {e}; trying next seat");
    i += 1;
}
```

The rotation itself was added for a real failure — a proxy pinned to a killed
seat sat reconnecting to a corpse while quorum stayed healthy — and that
reasoning is sound. What is missing is the distinction between "this seat is
dead" and "this seat has nothing to say yet".

## Why it matters more than the log noise suggests

- **On one seat** (this bench, and every single-CP deployment) `seats[i % 1]`
  is the same seat, so it re-subscribes every second and prints an error line
  every second. Noise, and an exit code that makes a green benchmark look red.
- **On three seats** it is not noise. An idle control plane makes the proxy
  walk its seats once a second forever, re-subscribing to each in turn. Every
  rotation is a fresh CPWATCH and a fresh filtered snapshot, so an idle fleet
  pays continuous control-plane work for nothing.
- It is invisible in the drills because they assert on the DATA path, which
  this never touches — the last-applied table keeps serving throughout, by
  design.

## Defect 1 FIXED 2026-09-02

An idle read no longer rotates. `watch_control_plane` keeps waiting on the SAME
connection when the read returns `WouldBlock`/`TimedOut`, so a quiet fleet stops
paying a fresh CPWATCH and filtered snapshot every ~31s for nothing.

**Bounded at `MAX_IDLE_READS` = 10 (~5 minutes), and that bound is a trade, not
a measurement.** A silently partitioned seat times out forever and would
otherwise never be abandoned. The two states are indistinguishable on this
socket because CPWATCH has no keepalive — so the constant is where the trade
gets made, and **the real fix is a keepalive**, which is a protocol change and
is not smuggled in here.

`tools/cp_watch_idle_drill.sh` asserts the consequence rather than the
classification: a healthy fleet idle for 40s — longer than one 30s timeout —
must produce zero rotations, with the proxy still subscribed and still serving.
Both of those extra checks matter, because "no rotations" is trivially true of
a proxy that never subscribed or has died.

Verified by mutation before being trusted: with `MAX_IDLE_READS` forced to 1
(the old behaviour) the drill fails at exactly one rotation in 40s, and the
failure line reads `read: silent for 1 consecutive reads (~30s)` — the phase
label from the other half of this bug doing its job. The mutation was confirmed
to have changed bytes before the result was believed.

The drill's FIRST version could not have failed at all: `grep -c` prints 0 and
exits 1 with no matches, so `|| echo 0` produced `0\n0` and the comparison
errored instead of comparing. It reported PASS. Fixed and recorded here because
a control that cannot fail is the thing this whole bug is about.

## AMENDED 2026-09-01 — the mechanism above does not fit the cadence

Both open questions below are answered, and answering the first invalidates
part of the diagnosis rather than confirming it.

**The read timeout is DELIBERATE.** `watch_control_plane`
(`crates/flint-proxy/src/main.rs:3547`) sets it itself:

```rust
stream.set_read_timeout(Some(Duration::from_secs(30)))?;
```

Nothing in `flint-tls` sets the socket non-blocking, so this is a blocking
read bounded at 30 s. By this file's own reasoning that makes the fix "treat
`WouldBlock`/`TimedOut` as keep-waiting and do not rotate".

**But that mechanism cannot produce the symptom.** The observed line repeated
ONCE A SECOND. Walk the timings:

- the rotation loop sleeps `Duration::from_millis(1000)` after each attempt
  (`main.rs:3909`), so one second is the loop's floor, not its period
- an attempt that reaches the read and finds an idle CP costs **30 s**
- the control plane sends NOTHING while idle: `watch`
  (`flint-controlplane/src/main.rs:1289`) pushes only when the version
  advances past what the proxy ACKed, waiting on a condvar in 500 ms slices
  with no keepalive on the wire

So an idle CP yields one log line every ~31 s. To log once a second,
`watch_control_plane` must return in ≈0 s — which means the error arises
**before any read**, in `connect_reloadable` or `write_all`.

`EAGAIN` on `connect()` has a well-known cause that fits where this was found:
ephemeral port exhaustion. It surfaced in the tail of an `elasticache-bench
--regime a` run, which is exactly the workload that leaves a large TIME_WAIT
population. **Unverified** — recorded as the leading hypothesis, not a
finding, because nothing here has been reproduced.

**What this changes.** There are TWO defects, not one:

1. Rotating on a benign idle timeout — real, still worth fixing, and it costs
   a needless re-subscribe every ~31 s on an idle fleet rather than the
   once-a-second churn described above.
2. Whatever actually produced the observed EAGAIN, which the evidence places
   before the read and which the proposed fix would NOT have addressed.

The original writeup asserted (2) was an instance of (1). It reasoned from the
error's meaning on a watch socket without checking the timings, and the
timings refuse it. Same shape as BUG-0083's neighbours: a reading of a failure
that named a cause nothing had checked.

**Before fixing either, make the error say where it came from.** `connect`,
`write` and `read` all reach `eprintln!("control-plane watch ({cp}): {e}")` as
the same undifferentiated `io::Error`, which is why this cost a code read to
establish and is still unresolved. That is ADR-0028's obligation and BUG-0083's
fix, in a third place.

## Not yet established

- ~~Whether the socket sets a read timeout deliberately or inherits one.~~
  ANSWERED above: deliberate, 30 s.
- ~~Which call actually produced the observed `EAGAIN`.~~ **NARROWED to one
  call 2026-09-02, by reading the dial path rather than the message.**

  `watch_control_plane` → `flint_tls::connect_reloadable` → `flint_tls::connect`
  → `TcpStream::connect_timeout` (`crates/flint-tls/src/lib.rs:614`). That is
  the only call before the read that can fail, and the timeout is not
  incidental: `connect_timeout` sets the socket non-blocking, calls `connect(2)`
  and returns anything that is not `EINPROGRESS`.

  **`connect(2)` documents `EAGAIN` for a TCP socket with no bound address when
  the whole ephemeral port range is in use.** That is the errno for port
  exhaustion — `EADDRNOTAVAIL` is the intuitive guess and the wrong one — so the
  hypothesis this file recorded as unverified is the one Linux's own
  documentation points at, and it now has a named call site instead of a
  location ("before the read"). It also closes the cadence argument from the
  other end: a `connect` that fails this way returns in ~0 s, so the rotation
  loop's `sleep(1000ms)` is the entire period. Once a second, as observed.

  **Still not a reproduction, and the difference matters.** A second documented
  cause — insufficient routing-cache entries — produces the same errno, and
  nothing at that call site can separate them. What changed is that the next
  occurrence identifies itself: the phase label prints `connect: …`, and the
  message now names both causes and points at `ss -s`. Neither was true when
  this was filed, which is why it cost a code read to get even this far.

  `connect_err` is a free function rather than a closure so the wording has
  controls: EAGAIN gets the phase and both causes, a refused connection gets
  the phase and nothing else, and the `ErrorKind` survives the rewrap. The
  negative control was confirmed to fail when the hint is made unconditional —
  without it, the positive test passes for a function that appends the hint to
  everything, which would be this bug's own defect reintroduced as its fix.
- ~~Whether the three-seat case actually churns as predicted.~~ **RUN, not
  inferred, 2026-09-02.** `cp_watch_idle_drill.sh` now stands up THREE control
  plane seats and leaves the fleet idle past the 30 s read timeout. It asserts
  zero rotations, and — the part this item actually asked for — that the count
  of applied filtered snapshots does not grow across the window, since a fresh
  snapshot per rotation is the cost the prediction was about. One seat
  exercises the same trigger; only three exercise the WALK.

  Two things follow. The current code does not churn at three seats, measured.
  And the prediction about the *pre-fix* code is now untestable without a seam,
  so it stays a prediction: what is guarded is that it cannot start happening,
  not that it once would have.

## Not the cause of anything measured

The `--regime a` numbers taken in that run are unaffected: the failure is
after the measurement legs, on the control path, and the data path never
consults it. Recorded so the exit code is not read as the benchmark failing.
