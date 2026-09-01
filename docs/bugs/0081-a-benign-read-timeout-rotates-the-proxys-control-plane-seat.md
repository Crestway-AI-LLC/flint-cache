# BUG-0081 — a benign read timeout rotates the proxy's control-plane seat (OPEN)

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

## Not yet established

- Whether `watch_control_plane`'s socket sets a read timeout deliberately (in
  which case the fix is to treat `WouldBlock`/`TimedOut` as "keep waiting" and
  not rotate) or inherits one (in which case the timeout is the bug).
- Whether the three-seat case actually churns as predicted. It is an inference
  from the same loop, and the loop was read rather than run — worth a drill
  that counts CPWATCH subscriptions on an idle fleet before believing it.

## Not the cause of anything measured

The `--regime a` numbers taken in that run are unaffected: the failure is
after the measurement legs, on the control path, and the data path never
consults it. Recorded so the exit code is not read as the benchmark failing.
