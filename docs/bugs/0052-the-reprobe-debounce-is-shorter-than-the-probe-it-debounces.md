# BUG-0052: the re-probe debounce is shorter than the probe it debounces

Status: FIXED 2026-08-26. The gate is now single-flight per address, with the
250 ms window demoted to the quiet period AFTER a probe rather than a bound on
how often one may start. · Severity: MEDIUM — the coalescing that
`flint-proxy` added to stop a routing-table herd works only while the probe is
fast, and the probe is slow in exactly the situation that produces the herd.

## The two constants

    crates/flint-proxy/src/main.rs:2854
      const REDISCOVER_DEBOUNCE: Duration = Duration::from_millis(250);

    crates/flint-proxy/src/main.rs, discover_master()
      stream.set_read_timeout(Some(Duration::from_millis(800)));

The read timeout is **3.2x the debounce window, per node**, and
`discover_master` walks every member of the pair — so one probe is permitted to
run up to 1600 ms against a two-member pair before it gives up. The window that
is supposed to absorb the herd closes after 250 ms of it.

## Why that breaks the coalescing

`rediscover_after_failure` stamps the gate and then releases the lock *before*
probing:

    gate.insert(addr.to_string(), Instant::now());
    }                       // lock dropped here
    self.rediscover_for(addr);

So the gate bounds the rate at which probes may *start*, not the number that
may be *in flight*. A caller arriving 260 ms into an 800 ms probe finds the
window expired, stamps it again, and starts a second concurrent probe. Over an
outage the herd is not absorbed — it is issued at roughly one probe per 250 ms,
each one racing a write into `routing.masters[i]`:

    if let Ok(mut routing) = view.routing.write()
        && let Some(slot) = routing.masters.get_mut(i)
    { was = slot.clone(); *slot = found.clone(); }

That is the same `addr -> none -> addr` flap the function's own doc comment
says this mechanism exists to prevent — 198 routing transitions from one
death, throughput at 0.12x, 10 s p99.9. The debounce reduced its rate. It did
not remove it.

**The regime matters.** A probe is fast when the pair answers, and a pair that
answers is not producing a herd. The 800 ms timeout is reached when a node is
connected but silent — which is #176's shape exactly, a process that binds
early and answers nothing until it has loaded. The coalescing is therefore
weakest precisely when it is load-bearing.

## Why the test does not catch it

`simultaneous_request_failures_cause_one_reprobe` fires 32 failures in a loop
and asserts none of them re-probed. It passes, and it would pass with no
debounce logic at all beyond the first stamp, because `two_pair_topo` routes to
`"a:1"` — an address the test's own comment notes "nothing answers on in a unit
test". `connect_reloadable` fails on resolution, `discover_master` returns
`None` immediately, and all 32 iterations complete in microseconds. Probe
duration in the test is ~0, so the loop can never leave the window.

The test asserts coalescing in the only regime where coalescing is free. It is
silent in the regime it was written for. Same shape as BUG-0049: an assertion
whose precondition is an ambient property of the run rather than something the
run establishes.

## The fix is single-flight, not a bigger number

Raising `REDISCOVER_DEBOUNCE` above 1600 ms would paper over it and cost real
failover latency — the debounce is also what delays legitimate rediscovery
after a genuine move. The property actually wanted is *one probe in flight per
address*, which a timestamp cannot express:

- hold an in-flight marker for the **duration** of `rediscover_for`, so
  concurrent callers are excluded outright rather than raced;
- keep a timestamp debounce layered on top for the quiet period *after* a
  probe completes, which is what the current constant is genuinely sized for;
- release the marker on every exit path, including the early `Err(_) => return`
  on a poisoned lock.

Note the existing carve-out must survive: `apply_promote_hint` is deliberately
NOT debounced, because a control-plane hint is news rather than a symptom.
Single-flight must coalesce the symptom and still never block the signal.

## What must land with it

A test that fails without the fix, which means a probe whose duration the test
controls — a seam over `discover_master`, or a pair member backed by a listener
that accepts and then stays silent past the window. Asserting "one probe" while
the fake probe returns instantly is how the current test came to prove nothing.


## The fix

`rediscover_gate` held `HashMap<String, Instant>` — which can only say "a probe
STARTED at t". It now holds a two-state value:

    enum ProbeState { InFlight, Idle(Instant) }

`InFlight` returns immediately for every other caller, however long the probe
takes; `Idle(t)` applies the 250 ms quiet period the constant was always sized
for. The claim is released by a `Drop` guard rather than by careful code,
because a probe that panics or returns early would otherwise wedge that address
forever — permanently and silently, which is worse than the flap being fixed.

The carve-out survives: `apply_promote_hint` still calls `rediscover_for`
directly and is not gated. Coalesce the symptom, never the signal.

## Measured, with a test that controls probe duration

The bug doc said the fix needed one, because the existing
`simultaneous_request_failures_cause_one_reprobe` fires 32 failures that each
resolve in microseconds and so never overlap a probe at all.

`a_second_caller_during_a_slow_probe_is_absorbed` binds a listener that accepts
and never answers, so `discover_master` blocks on its 800 ms read timeout. A
second caller arrives at 300 ms — deliberately PAST the 250 ms window and
INSIDE the probe, which is precisely the case the old gate got wrong:

    without single-flight : second caller took 801ms — it started its own probe
    with single-flight    : second caller returned in <100ms

It carries a capability assert too: if the first probe finishes in under 250 ms
the listener is not holding the connection open, the callers never overlapped,
and the test fails saying so rather than passing on nothing.
