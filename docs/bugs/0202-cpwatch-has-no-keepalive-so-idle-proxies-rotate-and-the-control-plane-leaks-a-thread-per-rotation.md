# BUG-0202: CPWATCH has no keepalive, so an idle proxy rotates every five minutes and the control plane leaks a thread per rotation (OPEN)

**Status:** **OPEN.** Filed 2026-10-02 by the ops session, for the public
session to fix (the same handling Jeff set for BUG-0201).
**Severity:** medium, and it grows with the number of proxies.
- The single-seat control plane leaks one thread and one socket each time a
  proxy abandons its watch. On an idle fleet that is every five minutes per
  proxy.
- The leak only clears at the next topology change, and an idle fleet can
  go days without one.
- On the playground it has reached 534 threads in 40 hours. A fleet with ten
  proxies would reach the CP's 65535-descriptor limit in about three weeks
  of quiet.
- The August crash in the playground's `cp.log` ("failed to set up
  alternative stack guard page: Cannot allocate memory", then an abort) is
  the shape thread exhaustion takes. That is not proven to be this bug.

## What happens

**Proxy side.** `MAX_IDLE_READS` (flint-proxy `main.rs`, BUG-0081) abandons
the CP seat after ten silent 30 s reads. Its own comment says "An idle
fleet must never rotate", and also that "with no keepalive on CPWATCH" an
idle fleet and a partitioned seat look the same on the wire. So an idle
fleet DOES rotate, every ~300 s. The old playground proxy logged
`control-plane watch (0.0.0.0:7500): read: silent for 10 consecutive reads
(~300s); treating the seat as gone; trying next seat` 505 times in its ~42
hours: about 12 an hour.

**Control-plane side.** `watch()` (flint-controlplane `main.rs`) loops on
`while st.version <= acked { shared.changed.wait_timeout(st, 500 ms) }` and
never touches the socket while it waits. A watch whose proxy has gone
therefore keeps its thread and its socket until a version bump makes it
write, fail, and return. On an idle fleet that can be days.

## Measured on the playground (2026-10-02, CP started 2026-09-30 20:18Z)

- `soak-stats.csv`: `flint-controlplane` went from 61 descriptors to 535 in
  39.8 h, about 12 an hour, the proxy's rotation rate. Its RSS went from
  292 MB to 346 MB.
- `/proc/<cp>`: 536 descriptors, 533 of them sockets, and 534 threads.
  Every thread but the accept loop and one sleeper sits in `futex_do_wait`:
  the condvar wait above. `ss` shows only 1 ESTABLISHED and 3 CLOSE-WAIT
  connections on :7500, so the rest are sockets whose peers are long gone.

## Directions (for the fixer)

- **A keepalive on CPWATCH.** For example, the CP sends a small frame on its
  500 ms wake every N seconds.
  - A write to a dead peer fails, so the thread exits. That fixes the leak.
  - The proxy counts the keepalive as liveness, so an idle fleet stops
    rotating. That meets BUG-0081's stated requirement, and turns its trade
    into a real measurement.
  - **Compatibility:** an older proxy must ignore the new frame, and an
    older CP never sends it. Every release must stay backward compatible.
- **Or, CP side only:** on the wait timeout, check the socket (a
  non-blocking peek for EOF or error) and return when the peer has gone.
  That fixes the leak alone and leaves the rotation.

## Not in scope here

The proxy connection leak toward the nodes (BUG-0199), and the controller's
reading of a full node as a dead one (BUG-0201), are separate bugs.
