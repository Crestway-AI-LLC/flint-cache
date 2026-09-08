# BUG-0123 — the proxy bounds the reply and leaves the dial on a backstop

Status: OPEN · Severity: medium — the discovery path's own stated bound is
800 ms per node and its actual worst case is 3800 ms, on the client request
path, during a failover
Found: 2026-09-08, following BUG-0122 into the path that actually carries
customer traffic
Component: `flint-proxy` master discovery (`discover_master`), and the
implicit contract of `flint_tls::connect`

## What happened

BUG-0122 established that the chaos harness cannot see its own connect phase
and closed with the observation that real clients are safe because they reach
the fleet through the proxy's fixed address. That claim deserved checking,
because the proxy dials backends through the same function.

It does, and it has the same asymmetry:

    crates/flint-proxy/src/main.rs:1239   flint_tls::connect_reloadable(addr, tls)
    crates/flint-proxy/src/main.rs:1242   stream.set_read_timeout(Some(Duration::from_millis(800)))

The **reply** is bounded at 800 ms, deliberately and visibly. The **dial**
above it is bounded only by `flint_tls::connect`'s
`TcpStream::connect_timeout(.., 3 s)` (`crates/flint-tls/src/lib.rs:614`).

So the worst case for one node is **3800 ms**, and `discover_master` walks
every member of the pair: **7600 ms for a two-member pair**, against a stated
intent of 800 ms per node.

## Why it matters: this is on the request path

`discover_master` is not only a background refresher. `refresh_pair_master`
(:1771) calls it, and that is called from inside client request handling at
:1827 and :1835 — on the `Err(e)` arm, when a backend call fails and the proxy
drops the connection and rediscovers before retrying.

That arm is the failover path. It is reached exactly when a master has just
died, which is also exactly when a member of that pair is least likely to
complete a TCP handshake promptly. The client is blocked on the proxy for the
whole of it.

## The 3 s was never a latency budget

`flint_tls::connect` says what it is for, and it is worth quoting because it
already anticipated this:

> Bounded connect. A bare `TcpStream::connect` has no timeout of its own, so a
> blackholed peer (host down harder than a RST — partition, SG change, hung
> NIC) parks the CALLER for the kernel's SYN-retry budget, minutes, regardless
> of any read timeout set after. Every internal dialer (controller sweep,
> server lease renewals, **proxy back-ends**) comes through here; none of them
> can afford an unbounded wait.

Three seconds is a **ceiling on a pathology** — it replaced *minutes*. It is
doing its job. The defect is that a caller which carefully chose 800 ms for
its own reply phase silently inherits that ceiling for the phase before it,
and the comment even names the caller.

Note what this means for the two conditions:

- A node that is **down** answers with RST at once. Refused dials cost
  microseconds, which is why this is invisible in ordinary operation — the
  instrumented soak measured 19 failed dials in one kill window at a
  `max_connect_ms` of 0.
- A node that is **blackholed** — the case the comment names — costs the full
  3 s. Nothing in the discovery path distinguishes them, and nothing measures
  which one it got.

## It also corrects BUG-0052

BUG-0052 (FIXED) analysed this same function and stated:

> The read timeout is 3.2x the debounce window, per node, and `discover_master`
> walks every member of the pair — so one probe is permitted to run up to
> **1600 ms** against a two-member pair before it gives up.

**1600 ms is the reply phase only.** The real figure is 7600 ms, and the ratio
against `REDISCOVER_DEBOUNCE` (250 ms, :3115) is not 3.2x per node but 15.2x.

BUG-0052's *fix* is not invalidated — single-flight per address bounds
concurrency regardless of how long a probe runs. What is wrong is the number
its reasoning rested on, and it is wrong by the same omission as BUG-0122's:
both accounted for every phase after the connect and none of the connect
itself. That is now three places (`max_hold_ms`, BUG-0052's 1600 ms, this)
where the connect phase was left out of a bound that claimed to cover it.

## What is NOT claimed here

**This is not a report that clients have stalled 3 s through the proxy.** No
such stall has been measured. Whether a dial in this fleet ever reaches the
full 3 s is precisely BUG-0122's open question, and it is unresolved.

The defect stands without it: the code states a bound of 800 ms for an
operation whose worst case is 3800 ms, on the request path, and cannot report
which it got. A bound that does not cover the operation it is attached to is
wrong when it is loose, not only when it is exceeded.

## The budget mechanism is demonstrated, measured here

`connect_within` against TEST-NET-1 (`192.0.2.1:9`, RFC 5737 — reserved and
unrouted, so the SYN is dropped rather than refused), on the development Mac:

| budget | elapsed | error |
|---|---|---|
| 250 ms | 251.3 ms | `TimedOut` |
| 2000 ms | 2001.2 ms | `TimedOut` |

So the budget is exactly what gives up, to the millisecond, and a blackholed
peer burns all of it. That is the behaviour the 3 s backstop was written for,
confirmed rather than assumed.

It also supplies the piece BUG-0122 was missing. That bug could not establish
that a 3 s dial is *reachable*; this shows it is, given SYNs that go
unanswered. It does **not** show that the soak's fleet produced that condition
— the two are different claims and only the first is settled.

## Fix

Give the dial a budget of its own, proportionate to the reply budget the
caller already chose, rather than letting it fall through to the pathology
ceiling. The backstop stays where it is for every caller that has no opinion.

## Next step

An instrumented probe on the discovery path, in the shape BUG-0122 added for
the harness: record the dial duration and whether it failed, so the two
conditions above — refused fast versus blackholed for 3 s — stop being
indistinguishable in the one path that carries customer traffic.
