# BUG-0201: the controller fails over a master at its connection cap, though it is serving (OPEN)

**Status:** **OPEN.** Filed 2026-10-02 by the ops session; handed to the
public session ("Cache technology vs Redis") to fix, by Jeff's decision.
**Severity:** high when it fires. A master that is serving every client is
promoted away, which on 2026-09-27 produced a fence split (ops OPS-0348) and
half an hour of refused writes (ops OPS-0349). BUG-0199 removed the leak that
got the playground there, but any other source of connections reaches the
same cap the same way.

## What happens

At `--max-conns` (2048 by default) a node accepts a new connection and drops
it at once (`flint-server` `main.rs`, the "B1: shed over the connection cap"
block: `drop(stream)`, no reply). Connections it already holds keep working.

Clients that ride long-lived pooled connections through the proxy notice
nothing. Every probe that opens a NEW connection fails with a reset, and the
controller's liveness check is one of those probes. It cannot tell a master
that is full from one that has died, and it promotes the replica.

## Evidence (the playground)

- `/var/lib/flint/logs/controller.log`, 2026-09-27:
  `[ctl][g0] no master for 41/3 ticks (t1: 172.31.64.94:7002 role:"replica"
  epoch:77, 172.31.64.94:7001 FLINTINFO Connection reset by peer (os error
  104); PING no; socket open | …` then `PROMOTED 172.31.64.94:7002 at
  (0,78)` and `FENCED 172.31.64.94:7001 at (0,79)`.
- Over the same window, ops OPS-0348 found the client path normal:
  - snapshots succeeded every 30 s;
  - the sequence climbed at ~35/s;
  - proxy p99 held at 0.25 ms, with 7001 serving ~50 commands/s;
  - no dial failures.

  Only probes saw 7001 as down.
- The cap was the cause: BUG-0199's leak held ~2048 proxy sockets on 7001
  by then. Measured 2026-10-02 at 904 and climbing ~24 an hour; the proxy
  was restarted at 16:29Z, and the count fell to 5.

## Directions (for the fixer to choose)

1. **Answer over-cap connections instead of dropping them.** Redis replies
   `-ERR max number of clients reached` and then closes. A probe that reads
   that knows the node is alive and full. The controller can then refuse to
   promote, or raise a page, instead of failing over.
2. **Reserve headroom for the mesh.** Connections on the internal-CA mTLS
   identity (controller, agents, replication, the CP) could be exempt from
   `--max-conns` or given their own small budget. Then a client-side leak
   can never lock out the control plane.
3. **Require a second signal before promoting.** For example, the replica's
   replication stream from the master is still live. Then "new connections
   reset, socket open" alone is not enough.

These are not exclusive. (1) is the smallest change and makes the cap
visible to every probe. (2) is the one that keeps the control plane working
however the cap is reached.

## Not in scope here

- **The leak.** Fixed as BUG-0199 (public `2f9c3f8`).
- **Seeing the cap coming.** The ops agent's early warning is OPS-0361:
  `active_conns`/`max_conns`, flagged by the sweep at 50%.
