# BUG-0201: the controller fails over a master at its connection cap, though it is serving (FIXED 2026-10-02)

**Status:** **FIXED 2026-10-02.** Filed the same day by the ops session and
handed to the public session ("Cache technology vs Redis") to fix, by Jeff's
decision. Held by the new CORE drill `conn_cap`, which reproduces the
playground's failover on the unfixed node and passes on the fixed one.
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

## Fix

Directions 1 and 2 together, at the node. Over the cap, a new connection is
no longer dropped. It is answered once, on a thread of its own, and closed
(`answer_over_cap`):

- **PING and FLINTINFO** answer as on a node with room. They are what every
  liveness check sends, so the controller, `flintctl status` and the ops
  agent read a full master as the master it is. FLINTINFO's `active_conns`
  and `max_conns` say why it is full.
- **Anything else** gets `-ERR max number of clients reached`, Redis's own
  reply at its `maxclients`. A proxy dialling a full node fails that dial
  with a reason, not a reset.
- **Bounded:** at most 16 such answers at once, each with 1 s for its TLS
  handshake and 1 s for its command. Past 16, the node drops the connection
  as before, so a connection storm still cannot exhaust its threads.

So the reserve for the mesh's probes is a reply, not a slot: a probe needs
one answer, not a connection to keep.

The controller is unchanged. It never sees the error, because it only sends
what the node now answers.

## Verification

`tools/conn_cap_drill.sh`, a CORE drill. It bootstraps a real fleet (CP, one
pair, proxy, controller, mutual TLS), warms the proxy's pool, and lowers the
master's cap with `FLINTCONFIG max-conns` to six above what it holds. A holder
fills the cap and keeps taking any slot that frees. Then each check runs on a
connection of its own:

- PING answers PONG.
- FLINTINFO answers `role:master`, with `active_conns` at `max_conns`.
- SET answers the max-clients error.
- For 10 s the replica still reports `role:replica`, and the master is still
  master at its epoch. That is past the controller's confirm ticks (3 × 150
  ms) and its 4 s slow-promote window.
- The edge takes a write every second throughout.
- Released, the master takes new connections again.

Measured on the gate box:

- **Fixed:** all of the above. The holder's next connection got the
  max-clients error, and FLINTINFO read `active_conns 12 of 12`.
- **Unfixed:** every probe saw the TLS stream close (EOF). With the probe
  checks made non-fatal, the controller logged
  `no master for 28/3 ticks (t1: 127.0.0.1:6513 FLINTINFO unexpected end of
  file; PING no; socket open, …)`, then `PROMOTED 127.0.0.1:6514`. The drill
  failed at 5 s: the playground's 2026-09-27 failover, reproduced.

## What this does not do

- **A replica cannot re-attach to a full master.** Its replication handshake
  is not a probe, so it gets the max-clients error until the master has room.
  A link that drops while the master is full stays down until then, and
  `min-replicas-to-write` may shed writes meanwhile. The ops agent's early
  warning (OPS-0361, at 50% of the cap) is the defence. A reserve for
  replication, by first command or by identity, would be the next step.
- **A node still loading** (the `-LOADING` acceptor) drops over-cap
  connections as before. A loading node is never a master.
- **Direction 3**, a second liveness signal before promoting, is not built.
  A full node now answers, so it is not needed for this failure.

