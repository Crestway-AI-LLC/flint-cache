# BUG-0206: a replica could not re-attach to a master at its connection cap (FIXED 2026-10-05)

**Status:** **FIXED 2026-10-05.** This is the half of BUG-0201 that file
recorded and left open: "a replica cannot re-attach to a full master". Held
by `conn_cap_drill`, which now restarts the replica while the master is
held at its cap.
**Severity:** medium. It only bites at the connection cap, and a node
reaches its cap rarely since BUG-0199 stopped the proxy's leak. But when it
does, a replication link that drops stays down until the master has room.
The pair runs single-copy meanwhile, and `min-replicas-to-write` may shed
writes. The 9/27 playground incident began at exactly this cap.

## Why

A replica opens its link to the master as a client would, with `FLINTSYNC`
(resume) or `FLINTFULLSYNC` (reseed) as the first command. BUG-0201 made a
node over `--max-conns` answer a new connection's first command and close
it: PING and FLINTINFO as normal, anything else `-ERR max number of clients
reached`. That kept a full master from reading as dead. It also refused the
replica's handshake, and the replica's retries met the same answer for as
long as the master stayed full.

## The fix

The first `REPL_RESERVE` (4) connections past the cap are held for a
replica's handshake:
- a connection in that band must send its first command within the same
  1 s bound BUG-0201 uses;
- if it is `FLINTSYNC` or `FLINTFULLSYNC`, it is served, and the command
  already read is handed to the normal connection loop unparsed;
- anything else gets the answer an over-cap connection gets (PING, FLINTINFO,
  or the max-clients error) and is closed, so tenants and the proxy cannot
  use the band;
- past the band, a connection is handled exactly as BUG-0201 left it.

FLINTINFO gains `conns_reserve_admitted:`, the handshakes admitted that way,
beside `conns_shed_total`. `active_conns` can read up to four over
`max_conns` while the reserve is in use.

## The drill

`conn_cap_drill`, on a mutual-TLS fleet whose master's cap is lowered and
filled by held connections, adds a section after the controller has kept
the full master for 10 s:
- `flintctl restart-node <replica>` succeeds;
- the master's `live_replicas` returns to 1;
- `conns_reserve_admitted` rises, so the link did come through the
  reserve;
- the master is still at its cap, and a data command on a new connection
  still gets the max-clients error.

With the reserve set to 0, the replica cannot rejoin and the drill fails at
the BUG-0206 assertion.

## Not covered

- A node still LOADING (`accept_while_loading`) drops over-cap connections
  as before. A loading node is a replica catching up, not a master others
  attach to.
- The reserve is per node, not per replica. Four is enough for a pair's one
  replica, with room for its resume probe beside its stream.
