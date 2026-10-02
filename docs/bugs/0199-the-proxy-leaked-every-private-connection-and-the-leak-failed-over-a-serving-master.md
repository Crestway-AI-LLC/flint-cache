# BUG-0199: the proxy leaked every private backend connection, and the leak failed over a serving master (FIXED 2026-10-02)

**Status:** **FIXED 2026-10-02.** Found by the ops session following up an
open question in ops OPS-0348: why only the probe path saw the master slow.
Held
by `a_sessions_private_connection_closes_when_the_session_ends` in
`crates/flint-proxy/src/main.rs`, which fails with the fix removed. Released in
the next cut after v0.1.0-rc.78.
**Severity:** high on any long-lived proxy. Every client session that used a
private connection left it open on the node for the life of the proxy. On the
playground that reached the master's `--max-conns` (2048) about every four
days. The master then refused every NEW connection while the proxy's pooled
ones kept serving clients, so the controller's liveness probe failed against
a healthy master and promoted its replica. That is the 2026-09-27 failover,
whose fence split and half hour of refused writes are ops OPS-0348/0349.

## What was wrong

`Backends` (one per client session in `flint-proxy`) keeps the connections
that must not be shared in `private`:
- a transaction's, because MULTI's queue and WATCH's watches are per
  connection on the node;
- the O(keys) admin class's (`DBSIZE`, `FLUSHALL`, `SCAN`'s per-master
  step), so a minute-long reply cannot block anyone else's reads.

`AsyncConn` says a connection must be `shutdown()`, never merely dropped: its
reader task owns the read half and stays parked on the socket forever.
Every path that discards a private connection called it (`drop_conn`, a
failed `private_call`) except one: the end of the session. `Backends` had no
`Drop`, so when a client disconnected its private connections were dropped
by going out of scope. The socket stayed ESTABLISHED on the node and
`LIVE_CONNS` (`pool_lanes` in PROXYSTATS) stayed counted.

## Measured on the playground (2026-10-02)

- `flint_proxy_pool_lanes`, read from the ops box Prometheus every 12 h:
  414 on 09-24, then 668, 928, … 3484 on 09-30. About 256 a day, linear,
  never shrinking, with exactly two active client connections throughout.
  The proxy restart at the rc.78 roll reset it to 139, and it was 903 at
  10-02 14:00Z.
- `ss` on the host: the master 7001 held 904 ESTABLISHED connections, 903 of
  them owned by `flint-proxy`. The other was the replica's link. The replica
  7002 held none.
- The client: sampling every 10 s, the count rose by 2 between 14:44:38 and
  14:44:48Z. The playground's verify, page and soak-sample timers had all
  fired at 14:44:37. `verify-watch.sh` runs `flintctl verify`, which
  connects to the proxy as a tenant and runs `DBSIZE` (to check the fan-out
  reaches every master) and `SCAN 0`, then disconnects: two private
  connections per run, every five minutes.
- 9/27, `/var/lib/flint/logs/controller.log` on the playground:
  `no master for 41/3 ticks (t1: 172.31.64.94:7002 role:"replica" epoch:77,
  172.31.64.94:7001 FLINTINFO Connection reset by peer (os error 104); PING
  no; socket open …` followed by `PROMOTED 172.31.64.94:7002 at (0,78)`.
  "Socket open" with a reset on every new connection is the node's
  connection cap at work: it accepts and drops (`main.rs`, B1). That
  promotion followed the 9/23 roll by about four days, which is 2048 at
  this rate.
- That is also why OPS-0348 found that "only the probe path saw 7001 slow":
  clients rode the proxy's existing pooled connections and never noticed.

## Fixed

`impl Drop for Backends` shuts down every private connection when the session
ends, the same call the other two discard paths already make.

The test runs a fake backend that counts its open connections. A session
runs `DBSIZE` on a private connection, and the test checks the connection
was open while the session lived and closes when it ends. With the `Drop`
removed, the backend still holds the connection after the session is gone,
and the test fails on that assertion.

## Not covered

- **Already-leaked connections.** Connections a running proxy has already
  leaked stay open until that proxy restarts. On the playground that means
  until the rc.79 roll, or a proxy restart before then.
- **No warning before the cap.** The node gives no early warning of a
  climbing connection count. The ops agent will (OPS-0361: `active_conns`
  against `max_conns`, flagged at 75%).
