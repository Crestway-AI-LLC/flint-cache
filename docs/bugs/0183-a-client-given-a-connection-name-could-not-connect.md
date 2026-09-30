# BUG-0183: a client given a connection name could not connect, because CLIENT was unknown (FIXED 2026-09-25)

**Status:** **FIXED 2026-09-25**. Not in v0.1.0-rc.77: it ships with the next
release.
**Severity:** high for anyone who names their connections, which is a
one-line option in every mainstream client and a common operations habit.

## What was measured

Through the proxy on a two-pair fleet on a gate box, 2026-09-25, each client
with its connection-name option set and nothing else changed:

| client, option | RESP2 | RESP3 |
|---|---|---|
| redis-py 7.0.1, `client_name=` | fails: `unknown command 'CLIENT' ... 'SETNAME'` | fails, the same |
| go-redis v9, `ClientName` | fails, the same | fails, the same |
| node-redis 5, `name` | never finishes connecting | |
| ioredis 5, `connectionName` | works: it ignores the refusal | |
| Jedis 5.2, `clientName` | works | works |
| Lettuce 6.5, `withClientName` | | works: it names itself inside `HELLO`, which the proxy already accepted |

Every `CLIENT` subcommand was unknown at the proxy: `SETNAME`, `GETNAME`,
`ID`, `INFO`, `SETINFO`, `LIST`.

## The fix

The proxy answers `CLIENT` for the caller's own connection (`ClientConn` in
`flint-proxy`): `SETNAME` (upstream's character rule and error; an empty name
clears it), `GETNAME`, `ID` (per proxy process), `SETINFO LIB-NAME|LIB-VER`,
and `INFO` reduced to the fields that are true at the proxy. A name given in
`HELLO ... SETNAME` is kept as the same name; `parse_hello` now returns it.
Inside `MULTI` a `CLIENT` command takes the transaction's path, so EXEC's
reply stays aligned with what the client queued.

`CLIENT LIST`, `KILL`, `PAUSE`, `TRACKING` and the rest answer upstream's
"unknown subcommand ... Try CLIENT HELP." A connection list at the proxy would
show other tenants' connections, and the rest act on connections the caller
does not own.

Covered by unit tests of the subcommands and of `HELLO`'s name, and by
`client_compat_drill`: redis-py (both protocols), node-redis and go-redis each
connect with a name set and read it back with `CLIENT GETNAME`.
