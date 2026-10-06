# ADR-0055: The rest of the JSON command family

Status: **ACCEPTED 2026-10-06** (Jeff: "go ahead with the missing JSON
commands"). Built. Every reply is RedisJSON v8.2.8's, checked against the
module, except where "Where we differ" says otherwise.

## Context

The JSON type served eight of RedisJSON's commands: SET, GET, DEL/FORGET,
TYPE, NUMINCRBY, ARRAPPEND and ARRLEN, plus multi-match paths since
ADR-0054. Common client calls hit the rest and got "unknown command":
- redis-py's `json().mget`, `objkeys`, `strappend` and `arrpop`;
- node-redis's `json.mGet` and `json.merge`;
- a JSON.GET with a second path or with `INDENT`.

RedisJSON v8.2.8 has 26 commands. This ADR adds the other 16.

## Decision

Each command answers as RedisJSON v8.2.8 does. Its answers came from the
module by probe, most of them before the code was written: 462 commands,
each run under RESP2 and RESP3.

| command | does | answers |
|---|---|---|
| MGET key [key ...] path | the path's GET answer from each key | nil for a key missing or not a document |
| MSET (key path value) ... | JSON.SET each triple | all or nothing |
| MERGE key path value | RFC 7396 merge patch at each match | OK |
| NUMMULTBY key path n | multiply, like NUMINCRBY adds | the same reply kinds as NUMINCRBY |
| STRAPPEND key [path] value | append a JSON string | new length in bytes |
| STRLEN key [path] | | length in bytes |
| ARRINDEX key path value [start [stop]] | first index, type-strict | index or -1 |
| ARRINSERT key path index value ... | insert before index | new length |
| ARRPOP key [path [index]] | remove, last by default | the element as JSON text, nil if empty |
| ARRTRIM key path start stop | keep the inclusive range | new length |
| OBJKEYS key [path] | | member names, in order |
| OBJLEN key [path] | | member count |
| TOGGLE key path | flip a boolean | 1/0 under `$`, `true`/`false` legacy |
| CLEAR key [path] | empty containers, zero numbers | how many changed |
| RESP key [path] | | the value in RESP terms |
| DEBUG MEMORY key [path] / HELP | | bytes; RedisJSON's help text |

JSON.GET now takes several paths, answering one object keyed by path. It
also takes the formatting arguments INDENT, NEWLINE and SPACE (and ignores
NOESCAPE), which may appear anywhere among the paths.

Per command, the module's answers for a missing key, a missing path and a
value of the wrong type differ. For example:
- With a missing key, STRLEN's legacy form is nil but its `$` form an error.
- OBJLEN's missing legacy path is nil, while STRLEN's is an error.

Each handler states its own, and the corpus pins them.

How a multi-match write applies. A command acting at several locations
(ARRINSERT, ARRPOP, ARRTRIM, CLEAR, TOGGLE, STRAPPEND) applies at the last
location first. An edit that moves array elements therefore cannot move a
location still to come. Replies still come back in document order. A
location a union names twice is acted on twice, in occurrence order, as
RedisJSON does (`$.l[0,0]` increments twice, answering `[2,3]`).

Routing:
- The new commands join `flint_commands`' read and write lists. An
  unclassified command skips the `-READONLY` gate and the write lock, and
  on a replica its write reaches a store that drops it while answering OK.
- `JSON.DEBUG`'s key follows its subcommand. One function,
  `flint_commands::json_debug_key`, gives it to the server's
  `command_key`, the proxy's `route_key` and the key-size check.
- JSON.MGET and JSON.MSET refuse CROSSSLOT on a seat, as MGET and MSET do.
  The proxy splits a JSON.MGET across slots the way ADR-0048 splits MGET,
  every per-slot command carrying the path. JSON.MSET, like MSET, is not
  split, because its atomicity is its contract.
- A JSON.MSET over more than one key takes every writer's lock (BUG-0188's
  rule), and the proxy's near-cache drops each of its keys.
- NUMMULTBY's RESP2 and RESP3 replies differ in kind, as NUMINCRBY's do, so
  `flint_resp::resp3_differs_in_kind` names both.

## Where we differ

One difference the corpus lists, in `tools/redisjson_compare.sh`:
- **NUMMULTBY refuses an integer overflow**, as NUMINCRBY has since BUG-0208.
  RedisJSON wraps (`3037000500 * 3037000500` answers a negative number).

Smaller ones, recorded in `command-support.md`:
- **A missing intermediate.** A JSON.MERGE or JSON.MSET that writes under one
  is refused here with a reason, as JSON.SET already is. RedisJSON answers
  nil from MERGE, and OK from MSET without writing that triple.
- **JSON.MSET is all or nothing.** A triple that cannot apply to what an
  earlier triple wrote refuses the command; RedisJSON answers OK and drops
  that triple.
- **Nested matches.** A multi-match ARRINSERT, ARRPOP, ARRTRIM or CLEAR whose
  matches nest applies to every one here. RedisJSON applies the first,
  then answers "Path does not exist" and keeps that first edit.
- **Strict arguments.** Arguments RedisJSON ignores are an arity error here:
  - one past the last that STRAPPEND, ARRPOP or CLEAR takes;
  - an ARRPOP index that is not an integer, where RedisJSON pops the last
    element.
- **Sizes.** JSON.DEBUG MEMORY counts the bytes a value occupies as stored.
  RedisJSON counts its in-memory tree.
- **Number spelling.** serde_json writes `1e+20` where RedisJSON writes
  `1e20`, and JSON.RESP renders doubles as the sorted sets do. The values
  are equal.
- **Multi-path order.** JSON.GET with several paths answers them in the
  order given. RedisJSON's order is its hash map's, so it varies from run
  to run.

## Found on the way

BUG-0210: the oracle run showed two of the first eight commands answering a
missing key differently from RedisJSON:
- JSON.ARRLEN with a `$` path answered nil where the module errors;
- JSON.TYPE under RESP3 answered null where the module answers `[null]`.

Both are fixed in this change.

## Verification

- `tools/redisjson_compare.sh` against RedisJSON v8.2.8 built from source:
  PASS, with exactly the listed divergences. The new corpus cases cover
  every new command, and each step agrees with the module.
- An ad hoc differential of the same 462 commands against both servers,
  under RESP2 and RESP3:
  every difference is one of the above or follows from one.
- The corpus against Flint under RESP2 and RESP3, the server's unit tests,
  and `json_drill`'s cross-pair JSON.MGET through the proxy.
