# BUG-0232: a script's typed table return answered an empty array (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by the corpus case "a script returns
redis 7's typed replies", which Valkey 9.1 also passes, and a server test
in `commands.rs` for the two forms the corpus cannot hold.
**Severity:** medium for the scripts it touches. The answer was silently
wrong, `[]`, and it needs a script written for Redis 7 that returns one
of these tables.

## What happened

Found by a Lua differential (2026-10-08), where Redis 8.2.8 and Valkey 9.1.0
agree:

```
EVAL "return {double=1.5}" 0          -> Flint: []   Redis: "1.5" (RESP3: ,1.5)
EVAL "return {map={a=1}}" 0           -> Flint: []   Redis: a 1   (RESP3: a map)
EVAL "return {set={a=true}}" 0        -> Flint: []   Redis: a     (RESP3: a set)
```

Redis 7 turns a returned table with a `double`, `big_number`,
`verbatim_string`, `map` or `set` field into that reply type, and does so
under RESP2 too, where a double is a bulk string, a map a flat array and a
set an array. `table_reply` knew only `err` and `ok`. It read the rest as
arrays, from index 1 to the first nil, which these tables do not have.

## The fix

Upstream's checks, in upstream's order: `err`, `ok`, `double` (a number),
`big_number` (a string, CR and LF as spaces), `verbatim_string` (a table
with a string `format` and `string`), `map`, `set`, then the array. A map's
pairs and a set's keys come in Lua's `next` order, as upstream walks them,
and each converts as a return value does.

One difference stays, under RESP3 only. Redis frames a big number as `(`
and a verbatim string as `=`. Our RESP parser reads neither, and a seat's
reply must be one the proxy can read, so both answer their text as a bulk
string. That is upstream's RESP2 reply exactly.
