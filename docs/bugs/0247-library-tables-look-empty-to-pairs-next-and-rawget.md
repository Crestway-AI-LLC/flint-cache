# BUG-0247: the libraries and `redis` look empty to `pairs`, `next` and `rawget` (OPEN)

**Status:** **OPEN**, recorded 2026-10-09 while checking the D3 libraries
against Valkey.
**Severity:** low. No client library we test lists these tables' contents. A
script that does sees a difference from Valkey.

## What happens

A script cannot change what the next script on the same Lua state sees,
because the libraries and `redis` are read-only (ADR-0051). Flint does that
with a stand-in: an empty table whose metatable reads through to the real one
(`__index`) and refuses writes (`__newindex`). Valkey marks the real tables
read-only inside its patched Lua instead. So a script calling a function sees
no difference, but one that lists a table does:

| Script | Valkey 9.1 | Flint |
| --- | --- | --- |
| `local n=0 for k in pairs(redis) do n=n+1 end return n` | 26 | 0 |
| `local n=0 for k in pairs(cjson) do n=n+1 end return n` | 13 | 0 |
| `local n=0 for k in pairs(string) do n=n+1 end return n` | 15 | 0 |
| `return next(cjson) ~= nil` | 1 | nil |
| `return rawget(cjson, 'encode') ~= nil` | 1 | nil |
| `return type(getmetatable(cjson))` | `nil` | `boolean` |
| `return cjson.encode(cjson)` | error: cannot serialise a function | `{}` |

## What a fix needs

Lua 5.1 has no `__pairs` metamethod, so the stand-in cannot fix this itself.
There are two routes:

- Let `next`, `pairs`, `rawget` and `getmetatable` see through a stand-in to
  the table behind it, and have `cjson` and `cmsgpack` do the same. This is
  small, but every function that walks a table has to remember the stand-ins.
- Mark the real tables read-only in the interpreter, as Valkey does. That
  needs a patched Lua 5.1 rather than the vendored one.
