# BUG-0247: the libraries and `redis` look empty to `pairs`, `next` and `rawget` (FIXED 2026-10-09)

**Status:** **FIXED 2026-10-09**, the day it was recorded, while checking the
D3 libraries against Valkey. A library now lists, walks and encodes as the
table behind it, and a write to one is refused with Valkey's message.
**Severity:** low. No client library we test lists these tables. Listing
them did turn up API that scripts do use: `redis.REDIS_VERSION`,
`cjson.new` and Valkey's `server`, all added.

## What happened

A script cannot change what the next script on the same Lua state sees,
because the libraries and `redis` are read-only (ADR-0051). Flint does that
with a stand-in: an empty table whose metatable reads through to the real one
(`__index`) and refuses writes (`__newindex`). Valkey marks the real tables
read-only inside its patched Lua instead. So a script calling a function saw
no difference, but one that listed a table did:

| Script | Valkey 9.1 | Flint before | Flint after |
| --- | --- | --- | --- |
| `local n=0 for k in pairs(cjson) do n=n+1 end return n` | 13 | 0 | 13 |
| `return next(redis) ~= nil` | 1 | nil | 1 |
| `return rawget(string, 'format') == string.format` | 1 | nil | 1 |
| `return type(getmetatable(cjson))` | `nil` | `boolean` | `nil` |
| `return cjson.encode(bit)` | error: cannot serialise a function | `{}` | as Valkey |
| `return #cmsgpack.pack(cjson)` | 205 | 1 | 205 |
| `pcall(setmetatable, cjson, {})` | "Attempt to modify a readonly table" | "cannot change a protected metatable" | as Valkey |

## The fix

Lua 5.1 has no `__pairs` metamethod, so the stand-in cannot list itself. The
sandbox's `next`, `pairs`, `rawget` and `getmetatable` now look a stand-in
up and use the table behind it. They never hand that table to the script:
`pairs(cjson)` answers `cjson` itself as its state, as Valkey's does, and the
values it yields are the library's functions, not tables of its own.
`setmetatable` and `rawset` on a stand-in raise Valkey's bare "Attempt to
modify a readonly table". `cjson.encode` and `cmsgpack.pack` encode a
stand-in as the table behind it, reading the same map, which the libraries
create and the sandbox fills.

Listing the tables exposed what they lacked. Each of these is now added, as
Valkey has it:
- `redis.REDIS_VERSION` and `redis.REDIS_VERSION_NUM`. They give `7.2.4` and
  459268, the version the proxy's `INFO` reports; both now read one constant
  in `flint-commands`.
- `cjson.new`, which makes another read-only module.
- `cmsgpack._COPYRIGHT` and `_DESCRIPTION`.
- `server`, Valkey's name for `redis`.

## What is still different

- **Valkey-only fields.** `redis.SERVER_NAME`, `VALKEY_VERSION` and
  `VALKEY_VERSION_NUM` are absent, since Flint's `INFO` does not claim to be
  Valkey either.
- **`redis.acl_check_cmd` is absent.** Flint has no ACL users.
- **Two metatables stay hidden.** `getmetatable('')` and `getmetatable(_G)`
  answer `false`, where Valkey shows the tables. Hiding them is the sandbox
  (ADR-0051).
- **A tail-called write loses its line.** A script that tail-calls `rawset`
  or `setmetatable` on a library (`return rawset(cjson, 'x', 1)`) gets the
  right message but line `?`, because the wrapper is a Lua function.

The conformance corpus holds 24 of these calls, which pass on Valkey too.
