# BUG-0246: `redis.call` ran out of Lua references near 8,000 arguments (FIXED 2026-10-09)

**Status:** **FIXED 2026-10-09**. `redis.call` and `redis.pcall` take their
arguments packed into one table by a small C function, so the Rust side holds
one reference for them however many there are.
**Severity:** low. Only a script passing close to 8,000 string arguments to one
call is affected (`redis.call('DEL', unpack(keys))` with a very long `keys`),
and the seat survived it. But the script failed where Valkey's succeeds, with
an error that names no line, and the seat printed a panic.

## What happened

mlua holds a reference for each Lua string or table that Rust has in hand.
They live on an auxiliary Lua stack, which Lua 5.1 caps at 8,000 slots
(`LUAI_MAXCSTACK`). Past that, mlua panics. `redis.call` was a Rust function
taking its arguments as they came, one reference each, so a call with about
7,990 string arguments ran out:

    EVAL "local t = {} for i=1,7990 do t[i]='x'..i end
          return redis.call('DEL', unpack(t))" 0
    -> ERR C stack overflow script: on @user_script:?.

The seat's log:

    thread '<unnamed>' panicked at .../mlua-0.12.1/src/state/extra.rs:295:17:
    cannot create a Lua reference, out of auxiliary stack space (used 7996 slots)

mlua catches a callback's panic and raises it as a Lua error. So the seat
lived and the script failed. Valkey answers the same call (0). Lua's `unpack`
stops at 7,997 values, so the affected range was a few dozen arguments below
that: 7,900 was fine.

Found while writing the D3 libraries (ADR-0052), whose functions had the same
limit: `cmsgpack.pack` of a 9,000-key table failed the same way.

## The fix

`script_libs::packed` puts a C function in front of a Rust one. The C
function moves its arguments into one table and calls the Rust function with
that table and the count, and the Rust function reads one argument at a time.
`redis.call` and `redis.pcall` are built this way now, as are the libraries.
A call with 7,997 arguments answers as Valkey's does. The conformance corpus
checks this against Valkey.
