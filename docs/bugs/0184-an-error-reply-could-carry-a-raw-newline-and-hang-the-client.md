# BUG-0184: an error reply could carry a raw newline, and redis-py then hung until its socket timeout (FIXED 2026-09-25)

**Status:** **FIXED 2026-09-25**. Not in v0.1.0-rc.77: it ships with the next
release.
**Severity:** high where it bites: a client hangs instead of failing, holding
its connection until a timeout, and many clients set none.

## What was measured

Through the proxy on a two-pair fleet on a gate box, 2026-09-25. django-redis
6.0.0's `incr` sends `EVAL` with a multi-line Lua script. `EVAL` is not
implemented, which is expected; the reply was not:

    ('SET', ':1:dr:n', 1, 'PX', 300000) -> True (4 ms)
    ('EVAL', "\n    local exists = redis.call('EXISTS', KEYS[1])\n ...) -> TimeoutError: Timeout reading from socket (5005 ms)

The same `EVAL` with the script on one line failed at once with
`ERR unknown command 'EVAL'`. So did `EVALSHA`, whose arguments hold no
newline, which is why redis-py's `Lock.release()` failed promptly rather than
hanging.

## The mechanism

The unknown-command error echoes the command's arguments (as upstream does,
truncated), so the script's newlines went into the error line: `-ERR ...
'\n    local exists ...'\r\n`. A RESP simple error is one line. redis-py's
parser reads to the first LF, finds no CR before it, reads more from the
socket, and does so until its timeout. Upstream never sends such a line: it
replaces CR and LF in an error with spaces before writing it.

## The fix

`flint-resp`'s encoder, the one both the seats and the proxy use, writes a
simple string or error as one line, replacing CR and LF with spaces
(`push_line`), in RESP2 and RESP3. Every error path is covered, including any
added later. A reply with neither character is written exactly as before.

Covered by a `flint-resp` unit test (red with the replacement removed) and by
`client_compat_drill`: redis-py sends a multi-line `EVAL` with a 5 s socket
timeout, must get `unknown command` at once, and its connection must still
answer `PING`.

Re-measured with the fix: django-redis's `incr` gets the error at once and
falls back to its own read-modify-write (`EXISTS`, `TTL`, `GET`, `SET`), so it
answers (`incr -> 2`). That fallback is not atomic, and two concurrent
increments can lose one. That is Lua's absence, which is by design and a
separate question, not this fix.
