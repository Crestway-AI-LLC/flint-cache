# BUG-0189: a Lua script longer than 4 KiB was refused as an oversized key (FIXED 2026-09-27)

**Status:** **FIXED 2026-09-27**. Not in v0.1.0-rc.77: it ships with the next
release.
**Severity:** medium: every script past the size failed, loudly, and through
the proxy caching it by SHA1 did not help.

## What was measured

ADR-0052's stage-1 probe, on a gate box, 2026-09-27. With `INFO` now
reporting a version, BullMQ 6.3.9 got past its version check and failed its
first `add`:

    ERR key exceeds maximum allowed size (max-key-bytes)

No key BullMQ sends is near 4 KiB. Its scripts are.

## The mechanism

The seat refuses a command whose key is longer than `--max-key-bytes`
(4,096 by default) before dispatching it. The check knows the key positions
of `DEL`, `EXISTS` and `MSET`, and reads every other command's key at
`args[1]`. For `EVAL`, `args[1]` is the script's text, so any script longer
than the cap was refused as an oversized key. `EVALSHA` did not escape it
through the proxy: the proxy keeps the texts and forwards an `EVALSHA` as
the `EVAL` it stands for (ADR-0051), so the seat saw the text.

The rate-limiter and lock scripts ADR-0051 measured are all under 4 KiB,
which is why nothing caught it earlier.

## The fix

For `EVAL` and `EVALSHA` the check reads the declared `KEYS`, and `SCRIPT`
has no key at `args[1]`. A key a script builds is checked when its
`redis.call` dispatches, as any command's is.

Covered by a unit test (a script past the cap runs; a declared key past it,
and a built one, are refused) and a conformance case against the Valkey
oracle running a script of more than 5,000 bytes.
