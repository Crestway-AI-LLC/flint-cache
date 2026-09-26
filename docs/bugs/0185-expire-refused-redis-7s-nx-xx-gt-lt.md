# BUG-0185: EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT refused Redis 7's NX, XX, GT and LT (FIXED 2026-09-26)

**Status:** **FIXED 2026-09-26**. Not in v0.1.0-rc.77: it ships with the next
release.
**Severity:** medium: a documented command refused a standard form of itself
with the wrong error, and the framework measured sending it takes that path
on any server that reports Redis 7.

## What was measured

`docs/command-support.md` lists `EXPIRE`, `PEXPIRE`, `EXPIREAT` and
`PEXPIREAT` as supported, with no caveat. Each took exactly three arguments,
so every form with an option answered as if miscounted:

    EXPIRE k 60 NX -> ERR wrong number of arguments for 'expire' command

Redis 7.0 added the options: `NX` (only if the key has no expiry), `XX` (only
if it has one), `GT` and `LT` (only if the new expiry is later, or earlier,
than the current one). Measured on a gate box, 2026-09-26: Rails 8.1's
`RedisCacheStore#increment(key, 1, expires_in: 60)`, which is what
Rack::Attack calls on every throttled request, sends `INCRBY` then
`EXPIRE key 60 NX` to a server whose `INFO` reports Redis 7 or later, as it
did to Valkey. On Flint it takes an older path (`TTL`, then `EXPIRE`) only
because Flint's `INFO` reports no version (ADR-0048). redis-py's
`expire(..., nx=True)` and its siblings send the same forms directly.

## The fix

The four commands take the options after the time, with upstream's rules,
as in `expire.c`:

- options are read before the number, so an unknown one is reported first
  (`ERR Unsupported option X`), then `NX` with any other
  (`ERR NX and XX, GT or LT options at the same time are not compatible`),
  then `GT` with `LT`;
- a missing key answers 0 whatever the options;
- a key with no expiry counts as expiring never, so `GT` never applies to it
  and `LT` always does;
- a condition is judged on the requested instant before it is clamped, and
  a past instant that passes deletes the key, as one without options does.

`EXPIRE` already held the key's exclusive write lock (it is not a pure
write), so reading the current expiry and setting the new one is one step.

Covered by a unit test of every rule above in `flint-server`, and a
conformance case (`ttl`: "expire conditions nx xx gt lt (redis 7)") that
the Valkey oracle and both engines pass, in RESP2 and RESP3.
