# BUG-0225: small refusals and error texts that differed from Valkey's (FIXED 2026-10-07)

**Status:** **FIXED 2026-10-07**. Held by two corpus cases, "scan options are
read after the key, in upstream's words" and "an empty error reply and the
script verbs' refusals, in upstream's words", which Valkey 9.1 also passes,
and by a proxy unit test for HELLO.
**Severity:** low. Each is a refusal worded differently or made in a
different order; none changed data.

## What differed

Found by the three-way differential run through flint-proxy (2026-10-07),
where Redis 8.2.8 and Valkey 9.1.0 agree:

- **HELLO** accepted a `SETNAME` that `CLIENT SETNAME` refuses (a space or
  newline), answered the handshake and dropped the name. Upstream refuses
  the HELLO. Its other errors were Flint's words: `HELLO abc` is "Protocol
  version is not an integer or out of range", `HELLO 4` is "NOPROTO
  unsupported protocol version", and a bad or short option is "Syntax error
  in HELLO option '<option>'".
- **HSCAN, SSCAN and ZSCAN** read their options before the key. Upstream
  reads the key first: a missing key answers an empty scan whatever options
  follow, and another type answers WRONGTYPE. A COUNT that is not an integer
  is "value is not an integer or out of range" (one below 1 stays a syntax
  error), in SCAN too, and NOVALUES outside HSCAN is "NOVALUES option can
  only be used in HSCAN".
- **`redis.error_reply('')`** sent an empty error line, `-`; upstream sends
  `-ERR `. **`redis.sha1hex()`** with no argument raised a Lua conversion
  error and its traceback; upstream answers "wrong number of arguments" on
  the script's line.
- **`SCRIPT EXISTS`** with no SHA and **`SCRIPT LOAD`/`KILL`** with the wrong
  count named `script` rather than `script|exists` and so on, and **`SCRIPT
  FLUSH BOGUS`** was an argument-count error rather than "SCRIPT FLUSH only
  support SYNC|ASYNC option".

## The fix

Each is upstream's order and words. Not changed: INCRBYFLOAT's arithmetic
width and its refusal of hexadecimal floats, recorded in
`command-support.md`, since upstream's answer there depends on the
platform's `long double`.
