# BUG-0210: JSON.ARRLEN and JSON.TYPE answered a missing key differently from RedisJSON (FIXED 2026-10-06)

**Status:** **FIXED 2026-10-06**, with ADR-0055. Held by
the corpus case "ARRLEN and TYPE answer a missing key as RedisJSON does
(BUG-0210)", which is checked against RedisJSON.
**Severity:** low. No data is touched. A client reading a missing key could
take a nil where it expects an error, or the reverse.

## Why

Measured against RedisJSON v8.2.8 on public `2604c2e`:
- `JSON.ARRLEN nokey $` (or `$.a`): RedisJSON answers "could not perform
  this operation on a key that doesn't exist"; Flint answered nil. Under the
  legacy dialect both answer nil.
- `JSON.TYPE nokey` under RESP3: RedisJSON answers `[null]`, the same extra
  array level its other JSON.TYPE replies carry under RESP3; Flint answered
  `null`. Under RESP2 both answer nil.

The July oracle run never asked for a missing key with a `$` path, or ran
under RESP3. ADR-0055's differential runs both protocols over every command.

## The fix

JSON.ARRLEN answers the error for a missing key under `$`. JSON.TYPE wraps
a missing key's nil in `Resp3Nested`, as it wraps every other reply.
