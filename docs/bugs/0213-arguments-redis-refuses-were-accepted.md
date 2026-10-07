# BUG-0213: arguments Redis refuses were accepted (FIXED 2026-10-07)

**Status:** **FIXED 2026-10-07**. Held by the corpus cases "set takes one
kind of expiry and refuses one out of range", "expire refuses an instant out
of range", "integers are canonical and overflow is named" and "a flush with
an unknown argument flushes nothing", which Valkey also passes.
**Severity:** medium. A client mistake that Redis refuses changed data here
instead: a key deleted, a keyspace flushed, an expiry other than the one
asked for. Each needs an argument no correct client sends.

## Why

The same differential as BUG-0212. Each of these is an error in Redis 8.2
and in Valkey, and was not an error here:
- **SET with two kinds of expiry option.** `SET k v EX 10 PX 10` and
  `SET k v KEEPTTL EX 10` kept the last option. Redis takes one kind (EX,
  PX, EXAT, PXAT or KEEPTTL) and allows only a repeat of it.
- **An expiry past the 64-bit range.** `SET k v EX 9223372036854775807`
  was clamped to the largest instant. `EXAT 0` and `PXAT -1` were taken as
  instants in the past. Redis refuses a count of zero or less, and one
  whose milliseconds, or whose sum with now, overflows.
  `SETEX k 9223372036854775807 v` multiplied without a check: in a release
  build the product wrapped and set some other expiry, and in a debug build
  the connection panicked.
- **EXPIRE out of range.** `EXPIRE k -9223372036854775808` was clamped to a
  past instant and **deleted the key**. Redis answers "invalid expire time"
  and leaves the key alone, as it does for an EXPIRE, PEXPIRE or EXPIREAT
  whose instant overflows.
- **FLUSHALL with an unknown argument.** `FLUSHALL FOO` **flushed the
  keyspace**. Redis takes ASYNC or SYNC, or nothing, and refuses the rest.
- **An integer that is not canonical.** `INCR` on a value of `01` or `+1`
  answered 2. Redis reads `-`, then digits with no leading zero, or `0`
  alone, in an argument and in a stored value. Rust's `parse` also takes a
  plus sign and leading zeros.
- **DECRBY by i64::MIN.** It saturated the negation to i64::MAX, one short
  of the decrement asked for. Redis answers `ERR decrement would overflow`.

Two error texts differed as well: an INCR or HINCRBY overflow said "not an
integer or out of range" where Redis says "increment or decrement would
overflow", and HINCRBY on a field that is not an integer says "hash value is
not an integer" there.

## The fix

- SET and GETEX parse options as Redis's `parseExtendedStringArguments`
  does, then judge the time, all through one function, `string_expiry`,
  which SETEX uses too. GETEX judges the time before it reads the key, as
  Valkey 9.1 does; Redis 8.2 reads the key first, so `GETEX nokey EX 0`
  answers nil there and an error here and in Valkey.
- EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT use checked arithmetic.
- `flint_commands::flush_args_ok` decides a flush's arguments for the seat
  and for the proxy, which refuses a bad one before it fans the flush out.
- `flint_storage::strings::parse_redis_i64` is Redis's `string2ll`. The
  server reads every integer argument with it, and INCR, DECR, INCRBY,
  DECRBY and HINCRBY read stored values with it.

A relative instant whose sum with now overflows,
`SET k v PX 9223372036854775807`, is refused, but no corpus case holds it.
Redis tests that sum after a signed addition, which C leaves undefined on
overflow, so the reference's answer depends on its compiler. Valkey 9.1.0
built on the gate box (Linux) refuses it, as the source means to; Homebrew
builds of Redis 8.2.8 and Valkey 9.1.0 on macOS answer OK, and then
disagree about the key. The first gate of this fix accepted it, measured
against the macOS builds, and the gate's oracle refused it. A server unit
test pins Flint's refusal instead. EXPIRE and PEXPIRE check the sum before
they add, so every build refuses there.

Left as it was: `DBSIZE` and `COMMAND COUNT` still ignore an extra argument
where Redis answers an arity error. Neither changes anything.
