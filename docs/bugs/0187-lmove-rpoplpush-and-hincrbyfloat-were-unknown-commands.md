# BUG-0187: LMOVE, RPOPLPUSH and HINCRBYFLOAT were unknown commands, and every job queue measured moves jobs with one of them (FIXED 2026-09-26)

**Status:** **FIXED 2026-09-26**. Not in v0.1.0-rc.77: it ships with the next
release.
**Severity:** medium: whole client libraries failed at their core operation,
loudly, with `ERR unknown command`.

## What was measured

Job-queue libraries on a gate box, 2026-09-26, each run first against a
plain Valkey with `MONITOR` recording what it sent (script calls included),
then through the proxy:

- **asynq 0.26** and **BullMQ 6.3.9** sent `RPOPLPUSH`, the move from a
  waiting list to an active one.
- **rq 2.8.0** sent `LMOVE` and `HINCRBYFLOAT`.

None of the three is blocking or needs pub/sub, so each is an ordinary gap.
The same libraries also need commands Flint does not serve by design
(`BRPOP`, `BZPOPMIN`, `SUBSCRIBE`, streams); those are ADR-0052's.

## The fix

- `LMOVE src dst LEFT|RIGHT LEFT|RIGHT` and `RPOPLPUSH src dst` (its
  `RIGHT LEFT` form) move one element and answer it, or nil for an empty
  source. The keys must share a slot: a cross-slot move is refused with
  `CROSSSLOT`, as the set operations are. Both types are checked before
  anything moves, so a destination of another type is refused with the
  source untouched. When source and destination are the same key the move
  is a rotation.
- `HINCRBYFLOAT key field increment` adds to a field and answers the new
  value, in Redis's float shape, as `INCRBYFLOAT` does. It keeps Valkey's
  three distinct errors: `hash value is not a float` for the stored value,
  `value is not a valid float` for the argument, and `value is NaN or
  Infinity` for an infinite argument, which it refuses before looking at the
  field.

Measuring the edges against Valkey found one defect shared by every float
argument. Rust's parser rounds a spelling past a double's range to infinity
(`1e400`) or to zero (`1e-400`), where `strtod` reports ERANGE and Valkey
answers `value is not a valid float`. `ZADD z 1e400 m` stored the member at
`inf`. The shared parser now refuses both, and a spelled-out `inf` or zero
still parses.

`LMOVE` and `RPOPLPUSH` also write their destination, so they take the lock
that excludes every writer (BUG-0188) and the proxy's local cache drops both
keys, as it does for `RENAME`.

## What remains different

Valkey's `strtod` and `strtold` accept a hexadecimal float (`0x10` is 16).
Flint refuses one, as it did before, for every float argument. No client we
have measured sends one.

`INCRBYFLOAT` and `HINCRBYFLOAT` compute in a double. Valkey computes them
in a `long double`, which is a double on arm64 and wider on x86, so results
past 17 significant digits can differ from an x86 Valkey in the last digit.
This was already true of `INCRBYFLOAT`.

## Coverage

- Conformance cases for all three commands and for out-of-range scores, in
  both protocols, against the Valkey oracle. The float cases use exactly
  representable values so that the oracle's platform does not decide the
  answer.
- Unit tests: a cross-slot move refused with nothing moved, and the parser's
  range rule.
