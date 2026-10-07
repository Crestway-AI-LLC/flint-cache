# BUG-0219: list commands checked an argument before the key, and LREM's i64::MIN count removed members (FIXED 2026-10-07)

**Status:** **FIXED 2026-10-07**. Held by the corpus case "the key is read
before a bad index, and i64::MIN is out of range", which Valkey also passes.
**Severity:** low, except LREM: `LREM k -9223372036854775808 v` removed
matches where Redis refuses the count and changes nothing.

## Why

A randomised differential of 54,000 string, hash, set, list and key
commands against Redis 8.2.8 and Valkey 9.1.0 (2026-10-07), after BUG-0212
to BUG-0215:

- **LREM with i64::MIN** removed matches from the tail. Redis reads the
  count in `-LONG_MAX..LONG_MAX` and refuses the rest, as LPOS's RANK
  already did here.
- **LINDEX and LSET read the index before the key.** Redis reads the key
  first: `LINDEX nokey abc` is nil there and was a parse error here, `LSET
  nokey abc v` is `no such key`, and either on another type is WRONGTYPE.
- **INCRBYFLOAT read its increment before the type**, so another type
  answered a parse error rather than WRONGTYPE.
- **INCRBYFLOAT on a stored `nan`** answered "increment would produce NaN
  or Infinity"; Redis does not read `nan` as a float, and answers "value is
  not a valid float".

## The fix

Each is matched. The key lookups run only when the argument fails to parse,
to choose the error Redis would give, so a well-formed command costs
nothing more.

Left as it was: `LSET k 9223372036854775807 v` overwrites the list's last
member in Redis 8.2 and Valkey 9.1 (so does any index from 2^62, and
i64::MIN), and answers `index out of range` here, as any index past the end
does. Theirs looks like an overflow in their own list index; it writes data
on a bogus argument, so it is not copied.
