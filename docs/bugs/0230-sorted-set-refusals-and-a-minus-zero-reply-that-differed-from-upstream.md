# BUG-0230: sorted-set refusals, and a `-0` reply, that differed from upstream (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by the corpus case "sorted-set
refusals in upstream's order, and a -0 reply", which Valkey 9.1 also
passes, a `fmt_double` unit test, and a step in the BUG-0228 unit test.
**Severity:** low. Each is a refusal worded differently or made in a
different order, or the spelling of a zero; none changed data.

## What differed

Found by the sorted-set fuzzer (2026-10-08), where Redis 8.2.8 and Valkey
9.1.0 agree:

- **ZRANGEBYSCORE and ZRANGEBYLEX** (and their REV forms) read their bounds
  before their options. Upstream's `zrangeGenericCommand` reads the options
  first, so `ZRANGEBYSCORE k x 1 LIMIT a 1` is "value is not an integer or
  out of range" and `ZRANGEBYSCORE k x 1 BOGUS` a syntax error, where both
  were "min or max is not a float". ZRANGE itself already read its options
  first.
- **ZPOPMIN and ZPOPMAX** answered a count that is not an integer with
  "value is not an integer or out of range". Upstream reads the count with
  `getPositiveLongFromObjectOrReply`, which words it as it words a negative:
  "value is out of range, must be positive".
- **`ZINCRBY k -0 new` and `ZADD k INCR -0 new`** answered `0`. Upstream
  answers `-0`: a new member's score is the increment itself, and
  `d2string` spells the sign of a zero. `fmt_double` printed every zero as
  `0`, and ZINCRBY added the increment to an implied 0.

## The fix

Each is upstream's order and words. A new member's INCR now answers the
increment as given, `-0` included, and `fmt_double` spells `-0`. The member
is still stored as `0` (BUG-0228), so its ZSCORE answers `0`, as Redis's
listpack does.
