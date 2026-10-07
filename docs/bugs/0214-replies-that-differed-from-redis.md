# BUG-0214: TTL, GETRANGE and sorted-set scores answered differently from Redis (FIXED 2026-10-07)

**Status:** **FIXED 2026-10-07**. Held by the corpus cases "ttl rounds to
the nearest second", "getrange clamps an end before the string" and "scores
are spelled as redis spells them", which Valkey also passes, and by
flint-resp's `fmt_double` tests.
**Severity:** low. No data is touched. A client read a TTL one second long,
an empty string where Redis gives one byte, or a score spelled differently
but equal in value.

## Why

The same differential as BUG-0212:
- **TTL rounded up.** It answered `ceil(ms / 1000)`. Redis answers
  `(ms + 500) / 1000`, the nearest second, so a key with 1.4 s left is 1
  there and was 2 here. PTTL was right.
- **GETRANGE treated an end before the string as LRANGE does.**
  `GETRANGE k 0 -100` on "Hello" answered empty. Redis clamps a negative end
  to the first byte, after one check of its own: two negative indexes in
  the wrong order answer empty. So it answers "H".
- **Scores were spelled out in full.** `1e20` read back as
  `100000000000000000000`, and `5e-324` as 326 characters. Redis's
  `d2string` prints an integral value within ±2^62 as an integer, and
  anything else with `fpconv_dtoa`, which switches to `1e+20` or `1e-7`
  past a small exponent. Every reply carrying a score was affected: ZSCORE,
  ZINCRBY, ZMSCORE, ZRANGE and ZPOPMIN with scores, ZSCAN, and JSON.RESP's
  doubles.

The claim row also named a timed-out BRPOPLPUSH, which answers a nil bulk at
a seat and a nil array from Redis under RESP2. That is by design and is
unchanged: a seat answers a blocking move as Redis does inside MULTI, which
is the nil bulk, and the proxy, which does the waiting (ADR-0052 D4),
answers a timeout with the nil array.

## The fix

TTL rounds to the nearest second. `StringStore::getrange` follows Redis's
rules. `flint_resp::fmt_double` lays digits out as `fpconv_dtoa` does.

Its digits are the shortest that round-trip, and Redis's come from Grisu2,
which does not always pick those. Measured 2026-10-07 by ZADD and ZSCORE of
4,999 random doubles on all three servers: 13 read back differently from
Redis 8.2 here, each with 16 or 17 significant digits
(`-144363141721108.88` here, `...87` there), and each spelling named the
same double. Valkey answered as Redis did. Timestamps, prices and other
scores of 15 significant digits or fewer matched exactly.
