# BUG-0212: ZINCRBY could store a NaN score (FIXED 2026-10-07)

**Status:** **FIXED 2026-10-07**. Held by the corpus case "zincrby refuses a
nan score", which Valkey also passes, and by the storage test
`zincr_by_refuses_a_nan_score_and_changes_nothing`.
**Severity:** medium. A sorted set kept a member it cannot order. No other
key is touched, and no client gets a wrong answer for any other member.

## Why

`ZINCRBY key -inf m` on a member scored `+inf` adds the two infinities, and
the sum is NaN. Redis 8.2 and Valkey answer
`ERR resulting score is not a number (NaN)` and change nothing. Flint stored
the NaN and answered `NaN`. Measured on the build before the fix, with
members `a` at 1 and `b` at 2 beside it: ZCARD answers 3 and ZRANGE by rank
lists `m NaN` last, but `ZRANGEBYSCORE -inf +inf` answers `a b` and
`ZCOUNT -inf +inf` answers 2. No score range holds the member, and it reads
back as `NaN`, a spelling Redis never sends.

ZADD could not reach it: it refuses a NaN score as an argument, and
ZUNIONSTORE and ZINTERSTORE already turned a NaN sum into 0, as upstream
does. ZINCRBY's sum was the one path left.

Found 2026-10-07 by a differential of about 650 core commands against Flint,
Redis 8.2.8 and Valkey 9.1.0, each run under RESP2 and RESP3. BUG-0213 and
BUG-0214 come from the same run.

## The fix

`ZSetStore::zincr_by` refuses a NaN sum with `StoreError::NanScore` before
it writes, and the server answers Redis's error.
