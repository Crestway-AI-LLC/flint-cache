# BUG-0208: JSON.NUMINCRBY stored a wrong integer without an error (FIXED 2026-10-06)

**Status:** **FIXED 2026-10-06**, with ADR-0054. Held by the unit test
`json_numincrby_keeps_integers_exact` and the corpus case "NUMINCRBY on
integers stays exact past 2^53 (BUG-0208)".
**Severity:** medium. It corrupts a stored value silently, but only for
integers past 2^53, increments past i64, or an overflow. Counters and ids
that large do occur: snowflake-style ids exceed 2^53.

## Why

The increment was done in f64 and the result cast back with `as i64`
whenever both sides were whole. Measured on `e24f98a`:

| document | increment | stored | right answer |
|---|---|---|---|
| `9007199254740993` | `0` | `9007199254740992` | unchanged |
| `1` | `1e19` | `9223372036854775807` | `1e19`, a float |
| `9223372036854775807` | `1` | `9223372036854775807` | an error |

The first row is the worst: a no-op increment changed the value. An f64
holds integers exactly only up to 2^53. `as i64` saturates instead of
failing, so the other two rows stored i64::MAX.

Increments written as floats were also handled differently from RedisJSON.
`2.0` added to an integer gave the integer `3`. RedisJSON gives the float
`3.0`, because it decides integer arithmetic from how the increment is
written.

## The fix

`json_add` adds in i64 when the stored number is an integer and the
increment is written as one, and refuses an overflow (`ERR increment or
decrement would overflow`), as Redis's INCRBY does. Anything else is f64,
which already refused a non-finite result. A refusal stores nothing. With
ADR-0054 a multi-match increment fails as a whole, so no match before the
refused one keeps its change either.

## Where we differ from RedisJSON

RedisJSON v8.2.8 wraps the overflow: `9223372036854775807` + 1 answers and
stores `-9223372036854775808`. We refuse it. The corpus case "NUMINCRBY
refuses an integer overflow (RedisJSON wraps)" is a listed divergence in
`tools/redisjson_compare.sh`.

## Not covered

- An increment past 2^53 written as an integer but stored as a float
  (`1.5` + `9007199254740993`) is f64 arithmetic, as in RedisJSON.
- An integer stored above i64::MAX (a u64) is incremented as a float, as
  before.
