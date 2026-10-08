# BUG-0231: ZINTERSTORE zeroed a later input's NaN where upstream aggregates it (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by a server test in `commands.rs` and
the corpus case "a later intersection input's nan goes into the
aggregate", which Valkey 9.1 also passes.
**Severity:** low. The stored score differed from Redis's, and only for
a weight of 0 against an infinite score.

## What happened

Found by the sorted-set fuzzer's state check (2026-10-08), where Redis 8.2.8
and Valkey 9.1.0 agree:

```
ZADD {n}a 5 m; ZADD {n}b inf m
ZINTERSTORE {n}d 2 {n}a {n}b WEIGHTS 1 0                 -> m: Flint 5, Redis 0
ZINTERSTORE {n}d 2 {n}a {n}b WEIGHTS 1 0 AGGREGATE MIN   -> m: Flint 0, Redis 5
```

A weight of 0 times an infinite score is NaN. `cmd_zstore` turned every
weighted NaN into 0 before it aggregated. Upstream does that for every
input of a union, but for an intersection's first input only. A later
input's value goes into `zunionInterAggregate` as NaN. There SUM's
`target + NaN` is NaN and becomes 0, and MIN's `val < *target ? val :
*target` keeps the score so far, as MAX does.

## The fix

A later intersection input's NaN goes into the aggregate as it is.
`f64::min` and `max` keep the score so far against a NaN, as upstream's
comparisons do, and SUM still turns a NaN into 0. The union, and the
intersection's first input, are unchanged.
