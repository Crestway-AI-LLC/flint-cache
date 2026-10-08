# BUG-0233: INCRBYFLOAT and HINCRBYFLOAT stored `-0` where Redis stores `0` (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by a step in
`incr_by_float_shapes_and_errors` (`flint-storage/src/strings.rs`) and the
corpus case "incrbyfloat writes a negative zero as 0", which Valkey 9.1
also passes.
**Severity:** low. The stored text differed from Redis's. Both read back
as zero, but GET answered `-0`.

## What happened

Found while checking how far BUG-0230's `fmt_double` change reached
(2026-10-08), where Redis 8.2.8 and Valkey 9.1.0 agree:

```
INCRBYFLOAT k -0.000000000000000001   -> Flint: -0   Redis: 0
GET k                                 -> Flint: -0   Redis: 0
HSET h f -0; HINCRBYFLOAT h f -0      -> Flint: -0   Redis: 0
```

Both commands write the result in Redis's LD_STR_HUMAN shape: `%.17f`, then
trailing zeros and a bare dot trimmed. A negative zero, or a negative too
small to show in 17 places, prints as `-0.00000000000000000` and trims to
`-0`. Redis's `ld2string` then rewrites `-0` as `0`. `fmt_float_human` did
not. It does not use `fmt_double`, so this predates BUG-0230.

## The fix

`fmt_float_human` writes `-0` as `0`, as `ld2string` does. A negative large
enough to show, `-0.00000000000000001`, keeps its sign, as upstream's does.
