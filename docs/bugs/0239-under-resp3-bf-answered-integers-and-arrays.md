# BUG-0239: under RESP3, BF.* answered integers and arrays where RedisBloom answers booleans and maps (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by `bloom_speaks_redisbloom`
(`flint-server/src/commands.rs`, wire bytes in both dialects), the boolean
round trip in `resp3_frames_decode_back_to_their_meaning` (`flint-resp`),
`a_seats_resp3_bloom_replies_reach_each_client_in_its_own_shape`
(`flint-proxy`), and the corpus run under `--proto 3` on the seat and
through the proxy.
**Severity:** medium. A RESP3 client library hands the caller a different
type: `1` where RedisBloom gives `True`, a list where it gives a dict.

## What happened

Found by the BF.* inventory against RedisBloom 8.2.8 under RESP3
(2026-10-08):

```
BF.ADD k x            -> Flint: :1          RedisBloom: #t
BF.MEXISTS k x y      -> Flint: *2 :1 :0    RedisBloom: *2 #t #f
BF.INFO k             -> Flint: *10 ...     RedisBloom: %5 ...
BF.INFO k CAPACITY    -> Flint: *1 :100     RedisBloom: %1 +Capacity :100
```

Flint had no boolean reply at all: `flint_resp::Value` had no variant for
one, and the decoder read `#t` as the integer 1. BF.INFO built the RESP2
array directly.

## The fix

`Value::Boolean`, sent as `#t`/`#f` under RESP3 and `:1`/`:0` under RESP2,
as Redis downgrades one, and decoded as itself so the proxy, which reads
seats in RESP3, can send each client its own spelling. BF.ADD, BF.EXISTS,
BF.MADD, BF.MEXISTS and BF.INSERT answer booleans. BF.INFO answers a map,
which RESP2 flattens to the same ten elements as before. A one-field
BF.INFO carries both shapes (`*1 :100` and `%1 +Capacity :100`), because
flattening the map would give two elements; the proxy and the corpus
runner rebuild the RESP2 one from the RESP3 map with
`flint_resp::bf_info_field_resp2`, as BUG-0235 taught for NUMINCRBY.
Scripts see RESP2, so a boolean is 1 or 0 there.
