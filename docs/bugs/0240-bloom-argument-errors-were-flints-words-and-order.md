# BUG-0240: BF.RESERVE and BF.INSERT refused arguments in Flint's words and order (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by
`bloom_arguments_are_refused_in_redisblooms_words_and_order`
(`flint-server/src/commands.rs`) and the corpus case "BF arguments are
refused in RedisBloom's words, before the key", which RedisBloom 8.2.8 also
passes.
**Severity:** low to medium. Mostly wording, but BF.INSERT also wrote
items where RedisBloom refuses the request.

## What happened

The BF.* inventory against RedisBloom 8.2.8 (2026-10-08) found 34 groups of
refusals that differed, for example:

```
BF.RESERVE f 0 100 (f exists)       -> Flint: ERR item exists
                                       RedisBloom: ERR error rate must be in the range (0.000000, 1.000000)
BF.RESERVE k 0.01 -1                -> Flint: ERR bad capacity
                                       RedisBloom: ERR capacity must be in the range [1, 1073741824]
BF.RESERVE k 0.01 100 EXPANSION     -> Flint: ERR bad expansion   RedisBloom: ERR no expansion
BF.INSERT f CAPACITY 0 ITEMS x      -> Flint: [1] (ignored, added)   RedisBloom: Bad capacity
BF.INSERT k WAT ITEMS x             -> Flint: ERR syntax error   RedisBloom: Unknown argument received
BF.INSERT k CAPACITY 10             -> Flint: ERR syntax error   RedisBloom: ERR wrong number of arguments ...
```

RedisBloom reads and bounds every argument before it opens the key. Flint
looked at the key first, bounded capacity and error rate only when a filter
was created (so BF.INSERT added items under a CAPACITY of 0), parsed
capacity as unsigned (`-1` was a parse failure, `+5` was accepted), and had
its own words. A capacity above 2^30 was taken here up to the value-size
cap; RedisBloom refuses it.

## The fix

Both commands read, bound and refuse every argument in RedisBloom's order
and words before the key, with Redis's integer parsing. BF.RESERVE answers
arity for more than seven arguments, as RedisBloom does. Kept different:
an unknown BF.RESERVE word is still refused (ADR-0016 D7.4), BF.INSERT's
option words are spelled out where RedisBloom takes their first letters,
and an EXPANSION above 255 is refused when it would make a filter, since a
growth factor is kept in one byte here.
