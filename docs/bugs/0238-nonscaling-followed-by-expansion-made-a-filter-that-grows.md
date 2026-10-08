# BUG-0238: NONSCALING followed by EXPANSION made a filter that grows (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by `nonscaling_holds_against_expansion`
(`flint-server/src/commands.rs`) and the corpus case "NONSCALING holds
against EXPANSION, and EXPANSION 0 is NONSCALING", which RedisBloom 8.2.8
also passes.
**Severity:** medium. A filter the caller capped could grow past its
capacity instead of refusing.

## What happened

Found by the BF.* inventory against RedisBloom 8.2.8 (2026-10-08):

```
BF.INSERT k NONSCALING EXPANSION 2 ITEMS x; BF.INFO k EXPANSION
                         -> Flint: [2]      RedisBloom: [nil]
BF.RESERVE k 0.01 100 NONSCALING EXPANSION 2
                         -> Flint: OK (scaling)
                            RedisBloom: Nonscaling filters cannot expand
BF.RESERVE k 0.01 100 EXPANSION 0
                         -> Flint: ERR bad expansion   RedisBloom: OK (NONSCALING)
```

Both parsers kept one `expansion` variable and wrote 0 into it for
NONSCALING, so a later EXPANSION overwrote it. RedisBloom keeps NONSCALING
as a flag: BF.INSERT lets it win whatever the order, and BF.RESERVE refuses
a growth factor beside it. Its `EXPANSION 0` means NONSCALING; Flint
refused it.

## The fix

NONSCALING is a flag in both parsers. BF.INSERT makes a NONSCALING filter
whichever side of EXPANSION it is written on; BF.RESERVE refuses the pair
with RedisBloom's words. `EXPANSION 0` makes a NONSCALING filter in both.
