# BUG-0250: ZUNIONSTORE and ZINTERSTORE combined their inputs in the order they were named (FIXED 2026-10-09)

**Status:** **FIXED 2026-10-09**. The sorted-set algebra combines its inputs
smallest first, as upstream does, with each weight kept with its input.
Found by the random differential written for ZUNION and its relatives,
which compares every reply and every key against Valkey 9.1 and Redis 8.2.
Held by the conformance case "ZUNION, ZINTER, ZDIFF, ZDIFFSTORE, ZINTERCARD,
ZRANGESTORE" and by a seat test.
**Severity:** low. Only scores where a positive and a negative infinity
meet were affected: the stored or answered score could differ from Redis's.

## What happened

Upstream sorts the inputs of ZUNIONSTORE, ZINTERSTORE (and, since this
change, ZUNION, ZINTER and ZINTERCARD) by cardinality before combining
them, smallest first (`zuiCompareByCardinality`). Flint combined them in
the order they were named.

Adding scores is otherwise order-free, but not where infinities meet.
`+inf + -inf` is not a number, which upstream, and Flint (BUG-0231), turn
into 0, so the order decides the result:

    ZADD a -inf m 1 x 2 y          (three members)
    ZADD b inf m                   (one member)
    ZUNIONSTORE d 3 a b b
    ZSCORE d m   -> Redis "0"   (b, b, a: inf + inf = inf, then + -inf = 0)
                    Flint "inf" (a, b, b: -inf + inf = 0, then + inf = inf)

The order also decides which input is the intersection's first, the one
whose not-a-number product BUG-0231 found upstream turning into 0.

## Fixed

The inputs' cardinalities are read from their metadata, one read each, and
the inputs are combined in ascending order. A sort that keeps ties in their
named order is used. A difference (ZDIFF, ZDIFFSTORE) keeps its first input
first, as upstream does: its result does not depend on the order of the
rest.
