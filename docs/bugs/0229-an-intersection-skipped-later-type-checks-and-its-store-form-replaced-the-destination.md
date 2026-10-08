# BUG-0229: an intersection skipped its later inputs' type checks, and its STORE form replaced the destination (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by a unit test in
`flint-storage/src/sets.rs`, a server test in `commands.rs`, and the corpus
case "an intersection checks every input's type before it answers", which
Valkey 9.1 also passes.
**Severity:** medium. A command Redis refuses deleted the destination's
data. It needs a WRONGTYPE input after an empty one.

## What happened

Found by the sorted-set fuzzer (2026-10-08), where Redis 8.2.8 and Valkey
9.1.0 agree:

```
ZADD {z}d 1 a; SADD {z}s x; SET {z}str v
ZINTERSTORE {z}d 3 {z}missing {z}s {z}str  -> Flint: 0      Redis: WRONGTYPE
ZCARD {z}d                                 -> Flint: 0      Redis: 1
```

An intersection or difference cannot grow back once it is empty, so
`cmd_zstore` and the set store's `sop` stopped reading inputs there. The
inputs after that point were never type-checked, so a string among them was
answered as the empty result. The STORE forms then wrote that result:
ZINTERSTORE and SINTERSTORE deleted the destination where upstream refuses
and leaves it alone. SINTER and SDIFF answered an empty set.

Upstream checks every input's type before it computes anything, and for
ZUNIONSTORE and ZINTERSTORE before it parses WEIGHTS and AGGREGATE. So
`ZUNIONSTORE d 1 astring BOGUS` is WRONGTYPE upstream; here it was a syntax
error.

## The fix

One type pass over every input, before any read: one metadata read per key.
The early stop stays, since it is what keeps an empty intersection cheap.
ZUNIONSTORE and ZINTERSTORE now check CROSSSLOT, then the types, then the
options. That is Redis Cluster's order: a cluster refuses CROSSSLOT before
the command runs. One consequence: a `numkeys` larger than the keys given,
which reads `WEIGHTS` as a key, is CROSSSLOT here, as in a cluster. A
standalone Redis answers a syntax error.
