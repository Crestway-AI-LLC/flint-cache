# BUG-0237: a Bloom batch hid a partial write behind one error (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by
`a_batch_that_fills_a_filter_answers_each_item_up_to_the_refusal`
(`flint-storage/src/bloom.rs`),
`a_bloom_batch_answers_each_item_up_to_a_full_filter`
(`flint-server/src/commands.rs`) and the corpus case "a batch that fills a
filter answers each item, the error in its place", which RedisBloom 8.2.8
also passes.
**Severity:** medium. Data was written that the reply said was not.

## What happened

Found by an inventory of BF.* replies against RedisBloom 8.2.8, built from
source on this Mac (2026-10-08):

```
BF.RESERVE f 0.001 2 NONSCALING; BF.ADD f a
BF.MADD f a b c d   -> Flint: (error) ERR non scaling filter is full
                       RedisBloom: [0, 1, (error) ERR non scaling filter is full]
BF.EXISTS f b       -> 1 on both: b was stored
```

`BloomStore::madd` added the items one at a time and collected into a
`Result<Vec<bool>>`, so the first refusal became the whole reply. The items
before it had already been written, and the caller, handed one error,
believed none had. BF.INSERT went through the same function.

## The fix

`madd` answers each item, up to and including the first refusal, and tries
nothing after it, as RedisBloom's `bfInsertCommon` does. The server puts
the refusal in its place in the array. A key of another type still refuses
the whole call, bare, before any item. A single BF.ADD has no array, so its
refusal is the reply, as before.
