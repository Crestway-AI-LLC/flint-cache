# BUG-0228: a sorted-set member moved between `-0` and `0` kept a stale index row (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by two unit tests in
`flint-storage/src/zsets.rs` (one plants rows as a build before the fix
wrote them) and the corpus case "a score of -0 is the score 0", which
Valkey 9.1 also passes.
**Severity:** high for the sets it touches. A member whose score passed
between `-0` and `0` was listed twice, ZRANGE then answered a stale score,
and a later `ZADD` read back as never applied. Only a score of `-0` reaches
it: a client must send `-0`, or ZINCRBY or ZUNIONSTORE/ZINTERSTORE's
WEIGHTS must compute it.

## What happened

Found by a new sorted-set fuzzer in the three-way differential (2026-10-08),
where Redis 8.2.8 and Valkey 9.1.0 agree:

```
ZADD k -0 m      (1)
ZADD k 0 m       (0)
ZADD k 1 m       (0)
ZRANGE k 0 -1 WITHSCORES   -> Flint: m 0      Redis: m 1
ZSCORE k m                 -> 1 on both
```

A sorted set keeps two rows per member: the member's score, and an index
row keyed by `(encode_score(score), member)` that the range commands walk.
`zadd` deleted the old index row only when `old != new` **as doubles**,
and `-0 == 0`, but `encode_score` keeps their bits, so the two zeros are
different index rows. Moving `m` from `-0` to `0` wrote the `0` row and
kept the `-0` row. From then on the set had more index rows than members:
ZRANGE, which stops at ZCARD, read the stale row first, and moving `m` to 1
deleted the `0` row and left the `-0` one.

Two smaller faults came from the same encoding:

- `-0` sorted below every `0`. Redis compares scores as doubles, so the two
  tie and the member breaks the tie: `ZADD k 0 b -0 g` ranges `b g`, not
  `g b`.
- A reverse walk bounded by `-0` started below the `0` rows, so
  `ZREVRANGEBYSCORE k -0 -inf` left out every member scored `0`.

## The fix

- **A score of `-0` is stored as `0`.** That is what Redis's listpack does:
  it stores an integral score as an integer, so its ZSCORE answers `0`.
  (Its skiplist, past 128 members, keeps `-0`, and only the spelling of
  ZSCORE differs.)
- **The index row is compared by bits** in `zadd` and `zadd_with`, as it is
  keyed. A set a build before the fix wrote may hold `-0`, and moving such
  a member now deletes its `-0` row.
- **A zero bound seeks across both zero encodings**: a forward walk from
  below `-0`, a reverse walk from above `0`. Rows written before the fix are
  reached by a `0` bound, and members scored `0` by a `-0` bound.

Nothing rewrites stored data. A set written before the fix keeps any `-0`
until that member's score changes, and until then it still sorts below the
`0` members.
