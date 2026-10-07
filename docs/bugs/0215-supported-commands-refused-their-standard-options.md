# BUG-0215: supported commands refused their standard options (FIXED 2026-10-07)

**Status:** **FIXED 2026-10-07**. Held by the corpus cases "lpop and rpop
take a count", "zadd takes nx xx gt lt ch and incr", "zrange takes byscore
bylex rev and limit" and "zrank and zrevrank take withscore", which Valkey
also passes under both protocols.
**Severity:** medium. command-support.md listed each command as supported
with no caveat, and a client that sent one of these options got an error:
a queue worker taking a batch with `LPOP key 10`, a leaderboard keeping a
best score with `ZADD key GT`, a rate limiter adding only new entries with
`ZADD key NX`. No data was touched, because each was refused outright.

## Why

The three-way differential of BUG-0212 (Flint, Redis 8.2.8, Valkey 9.1.0):
- **`LPOP key count` and `RPOP key count`** (Redis 6.2) were arity errors,
  and RPOP's error named LPOP.
- **ZADD's flags**, NX, XX, GT, LT, CH and INCR, were read as scores, so
  `ZADD k NX 1 a` answered "value is not a valid float".
- **ZRANGE's Redis 6.2 form**, BYSCORE, BYLEX, REV and LIMIT, was a syntax
  error; only WITHSCORES was taken.
- **ZRANK and ZREVRANK with WITHSCORE** (Redis 7.2) were arity errors.

Found while writing the corpus cases for these, and fixed with them:
- **A negative LIMIT offset** in ZRANGEBYSCORE and ZRANGEBYLEX was read as
  0, so `LIMIT -1 2` answered the first two members. Redis and Valkey answer
  none.
- **The lex walk on a set with mixed scores** started at the first member
  inside both bounds. Redis first answers empty if the last member, in
  score order, is below the lower bound or the first is above the upper
  one. Then it seeks the first member at or past the lower bound and stops
  at once if that member is past the upper one. command-support.md promised
  Redis's walk; `ZRANGEBYLEX k [b (e` on members `a g b c d e f`, scored in
  that order, answered `b c d` here and nothing there. ZLEXCOUNT and
  ZREMRANGEBYLEX share the walk.

Found by a randomised differential of 40,000 sorted-set and list commands
(five seeds, both protocols) once the above passed, and fixed with them:
- **A score bound read as Redis 8.2 reads one.** An empty number, `""` or a
  bare `(`, is 0 there, and NaN or a number with spaces around it is not a
  float. This trimmed spaces and took `nan` as a bound nothing satisfies.
  ZRANGEBYSCORE, ZCOUNT and ZREMRANGEBYSCORE share the parser.
- **`ZRANGE k 0 -1 LIMIT 1 -1`** is accepted by Redis, which tells LIMIT
  from its absence by a count other than -1, and the rank range ignores it.
  The first build of the new ZRANGE refused it.

After these, the same 40,000 commands matched both servers exactly.

## The fix

- `ListStore::pop_n` takes up to `count` elements under one metadata write.
  A missing key answers a null array, an existing one an array, empty for a
  count of 0. A pop with a count is sized for BUG-0060's admission as the
  LRANGE of the slice it takes.
- `ZSetStore::zadd_with` applies the pairs in order, as Redis's `zsetAdd`
  does, so `GT 5 a 3 a` leaves 5 and `CH 1 a 2 a` on a new member counts
  2. It decides every outcome before it writes, so a NaN from INCR or a
  max-value-bytes refusal changes nothing. Plain ZADD takes the same path.
- `cmd_zrange` parses as Redis's `zrangeGenericCommand` does, options first
  and then the range in the kind they chose, and serves it from the stores
  ZRANGEBYSCORE and ZRANGEBYLEX already use.
- ZRANK WITHSCORE answers `[rank, score]`, and a missing member a null
  array.
- `flint_resp::null_is_array` names the commands whose null is an array.
  The proxy reads seats in RESP3, where there is one null, and used to send
  a RESP2 client `$-1` for these; it now sends `*-1`, as it already did for
  EXEC and a queued BLPOP. A transaction's queued commands use the same
  list.
- The lex walk follows Redis's listpack walk (`zzlFirstInLexRange`,
  `zzlLastInLexRange`). Past 128 members Redis keeps a skiplist, whose lex
  seek depends on its random levels when scores are mixed, so no
  implementation can match it there. Mixed-score lex ranges are undefined
  in Redis's own documentation.
