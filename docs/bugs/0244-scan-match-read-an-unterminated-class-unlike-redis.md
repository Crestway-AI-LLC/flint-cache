# BUG-0244: `SCAN … MATCH` and `KEYS` read a pattern with an unterminated `[` unlike Redis (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. The keyspace and collection scans match
with `crate::glob`, the matcher pub/sub's pattern subscriptions use, which
agrees with Valkey 9.1 on 2.4 million randomly drawn cases. The scans had
their own copy, which disagreed on about 0.4% of them.
**Severity:** low. Only a pattern holding a `[` with no closing `]` is
affected, and such a pattern is almost always a typo. But a `KEYS` or
`SCAN` that silently returns fewer keys than Redis does is the kind of
difference nobody would ever trace back to the pattern.

## What happened

Building pattern subscriptions (ADR-0052 D5) needed Redis's glob. The parked
pub/sub work carried its own matcher, and `commands.rs` already had one for
`SCAN`. Two definitions of one function is how one of them goes wrong, so
both were judged against Valkey. Two seeds were used, each 4,000 random
patterns against 300 random keys over an alphabet of glob metacharacters
(`[`, `]`, `^`, `-`, `\`, `*`, `?`) and a few letters. Valkey's `KEYS`
decided each case. The pub/sub matcher agreed with Valkey on every case. The
`SCAN` matcher was wrong on 4,796 and then 5,506 of 1,196,000.

Every miss had a `[` with no `]` after it. Redis reads such a class to the
end of the pattern: `?[b` is "any byte, then a class holding `b`", so it
matches `ab`. The `SCAN` matcher treated an unterminated class as matching
nothing, so `KEYS ?[b` returned no key where Redis returns `ab`.

## The fix

One matcher, `crates/flint-server/src/glob.rs`, used by `SCAN`, `KEYS`
(which the proxy answers from `SCAN`), `HSCAN`, `SSCAN`, `ZSCAN`,
`PSUBSCRIBE` and `PUBSUB CHANNELS`. Its tests are Redis's own
`stringmatchlen` cases plus the fuzz's misses. The comparison harness is not
in the repository: it is a 1.2-million-line table from a live Valkey, and
its result is what the tests pin.
