# BUG-0107 — the retry-safety page omitted 37 writes, including every pop but `LPOP` (FIXED 2026-09-06)

**Status: FIXED 2026-09-06**, and held by
`assert_every_write_command_is_retry_classified` in `tools/gates.sh`. Found
2026-09-06 by continuing the audit that produced BUG-0103 into the next
customer-facing doc with no mechanical check · Severity: **high for a
document** — this page is what a client reads to decide whether re-sending a
command after an ambiguous failure is safe, and the omissions were the
data-losing ones.

## Symptom

`docs/retry-safety.md` presents two lists: safe to retry, and not safe. It
classified **25 of the 62 commands** `flint_commands::is_write_command` calls
writes. Nothing said the lists were partial, and nothing checked them.

The page explicitly warns that `LPOP`/`RPOP` "pops an EXTRA element — silent
data loss on retry". It did not mention `SPOP`, `ZPOPMIN`, `ZPOPMAX`,
`LTRIM`, `ZREMRANGEBYRANK`, or counted `LREM` — all of which do exactly that.

A reader looking up `SPOP`, seeing `LPOP` flagged two lines away and `SPOP`
absent, draws the reasonable and wrong conclusion.

## Measured, not reasoned about

Every classification added here was run against a live seat rather than
derived from Redis semantics, and two of them came out the opposite of the
obvious reading:

    SADD s a b c ; SPOP s -> "c" ; SPOP s -> "b"     # 1 of 3 members left
    ZADD z 1 a 2 b ; ZPOPMIN -> a ; ZPOPMIN -> b     # both gone
    RPUSH l a b c d ; LTRIM l 1 2 -> "b c" ; again -> "c"
    ZADD Z 1 a 2 b 3 c ; ZREMRANGEBYRANK Z 0 0 twice -> "c"
    RPUSH L a b a ; LREM L 1 a twice -> "b"          # both a gone

**`LTRIM` and `ZREMRANGEBYRANK` are not idempotent**, which is not what they
look like — they read as absolute-range deletes. They are position-addressed,
so the second delivery addresses a different set. Had these been classified
from first principles rather than run, both would have landed in the safe
list, which is worse than leaving them out.

The value-addressed twins were run too, and do converge:
`ZREMRANGEBYSCORE`, `ZREMRANGEBYLEX`, `LREM key 0 m` (count 0 = all
occurrences), `LSET`, `SETRANGE`.

## The rule that fell out of it

The page now leads with the principle, because a table is always one command
behind the server:

> A retry is safe when the command names WHAT to change. It is unsafe when
> the command names WHERE, HOW MANY, or HOW MUCH.

That covers the pops, the trims, the counted `LREM`, and every `INCR`-shaped
command with one sentence, and it lets a reader classify a command the table
does not list.

## A second hazard shape the page had no name for

Five commands land the write and then report failure on the retry:
`SETNX`/`HSETNX` (0), `COPY` without `REPLACE` (0), `GETDEL` (nil),
`RENAME`/`RENAMENX` (`ERR no such key`), `BF.RESERVE` (`ERR item exists`).
The page documented this only for `SET … NX`, as "the classic lock hazard".
It is a general shape and is now named as one: **the write succeeded and the
caller is told it did not**, which is worse than a visible error because both
natural responses — retry again, or fall back — are wrong.

## Fix

The lists are complete, and `assert_every_write_command_is_retry_classified`
keeps them so: it reads `is_write_command` — the classifier the proxy and the
server already share, rather than a second list that would itself drift — and
fails a build where a write has no mention on the page.

**It establishes presence, not correctness.** A command filed in the wrong
table still passes. That limit is stated in the failure text and in the page,
because a check that overstates what it proves gets trusted for the thing it
does not do. It stops the page falling behind the server, which is how it got
37 behind.

Both failure modes were confirmed by planting them: a removed row names the
command, and renaming `is_write_command` away fails with "read no commands at
all" rather than reporting agreement between two empty sets.
