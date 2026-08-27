# BUG-0066 — a declared config key that nothing reads

**Component:** `s3-accelerator`, S3A adoption path 1 (`FlintStreamFactory`)
**Found:** 2026-08-27, while correcting a stale number on the website.

## Symptom

None. That is the entire problem.

`fs.s3a.flint.max.object.bytes` was a `public static final String` constant on
`FlintStreamFactory`, listed in the README's configuration table as a setting
for "S3A (paths 1, 2)", and printed by the preflight script. Setting it did
nothing. Three further keys the README listed for this path —
`fs.s3a.flint.max.part.bytes`, `fs.s3a.flint.immutable`, and
`fs.s3a.flint.meta.ttl.immutable.seconds` — had no constant at all.

An ignored setting behaves exactly like a setting left at its default, so a
customer who set one saw the default behaviour and no error, and every test
passed because every test used defaults.

## The wrong conclusion drawn first

That this was a documentation error — the README claiming support that had
never been intended — and that the fix was to strike the rows. It reads that
way: the constant is declared but unused, which looks like a leftover.

It is the opposite. Path 2 (`FlintS3AFileSystem`) supports all four, because it
maps `flint.*` to `fs.s3a.flint.*` **by rule**. The README rows were accurate
for path 2 and silently false for path 1, and the two paths are documented in
one column. Striking the rows would have removed working, documented behaviour
from path 2 to match a defect in path 1.

## Root cause

Path 1 enumerates the keys it reads. `bind()` read the tier URI, chunk size,
budget, metadata TTL and the KMS opt-in, then built the client with the
**short constructor**, which supplies defaults for everything else. The
`MAX_OBJECT` constant was declared for a `conf.getLong` call that was never
written.

This is the same defect `FlintS3AFileSystem` already carries a comment about,
in this repository, in a class fifty lines away:

> An allowlist fails closed, which is right, but it does so SILENTLY and one
> key at a time, and nothing fails when a key is forgotten.

That comment was written when `flint.cache.sse-kms` was found unreachable for
the same reason. Path 2 was fixed by replacing the enumeration with a rule.
Path 1 kept the enumeration, so the bug recurred in the class next door.

**Path 1 cannot use path 2's rule**: it builds `FlintObjectClient` directly
rather than through `TierSupport`, so there is no single lookup to route. The
enumeration stays, which means the enumeration needs a check.

## The check that now holds it

`S3aSuite` sets `fs.s3a.flint.max.part.bytes=1` on path 1 and asserts that
**nothing is cached**, with a control that the identical read without the cap
**does** cache. Asserted by EFFECT, not by reading the field back: a check that
read the value back would pass on a client that stored it and ignored it, which
is a nearby version of this same bug.

Verified armed — with the pre-fix wiring restored, the check fails with
`2 chunk keys` cached under a 1-byte cap.

## What is still true and worth watching

The check covers one key. The class of bug — a declared key nobody reads —
is not structurally prevented on this path, only detected for the key that has
a test. Two paths with different config mechanisms is the underlying cost, and
a suite that walked every declared `fs.s3a.flint.*` constant and proved each one
changes behaviour would be the real fix.
