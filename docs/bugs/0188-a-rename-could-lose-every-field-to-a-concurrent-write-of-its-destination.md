# BUG-0188: a RENAME could lose every field it moved to a concurrent write of its destination (FIXED 2026-09-26)

**Status:** **FIXED 2026-09-26**. Not in v0.1.0-rc.77: it ships with the next
release.
**Severity:** high: silent data loss on an acknowledged write.

## What was measured

Found by reading while adding `LMOVE`. Then measured on a local seat (mem
engine, release build of `f2f9fb6`), 2026-09-26, with two connections:

    setup:  DEL {t}a {t}b; HSET {t}a f0 v ... f49 v
    A:      RENAME {t}a {t}b                       -> OK
    B:      HSET {t}b x 1 (eight times, released at the same instant)

Afterwards `{t}b` must hold 50 fields, if B's writes came first and the
rename replaced them, or 51. In 17 of 3,000 races, and 19 of 3,000 in a
second run, `{t}b` held one field, `x`: the rename answered OK and all 50
fields it moved were gone. With the fix, 0 of 3,000.

## The mechanism

`write_lock.rs` requires a write to more than one key to hold the lock that
excludes every writer. `main` applied that to `MSET` and to a multi-key
`DEL` or `UNLINK` (and, since ADR-0051, to a script declaring several keys).
Every other write took the stripe of the one key `command_key` names, its
first. `RENAME`, `RENAMENX` and `COPY` write a second key, the destination,
under the source's stripe, so a writer of the destination on another stripe
ran at the same time. A hash is a metadata row plus a row per field: the
rename wrote the destination's fields and metadata while `HSET` read the old
metadata and wrote its own, which kept only its one field.

The lock is taken above the storage engine, so the engine does not matter.

## The fix

One predicate, `locks_every_writer`, now decides it for every command that
writes more than one key. `RENAME`, `RENAMENX`, `COPY`, `LMOVE` and
`RPOPLPUSH` join `MSET`, `DEL`, `UNLINK` and multi-key scripts, whenever
source and destination differ. The same key twice is one key, so it keeps
its stripe.

Unit-tested: the predicate over every multi-key write and over the one-key
forms. The race above is the measurement; it is not a test, since a race
that loses 0.6% of the time would be a flaky one.
