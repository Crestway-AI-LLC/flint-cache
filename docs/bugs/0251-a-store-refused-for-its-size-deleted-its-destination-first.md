# BUG-0251: a STORE refused for its size deleted its destination first (FIXED 2026-10-10)

**Status:** **FIXED 2026-10-10**. A sorted-set or set STORE sizes its
result against max-value-bytes before the old destination goes, so a
refused one leaves the destination as it was. Found reading `zreplace` for
GEOSEARCHSTORE, confirmed by a seat test before the fix, and held by it.
**Severity:** low. Only a STORE whose result passes max-value-bytes (64 MiB
by default), and only one that reaches the cap from inputs that each fit
under it: a union, in practice. The command was refused either way; what
went wrong was that the refusal was not the whole effect.

## What happened

ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE, ZRANGESTORE and now the geo stores
write their result with `ZSetStore::zreplace`; SINTERSTORE, SUNIONSTORE and
SDIFFSTORE with `SetStore::sreplace`. Both dropped the destination's
metadata row, then added the result to an empty key. The add checks
max-value-bytes before it writes anything, as every Flint write does
("a max-value-bytes violation must leave the set untouched"), but by then
the destination was already gone:

    (max-value-bytes 40)
    ZADD {u}a 1 aaaaaaaaaaa1 2 aaaaaaaaaaa2      each member costs 20
    ZADD {u}b 3 aaaaaaaaaaa3
    ZADD {u}d 9 kept
    ZUNIONSTORE {u}d 2 {u}a {u}b   -> ERR value exceeds maximum allowed size
    ZSCORE {u}d kept               -> nil (was 9)

A client told its write was refused had lost the key it wrote to.
Upstream has no max-value-bytes, so this is Flint's own rule, and it is the
rule that a refusal changes nothing.

Only a result larger than every input can reach the cap where its inputs
did not, so ZUNIONSTORE and SUNIONSTORE are the commands that met it; an
intersection, a difference, a range or a geo search stores a subset of a
key that already fits.

## Fixed

`zreplace` and `sreplace` cost the result's distinct members first, as
the add would, and refuse it before the destination is touched. The add
that follows cannot then be refused for its size.
