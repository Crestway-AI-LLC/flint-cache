# BUG-0179: a multi-key DEL, UNLINK or EXISTS across two pairs answered for the first pair only (FIXED 2026-09-24)

**Status:** **FIXED 2026-09-24**, found the same day while surveying framework
cache stores (BUG-0178). It settles the question BUG-0053 left open. Not in
v0.1.0-rc.77: it ships with the next release.
**Severity:** high on any fleet with more than one pair. A delete that
silently leaves keys behind is a stale cache with no error anywhere, and
multi-key `DEL` is how framework cache stores invalidate (`delete_many`,
`delete_multi`).

## What was measured

Through the proxy, on a two-pair fleet on a gate box, 2026-09-24. `a` is slot
15495 and `b` slot 3300, the two pairs' halves of the range:

| command | answer | right answer |
|---|---|---|
| `EXISTS a b` | 1 | 2 |
| `DEL a b` | 1, and `GET b` still returned the value | 2, both gone |
| `MULTI` / `DEL a b` / `EXEC` | `1`, and `b` survived | refused (the docs promise it) |

## The mechanism

The proxy forwards a multi-key command whole, to the pair that owns its FIRST
key. BUG-0053 made `MGET` and `MSET` refuse cross-slot keys at the server, and
the set operations already did. `DEL`, `UNLINK` and `EXISTS` go through
`multi_key`, which checks no slot, so the first key's pair answered for keys it
does not hold: absent, not deleted, not counted. The proxy's near-cache had
already dropped every named key, so the next `GET b` fetched the value the
caller had just deleted.

Inside a transaction the same happened one level down: the queue-time slot
check read only each command's first key, although `command-support.md` says
"a later command naming a key elsewhere is refused with `CROSSSLOT` at QUEUE
time".

## The fix

- **The proxy splits these three by owning pair** (`split_by_owner`) and sums
  the counts. Keys on one pair (every single-pair fleet, and any colocated set)
  take the unchanged path. Refusing instead would have broken every single-pair
  fleet, where these always answered correctly. What a split cannot give is one
  atomic step across pairs, and `command-support.md` now says so.
- **They are never staged** for pipelining, which would forward them whole
  again.
- **A transaction checks every key they name at queue time**, so a cross-slot
  `DEL` in `MULTI` is refused with `CROSSSLOT` and poisons the transaction, as
  documented.

Covered by unit tests of the grouping and of staging, and by
`client_compat_drill` on its two-pair fleet: cross-pair `DEL`/`EXISTS` counts
and the deleted key is gone; a cross-slot `DEL` in a transaction is refused and
deletes nothing.
