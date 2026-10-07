# BUG-0218: SRANDMEMBER with a large negative count took a seat down (FIXED 2026-10-07)

**Status:** **FIXED 2026-10-07**. Held by the server test
`srandmember_refuses_a_reply_past_the_seats_limit` and the corpus case "the
key is read before a bad index, and i64::MIN is out of range".
**Severity:** high. One command from any tenant with a set stopped the seat
that served it, a master unless replica reads were on. The pair failed
over, and a tenant repeating it could keep doing so.

## Why

A negative count asks for that many members with repeats, so the reply is
`|count|` members however small the set. `SetStore::srandmember` built them
all at once, `(0..count.unsigned_abs()).collect()`. Measured 2026-10-07 on
a release build:

- `SRANDMEMBER k -2000000000000` on a three-member set: the process exited
  with `capacity overflow`, and every connection to the seat went with it.
- `SRANDMEMBER k -9223372036854775808`: the connection was dropped. Redis
  and Valkey refuse that count as outside their symmetric range.
- A count small enough to allocate, but large, would have been built in
  full, outside BUG-0060's admission.

Found by the randomised differential behind BUG-0219, probing i64::MIN.

## The fix

- i64::MIN is refused with Redis's out-of-range error.
- A negative count whose reply would pass the seat's `max-value-bytes`
  (512 MiB by default), counting each member at the set's mean size plus
  64 bytes of reply overhead, is refused with a reason. Redis builds any
  reply asked for; this is a deliberate difference, recorded in
  command-support.md. A shared seat cannot let one tenant's reply take
  every tenant's memory.
- BUG-0060's admission sizes SRANDMEMBER: the set, which it builds, plus
  the repeated members of a negative count.
