# BUG-0258: a woken XREADGROUP answered a RESP2 client in RESP3's shape (FIXED 2026-10-12)

**Status:** **FIXED 2026-10-12**, before any release carried it. It was on
main from ADR-0057 part 2 (`8bacc74d`) and was fixed the same day.
**Severity:** high for what it touched. A RESP2 client blocked in
`XREADGROUP … BLOCK` through the proxy could not read the entry that woke
it. Consumer groups are created only on seats started with
`--stream-groups`, which is off by default.

## What happened

The proxy waits for a blocked `XREADGROUP` itself (ADR-0057 D4). It sends
the client's command once, then polls the seat with `FLINTXREADGROUP`,
which writes nothing when it finds nothing. The proxy reads seats in RESP3,
and `forward` puts a reply into the client's protocol by looking at the
command it forwarded (`repair_reply`). For the poll that command is
`FLINTXREADGROUP`, which the repair does not know. So the entry a poll
found kept RESP3's map. Encoded for a RESP2 client, the map became a flat
array of the stream's name and its entries:

    RESP2 client:  XREADGROUP GROUP g w BLOCK 5000 STREAMS jb >
    (an XADD to jb while it waits)
    redis-py 7.0.1: IndexError: string index out of range
                    (it read the stream's name as a list of entries)

The client's first attempt was repaired, because it is the client's own
command. So only an entry that arrived during the wait was affected.

**Why nothing caught it before the gate:**
- **The conformance corpus has no blocking hit.** A seat answers a
  blocking `XREADGROUP` at once, so a corpus case cannot wait.
- **The laptop's wake check (part 2's `block_check`)** asserted that the
  entry's ID appeared somewhere in the reply's text, which a malformed
  reply also passes.
- **Part 3's redis-py check** used the client's default protocol. That is
  RESP3 in the laptop's redis-py 8.1.0, and RESP2 in the gate box's 7.0.1.
  Gate g574 failed on it.

## Fixed

- `blocking_xreadgroup` repairs a poll's reply by the client's command.
- `client_compat_drill` runs the blocking check in RESP2 and RESP3 by name,
  rather than in whatever a client defaults to. It asserts the reply's
  shape: the stream, then exactly the one entry added.

`XREAD`'s wait polls with `XREAD` itself, so its replies were repaired, and
it was not affected.

## Verification

- On the laptop, before the fix: the RESP2 check fails with redis-py's
  `IndexError`, and RESP3 passes.
- After the fix, all four group checks pass in both protocols, and the same
  checks pass against Valkey 9.1.
