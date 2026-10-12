# BUG-0256: stream reads were not priced by read admission (FIXED 2026-10-12)

**Status:** **FIXED 2026-10-12**. A stream read that can build a large reply
is priced before it runs, as a hash, set, zset or list read is.
**Severity:** medium, on seats started with `--streams`, which is off by
default in every release so far. One read of a large stream built its whole
reply whatever the seat's memory, which is the failure BUG-0060's admission
exists to refuse.

## What happened

BUG-0060's read admission prices a read from the collection's metadata
before anything is materialised, and refuses one that would not fit in
`--collection-read-budget-pct` of the node's memory. `collection_read_bytes`
names the commands it prices. Streams (ADR-0052) and consumer groups
(ADR-0057) added these reads, and none of them was named:
- `XRANGE`/`XREVRANGE` without `COUNT`;
- `XREAD`/`XREADGROUP` without `COUNT`, over each stream named;
- `XPENDING`'s extended form and `XAUTOCLAIM` with a large count;
- `XINFO STREAM FULL COUNT 0`.

Found reviewing ADR-0057 part 1's quota paths.

## Fixed

`stream_read_bytes` prices each of them on what it can return:
- **Entries:** the read's `COUNT` when it gives one, and the whole stream
  when it does not. Each entry costs the stream's mean stored size plus its
  16-byte ID.
  - An ID range without `COUNT` is charged the whole stream, as a score
    range without `LIMIT` is charged the whole zset.
  - `XRANGE`'s last `COUNT` wins, as in the command, and `COUNT 0` costs
    nothing.
  - `XREAD`'s `$` costs nothing (a seat never waits), and its `+` costs one
    entry.
- **`XREADGROUP` and the proxy's `FLINTXREADGROUP`:** as `XREAD`, for `>`
  and for a history read alike.
- **Pending entries cost 32 bytes each:** their ID, delivery time and count,
  without the consumer's name.
  - `XPENDING`'s extended form: its count, at most the group's pending
    entries.
  - `XAUTOCLAIM`: its `COUNT` (100 by default), at most the group's pending
    entries, as entries, or as IDs with `JUSTID`.
  - `XINFO STREAM FULL`: its `COUNT` (10 by default, everything for 0) of
    entries, and of each group's pending entries twice: once in the
    group's list and once in its consumers'.

A read the command will refuse anyway, such as a syntax error, a missing
group or another type, is not priced, so the command's own error answers it.

## What this does not cover

- `XCLAIM`'s reply is bounded by the IDs in the request.
- `XINFO CONSUMERS` and `XINFO GROUPS` grow with a group's consumers and a
  stream's groups. They are not priced.

## Verification

- `a_stream_read_is_sized_by_what_it_returns` checks each command's price
  on a stream of 100 entries with a group holding 30 pending, including
  `COUNT 0`, the last `COUNT`, `$`, `+`, a missing key, another type and a
  syntax error.
- `collection_admission_drill` builds a stream as large as its hash at a
  1% budget. `XRANGE s - +` and `XREAD STREAMS s 0` are refused, and
  `XRANGE s - + COUNT 1` on the same stream is not. The drill runs where
  node memory is readable (the gate's Linux box); macOS skips it, as
  before.
- **Six mutants failed the unit test:**
  - the stream commands left out of `collection_read_bytes`;
  - `COUNT` ignored;
  - the ID left out of an entry's price;
  - `$` charged as a read;
  - pending entries counted once in `XINFO STREAM FULL`;
  - the first `COUNT` winning.
