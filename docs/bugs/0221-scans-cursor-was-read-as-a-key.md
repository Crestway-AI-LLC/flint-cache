# BUG-0221: SCAN's cursor was read as a key (FIXED 2026-10-07)

**Status:** **FIXED 2026-10-07**. Held by `slot_moved_drill` (`SCAN 0` is
served with the slot `0` hashes to handed off) and by unit tests on both
key extractors.
**Severity:** medium. After a slot move, keyspace iteration could fail for
a tenant, on its first page or part way through.

## What happened

The seat's `command_key` and the proxy's `route_key` each list the commands
whose first argument is not a key. SCAN was in neither, so its cursor was
taken for a key wherever a command's key matters:

- **The migration gate.** A seat answers `-MOVED` for a key in a slot it
  has handed off. `0` hashes to slot 13907, so once that slot had moved off
  a seat, `SCAN 0` answered `MOVED 13907 …`; any later cursor hashing to a
  moved slot did the same. Through the proxy, `redis-cli --scan` failed
  with `SCAN error: MOVED 13907 127.0.0.1:7000`. Measured 2026-10-07 on a
  rocks seat after `FLINTSLOTMOVED 13907`. The balancer moves slots on a
  live fleet, so this needs no operator mistake.
- **Transactions.** Inside MULTI the cursor bound the transaction to its
  slot: after a keyed command, `SCAN 0` was refused with CROSSSLOT.
- **Observability.** Slot heat counted every `SCAN 0` against slot 13907,
  and the proxy's hot-key sampler recorded reads of a key named `0`.

## The fix

SCAN is in both lists. HSCAN, SSCAN and ZSCAN are unchanged: their first
argument is their key.
