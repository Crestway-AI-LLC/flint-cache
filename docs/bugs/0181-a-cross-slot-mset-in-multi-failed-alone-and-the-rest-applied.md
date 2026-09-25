# BUG-0181: a cross-slot MSET inside MULTI failed alone at EXEC, and the rest of the transaction applied (FIXED 2026-09-25)

**Status:** **FIXED 2026-09-25**, found while building ADR-0048. Not in
v0.1.0-rc.77: it ships with the next release.
**Severity:** medium. Nothing is written to the wrong node: the command that
spans slots still refuses itself. But a transaction the client was told would
refuse a cross-slot key applied everything queued around that key.

## What was measured

On a seat, over the wire, with the fix reverted (`a` is slot 15495, `b` 3300):

| sent | answer |
|---|---|
| `MULTI` | `OK` |
| `SET a 1` | `QUEUED` |
| `MSET a 2 b 3` | `QUEUED` |
| `EXEC` | `[OK, CROSSSLOT Keys in request don't hash to the same slot ...]` |
| `GET a` | `1`: the `SET` applied |

`command-support.md` says a cross-slot key is refused "at QUEUE time, which
also poisons the transaction", and that queue-time errors, unlike runtime ones,
leave EXEC to apply nothing.

## The mechanism

The queue step checks each command's slot against the transaction's slot, but
it reads one key per command: `command_key`, the first. BUG-0179 made it walk
every key of `DEL`, `UNLINK` and `EXISTS`, which check no slot of their own.
The other multi-key commands (`MSET`, `MGET`, the set operations and their
STORE forms, `ZUNIONSTORE`, `ZINTERSTORE`, `COPY`, `RENAME`) do check their own
keys, but only when they run. Inside a transaction that is EXEC, where a
refusal is a runtime error: one element of the reply, with every other command
applied.

## The fix

`queue_time_error` already decides at queue time by dispatching the command
against a throwaway empty store, and it accepted two verdicts from that probe:
an unknown command and a wrong argument count. It now accepts a third, the
command's own `CROSSSLOT`. That verdict depends on the keys alone, and every arm
checks it before reading the store, so the probe's answer is the real one. No
list of multi-key commands was added: the arms that refuse cross-slot keys are
the list. `DEL`, `UNLINK` and `EXISTS` are still walked by the queue step.

Covered by a unit test over every such command (refused when their keys span
slots, queued when colocated), a wire test that fails with the fix reverted,
and a `client_compat_drill` check through the proxy: a transaction of `SET a`
and a cross-slot `MSET` is refused, and `a` is never written.
