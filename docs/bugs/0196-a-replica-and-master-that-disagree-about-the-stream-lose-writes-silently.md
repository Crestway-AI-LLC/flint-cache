# BUG-0196: a replica and its master that disagree about where the stream is lose writes silently (FIXED 2026-09-30)

**Status:** **FIXED 2026-09-30.** Found writing up BUG-0194, whose drill
measured one of the two shapes. Held by two tests that fail before the fix:
`a_batch_at_or_below_the_cursor_is_a_disagreement_not_a_duplicate` and
`a_cursor_ahead_of_the_master_is_refused_but_not_as_a_walgap`. Released in the
next cut after v0.1.0-rc.78.
**Severity:** high in effect, and latent. Neither shape is known to happen with
BUG-0194 fixed, but when either one does, a replica silently holds less than
its master says it does. It serves replica reads without those writes, and a
failover to it loses them, with nothing logged and lag reading zero.

## The two shapes

Replication is correct only while the replica's cursor and the master's served
position name the same point in the same sequence space. BUG-0194 was one way
to break that. Whatever the cause, the code had two places where the breakage
went quiet.

### 1. The replica dropped a whole batch at or below its cursor

`RocksKv::apply_batch` returns `Ok(())` for a batch whose `last_seq` is at or
below the cursor: "already applied: idempotent no-op". That idempotence is the
library's documented contract (ADR-0003, the module header), and it is sound
for re-applying a batch the copy really has. But the tail never re-receives a
batch it has. The master filters every batch ending at or below the cursor
and clamps the first one to `cursor + 1` (`updates_since_budgeted`), so a
correct stream never sends one. When one arrives anyway, the two sides
disagree about where the stream is, and dropping it skips data.

That is exactly how BUG-0194 lost data: the copy kept a cursor from the old
space, the master streamed from a lower translated one, and every batch below
the old number vanished. `tools/rewind_lower_space_drill.sh` measured 900 of
7,500 keys missing. The straddling batch of the same disagreement was already
caught (`SequenceGap`, then a reconnect). The whole-batch case was not.

### 2. The master admitted a cursor ahead of its own tip

`flintsync` admitted any same-epoch cursor that its retention check passed, and
`updates_since_budgeted` answers an empty `Ok` for a cursor at or past `latest`.
So a cursor AHEAD of the master was accepted, the stream idled until new writes
passed it, and from then on it shipped only batches after the cursor. Every
write numbered in the gap (the master's old tip, the cursor] was never sent.

A correct replica cannot be ahead: same-epoch cursors are in the master's own
space, translated ones are mapped into it, and a replica only applies what the
master served. The realistic way to get here is a master that loses an unsynced
WAL tail with its host (`--wal-fsync-ms` bounds it, 500 ms by default) and comes
back as master before any failover. Its replica then holds writes it does not.
The new test caught the unfixed master answering `FLINTSYNC-OK 1005` while
holding 5 sequences.

## Fix

1. **The tail refuses a batch ending at or below its cursor**, before
   `apply_batch` sees it, with a `Transient` error naming both numbers:
   `the master sent a batch ending at 60 (from 50), at or below this copy's
   cursor 100: the two disagree about where the stream is`. It logs as
   `replication link lost (...)` and re-requests from the durable cursor, the
   same path the straddling case already took. The library's idempotence is
   unchanged; the tail just no longer relies on it silently.
2. **The master refuses a cursor ahead of its latest sequence**:
   `ERR FLINTSYNC cursor N is ahead of this master's latest sequence M ...`.
   **Deliberately not a WALGAP.** A WALGAP sends the copy to a full re-seed,
   and here that would delete the very writes the master lost. Refused this way,
   the copy keeps its data and its reconnect loop says why every second.
   Choosing which side is right (promote the copy, or re-seed it) is left to
   the controller or an operator. A marked copy's boot probe reads any refusal
   as a verdict and re-seeds, as before; a marked copy has already been declared
   superseded.

No wire change. An older replica reads the new refusal through its existing
`master error` path. A mixed pair works either way round.

## Verification

- `a_batch_at_or_below_the_cursor_is_a_disagreement_not_a_duplicate`: a fake
  master accepts at cursor 100 and ships 50..=60. Fixed, the tail returns the
  disagreement within a second, the cursor stays at 100, and the key is not
  applied. With the check disabled, the batch vanishes, and the tail ends only
  when the master hangs up 1.5 s later (`Connection reset by peer`).
- `a_cursor_ahead_of_the_master_is_refused_but_not_as_a_walgap`: a real rocks
  seat with 5 sequences refuses cursor 1005, not as a WALGAP, and still admits a
  cursor at its tip. With the refusal disabled: `FLINTSYNC-OK 1005 e0.0`.
- The rejoin and failover drills, run locally: `rewind_lower_space`,
  `rewind_rejoin`, `rewind_ack_space`, `reseed`, `walgap_quarantine`, `repl`,
  `failover`, `three_member_repoint`, `loaded_promote`, `restart`,
  `fullsync_ckpt_dir`, `promote_notice`.

## What this does not do

It does not make a disagreement impossible. A cursor in the wrong space can
still sit inside the master's range, below its tip, where neither check sees
it. BUG-0194's exact adoption is what prevents that, and this is the tripwire
for the two shapes that can be seen.
