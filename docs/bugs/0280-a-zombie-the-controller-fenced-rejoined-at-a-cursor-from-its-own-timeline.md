# BUG-0280: a zombie the controller fenced rejoined its new master at a cursor from its own timeline, and kept writes the master never had (FIXED)

**Status:** **FIXED 2026-11-06.** Found the same day by the error-handling
review's area B question, "`mark_needs_reseed` is best-effort. If writing the
marker fails, what does the next start do with the data directory?"
(`docs/error-handling-review.md`), and measured on release binaries. In
public main (`93dcd0c3`) and v0.1.0-rc.86.
**Severity:** high. The rejoined copy silently differs from its master. It
keeps the writes the zombie took that the master never had, and it lacks as
many of the master's. The master counts it a live replica at `seq_lag` 0, so
nothing looks wrong. A later failover to that copy loses the master's
acknowledged writes it skipped and brings back writes the pair had dropped.
The marker the question was about does not prevent it.
**Area:** `flint-server`, the FLINTSYNC attach guard; the controller's zombie
fence, `fence(.., max_epoch + 1)`.

## What happens

- **The controller fences a zombie above the new master.** A master that
  comes back after its pair failed over still claims master at its old
  epoch. The controller demotes it with `FLINTDEMOTE` at `max_epoch + 1`,
  one above the master it promoted.
- **A demoted copy keeps the cursor it held as a master.** `FLINTDEMOTE`
  sets its replication cursor to its own last sequence (BUG-0082), a
  number in its own WAL's space, including the writes nobody copied.
- **The epoch a replica presents names the space its cursor is in**
  (BUG-0194). The master fence-checks and translates a cursor presented at
  an epoch below its own, and takes one at its own epoch as a position in
  its own WAL. One at an epoch ABOVE its own took the second path. Nothing
  reaches that state by following a master: syncs copy the master's epoch,
  and adoption only moves up to it. Only a demotion above the master does.
- **So the zombie's cursor was read as the master's.** Once the master had
  written past that number, the cursor passed the retention check, the
  probe answered OK, and the zombie tailed from there: everything in the
  master's WAL below that number it never received, and its own writes
  stayed.
- **The re-seed marker does not help.** `FLINTDEMOTE` writes
  `NEEDS_RESEED`, and every `flintctl` rejoin path writes it too. But a
  marked start first asks the master whether the copy can be resumed as it
  is, and a demoted copy's role is Replica, so it asks. The comment there
  said ex-masters never pass that probe because their role is Master. A
  demoted one is not.

## Measured, 2026-11-06

Release binaries of public `93dcd0c3`, two rocks seats on a laptop. A
master A and its replica B copied 1,000 keys. B was killed, and A took 200
writes B never saw (`zomb:*`). B was started again, promoted at A's epoch + 1
(`(0,2)`), and took 2,000 writes (`new:*`). A was demoted as the controller
fences a zombie, at `(0,3)`, then restarted with `--replica-of` B.

| A's restart | A's log | keys only on A | B's keys missing on A | B reports |
|---|---|---|---|---|
| with `NEEDS_RESEED`, as `FLINTDEMOTE` leaves it | `marked copy verified against the lineage ... warm rejoin at seq 1202 (epoch (0,3))` | 200 (every `zomb:` key) | 196 (between `new:0` and `new:255`) | `live_replicas 1`, `seq_lag 0` |
| with the marker removed, as a failed write leaves it | `replicating from ... starting at seq 1202 (epoch (0,3))` | 200 | 196 | `live_replicas 1`, `seq_lag 0` |

A's cursor 1202 is in A's own space: two startup rows, the 1,000 writes B
copied, and the 200 B never saw. B's own sequences run to 3,006, and its
2,000 writes are 1,007 to 3,006. B streamed its own sequences from 1,203 on,
so A never received B's writes at 1,007 to 1,202: 196 of them.

## The fix

**The master refuses a copy whose role epoch is newer than its own.**
FLINTSYNC answers `ERR FLINTSYNC this copy's role epoch (0,3) is newer than
this master's (0,2), so its cursor 1202 is not a position on this node's
timeline and it is not streamed ...`. It is an `ERR`, not a `WALGAP`, for
BUG-0196's reason: the master cannot tell a fenced zombie from a newer copy
pointed at a master that is itself stale, and a `WALGAP` would send the
second to a re-seed that deletes it.

- **Marked, as `FLINTDEMOTE` and every rejoin path leave it,** the refused
  probe sends the start to its rewind or its re-seed, which is the
  `FLINTDEMOTE` contract ("wipe + resync").
- **Unmarked,** the tailer is refused on every reconnect, saying why, and
  the copy is left as it was. The pair runs without that replica, and the
  master says so (`live_replicas 0`), until the seat is restarted marked.

`FLINTFOLLOW` already refused the same thing from the replica's side: a
target must be a master at or above this node's epoch.

**Not in this fix:** `mark_needs_reseed` writes its marker without syncing
it, so a host that crashes soon after a demotion can lose it. With this fix
that leaves the copy refused, not diverged.

## Acceptance

`zombie_rejoin_drill`, on real seats, with the zombie made as above:
- **marked:** refused, saying why, re-seeded, and holding exactly the
  master's keys;
- **unmarked:** refused at least twice and still up, its copy unchanged
  (its 200 own writes, none of the master's), and the master reporting
  `live_replicas 0`.

On the binaries of `93dcd0c3` the drill fails at its first case: `the
rejoined zombie holds 200 key(s) its master does not, and lacks 196 of the
master's`.

Unit test: `a_copy_with_an_epoch_above_the_master_is_refused_but_not_as_a_walgap`,
with the same cursor at the master's own epoch as the control.
