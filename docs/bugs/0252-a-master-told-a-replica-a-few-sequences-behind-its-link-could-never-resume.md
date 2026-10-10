# BUG-0252: a master told a replica a few sequences behind that its link could never resume (FIXED 2026-10-10)

**Status:** **FIXED 2026-10-10**. A walk of the WAL that ends in a gap is
walked again from scratch, after 50, 200, 500 and 1,000 ms, before the gap
is believed. A cursor resting inside a batch is served from that batch on both
of the gap's shapes, not only the empty one. The master logs every refusal it
sends. Reported as ops OPS-0386 by the Running Cache Agent.
**Severity:** medium. Nothing was lost, but each false refusal sent a healthy
replica to its rewind: about a minute on one copy, with the master refusing
every write after its 10 s widowed grace (about 490 on 2026-10-09), and every
older snapshot quarantined as unresumable (2,880 the second time).

## What happened

On the playground, after the rc.81 roll, the replica exited twice with:

    FATAL: WALGAP full sync required: oldest retained batch starts at
      350886260, past the 350886258 needed (latest is 350886261) ...
    FATAL: WALGAP full sync required: sequence 354670050 is no longer in
      the WAL (latest is 354670065) ... — this link can never resume.

It was three and fifteen sequences behind a master whose archive held 11 to
12 hours, newest segment 30 s old. Both times the replica, restarted, was
served the same span at once, so the WAL held it.

Both refusals fell within about a second of a master snapshot. The
controller snapshots the master every 30 s, and FLINTSNAPSHOT is a RocksDB
checkpoint, which flushes the memtable and rolls the WAL to a new file:

- gap 1: the snapshot that began at 09:46:01.009 reads latest 350886259,
  inside the span the refusal names (350886258 needed, 350886261 latest);
  the supervisor restarted the replica at 09:46:01.172;
- gap 2: the snapshot at 16:09:06.997; the ops agent saw the replica gone at
  16:09:08.311.

At gap 2 the master's RocksDB LOG has no message from the WAL iterator (no
"Start sequence was not found", no "Discontinuity"): the walk found the
batch holding the cursor and then nothing, though 15 later sequences
existed. What made the batches after a roll briefly invisible to a walk was
not established. A stress test, `checkpoint_rolls_never_make_a_tailer_see_a_gap`
(tailing from batch ends while checkpoints roll the WAL back to back, with
writers and the fsync tick going), made 9,245 rolls on macOS and 16,878 on
Linux (XFS, a gate box) without either of the playground's shapes. The Linux
run did meet one false gap of another shape, BUG-0162's: a stat of an
archived segment deleted underneath, which outlasted that fix's two 50 ms
retries. So the fix below does not wait on the mechanism, and the lines it
logs record the WAL files a walk could see the next time one happens.

## The second shape, found while reading

`get_updates_since(n)` skips every batch whose first sequence is at or
below `n`. A cursor resting INSIDE a batch is therefore offered only the
next batch, which the 0031 check reads as "oldest retained batch starts
past the needed". BUG-0050 had taught the empty walk to ask whether a batch
covers the cursor; the non-empty walk did not ask. A test with a cursor at
3 inside a batch 1..=5 and a batch 6..=8 after it was refused before this
change and is now served 4..=5, then 6..=8.

## Fixed

- `updates_since_budgeted` re-walks a gap verdict (`rewalk_on_gap`): a
  re-walk can only turn a transient verdict into a success, never a real
  gap into a pass, and the 1.75 s it can cost is paid only on the gap path,
  under the 3 s a replica waits for its master before it stops serving
  reads. A recovered walk logs one line naming the first verdict and the
  newest live and archived WAL files when it was given.
- The 0031 branch asks `batch_covering`, and serves the covering batch
  when it ends where the offered span begins.
- The master logs `replica refused at seq N: WALGAP ...`, with the same
  file listing, when it sends one, and `rejoin refused at seq N` for a
  rejoin's admission check. It used to send the refusal and say nothing, so
  the master's side of both playground exits was blank.
- Both are counted where the ops agent reads, FLINTINFO
  `wal_gap_refusals_total` and `wal_walk_recovered_total`: a rise in the
  second after rc.82 says the transient was real and was caught.
