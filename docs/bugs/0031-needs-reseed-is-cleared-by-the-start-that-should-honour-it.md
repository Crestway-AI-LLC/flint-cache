# BUG-0031: `NEEDS_RESEED` is cleared by the very start that should honour it, so a marked replica loops forever (FIXED)

Status: FIXED 2026-08-20, shipped in v0.1.0-rc.57 · found 2026-08-19 on the
playground · Severity: **high** — a replica that needs a full sync could
never get one. It restarted every two minutes until a human wiped its data
directory by hand, and the pair stayed single-copy the whole time.

## Symptom

`/var/lib/flint/logs/node-7002.log`, repeating verbatim every restart:

    cleared NEEDS_RESEED: this node is the lineage now
    marked copy verified against the lineage held by 172.31.64.94:7001:
      warm rejoin at seq 100817895 (epoch (0,39))
    marked rejoin continues; tailing from seq 100817895
    replicating from 172.31.64.94:7001 starting at seq 100817895 (epoch (0,39))
    FATAL: master's oldest batch starts at 100817900, past the 100817896 we
      still need — this link can never resume. Marking for re-seed and
      exiting; the next start will full-sync from a checkpoint.

Then the next start prints the first line again.

## The loop, and why it cannot break itself

1. Something marks the node: `flintctl host-mark-reseed` writes `NEEDS_RESEED`.
2. The node starts, **clears the marker**, and decides to warm-rejoin —
   before it has established that a warm rejoin is possible.
3. The warm rejoin needs seq 100817896. The master's oldest retained batch
   is 100817900. Four batches short, and no amount of waiting fixes it: the
   WAL only moves forward.
4. The node correctly diagnoses this, says *the next start will full-sync
   from a checkpoint*, re-writes `NEEDS_RESEED`, and exits.
5. The next start clears the marker. Go to 2.

The last line of the FATAL is a promise the next start breaks. The node is
right about what it needs and never does it.

## Root cause — MEASURED, and it is not the clear (2026-08-19)

The filed cause was "the clear is unconditional and happens too early". The
clear is real and is worth hardening, but it is not why the loop ran: an
admission guard for exactly this livelock **already shipped** in `0a763ff`
(BUG-0015) and **is present in rc.52**, the build the playground was running.
Its comment describes this loop almost word for word. It did not fire.

It did not fire because **a WAL gap has two shapes and the guard only saw
one.**

`updates_since_budgeted` reports a gap when the iterator yields NOTHING:

    if out.is_empty() { return Err(ReplError::WalGap(...)) }

It said nothing about the other shape — a span that is non-empty but STARTS
past the sequence the replica needs. RocksDB answers a recycled sequence
either way, and the playground got the second: batches existed, beginning at
100817900, when 100817896 was needed.

So the two checks were asking different questions:

| | question | answer on 2026-08-19 |
|---|---|---|
| admission (`FLINTSYNC`) | are there batches after my cursor? | yes → **OK** |
| the stream (`main.rs:4037`) | does the span START where I need it? | no → **FATAL** |

Admission said the copy was good, the replica cleared `NEEDS_RESEED` on the
strength of that, attached, and only then did the contiguity check see the
hole. Exit, re-mark, restart, admitted again.

**The existing unit test could not have caught it.** `a_cursor_the_wal_cannot_
reach_is_a_gap_not_silence` explicitly ACCEPTED both outcomes — the non-empty
arm asserted only that the span started past the cursor, with the comment
"the replica's own contiguity check catches this one". It does catch it, after
admission, which is the whole bug. A test that permits both branches of a
question is not testing the question.

### Fix

`updates_since_budgeted` now captures the RAW `first_seq` of the first batch
before `first_seq.max(last_applied + 1)` clamps it, and returns `WalGap` when
that start is past `last_applied + 1`. Admission refuses, `warm` stays false,
and the boot falls through to rewind-or-re-seed — the branch BUG-0015 intended.

The clamp has to be read around rather than through: it deliberately hides a
batch that BEGINS below the floor, which is legitimate at the cursor boundary.

### Verified by removing the fix

The tightened test now REQUIRES a `WalGap` from admission. With the fix
removed it fails with the playground's exact shape:

    a cursor outside the retained WAL must be a WalGap at ADMISSION ...:
    Ok([ReplBatch { first_seq: 9, last_seq: 9, ... }, ...])

cursor stranded at 4, oldest retained batch at 9, returned as OK. 133 storage
tests pass with the fix in place.

**One thing this does not settle**: whether the clear-on-probe should also be
deferred until a sync completes. It should — a prediction that fails must not
consume the marker — but with admission now refusing, the loop cannot form,
so that change is defence in depth rather than the fix, and is best made with
its own test.

## Root cause as originally filed

The clear is unconditional and happens too early. `NEEDS_RESEED` is a
REQUEST for a full sync; clearing it on start turns it into a record that a
request was once made. The only state that should clear it is a completed
full sync.

Note the wording of the line that does the damage — *"this node is the
lineage now"*. That is a conclusion about data, drawn before the data was
checked, at the point where the marker was the only evidence available.

## What it looked like from outside

Pair 0 single-copy for 13 minutes. `flintctl status` showed `7002 DOWN`
while `flint-supervise` restarted it every two minutes; nothing said "this
node has asked for a re-seed 6 times". Recovery was manual:
`host-stop-seat`, `host-wipe-node`, `restart-node` — after which the full
sync completed in seconds and `verify` came back clean, which is the proof
that a full sync was both possible and sufficient the whole time.

## Fix

Clear `NEEDS_RESEED` only after a full sync completes. On start, a present
marker must force the checkpoint path and must not be consulted as evidence
about lineage.

Worth checking at the same time: whether the warm-rejoin decision can be
made against the master's retained WAL floor BEFORE committing to it. The
node already learns that floor one step later; asking first turns a fatal
into a branch.

## Verification — done, and by the strongest form available

The fix is `WalGap` on the narrow shape (`repl.rs:177`): a retained span that
begins PAST the cursor is a gap, not silence. A unit test pins it —
`a_cursor_the_wal_cannot_reach_is_a_gap_not_silence` — and that test earned
its place: as first written it accepted BOTH arms and passed against the code
with the fix removed. Tightened to REQUIRE `WalGap`, it fails against the
stashed fix with `Ok([ReplBatch { first_seq: 9 }])` for a cursor stranded at
4.

The end-to-end check was run by the ops session, not here, and it is a
BRACKET rather than a green: one acceptance file frozen at `b68dca7`, run
unchanged against two release bundles.

    rc.56 bundle:  FAIL  the re-seed loop never converged: 3 starts,
                         3 warm-rejoin refusal(s), span 8503 vs needed 7003
    rc.57 bundle:  ok    narrow gap refused at admission and converged in
                         1 start(s), marker cleared

Red without the fix, green with it, same code judging both sides. That covers
the first item this section used to list — a replica whose cursor the master's
WAL can no longer reach recovers with NO intervention, where on 2026-08-19 it
took a wipe by hand. The playground has run rc.57 clean since.

**The second call site is now covered too, which it was not when this was
filed.** `probe_resume` is called from `try_rewind` (`main.rs:329`) as well as
from the marked-boot path, and a production fleet reaches the rewind one
FIRST, because it has snapshots and a fresh drill fleet does not. On refusal
there the code does `remove_dir_all(data_dir)` before falling through to the
checkpoint path — same correct outcome, roughly double the single-copy
window. The ops acceptance suite now stages a rewind-eligible snapshot and
asserts `rewind entered and probe_resume REFUSED at main.rs:329`.

### Still not covered

- `NEEDS_RESEED` surviving a start that does not COMPLETE a full sync. The
  bracket shows the marker cleared after a successful convergence, which is
  the happy path; the interesting case is a start that fails midway.
- a marked node killed mid-full-sync full-syncing on the next start rather
  than warm-rejoining.

Both are about crash-during-recovery, and neither is exercised by anything
today. Recorded here rather than dropped, because a fix verified on its happy
path is not the same as a fix verified.

## Related

- [BUG-0032](0032-flintctl-start-asserts-pair-0-is-the-master.md) — why one
  dead replica became a crash loop instead of one dead replica
