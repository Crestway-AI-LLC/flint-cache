# BUG-0050: a replication cursor resting INSIDE a multi-key batch can never advance

*(filename says "six hours of archived WAL and ten seconds of reachable tail" —
that was the first framing, refuted the same day. Kept so links survive.)*

Status: FIXED 2026-08-26 in `fcc0028`. Both halves landed: the read side serves
an archived-but-covering batch instead of raising `WalGap`, and the cause side
snaps an interior cursor forward to its batch end. Six tests in
`repl::bug_0050_iterator_shape` and `repl::tests`, all confirmed to execute
under `--features rocksdb` (155 storage tests, 0 failed), plus `PASS repl`,
`PASS reseed` and `PASS chaos` on the gate box.

Was: OPEN, mechanism CONFIRMED and reproduced deterministically
2026-08-26 · Severity: HIGH on a live fleet — **18 WALGAP re-seeds** on the
playground pair (10 on :7001, 8 on :7002), each costing single-copy exposure
for the length of a re-seed. Not a retention defect. Retention has been raised
twice for this and it recurred both times.

## The mechanism, in one experiment

`flint-storage/src/repl.rs`, test `bug_0050_is_an_archived_cursor_reachable`.
Same WAL, same instant, two cursors one apart:

    an 8-key write consumed 8 sequences (1401 -> 1409), producing batches [1402..=1409]
      cursor at batch START-1 (1401) -> SERVED 1 batch
      cursor at INTERIOR      (1402) -> REFUSED: WalGap("sequence 1403 is no longer
                                                        in the WAL (latest is 1409)")

**RocksDB advances the sequence per KEY but iterates per BATCH.** An 8-key
write consumes 1402..1409 and begins exactly one batch, at 1402. Sequences
1403..1409 are therefore numbers that no batch starts at. Asking
`get_updates_since(1402)` skips the batch that begins at 1402 — it is
"already delivered" for that request — so the iterator yields nothing, `out`
is empty, and `repl.rs:178` concludes the WAL has been recycled:

> We know there are newer sequences (checked above), so producing no batch at
> all means the WAL cannot reach back to `last_applied`

**That inference is the bug.** An empty result has a second cause, and this is
it. The data is present, one live segment away, and the replica is told to
full-sync.

## Why a cursor lands there at all

Applying a batch records `last_seq`, which is always a batch END — safe. The
playground's cursor did not come from applying a batch. From the node log,
seconds before the failure:

    adopted the master's translated cursor 125747825: tailing its sequence space now
    adopted the master's role epoch (0,53): this copy is on its timeline now
    FATAL: WALGAP ... sequence 131869493 is no longer in the WAL (latest is 131869494)

A TRANSLATED cursor is computed, not observed, so nothing constrains it to a
batch boundary. Land it on an interior sequence and the link is dead on
arrival — which is exactly the "one sequence short" signature, at any depth,
immune to retention, and rare enough to look random.

## What this explains that the retention framing never could

  - **One sequence short, always.** An interior sequence is by definition
    inside a batch that starts just below it.
  - **Immune to retention.** Raised 1 h -> 6 h after BUG-0012, where a replica
    also died "one sequence short of the boundary" (`rocks.rs:256` still says
    so). Recurred at 6 h. A third raise would be the third wrong fix.
  - **Rare and unpredictable.** It needs a cursor to land interior, which
    needs a translation or adoption, not ordinary tailing.
  - **735 archived segments spanning 6 h sitting unused** while the master
    reported the tail unreachable.

## Refuted along the way, kept so nobody re-runs them

  - *The tail path does not read the archive.* **False** — probe A serves a
    cursor 800 sequences back from 5 archived segments.
  - *A checkpoint truncates reachability.* **False** — probe B serves a
    pre-checkpoint cursor after checkpointing.
  - *It is a WAL-roll boundary.* **False** — probe C serves a cursor at
    latest-1 across a roll.

## Fix options — NOT taken here, because this is replication correctness

1. **Make the empty case honest.** `raw_first` is already captured for the
   other gap shape. If the iterator handed back a batch at all, the WAL
   reaches back and an empty `out` means "nothing new", not "recycled". This
   is the smallest change and it stops the false re-seed — but it leaves the
   cursor stuck at an interior sequence, making no progress, which is the
   frozen-replica failure the comment at `:170` was written to prevent. Not
   sufficient alone.
2. **Snap adopted cursors to a batch boundary.** Fixes the cause rather than
   the symptom: a cursor that is always a batch END can never be interior.
   Needs the translation path to know batch boundaries, which it may not.
3. **Serve from the containing batch.** On an interior cursor, re-request from
   below and clamp the ops — the clamp already exists
   (`first_seq.max(last_applied + 1)`). Most robust, most invasive.

(1)+(2) together look right: (2) removes the cause, (1) stops a false re-seed
if one ever lands interior again. But this is the write path a fleet's
durability rests on, and the choice should be deliberate.

## What was measured

Playground, 2026-08-26 03:03 UTC. Replica :7001 lost its stream
(`replication stream ended: ack read error Connection reset by peer`),
reconnected, and was refused:

    WALGAP cursor 131869492 is no longer reachable from this WAL
    (oldest retained batch starts at 131869495, past the 131869493 needed
     (latest is 131869852))

**It missed by two sequences.** The reachable span at that instant was
131869495..131869852 — 357 sequences. Snapshots 30 s apart differ by ~1090
sequences, so the fleet writes ~36 seq/s and 357 sequences is about **ten
seconds** of history.

Meanwhile, on the master's data dir at the same time:

    live WAL segments:   1
    archived segments:   735
    archive size:        142M
    oldest archived:     2026-08-25 21:33:55
    newest archived:     2026-08-26 03:42:33

**Six hours and one minute of WAL, on disk, in `archive/`.** Retention is
configured and it is working: `DEFAULT_WAL_TTL_SECONDS = 21_600` and
`DEFAULT_WAL_SIZE_LIMIT_MB = 8_192` (`flint-storage/src/rocks.rs:256,260`).
Both were raised deliberately — "was 1 h", "was 1 GiB".

So the retention setting is not the problem. **The tail path is not reading
what retention is keeping.**

## Why this is worth more than the alarm it caused

The visible symptom is noise: `FlintOps-playground-IncidentOverdue` has been
on for 17 days over `attach_172_31_64_94_7002`, a condition whose underlying
fault clears in about five seconds each time (see `docs/alerts.md` in the ops
repo for why a recurring subject can never expire).

The invisible symptom is the real one. **A replica that blips for longer than
~10 seconds must re-seed**, and a re-seed at fleet dataset sizes is minutes to
tens of minutes of single-copy exposure — the exposure BUG-0046 was filed
about, arriving by a different route. The cost model's anchor node carries
~96 GB. Ten seconds of tolerance against a network hiccup is not a margin.

It also makes `--rewind-snaps` load-bearing rather than a fallback: the
playground survives these only because a snapshot from seconds earlier is
usually within the fence, and the log shows it taking that path
("rewound to .../snap-...-seq131869494-e0.53 ... tailing incrementally
instead of a full re-seed").

## What would confirm or refute it

1. On a master with archived segments present, ask for a cursor known to be
   inside the archive window and see whether the iterator serves it. That is a
   direct test of the hypothesis and needs no fleet.
2. If it does not: find which RocksDB option or path makes archived segments
   visible to `get_updates_since`, and assert the reachable span in a drill —
   **a retention setting nothing reads is worth exactly nothing**, and the
   current tests cannot tell 6 hours from 10 seconds.
3. A drill that reconnects a replica after N seconds and asserts no WALGAP
   for N well inside the configured TTL. There is no such drill today, which
   is why 18 occurrences on a live fleet were invisible to a green suite.


## Field evidence, 2026-08-26 — this was 4 of the playground's 10 forced re-seeds

Every `FATAL: WALGAP` in `node-7001.log` on the playground, classified by cause
and dated from the preceding `flintctl start` banner:

| when | cause |
|---|---|
| 08-08 04:42 | archive segment deleted (BUG-0012) |
| 08-11 21:07 | **interior cursor (this bug)** |
| 08-11 23:27 | archive segment deleted |
| 08-12 07:28 | **interior cursor** |
| 08-13 11:01 | archive segment deleted |
| 08-14 14:54 | **interior cursor** |
| 08-16 06:58 | archive segment deleted |
| 08-16 15:09 | archive segment deleted |
| 08-20 01:51 | archive segment deleted |
| 08-24 16:29 | **interior cursor** |

Two causes, and only two. The signature separates them cleanly: this bug reads
`sequence N is no longer in the WAL (latest is N+1)` — the cursor one short of
the tail, resting inside a batch — while BUG-0012 reads `IO error: No such
file or directory: ... /archive/NNNNNN.log`, a segment RocksDB retention
removed without consulting the replica.

**BUG-0012's fix is confirmed working in production.** It was gated 2026-08-21
and there has not been an archive-deletion WALGAP since 08-20 01:51 — six days,
against a prior rate of roughly one every two days.

**Every forced re-seed after that date was this bug.** The 08-24 entry is the
one that killed the link and left the seat marked for re-seed, producing the
`attach_172_31_64_94_7002` incident at 03:03 on 08-26 — the incident whose
`escalate` verdict held `FlintOps-playground-IncidentOverdue` on for thirteen
hours (OPS-0045).

## The prediction this makes, and how to falsify it

If these really are the only two causes, then with BUG-0012 gated and this
fixed in `fcc0028`, node-7001 should stop needing forced re-seeds — and the
recurring `attach_*` incidents behind the standing alarm should stop with them.

That is a claim about the future, so it is worth stating in a form that can be
wrong: **a `FATAL: WALGAP` of either signature on the playground after
2026-08-26 falsifies it.** The interior-cursor form would mean the fix is
incomplete; the archive form would mean BUG-0012's shedding stopped holding at
a load it had not previously seen. Either is worth knowing quickly, and the
cheap check is one grep of `node-7001.log`.

Not claimed: that this fixes the attach incidents *generally*. A replica can
die for reasons that never touch the WAL, and nothing here observed one.

## 2026-08-26 — the regression test for this is FLAKY under load (open)

`repl::bug_0050_iterator_shape::a_genuinely_recycled_wal_still_raises_walgap`
fails intermittently. Observed once during a full-workspace run on a laptop at
load average 24-61, from concurrent sessions:

    panicked at crates/flint-storage/src/repl.rs:1197:14:
    iter: Error { message: "IO error: No such file or directory: while stat a
    file for size: /var/folders/.../flint-b50-recyc-66333/000018.log" }

    test result: FAILED. 158 passed; 1 failed

Run alone it passes 5/5. The re-run of the full suite was green at 498. So this
appears only when the workspace suite runs tests concurrently AND the machine
is busy.

**Ruled out: a temp-directory collision.** Every test in that module uses a
distinct prefix (`flint-b50-snap-`, `-reg-`, `-recyc-`, `-acct-`, `-shape-`)
plus the pid, so two of them cannot share a path. That was the first guess and
it is wrong.

**Likely cause.** The test opens with `RocksKv::open_with_retention(&d, 0, 0)`
— retention OFF, so flushed segments are DELETED rather than archived, which is
exactly the "recycled" condition it wants to create. It then asserts
`updates_since` raises `WalGap`. But if the segment is removed between the
iterator being constructed and being read, RocksDB surfaces an IO error
instead, and under load that window widens.

**The trap in fixing it.** Accepting the IO error as equivalent to `WalGap`
would make the test pass and would weaken it: those are different observations,
and the test exists to prove the empty-iterator fix does not SWALLOW a genuine
`WalGap`. Whichever of the two the test actually needs should be decided
deliberately, and the deletion made deterministic rather than raced if the
answer is `WalGap`.

A flaky test "fixed" by a re-run is the failure mode here, so the fix needs a
control that fails without it.
