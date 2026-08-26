# BUG-0050: a replication cursor resting INSIDE a multi-key batch can never advance

*(filename says "six hours of archived WAL and ten seconds of reachable tail" —
that was the first framing, refuted the same day. Kept so links survive.)*

Status: OPEN, mechanism CONFIRMED and reproduced deterministically
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
