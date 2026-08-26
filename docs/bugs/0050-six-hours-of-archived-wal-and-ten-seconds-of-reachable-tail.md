# BUG-0050: six hours of archived WAL, ten seconds of reachable tail

Status: OPEN, found 2026-08-26 · Severity: HIGH on a live fleet — **18 WALGAP
re-seeds** on the playground pair (10 on :7001, 8 on :7002), each one costing
single-copy exposure for the length of a re-seed.

**The title is kept for the trail but the framing below it was CORRECTED the
same day** — see "CORRECTION". The retention depth is not the variable: the
replica that failed was ONE SEQUENCE behind latest, which no retention setting
can help. This is a WAL-roll boundary condition.

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

## CORRECTION 2026-08-26: the first framing was wrong

This file originally said the tail path was not reading the archive that
retention keeps, and proposed the local test that would settle it. **That test
was run and it REFUTES the hypothesis.** Both probes are in
`flint-storage/src/repl.rs` (`bug_0050_is_an_archived_cursor_reachable`):

    probe A (no checkpoint):  5 archived segments, cursor 800 back -> SERVED 800 batches
    probe B (after checkpoint at seq 1000):  pre-checkpoint cursor -> SERVED 1000 batches

So `get_updates_since` DOES serve from archived segments, and taking a
checkpoint does NOT move the reachable floor. Neither mechanism reproduces.

**And re-reading the evidence with that ruled out, the "6 hours vs 10 seconds"
framing is the wrong reading of it.** The first failure was:

    FATAL: WALGAP ... sequence 131869493 is no longer in the WAL
           (latest is 131869494)

The replica needed **131869493** while the master's latest was **131869494**.
It was ONE SEQUENCE BEHIND. This is not a replica that fell out of a retention
window; it is a cursor at the very tail being declared unreachable. The
retention depth is a red herring — no retention setting, however large, helps
a cursor that is one behind latest.

The second message, from the restart, is consistent with a WAL ROLL at exactly
that boundary:

    oldest retained batch starts at 131869495, past the 131869493 needed
    (latest is 131869852)

Oldest reachable is 131869495 — one past where the replica died. The segment
holding 131869493 existed (735 archived segments, 6 h, 142 MB were on disk)
and was not offered.

## What is actually established

  - A replica ONE SEQUENCE behind latest was refused with WALGAP, on a live
    fleet, 18 times across two seats.
  - The failure sits at a WAL roll boundary: the oldest reachable batch
    afterwards is exactly one past the failed cursor.
  - Archived segments ARE reachable in the general case (probe A), and
    checkpoints do NOT truncate reachability (probe B). So the general
    mechanisms are sound and this is a BOUNDARY condition.
  - Retention depth is not the variable. It was raised once already after
    BUG-0012 — where a replica also died "one sequence short of the boundary"
    (`rocks.rs:256`) — and the same failure has recurred at 6 h that occurred
    at 1 h. **Raising it a third time would be the third wrong fix.**

## What would find it

The local probes exercise a quiet master. The playground's differs in ways
worth reproducing one at a time:

  1. **Roll the WAL while a tail is mid-poll**, with the cursor at latest-1.
     That is the exact geometry of the failure and neither probe covers it.
  2. **Sequence-space translation.** The node log shows "adopted the master's
     translated cursor 125747825: tailing its sequence space now" shortly
     before the failure. A cursor translated between spaces landing one off at
     a roll boundary would produce precisely this, and nothing local tests a
     translated cursor against a rolling WAL.
  3. Only then, if both are clean, look again at retention.

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
