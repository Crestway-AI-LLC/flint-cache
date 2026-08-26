# BUG-0050: six hours of archived WAL, ten seconds of reachable tail

Status: OPEN, found 2026-08-26 · Severity: HIGH on a live fleet — every
reconnect that takes longer than the reachable window costs a re-seed, and the
window measured on the playground is ~10 seconds against a configured 6 hours.
Measured, not modelled: **18 WALGAP re-seeds** on the playground pair (10 on
:7001, 8 on :7002).

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

## The gap between the two numbers

`Repl::updates_since_budgeted` calls `get_updates_since` and the admission
check at `flint-storage/src/repl.rs:207` reports `raw_first`, the first batch
that iterator yields. On this fleet that first batch tracked the LIVE segment,
not the archive — one live segment was present and 735 archived ones were not
being offered.

Stated as measurement and hypothesis separately, because only the first is
established:

- **Measured:** reachable span ≈ 10 s while `archive/` holds 6 h. Repeatable —
  18 occurrences across two seats.
- **Hypothesised:** the transaction-log iterator is not picking up archived
  segments here, so effective replication retention is "since the last WAL
  roll" rather than the configured TTL. Whether that is a RocksDB option we do
  not set, an archive path it does not scan, or a roll that resets the
  iterator's floor, is NOT established and is the first thing to check.

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
