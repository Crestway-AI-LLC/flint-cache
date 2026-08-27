# BUG-0062: the probe clears a stale snapshot, then the attach races the WAL (FIXED)

**Found** 2026-08-27, soak cycle 2, on an rc.64 data plane with live ingestion
(4 feeders) and a killed-then-restarted replica. The harness reported

```
flintctl: restart-node 172.31.67.237:7001: 172.31.67.237:7001 never reconverged behind 172.31.66.190:7002
```

which is the symptom, not the fault. The seat's own log has the fault:

```
rewound to .../snap-1787809927380-seq207196-e0.7 (seq 207196 <= fence 236839, epoch (0,7)):
  tailing incrementally instead of a full re-seed
FATAL: WALGAP cursor 207196 is no longer reachable from this WAL
  (oldest retained batch starts at 207198, past the 207197 needed)
```

## This is a recurrence, and the existing fix is the interesting part

`main.rs:443` already records the first occurrence and the fix for it:

> A replica takes no snapshots, so its newest labeled snapshot dates from its
> LAST MASTERSHIP and ages without bound; soak run 34 cycle 6 rewound a
> caught-up replica to a 15-minute-old snapshot whose catch-up span the master
> had long recycled, and the seat died on the attach's WALGAP where a probe
> here would have chosen the re-seed while it was still cheap to choose.

That fix is `probe_resume`, called immediately before committing to the rewind.
**It ran here and it passed** — the "tailing incrementally" line only prints
after `probe_resume` returns `Ok`. Then the attach died anyway.

The margin says why: the tailer needed batch **207197** and the oldest retained
was **207198**. It missed by ONE batch. That is not a stale-snapshot
misjudgement, it is a **check-then-act race** — the WAL recycled between the
probe answering "reachable" and the tail actually asking. Under live ingestion
the master's WAL rotates continuously, so the window `probe_resume` narrowed
cannot be closed by narrowing it further.

## Why the retry does not save it

The design's recovery is to mark and restart (`main.rs:4795`):

> Retrying a purged span cannot work: the next request asks for the same
> missing sequence and fails identically, forever. [...] Marking for re-seed
> and exiting; the next start will full-sync from a checkpoint.

and the marker is deliberately worded so that

> the next boot's rewind decision keys off it (retrying a fence-refused
> snapshot loops forever).

**The next boot rewound to the same snapshot anyway.** From the evidence
bundle, counting rewind lines across the seat logs:

```
1  rewound to .../snap-1787809837104-seq130042-e0.4
1  rewound to .../snap-1787809867201-seq196517-e0.6
2  rewound to .../snap-1787809927380-seq207196-e0.7      <-- same snapshot, twice, fatal both times
```

So the retry is **not independent of the attempt that failed**: it re-selects
the identical snapshot, re-runs the identical probe, and re-loses the identical
race. That is the livelock `main.rs:4405` warns about — "a livelock that no
supervisor can break" — reached by a path the fence-refusal wording does not
cover. `flintctl` then gives up at its deadline and the pair is left running
unprotected.

## The fix: quarantine, and what it does NOT do

On `WalPurged` the tailer now disqualifies every local snapshot at or below
the unresumable cursor before it writes the marker and exits, by renaming
`snap-…` to `unresumable-snap-…`. `parse_rewind_candidate` only accepts names
beginning `snap-`, so a renamed snapshot stops being a candidate while its
bytes stay on disk for forensics.

**Be clear about the scope: this does not stop the race.** The probe and the
attach are still two reads of a moving boundary, and a replica can still lose
that race. What changes is what losing costs. Before: lose, mark, exit,
re-pick the same snapshot, lose identically, forever — a livelock ending in
`flintctl` giving up and the pair sitting at `live_replicas 0`. After: lose
once, and the next boot has no candidate to lose with, so it takes the full
re-seed that was always the correct answer. The recovery becomes terminating;
the race is still there.

**Why `seq <= cursor` and not only the snapshot that failed.** A master's WAL
only advances, so a span already recycled never returns: a snapshot at the
unresumable cursor is unresumable permanently, and an older one needs an even
earlier batch, so it is unresumable too. Quarantining just the loser would
leave the next boot to pick the next-older, fail identically, and repeat — one
boot per snapshot, each burning a reconvergence deadline. This run held three
(seq 130042, 196517, 207196), so three deadlines to reach a re-seed available
at once.

**Renaming, not deleting**, which retires the objection this file opened with.
The worry was that quarantine "discards work that is usually still good". It
does not discard anything — the bytes remain — and the work in question is not
good for this purpose: a snapshot whose catch-up span the master has recycled
can never again be resumed from, by the same monotonicity that makes the rule
sound. What it can still be is evidence.

Deliberately NOT a second string to match on. The boot's existing guard is
`why.contains("promotion fence")`, and this failure got through precisely
because the purged-WAL marker does not contain that phrase. Adding
`|| why.contains("WALGAP")` would have fixed this instance by the same
mechanism that produced it. Changing the candidate set is a fact on disk; a
message match is a fact about wording.

Six tests, mutation-checked two ways: making the quarantine a no-op fails
four, and quarantining only the exact failing snapshot (`seq == cursor`) also
fails four — the scope decision is defended, not just the feature. The two
that stay green in both are the pure name-property and the missing-directory
case, neither of which depends on scope.

**DEMONSTRATED 2026-08-27 by `tools/walgap_quarantine_drill.sh`**, which forces
the condition instead of waiting for the race. The 60-minute soak had passed
all five cycles without once entering this path — zero WALGAP, zero quarantine
— so a green soak said only that the failure did not recur. Hoping an hour of
cloud time trips a TOCTOU window is not a test, so the drill reaches
`WalPurged` through the front door: rewind A onto its own snapshot while the
span is retained, `SIGSTOP` it, churn B until its 1 MB/1 s archive has recycled
past A's cursor, then resume.

It proves its own precondition on the wire before drawing any conclusion —
after each churn round it sends the same `FLINTSYNC` admission the tailer gets,
and proceeds only once B answers `WALGAP` for A's exact cursor:

```
round 2: cursor 1256 is now UNREACHABLE (oldest retained batch starts at 3192,
         past the 1257 needed (latest is 3925)): full sync required
quarantine: snap-…-seq302-e0.1 (seq 302 <= unresumable cursor 1470) is no longer
            a rewind candidate; kept as unresumable-snap-…-seq302-e0.1
snaps-a now: LATEST unresumable-snap-…-seq302-e0.1
PASS
```

so the span is provably gone, A provably asked for it, the quarantine fired,
the snapshot was renamed rather than deleted, no `snap-` candidate survived,
and the restart re-seeded and converged including new-timeline data.

The first version of the drill wrote a fixed 4000 keys and recycled nothing:
retention alone archives nothing, because a segment is only archived once its
memtable has been FLUSHED. `FLINT_WRITE_BUFFER_MB=1` is the load-bearing half.
That run reported "neither WALGAP nor quarantine" and was correctly read as a
SETUP failure rather than evidence about this fix — which is why the drill now
distinguishes the two in its failure text.

Still true, and not weakened by the demonstration: **the fix does not stop the
race.** Losing it now terminates instead of livelocking. And the soak's silence
still cannot be credited to this fix — that run used a HEAD data plane where
the failing one used rc.64, so something else may have moved. Unknown, and
recorded as unknown.

## The shape

The probe and the attach are two reads of a moving boundary with a gap between
them, and the remedy for losing that race is to repeat both reads against a
boundary that has only moved further. **A retry that re-derives the same inputs
is not a retry.** For this to converge, the failed attempt has to change
something: the snapshot that just lost the race must be taken out of the
running before the next boot chooses, or the WAL span it needs must be pinned
across probe→attach so the boundary cannot move underneath it.

Not fixed here because the choice between those is a real design call --
quarantining a snapshot discards work that is usually still good, and a
retention pin puts a replica's boot in the master's retention path -- and
guessing at it is how the last one came back.

## Secondary, and worth separating

`flint-ctl/src/main.rs:4892` waits a **hardcoded 60 s** for reconvergence,
while `scale-cluster/run.sh` computes a restart-readiness budget explicitly
sized for "a full re-seed: a wiped node transfers its whole checkpoint before
it binds its listener". Those two numbers are answering the same question and
do not agree. Raising the 60 s alone would have converted this failure into a
slower failure, not a pass, so it is not the fix -- but a deadline that is
independent of dataset size will keep producing "never reconverged" for
reasons that have nothing to do with the fault.

## Reproduction

`FLINT_SRC=<core worktree> ./packaging/aws/scale-cluster/run.sh --soak-mins 60 --kill-interval-mins 12 --ctl-from-source`

Cycle 1 green (final walk 275 present, 0 missing-or-regressed); cycle 2 as
above. Chaos replay seed `1787810654671675139`. Evidence bundle
`/tmp/flint-scale-evidence-20260826-230527`. Teardown verified clean.

Unrelated to OPS-0058's per-run keyspace change, which was in this same run:
that change touches only the chaos harness's key names, and `flint-ctl` and
the data plane were byte-identical to rc.64 here (14 commits in
`v0.1.0-rc.64..HEAD`, **0** of them touching `crates/flint-ctl/`, and the data
plane came from the published bundle).
