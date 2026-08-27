# BUG-0062: the probe clears a stale snapshot, then the attach races the WAL (OPEN)

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
