# BUG-0149 — a drill arm asserts on the second step and reports the first

**Status:** **FIXED 2026-09-15** (the misreport). The condition that exposed it
— an arm E that fails when upstream WAL retention moves — is **still open**;
see the last section.

## What it said, and what its own log said four lines up

`rewind_rejoin` arm E, on a gate box:

```
FAIL: arm E — a quarantined snapshot was not reconsidered under a LOWER fence.
      This is BUG-0071: the re-seed holds the write gate shut for the whole
      transfer at min-replicas-to-write=1 (94.2 s measured).
```

The dump printed directly underneath that message contained:

```
rewind: candidate unresumable-c999999-snap-...seq202-e0.1 clears the fence
        for epoch (0,1) (202 <= 202)
rewind: cannot resume from the restored copy against 127.0.0.1:6406 (refused:
        WALGAP cannot map upstream cursor 202 into this WAL ...: the span
        needed for an incremental rejoin is gone); full re-seed
```

**The reconsideration this arm exists to prove had worked.** The snapshot was
reconsidered, it cleared the lower fence, and the rejoin then failed at the
NEXT step because the upstream WAL no longer retained the span an incremental
rejoin needs.

## Why the message was wrong

The assertion is `grep -q "rewound to"`, and `rewound to` is only reached if the
quarantined snapshot is **both** reconsidered **and** resumable. So every
failure of the resume was announced as a failure of the quarantine rule, with
BUG-0071 named as the cause.

Those are different findings with different owners — one is the quarantine rule
this arm covers, the other is WAL retention against the drill's own timing — and
a reader who believed the message would have spent the afternoon in the wrong
file. This is the session's recurring shape: not a check that cannot fail, but
**a check whose failure cannot be attributed**.

## The fix

Arm E now asks which of its two steps failed. If `clears the fence` is present
and `rewound to` is not, it says so, prints the refusal line itself, and points
at WAL retention rather than at BUG-0071. Otherwise the original message stands,
because for a genuine reconsideration failure it was right.

Positive control: the branch was run against the recovered `qe-a2.log` from the
real failing run before the change was committed, and selects the
resume-refused arm with the `WALGAP` line quoted.

## Still open: why arm E failed at all

The product bytes did not change. `rewind_rejoin` passed twice today on this
same tree — 11.3 s and 12.4 s — and failed at 9.1 s on the third run, where the
**entire** delta from the passing commit was two lines inside a `#[cfg(test)]`
module (an `unwrap()` changed to `expect()` for a lint), which is not in the
release binary the drills run.

So the arm depends on something the drill does not pin: whether upstream still
retains the WAL span reaching seq 202 when the rejoin is attempted. Under a
4-wide gate that timing moves. Pinning it — a retention floor for the duration
of the arm, or a wait that proves the span is present before the rejoin — is
the real fix and is **not** done here: it is a change to a bring-up under a
drill that is green most of the time, and it wants its own run rather than
riding a diagnostic fix. The same order BUG-0147 argued for, for the same
reason: the decision needs the diagnostic first, and now the next occurrence
will say which half it is.
