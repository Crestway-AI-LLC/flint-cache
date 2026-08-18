# BUG-0015: the FLINTSYNC probe admits a cursor the WAL cannot serve, so a gapped replica never re-seeds (RESOLVED)

Status: **RESOLVED** 2026-08-18, landed in `0a763ff` ("repl: make WAL retention
an admission term, not a discovery the stream makes"), now on `main` ·
Severity was **high** — a replica that falls out of the WAL window could never
recover unattended, and under a supervisor it crash-looped forever

**This doc read OPEN for several hours after the fix was on `main`.** Caught
2026-08-18 when a peer session auditing stale statuses checked the code rather
than the docs. A bug that is fixed but still reads OPEN costs the next person a
re-investigation, which is the same waste as a bug that is broken but reads
closed — the status is a claim like any other, and it needs the same
verification. Confirmed here by grepping `origin/main` for the admission term,
not by trusting this file.

## Symptom

`tools/reseed_drill.sh` fails its last stage on every gate run since
2026-08-16. The replica log, captured by the drill's new forensics:

    marked copy verified against the lineage held by 127.0.0.1:6471:
      warm rejoin at seq 3 (epoch (0,1))
    marked rejoin continues; tailing from seq 3
    replicating from 127.0.0.1:6471 starting at seq 3 (epoch (0,1))
    flint-server listening on 127.0.0.1:6472 (plaintext)
    FATAL: WALGAP full sync required: sequence 4 is no longer in the WAL
      (latest is 20003) — this link can never resume. Marking for re-seed and
      exiting; the next start will full-sync from a checkpoint.

    still running? no
    exit status: 3
    marker still present: cannot resume this tail: WALGAP ... sequence 4 ...

## Root cause

`f9782c4` made the `NEEDS_RESEED` marker mean "verify this copy" instead of
"discard it". A marked boot calls `probe_resume` (`flint-server/src/main.rs`),
whose docstring promises:

> The master runs its whole admission logic — fence check, cursor
> translation, **WAL-span retention** — and answers before any byte ships.

**The retention half of that promise is not kept.** The FLINTSYNC handler
does the fence check, then translates the cursor only when the epoch differs,
then writes `FLINTSYNC-OK` — there is no retention check on that path.
Retention is discovered somewhere else entirely: `batches_since` in
`flint-storage/src/repl.rs:168-175` finds it by BUILDING a batch and noticing
the result is empty. `probe_resume` drops the connection after the first
reply, so it never reaches the code that would have refused it.

So the loop is closed and permanent:

1. marked boot → probe → master answers OK for seq 3
2. node clears `NEEDS_RESEED` and warm-rejoins at seq 3
3. the tailer asks for seq 4, which is gone → FATAL → re-marks → exit 3
4. next start: identical, forever

Before `f9782c4` the marker meant "discard and re-seed", which worked. The
regression is that a node now *clears its own recovery marker* on the strength
of an answer that did not check the thing the marker was recording.

## The fix, and why it must be shaped this way

In the FLINTSYNC handler, before answering OK, attempt the same batch build
the stream would (`batches_since(cursor)` with a small byte budget) and refuse
with the WALGAP error when it returns `ReplError::WalGap`. A caught-up replica
does not false-refuse: `batches_since` returns an empty Ok when there are
simply no newer sequences, and only reports `WalGap` when newer sequences
exist but the span back to the cursor does not.

**Reuse the streaming check; do not write a second one.** A standalone
"is seq N retained?" predicate would be a second implementation of the
retention rule, free to drift from the one that actually serves bytes — which
is exactly how the probe and the stream came to disagree in the first place.

## The coverage gap this exposes, which the fix must close with it

The public gate does NOT cover the case `f9782c4` exists to enable. The
reseed drill's positive control — "a warm restart must NOT re-seed" — is an
UNMARKED restart, which never calls `probe_resume`. The MARKED warm rejoin is
exercised only by the soak. So a fix that refuses too eagerly would break
every warm rejoin, make every dead seat pay a full re-seed, and **pass this
gate**. A drill for the marked-warm-rejoin positive case belongs in the same
change as the fix.

## Not yet done

The fix is not implemented. Implementing it without being able to run the
drill would mean changing replication admission logic on reasoning alone,
against a suite that (see above) cannot catch the dangerous direction of a
mistake.

## Related

- `f9782c4` — "rejoin: verify the copy before discarding it"; the commit whose
  first gate run went red, and whose docstring states the guarantee that is
  missing
- `crates/flint-storage/src/repl.rs:168-175` — where retention IS enforced,
  and the comment explaining what a missed gap looks like from outside
- #176 — a syncing replica is invisible; why the port binds late here
