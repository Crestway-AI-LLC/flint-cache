# BUG-0059: the D12.9 resilience spike had been failing, and the gate never ran it (FIXED)

Found 2026-08-26 while fixing BUG-0058, by looking for where a "tier down"
check belonged and discovering the existing one was not wired to anything.
**Severity: medium as a defect, high as a signal** — the spike covers what
ADR-0023 D12.9 calls "the property that decides deployability", it had been
returning FAILED, and nothing was reading its result.

## Symptom

Run by hand against a healthy tier and a standard fixture:

```
[FAIL] healthy tier: cold and warm reads both correct (1 tier hits)
     -- killing the tier --
[FAIL] TIER DOWN: reads still succeed and verify (4 tier failures observed, 4 degraded to origin)
[ok] and they degrade FAST, not merely eventually (109 ms for 4 reads)
[ok] armed-check: the failures were actually OBSERVED, not silently absent
RESILIENCE SPIKE FAILED
```

Confirmed against the unmodified file at `HEAD`, so this was not introduced by
the change that found it.

## The wrong conclusion drawn first

That reads through a dead tier were returning **wrong bytes** — the two failing
checks are both content checks, and "degrades fast" and "failures observed"
passing made it look like the fall-through worked but delivered corruption.

The data was fine. The *expectation* was stale.

## Root cause

Two independent defects, and the second is why the first survived.

**1. The content expectation lost a field.** `counting_s3.py` generates block
`i` of a key as `md5("{key}:{gen}:{i}")`. The `gen` field was added so an object
could be MUTATED — without it the staleness contract is untestable.
`ResilienceSpike.expected()` still computed `md5("{key}:{i}")`, so every byte it
predicted was wrong, for correct data.

**2. The gate never ran it.** `grep -rn ResilienceSpike --include='*.sh'` across
the repo returned nothing. It was a spike that nobody promoted, so its result
went to a terminal nobody was watching, and it drifted out of date in silence.

Two further defects were latent in it, invisible while it was never run:

- It shelled `valkey-cli -p 9399 shutdown` while dialling whatever `redis`
  argument it was given. **On any other port it killed nothing**, and every
  check after "-- killing the tier --" would have passed against a tier that
  was fully healthy. This is the same split Suite.java's `tierPort()` comment
  records having already been caught once.
- It assumed valkey. **Flint implements no `SHUTDOWN`**, so under
  `FLINT_TIER_ENGINE=flint` the kill was a no-op and the resilience checks
  measured a live tier.

## Fix

- `expected()` includes the generation, with the constant named and the reason
  written down.
- The kill goes through `Suite.killTier(conn)`, which shuts down over the
  connection already held, escalates to a signal on whoever holds the port, and
  returns only once the port REFUSES — a socket check, because a failed PING is
  also true of a hung server that is still alive. Suite's tier helpers were made
  public for this.
- The spike restarts the tier before exiting. `gate.sh`'s `start_svcs` exits
  when the tier does not answer, so a suite that kills the tier and walks away
  takes down whichever suite runs next, reported against the wrong one.
- **Gated** as "tier killed mid-job (4 checks)".

## Verification

4/4 pass, and the tier answers PING afterwards. The full accelerator set is
green with the change: client 25, S3A 9, adoption 10, SSE-C 5, integrity 21,
tier-down-at-build 10, mid-job-kill 4 — 84 checks, 0 failures.

**The positive control here is the history, not a construction:** the corrected
expectation turns a FAILED spike green without touching a line of product code,
which is what distinguishes a stale test from a real defect. Had the bytes truly
been wrong, no change to `expected()` could have fixed it.

## Related

- `docs/bugs/0058-*.md` — the defect this hunt was for. A tier down at *init*
  reached a real Spark job partly because the tier-outage test was not gated.
- `flint-cache:docs/adr/0023-*.md` D12.9.
- The family: a check that cannot fail is worth less than no check, because its
  silence reads as a pass. Here the check could fail, did fail, and was not read.
