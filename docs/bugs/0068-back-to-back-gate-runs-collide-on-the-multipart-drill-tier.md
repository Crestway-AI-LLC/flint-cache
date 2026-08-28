# BUG-0068 — back-to-back gate runs collide on the multipart drill's tier (OPEN, undiagnosed)

**Component:** `s3-accelerator`, `tools/gate.sh` + `tools/multipart_etag_drill.sh`
**Observed:** 2026-08-27, once. **Cause not established** — this file is
evidence, not a diagnosis.

## Symptom

Running the two engine arms back to back:

```
bash tools/gate.sh                 # valkey
bash tools/gate.sh --engine flint  # flint
```

the valkey arm failed one stage:

```
FAIL  multipart etag key shape (7 checks) (rc=1)
```

and the drill's own log said:

```
FAIL: port 9400 in use -- refusing to adopt a tier this drill did not start.
```

The refusal is correct behaviour and was added deliberately: the drill owns its
tier on 9400 and will not adopt one it did not start. So the failure is not the
guard, it is whatever was still holding 9400.

## What made this expensive to read

The first log I opened showed **7 passed, 0 failed** — because the second arm
had already run and `gate.sh` rotates `/tmp/gate_*.log` at the start of a run.
The passing arm's log had replaced the failing arm's. The failing one was in
`/tmp/gate_prev/gate_multipart.log`, which is where the real message was.

**A drill's exit code is not its oracle** is already in the field notes; this is
the neighbouring trap — *a drill's log is not necessarily this run's log*.

## Evidence

- 9400 was **free** immediately after the run, so nothing was leaked
  permanently.
- Re-running the valkey arm alone, with 9400 verified free first, passed
  **33/33**. The flint arm had already passed 33/33.
- Nothing added in this change binds 9400. The stage added alongside it uses
  9314 and the slow proxy on 9398.
- Both the multipart drill and the cross-language drill use `TIER_PORT=9400`,
  and each starts and shuts down its own tier there.

## What has NOT been established

Which process held the port, and whether it was a tier from the *preceding*
gate run still shutting down or something within the same run. The window is
short and the run that showed it has been overwritten. Reproducing it means
running the two arms back to back again and sampling `lsof -ti tcp:9400` at the
moment the drill refuses.

## Why it is written down anyway

Because the next person to hit it will see a stage fail whose own log says
everything passed, and will lose the same twenty minutes finding
`/tmp/gate_prev`. If it recurs, the fix is probably for a drill that owns a port
to wait for it to be released rather than for the next user to discover it is
not — the same shape as BUG-0061, where a cleanup returned before the deaths it
asked for had happened.

## Update: a pegged core, and what it does and does not explain

A second transient failure the same day, a different stage: `client suite
(24 checks)` returned **rc=124** — the harness timeout, not an assertion — hung
at the 24-concurrent-reader phase with a `TimeoutException` out of AAL. The
other engine arm ran the identical suite and passed.

The machine was carrying a **runaway process**:

```
python -c "import glob,json,sys
for p in glob.glob('/**/botocore/data/dynamodbstreams/*/service-2.json*',
                   recursive=True)[:1]: ..."
```

A recursive glob rooted at `/`, started the previous day, **25.8 hours of CPU
consumed**, one core pegged continuously. It cannot usefully finish and its
whole output is a single `print`.

**What that explains:** a concurrency assertion with a wall-clock bound is
exactly what a permanently-missing core turns intermittent. Both failures were
load-shaped — one a timeout, one a race — and neither was a wrong answer.

**What it does NOT explain, and this is the honest limit:** nothing here shows
the pegged core caused the 9400 collision above. That is a plausible common
cause, not a demonstrated one, and writing it down as the answer would close a
file that is still open. The port question stands as stated: who held 9400.

**The reusable part.** Before diagnosing an intermittent gate failure as a
defect in the code under test, look at what else is running on the box. Two
different stages failed two different ways in one afternoon on a machine that
was quietly a core short, and both looked like product regressions. `uptime`
and a CPU sort cost seconds; the first of these cost considerably more.

