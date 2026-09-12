# BUG-0134: a failing drill assertion hangs the job instead of failing it (FIXED 2026-09-12)

Status: **FIXED 2026-09-12**, found the same day · Severity: **high for CI** —
one drill's assertion cost a 60-minute job that reported **nothing at all**,
for itself or for the 129 drills that finished beside it, and GitHub records
the result as `cancelled`, which reads as somebody's decision.

**This is why the run hung, not why the assertion fired.** The assertion
itself — an unquotad tenant receiving `-THROTTLED` — is open as
[BUG-0135](0135-an-unquotad-tenant-was-throttled-while-a-pinned-neighbour-ran.md).
Neither is BUG-0132.

## What was observed

`gate` on `cb9ede6`, pushed 2026-09-12 04:40:43Z. Job `gate (drills)` ran
**60m16s** against `timeout-minutes: 60` and was killed. Its console log ends:

```
04:42:36  PASS  evictable_pressure     (4.8s)
04:42:36     130 drills, 4 at a time
05:40:59  ##[error]The operation was canceled.
```

**Fifty-eight minutes, no output.** The orphan processes the runner terminated
afterwards name the shape of it: one `flint-controlplane`, one `flint-server`,
two `flint-proxy`, one `python3` — a single drill's fleet, still up.

The job's log says nothing about which drill. `gate-logs-drills` does, because
`upload-artifact` runs even on a cancelled job: `drill-tenant_quota.log` ends
mid-arm with no verdict.

```
== isolation: globex (unlimited) at full speed while acme is pinned
Traceback (most recent call last):
  File "<stdin>", line 31, in <module>
  File "<stdin>", line 20, in measure
AssertionError: unquotad tenant got throttled
```

## The defect

`tools/tenant_quota_drill.sh`, four lines as they stood:

```python
stop=[False]
def acme_hammer():
    pin(7911,"tok-acme",200,stop)
...
t=threading.Thread(target=acme_hammer); t.start()
beside,beside_p99=measure(g,3,"beside")   # line 31 -- asserts on line 20
stop[0]=True; t.join()                    # never reached
```

`measure` asserts. The assertion skips the statement that stops the hammer
thread; the thread loops `while not stop_flag[0]`; it is **not a daemon**, so
CPython's `threading._shutdown()` joins it at interpreter exit — forever. The
traceback is printed, and then the process never leaves.

Verified rather than reasoned about, with a positive and a negative control:

| control | result |
|---|---|
| non-daemon thread on a stop flag, exception before the flag is set | **hangs** past 6s, traceback on stderr |
| same, with `stop[0]=True` before the raise | exits, status 1 |
| same, with `daemon=True` and the flag never set | exits, status 1 |

So `daemon=True` is the property that separates a hang from a report. It is
also the only one of the three a regex can see, which is what the new check
below enforces.

## Why it cost the whole job, and read as `cancelled`

Two amplifiers, neither of them in the drill:

**The parallel batch reports only when it is complete.** `run_core_drills`
writes each drill's output to a file and replays every result through
`step_report` *after* `xargs` returns (the comment says why: the leak check is
most accurate once nothing else legitimately holds seats). So one drill that
never exits suppresses **129 verdicts that already existed on disk**. The
serial path does not have this property — it reports each drill as it
finishes.

**Nothing capped a drill.** `timeout-minutes: 60` is the only bound, it is the
whole job's, and GitHub reports a job it kills at that bound as `cancelled` —
the same word a human cancel produces. This survey missed the failure for that
reason: it filtered on `conclusion == failure` over the last 40 runs, and the
one live intermittent in the repo was `cancelled`.

## The class, which is three instances

The same shape shipped twice more, and in both of the others the statement
that never runs is skipped by an assertion **written to fire**:

| file | thread | what skips the stop |
|---|---|---|
| `tools/tenant_quota_drill.sh` | `acme_hammer` | `assert b"THROTTLED" not in b` |
| `tools/async_writes_drill.sh` | 16 × `storm` | a `connect`/`recv` that times out |
| `tools/rw_isolation_drill.sh` | `storm` | `assert lanes == "2"` |

This is the sibling of the defect this repo already refuses four shapes of. A
check that **cannot fail** certifies whatever it is pointed at; a check whose
**failure cannot be reported** does the same thing one step later. `rw_isolation`
is the clearest case: its assertion exists to catch ADR-0029's lane split not
being in effect, and the moment it succeeded at that, the drill would hang and
the job would die with no verdict.

## The fix

**1. The three sites.** `daemon=True` on every thread, and the stop flag moved
into a `finally` where that was a three-line change (`tenant_quota`,
`async_writes`). `rw_isolation` takes the daemon flag alone rather than a
forty-line re-indent; the property that matters is satisfied either way.

**2. Every thread in `tools/` is a daemon** — 18 of them, across 12 files.
Uniform on purpose: `daemon` governs interpreter shutdown and never `join()`,
so it is free wherever a thread is already joined, and a uniform rule leaves
nothing to reason about at the next edit.

**3. `assert_tools_threads_are_daemons`** in the `check` stage refuses a
`threading.Thread(` without `daemon=True` in `tools/*.sh`, `tools/lib/*.sh`
and `tools/lib/*.py`, naming file, line and source. It carries its own
positive and negative control on a planted file, and refuses if it matched
**no** threads at all. Its control lines are **assembled** (`threading.%s(`),
because spelled literally they are matched by its own regex — this function
lives in a file it scans, and the first run of it reported itself as the
defect. The license-header needle solves the same trap the same way.

**4. A per-drill cap, `tools/lib/drill-timeout.sh`.** The rule above cannot
see a drill wedged in a syscall, a fleet that never comes up, or a `wait` on a
process that will not exit; this layer does not need to know why. After
`FLINT_DRILL_TIMEOUT_S` (default **900s**) the drill gets `SIGTERM`, ten
seconds for its `EXIT` trap to tear the fleet down, then `SIGKILL`; the batch
records exit **124** and a `FAIL:` line that `step_report` surfaces, and any
seats left behind are named by the leak check that already runs.

- **900s is 2.3x the slowest drill, measured.** `ns_escape` takes 383-387s
  across four consecutive green CI runs (4 vCPU, 4-way parallel); `fleet_guard`
  is next at 122-125s. A cap sized to the slowest drill of the day is a cap
  that will kill a green run.
- **No `timeout(1)`.** GNU coreutils has it, this Mac has neither it nor
  `gtimeout`, and the gate runs on both. A cap present on only one of its two
  platforms is the rc.15 shape exactly — `wait_port_free` bound an address
  macOS permits and Linux refuses, so every local drill passed while the first
  real roll failed. The watchdog is a bash 3.2 poll, which is what
  `/bin/bash` is here.
- **One implementation, two callers**: the `ALONE` loop (still through
  `step`, so the accounting is identical) and the generated `worker.sh`.
- **The serial path is deliberately uncapped.** It is the path
  `gates_drill.sh` forges — forcing `FLINT_GATE_JOBS=1` and stubbing `step` in
  a tree that holds only `gates.sh` — so anything added there has to exist in
  that tree, and it is also the path that does not need a cap, because it
  reports each drill as it finishes. The library is therefore sourced inside
  the `JOBS>1` branch, and a missing library **refuses to run** rather than
  running uncapped.

## Verification

- The three controls in the table above (hang / stop-first / daemon).
- Cap fires: the BUG-0134 shape as a temporary drill under a 3s cap returns
  **124 in 3s**, log carries the traceback then the cap's `FAIL:` line, and no
  `python3` survives it.
- Cap does not fire when it should not: a fast passing drill returns 0 with no
  cap line; a drill that exits 7 returns **7**, not 124.
- Through the generated worker under `xargs -P2`: `.res` files read
  `124 3051` and `0 33`.
- The new check: green on the tree (`18 thread(s)`), **FAIL naming file:line**
  with one non-daemon thread planted, green again once removed.
- 12 embedded python blocks in the six edited drills parse; `quota_load.py`
  parses.

## What this does not explain

**Why the assertion fired.** An unquotad tenant received `-THROTTLED` while a
pinned neighbour ran — once, on a loaded 4-vCPU runner, at the `beside`
measure and not at `solo`. That is either tenant isolation leaking under load
or a measurement artifact, and one log line does not decide it:
[BUG-0135](0135-an-unquotad-tenant-was-throttled-while-a-pinned-neighbour-ran.md).

What changed is that the next occurrence **reports**. It did not, here.

## How it was found

By watching CI after a push, which the push discipline in
[BUG-0131's write-up](0131-the-unknown-lag-sentinel-reads-as-healthier-than-healthy.md)
already says to do and this session did not: `6ecc790` was pushed while
`cb9ede6`'s gate was still running, and that run was never opened. The word
`cancelled` is what kept it invisible afterwards — including from a survey of
CI failures written the same day.
