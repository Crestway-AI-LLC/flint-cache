# BUG-0054: promote_notice's negative control subtracts two spawn-dominated samples

Status: FIXED 2026-08-26 — both wall-clock assertions replaced with mechanism
assertions, each verified by a positive control. · Severity: LOW as a product
matter, MEDIUM as a gate matter: it red main on a machine rather than on a
regression.

*(filename says "two readonly handlers disagree on whether to sleep" — that was
the first diagnosis, refuted the same day by measurement. Kept so links
survive. The retraction is below, because how a wrong diagnosis was killed is
the useful part.)*

## The red

`main` at `d7781ed` failed `gate (drills)` on `promote_notice`. The commit was
`accel: colocate an object's chunks in one slot`, which has nothing to do with
the proxy's promotion path; the other 126 steps passed. What failed was the
drill's **negative control**:

    == A) NEGATIVE CONTROL: failover with NO notice (today's behaviour)
      steady-state write (incl. cli spawn): 62ms — the yardstick
      first write after failover: 90ms
      that is 28ms above steady state
    FAIL: expected the reactive path to cost >=30ms above baseline (the 50ms
          retry sleep), got 28ms.

The drill is behaving well. It refuses to report run B's comparison when it
could not establish that run A is slow, which is the discipline that makes the
rest of the suite worth reading. The defect is that the condition it needs is
not one the run controls.

## The first diagnosis, and how it died

I proposed that the proxy's two `-READONLY` handlers explain it:
`forward_collect` (`main.rs:1426`) rediscovers and retries immediately, while
`forward` (`:1526`) rediscovers, sleeps 50 ms, then loops. Whichever serviced
the write would decide the number, giving a bimodal 28-or-50 rather than drift.

**That asymmetry is real in the source and is NOT the cause here.** Six local
runs of the unmodified drill:

    delta: 54, 62, 49, 57, 56, 54 ms      base: 37-39 ms

Tightly clustered around 55, every one on the sleeping path. No bimodality.

Second hypothesis, also mine, also dead: that a slow box gives the proxy time
to learn the promotion independently before the measured write. Inserting a
pause between the failover and the write refutes it — nothing tells the control
plane in run A, so the proxy stays stale however long you wait:

    pause:  0s    0.25s  0.5s   1s    2s
    delta:  55    61     64     61    55  ms

## What it actually is

Arithmetic, once the numbers are laid side by side:

    local:  base 37   slow 91-99   delta 49-64      (passes)
    CI:     base 62   slow 90      delta 28         (fails)

A 50 ms sleep cannot fit inside a 28 ms delta, so on CI the sleep DID fire and
the subtraction hid it. `base` is a median of three `valkey-cli` spawns; `slow`
is one more, independent. Spawn is the dominant term in both — measured here at
34-40 ms, spread 6 ms, against a 50 ms signal, which is why an unloaded box
never trips the floor. On the 4-vCPU runner spawn nearly doubles to ~62 ms and
its spread grows with it, so `slow`'s own spawn drawing 40 ms against a
baseline that drew 62 ms erases 22 ms of a 50 ms signal.

**The delta is a difference of two independent draws from a noisy distribution,
and the noise scales with load while the signal does not.** That is the whole
defect. It is a measurement-design flaw, not a product one, which is why the
red carried no information about the commit that triggered it.

Two tempting fixes are both wrong. Lowering the floor weakens a check to fit a
measurement. An absolute floor on `slow` does not discriminate either — on a
slow box, spawn alone clears it with no sleep at all.

## The fix

Both wall-clock assertions are gone; both sides now assert the mechanism.

The proxy logs every re-probe and names its trigger:

    [1787769502536] pair 0 master 127.0.0.1:6910 -> 127.0.0.1:6911 (re-probe triggered by 127.0.0.1:6910)

- **Run A** must contain a re-probe triggered by the DEMOTED seat. Nothing told
  the control plane, so the only way the proxy can learn is the hard way.
- **Run B** must contain NO such line. Told by the CP, the proxy must not have
  had to discover it by bouncing a write off `-READONLY`.

B's old `FAST_DELTA < 20` had A's problem mirrored — same subtraction, same
noise — and is implied by B's new assertion, since no reactive re-probe means
no retry sleep. Both deltas are still printed: the numbers are the evidence the
feature is worth having, they just no longer gate the run.

## Verified by positive control, both sides

A check that cannot fail is not a check, so each assertion was run against a
world where its mechanism is absent:

| scenario | expected | result |
|---|---|---|
| unmodified drill | passes | PASS, both mechanisms observed |
| run A, CP told *before* the measured write | A fails | failed on A's assertion |
| run B, notice pushed *after* the measured write | B fails | failed on B's assertion |

The first attempt at the second control simply deleted the notice, and it
failed on the pre-existing "hint must be in the snapshot" check without ever
reaching the new assertion — a shadowed control, which proves nothing. Pushing
the notice late instead keeps the hint present and moves only the timing.

## Still open, and deliberately not fixed here

The two-handler asymmetry is real even though it did not cause this. `forward`
sleeps 50 ms *after* rediscovery has already found the new master, which is
pure added latency on every controlled failover, and `forward_collect`
demonstrates that retrying immediately works. Worth its own bug and its own
decision; it is not a drill matter, and now that this drill asserts the
mechanism rather than the stall, fixing it will no longer red the gate.
