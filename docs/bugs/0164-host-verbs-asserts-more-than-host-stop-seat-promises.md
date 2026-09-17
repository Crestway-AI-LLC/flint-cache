# BUG-0164: `host_verbs` asserts a property `host-stop-seat` does not promise, and lost a gate run on it (FIXED 2026-09-17)

Status: **FIXED 2026-09-17**, found the same day when a public gate run failed on
it · Severity: **low as a defect, medium as a gate** — nothing in the product is
wrong; a drill that can fail for a reason its subject never promised is a red
main somebody has to diagnose, and the diagnosis is not obvious.

## What failed

The 17:52 gate run on the `bug0160` lane:

```
== host-stop-seat: the seat goes, and the port comes back
FAIL: pid 261301 still alive after host-stop-seat
```

`GATES FAILED: host_verbs` after 161 steps, and nothing else in the run failed.

**The verb had exited 0.** The line above it is
`|| fail "host-stop-seat exited non-zero"`, so `local_stop_seat` had returned
`Ok` — which it only does after `wait_port_free` succeeds. The port was already
free and the seat was not serving anything when the drill called it a failure.

## Why the assertion is stronger than the contract

`local_stop_seat` in `crates/flint-ctl/src/main.rs` kills the pidfile, then
loops until `pids_matching(bin, ident)` is empty — an args match over `ps` —
and then waits for the port with `wait_port_free`. It never waits for the pid to
be **reaped**, and it should not: reaping is the parent's business, and what the
caller needs is the port, which is what the next `host-spawn` binds.

`kill -0` succeeds for a process that has exited and has not yet been reaped.
That is the gap: a seat can be gone from `ps -eo args=` (a zombie carries no
cmdline), have released its port, and still answer `kill -0`.

## What is measured, and what is inference

**Measured:** the failure above, with `host-stop-seat` exiting 0, under the
gate's 4-way parallel drills (the drill's own env line recorded load 4.20 on 4
cores, and 6 seats belonging to 4 live peer drills). Then **5 of 5 sequential
re-runs passed** on the same box with the same tree.

**Inference, not proof:** that the pid was a zombie in that window. Nothing
sampled `ps` at the moment of the failure, so the zombie explanation is the one
consistent with the evidence rather than the one demonstrated. What the evidence
does establish is the part that matters: the verb kept its contract and the
drill failed anyway.

## The fix

The assertion gets a budget, matching the idiom already used in
`batch_commit_failure_drill.sh` and `collection_admission_drill.sh`: poll
`kill -0` for ten seconds, then fail.

**It keeps what the check is for.** A `host-stop-seat` that killed nothing —
the rc.12 class of failure, where a roll reports success having changed nothing
— leaves the pid alive far past ten seconds, and still reddens this drill. What
it drops is the race against reaping, which no caller depends on.

**Not changed: the verb.** Waiting for a reap would mean waiting on a parent
process flintctl does not control, for a property nothing needs.

## Filed as its own number rather than fixed silently

The failure sat in a gate run for a change to the control plane's Raft
dispatch, which cannot affect a drill that never starts a control plane. Writing
it down is what stops the next person reading it as "the control-plane change
broke host-spawn" — the reading I had to rule out before I could push.
