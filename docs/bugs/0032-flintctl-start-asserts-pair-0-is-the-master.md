# BUG-0032: `flintctl start` asserts the inventory's FIRST pair member is a live master (OPEN)

Status: OPEN, found 2026-08-19 on the playground · Severity: medium — after
any failover, `start` panics whenever the declared-first member is down,
which converts "one seat needs restarting" into "the start path is dead"

## Symptom

    thread 'main' panicked at crates/flint-ctl/src/main.rs:3085:9:
    master 172.31.64.94:7002 up

Six times in eleven minutes, once per `flint-supervise` tick.

## Root cause

`crates/flint-ctl/src/main.rs`, in the start path:

```rust
for pair in &inv.pairs {
    assert!(
        wait_pong(&pair[0], &tls, Duration::from_secs(10)),
        "master {} up",
        pair[0]
    );
}
```

`pair[0]` is the first member **as written in the inventory**, not the
current master. The playground declares `pair 172.31.64.94:7002,172.31.64.94:7001`
and 7001 has been master since the failover of 2026-08-18. So the assert
demanded a PONG from the REPLICA and called it the master.

Two defects in one line. The label is wrong — it will mislead anyone reading
the panic, and it misled this investigation for several minutes. And the
check is fatal where it should be tolerant: `start` exists to bring seats up,
so a seat that is down is its input, not an error.

## Consequence

`flint-supervise` runs `start` every two minutes to restore stopped seats.
With the declared-first member down, `start` panicked before it could
restart anything — so the one mechanism that would have kept trying was
disabled by the condition it exists to fix. Combined with
[BUG-0031](0031-needs-reseed-is-cleared-by-the-start-that-should-honour-it.md)
it produced a stable failure: a node that could not re-seed, and a
supervisor that could not survive noticing.

## Fix

Assert on the pair having a reachable MASTER, resolved by role rather than
by position — the CP already knows which member holds the epoch. If no
member answers, that is the real finding and deserves a message that says
so. A member being down while the other serves is normal after a failover
and must not be fatal to `start`.

## Fixed 2026-08-19 — and the second site is NOT the same bug

`start` now requires that SOME member of each pair answers, stopping at the
first that does (the common post-failover case is that the second one does, and
paying the full 10 s budget on a dead first member would add ten seconds to
every supervise tick). One member down while the other serves is reported and
tolerated; no member answering keeps its non-zero exit and says plainly that
this is not a stopped seat.

**There is a second `wait_pong(&pair[0], …)` in this file, and it is correct.**
`expand()` asserts on `pair[0]` of a BRAND-NEW pair it has just started — no
failover history exists, so position and role coincide, and "new master up" is
accurate. A pair that will not come up is also a genuine error for `expand`,
where it is not for `start`. Two lines that look identical; only one is the
defect, and changing both would have been pattern-matching rather than fixing.

### Verification status: BLOCKED ON THE BOX, not done

Compiles and lints clean in both feature configs. The behavioural checks below
have NOT been run: this machine is at 92% swap and cannot bootstrap a fleet —
`cold_start_roles_drill.sh` fails at bootstrap with "127.0.0.1:7403 is not
master after bootstrap", and it fails **identically with this change stashed**,
so the failure is environmental rather than a regression. That baseline
comparison is the only reason the fix is committed unverified; it is not
evidence the fix works.

## Three drill constructions that do NOT reproduce this, 2026-08-19

Attempted to add the verification below to `cold_start_roles_drill.sh`, which
ends with exactly the right precondition — a failed-over pair where inventory
`pair[0]` is the replica. Three constructions failed, each for a different and
instructive reason. Recorded so the next attempt starts from the fourth.

**1. Stop `pair[0]`, run `start`.** PASSED against the UNFIXED binary, so it
tested nothing. `start` SPAWNS every seat before it checks any of them, so the
command under test restarts the stopped seat and it then answers. A merely
stopped seat cannot reproduce a bug about a seat that will not answer.

**2. Hold `pair[0]`'s port via `HOLDER=$(hold_ports 7403)`.** The holder never
held: command substitution runs the function in a SUBSHELL, so the background
process did not outlive it. `start` brought `pair[0]` up and reported
`pair 0: 127.0.0.1:7403 answering`. The precondition was set up and never
checked — which is the same defect as the bug being tested, one level up. Any
future version must ASSERT the port is held (bind-probe) before running `start`.

**3. Hold the port correctly.** `start` refuses earlier, with its own guard:

    re-seeding 127.0.0.1:7403 onto 127.0.0.1:7404: port 7403 still bound after
    the process was gone — refusing to start a replacement that would die with
    AddrInUse and leave nothing serving

That refusal is correct and unrelated to this bug. A held port is a DIFFERENT
condition from the one on the playground, where `pair[0]`'s port was FREE: the
seat started, marked itself for re-seed, and exited, over and over
(docs/bugs/0031). It never answered because it kept dying, not because
something else held its address.

**So the fourth construction needs a seat that STARTS AND EXITS**, leaving its
port free — the crash-loop shape. Candidates not yet tried: a data directory
the engine cannot open, or a pair whose roles AGREE so `start` takes no
re-seed path and the AddrInUse guard is never reached.

**Status: this fix remains behaviourally unverified.** It lints clean in both
feature configs and the reasoning is in the commit, but no test has yet
demonstrated it fixes anything, and three that looked like they had did not.

## Verification the fix still needs

- a pair whose inventory-first member is down: `start` restarts it and exits
  0, with a positive control that the same command still fails when NEITHER
  member answers
- the message names the role it actually checked, and a failover before the
  check does not change which member the message refers to

## Related

- [BUG-0031](0031-needs-reseed-is-cleared-by-the-start-that-should-honour-it.md)
