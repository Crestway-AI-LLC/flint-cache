# BUG-0102 — three more remote call sites read a failed ssh as an answer (FIXED 2026-09-05)

**Status: FIXED 2026-09-05.** Found the same day by auditing every
`Runner::output` call site in `flint-ctl` after [BUG-0100](0100-a-remote-host-that-could-not-be-asked-counts-as-clean.md)
fixed two of them · Severity: medium-high, carried by one of the three.

## The class, restated once

`Runner::output` returns `Err` **only when the `ssh` binary cannot be
spawned**. An ssh that never reached the host is a successful spawn of a
process that exits 255 — `Ok`, non-zero status, empty stdout. Any call site
that branches on `Err`, or on the shape of stdout, treats "I could not ask"
as an answer.

BUG-0100 fixed `stop` and `sweep_orphans`. The audit found six more sites;
three were already correct and are worth naming, because they are the model:

| site | what it does | verdict |
|---|---|---|
| `spawn` | `assert!(out.status.success(), …)` | correct |
| `mark_reseed` | checks status, returns `Err` with the stderr | correct |
| `stop_seat` | checks status, and explains that 255 means the connection | correct, and the best of them |
| `push-bins` unpack | matches on `Ok(out) if out.status.success()` | correct |
| **`seat_alive`** | `let Ok(out) = … else { return false }`, no status check | **broken, and the dangerous one** |
| **`kill_pidfile`** | `let _ = r.output(&argv);` | **broken** |
| **statedir `mkdir -p`** | `if let Err(e) = …` only | **broken** |

## 1. `seat_alive` — an unreachable host read as "no seat is running"

This is the one with teeth. Every caller uses it in the same shape:

```rust
if seat_alive(…) { eprintln!("… left alone"); continue; }
spawn(…);
```

so `false` means **spawn one**. And the call sites already say what a wrong
`false` costs, in comments written before this defect existed:

> *spawning beside it gives the duplicate a lost port race and a clobbered
> pidfile — after which every stop aims at a corpse.*

An ssh that failed produced empty stdout, which parsed to no pids, which is
`false`. So a host that could not be reached was reported as a host with no
seat, and `start` would spawn beside whatever is actually there. The author was
alert to this hazard from a different cause — the function carries a comment
about not passing a local pid into the remote parse for exactly this reason —
and the transport one was missed.

**Fixed by answering TRUE when the host cannot be asked**, and saying so.
Leaving a seat alone that may not exist costs a `start` that did nothing;
spawning beside one that does costs the pair.

### and that fix broke a diagnosis, which is why the probe is now three-valued

`bootstrap`'s post-PING failure exists to say WHICH of two causes it saw:

> *A TIMEOUT HERE HAS TWO CAUSES AND THEY LOOK IDENTICAL FROM `wait_pong`.
> Either the seat never really started … or it started and something blocked
> it. "control plane seat up" said neither, and that is the whole of what an
> operator got.*

With `seat_alive` conservatively answering `true`, that site began asserting
**"its PROCESS IS RUNNING — it started and something is holding it"** about a
host that does not resolve. A boolean forces a third cause to be reported as
one of the two, and it picked the confident one.

So `seat_probe` is split out, returning `Option<bool>`, and the diagnosis names
three causes. `seat_alive` is `seat_probe(…).unwrap_or(true)` — the safe answer
for the decision, with the honest answer still available to the one caller
whose job is to name a cause.

Caught by the mutation control for the statedir fix below: with the statedir
check removed the run proceeds far enough to reach this panic, which is how the
wrong message was seen at all.

## 2. `kill_pidfile` — `let _ = r.output(&argv)`

Both the `Err` and the status discarded, so a kill that never reached the host
was indistinguishable from one that worked, and every caller goes on to treat
the seat as dead. Now reports a non-zero status, saying the seat may still be
running.

## 3. the statedir `mkdir -p` — only `Err` was checked

A `mkdir` refused for permissions, or an ssh that never landed, was accepted.
The run then failed further down at a spawn that could not write its pidfile —
a message naming the wrong step, on a host the operator had no reason to
suspect. Now dies at the step that actually failed, with the exit status and
the remote stderr.

## Held by `tools/host_verbs_drill.sh`

Only the third is reachable from one machine, and that is a fact about the
code rather than a gap in the drill: with the fix in place `start` **dies at
the statedir** before it can reach `seat_alive`, and `kill_pidfile` is called
only from the roll and swap paths, which need a live fleet. Stated here so the
coverage is not read as broader than it is.

    == start refuses when it cannot prepare a host's statedir
       flintctl: preparing statedir on nobody@nosuchhost.invalid: exit 255
         (ssh: Could not resolve hostname nosuchhost.invalid: …)

The arm asserts three things: that the failure names the **statedir step**,
the **host**, and the **ssh exit status** — the last because 255 is what says
the connection failed rather than the command.

**Mutation-checked**: with the statedir check back to `if let Err(e)`, the
drill fails, and the run gets far enough to print the three-valued diagnosis
above.

### The fixture host changed, and it paid for itself

`nosuchhost.invalid` (RFC 2606's reserved TLD, guaranteed never to resolve)
replaces `192.0.2.1` (TEST-NET-1). Both are safe and need no fixture machine;
the address cost **twenty seconds a run** in `ConnectTimeout`, and a name that
does not resolve fails in milliseconds. Nothing under test can tell them apart
— both are ssh exit 255 with a line on stderr, which is exactly what these
arms read. The whole drill went from 27 s to under 3.

## Found by

Not by a failure. By reading every `Runner::output` call site after BUG-0100,
on the assumption that a defect which appeared twice in one file appears more
than twice. It appeared three more times, and the audit's cheapest finding —
that three OTHER sites already did it right — is what made the wrong ones
obvious.
