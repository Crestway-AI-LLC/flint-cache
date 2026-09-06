# BUG-0100 — `stop` reports a host it never reached exactly as it reports a clean one (FIXED 2026-09-05)

**Status: FIXED 2026-09-05**, and held by `tools/host_verbs_drill.sh`. Found
2026-09-05 by reading `flint-ctl`'s remote-runner call sites while narrowing
which of them the roadmap could still call untested · Severity: medium — the
fleet-wide `stop` is what a roll and a teardown are built on, and its silence
was indistinguishable from success.

## Symptom

None, which is the whole defect. `flintctl stop` against a fleet with an
unreachable host printed:

    no pidfiles — sweeping by process instead

That is what a clean single-host fleet prints. Seats on the unreachable
machine were untouched and nothing said so.

## Root cause: `Err` is not how a remote command fails

`Runner::output` is

```rust
fn output(&self, argv: &[String]) -> std::io::Result<std::process::Output> {
    let full = self.wrap(argv);
    Command::new(&full[0]).args(&full[1..]).output()
}
```

so it returns `Err` **only when the `ssh` binary itself cannot be spawned**.
An ssh that never reached the host — unreachable address, refused key, sudo
denied, no `flintctl` on the far side — is a perfectly successful *spawn* of a
process that then exits 255. That is `Ok`, with a non-zero status and no
stdout.

Both remote call sites branched on the wrong thing:

- **`stop`** matched `Ok(out)` and looped over `out.stdout.lines()`. There
  were no lines, so it printed nothing. Its `Err` arm — the one that says
  `stop failed` — fires for the single failure that never happens in the
  field.
- **`sweep_orphans`** had no error arm at all: `if let Ok(out) = …`, then
  `.parse().unwrap_or(0)`. A host that could not be asked contributed `0`
  swept, and `stop`'s only summary is `if swept > 0`, so nothing printed.

Two arms of the same loop, in the same function's call graph, both silent —
and `sweep_orphans` folded three different outcomes into the number `0`:
*no orphans*, *could not reach the host*, and *the remote verb printed
something unexpected*.

## Why it matters more than a missing log line

`stop`'s own opening comment states the invariant it breaks:

> Each host keeps its OWN pids dir, so "stop the fleet" means asking every
> machine to stop what it is holding. Reading only the local directory would
> leave remote seats running while reporting success.

That is exactly what happened, one level down: it *asked*, the ask failed, and
the failure was discarded. A `stop` that reports success while a remote seat
keeps serving is the precondition for the failure class this tree already has
several of — a replacement started against a live predecessor, two masters on
one pair, a roll that "succeeded" having changed nothing.

## Fix

Check the STATUS, in both arms, and say what the failure means for the thing
the caller cared about:

    [nobody@192.0.2.1] host-stop-all exited 255 (ssh: connect to host
      192.0.2.1 port 22: Operation timed out) — seats there may STILL BE RUNNING
    [nobody@192.0.2.1] host-sweep exited 255 (ssh: connect to host
      192.0.2.1 port 22: Operation timed out) — orphans there are UNKNOWN, not zero

The `Err` arms stay, reworded to say what they actually mean ("could not run
stop"), and `sweep_orphans` now separates an unparseable count from a zero
one. The last line of the remote stderr is included because "exited 255" alone
sends the reader to the wrong machine.

**No behaviour changes beyond reporting.** `stop` still proceeds to the next
runner and still sweeps; the point is that its output now distinguishes what
it did from what it could not do.

## Held by `tools/host_verbs_drill.sh`

The drill covered the CALLEE half — the `host-*` verbs run directly, without
ssh. This is the caller half, for the one question it can be asked without a
second machine: what does `stop` say about a host it could not reach?

`192.0.2.1` is TEST-NET-1 (RFC 5737), guaranteed unroutable, so the arm needs
no fixture host and cannot accidentally reach one. Every seat in the fixture
inventory is placed there, so nothing local is stopped either. It asserts four
things: the output names the host, and for each arm says what is unknown —
`STILL BE RUNNING` for the seats, `UNKNOWN, not zero` for the orphans.

**Both arms are asserted, deliberately.** They failed for the same reason and
one assertion would have left the other uncovered — and the uncovered one is
the more expensive: an unswept orphan is a resource leak, a seat that was
never stopped is a correctness problem.

**Mutation-checked, one per arm:**

| mutation | result |
|---|---|
| `stop`'s status arm removed | FAIL: stop did not report that host-stop-all failed |
| `sweep_orphans` back to `if let Ok(…)` + `unwrap_or(0)` | FAIL: stop said nothing about the unreachable host — output was `no pidfiles — sweeping by process instead` |

The second mutation's output is the bug verbatim: against a fleet with an
unreachable host, `stop` printed exactly what a clean fleet prints.

## Not covered

Whether `stop` should FAIL rather than report. It currently exits 0 having
told the operator that seats may still be running, which is right for a
teardown driven by a human reading the output and wrong for one driven by a
script. Changing the exit code touches every caller of `stop` and is a
separate decision; the reporting had to come first, because until now there
was nothing for a caller to act on.
