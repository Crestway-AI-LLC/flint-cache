# BUG-0105 — `pipeline_nodelay`'s positive control cannot be armed on macOS, so the drill is a permanent local red (OPEN)

**Status: OPEN.** Found 2026-09-05 while establishing which of six drill
failures in a local `gates.sh drills` run belonged to an unrelated change ·
Severity: low for the product, higher for the gate: a check that can never
pass on the machine people run it on is training to ignore reds.

## Symptom

    == arm A (negative control): the shipped server
      depth 32 x 1 KiB: 0.2ms per round trip
    == arm B (positive control): the same server, FLINT_NAGLE_TEST=1
      depth 32 x 1 KiB, Nagle left on: 0.3ms per round trip
    FAIL: the positive control COULD NOT BE ARMED

Reproduced serially, with no other drill running, and **reproduced
identically on a clean `HEAD` with the working tree stashed** — so it is not
a regression and not a side effect of whatever else is in the tree.

## What is actually wrong

Nothing in the product. The drill's design is right: arm B deliberately skips
`TCP_NODELAY` and requires the round trip to blow past 20 ms, because arm A's
0.2 ms means nothing unless the slow case can be produced. On this kernel
(darwin 25.5) a 32-deep 1 KiB pipeline still round-trips in 0.3 ms with Nagle
left on, so the condition the control needs does not occur.

**The drill is behaving correctly by failing.** It refuses to report a pass it
cannot substantiate, which is exactly the discipline that makes a positive
control worth having. The defect is that nothing says so in advance, so the
red looks like a product failure every time and costs someone the same
twenty minutes it cost here.

## Why it matters more than a skipped test

A local `drills` run cannot go green on this platform. A gate that is always
one-red is a gate people learn to read past, and the next real failure lands
in a list that already had a failure in it.

## Remedy, recorded not taken

Three options, none free:

- **Detect the platform and skip with a stated reason.** Cheapest, and the
  shape the repo already uses elsewhere for "could not measure". The cost is
  that a skip must be loud, or it becomes a silent hole on the platform where
  the seam is most likely to be edited.
- **Find a load that does produce the stall on darwin** — a larger depth, a
  smaller write, or a receiver that delays its ACK. This keeps the drill
  meaningful everywhere and is the only option that preserves coverage.
- **Declare it Linux-only** and run it solely on the gate box, which is
  already the documented default for a core gate.

Not chosen here because it is a decision about what the local gate is FOR,
which is a wider question than the change that surfaced it.
