# BUG-0112 — the README dismissed a 21% gap using a spread that does not cover it (FIXED 2026-09-06)

**Status: FIXED 2026-09-06.** Found 2026-09-06 by checking whether the front
page's published numbers trace to a recorded measurement — the audit BUG-0013
makes obligatory, since it records a headline figure that was measured under a
stall and published for a month · Severity: medium — the numbers themselves
were right; the sentence that told readers what they mean was not.

## What was right

Every throughput figure in README's "Our numbers" matches
`docs/bench/2026-09-01-published-tables-re-measured.md` **exactly**, both
tables, all ten rows. The methodology paragraphs are honest, including the
admission that the RTT probe failed to build on the beyond-RAM fleet so those
rows have no wire decomposition. Nothing was invented or rounded in a
flattering direction.

## What was wrong

The README read the comparison against 2026-08-17 like this:

> GETs, Mixed and **SETs** are within this rig's run-to-run spread — the two
> SET runs behind that row differed by 11% between themselves, which is wider
> than the gap to the older figure, so it is not read as a change.

Three rows are named and the arithmetic covers two of them:

| row | published | 2026-08-17 | gap | inside an 11% spread? |
|---|---|---|---|---|
| GETs, hot slice | 90,775/s | 96,116/s | 5.6% | yes |
| Mixed 1:10 | 90,730/s | 96,480/s | 6.0% | yes |
| **SETs** | **69,340/s** | **87,449/s** | **20.7%** | **no — nearly double it** |

The SET row is the one the sentence cites as its evidence, and it is the one
the argument does not cover. A reader doing the subtraction gets 21% and a
justification that says 11%.

## The stronger evidence existed and was left behind

The bench doc does not rest its conclusion on the spread. It says:

> A cross-day comparison cannot distinguish a code change from a different
> afternoon. The same-fleet A/B can, and that is what the conclusion rests on.

and reports that A/B: this build against the pre-batching one, three
repetitions each at depth 1, writes within about 2% of each other and reads
within 1%.

So the private measurement reasoned correctly and the public page kept the
weaker half. That is the defect worth naming — not a wrong number, but a
**downgrade in the quality of the argument as it moved from the bench doc to
the README**, in the direction that made the conclusion look easier than it
was.

## Fix

The README now states the 21% gap plainly, says the spread does not cover it,
and attributes the conclusion to the same-fleet A/B — which is both true and
the stronger claim. No published figure changed, because none was wrong.

## What is still not recorded

The two individual SET runs behind "differed by 11%" are not in the
repository. `docs/bench/raw/` holds per-run logs only up to 2026-08-17, so the
11% cannot be checked by anyone reading the file, in either repo. Noted in the
bench doc rather than fixed: the runs are gone, and re-running them would
answer a question the A/B has already answered better.
