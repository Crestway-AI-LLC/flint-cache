# BUG-0130: the p99 probe could not see the window it named

**Status:** FIXED 2026-09-10. Found 2026-09-10 while auditing M1's exit
criteria. **Severity:** low as a defect, higher as a claim — nothing was
broken, but a roadmap exit clause rested on a measurement that could not
observe what the clause is about.

M1's exit asks that *"a 1 M-element hash delete is O(1) and invisible in
p99"*. `bigkey_delete_drill.sh` was written for it and its header said it
asserted:

> An unrelated GET issued **WHILE the big DEL is in flight** comes back inside
> a bound — the "invisible in p99" half, at one sample.

The code:

```python
t0 = time.perf_counter(); cmd("DEL", "big:h");   big_del   = time.perf_counter() - t0
t0 = time.perf_counter(); by = cmd("GET", "bystander"); bystander = ...
```

`cmd()` sends and waits for the reply, and both use the **same socket**. The
GET is therefore issued strictly *after* the delete completed. It never
overlapped anything.

## The roadmap was half-right about it, and the other half is worse

The 2026-09-09 audit recorded the clause as **"MET (O(1)); the p99 half is one
sample"** — honest about the sample count, and understating the problem. It is
one sample **of the wrong window**.

## And the window that matters is not the DEL

At a million fields the DEL costs about **0.08 ms**, because the mechanism is a
version bump: `DEL` increments a version and leaves the bodies to the
compaction filter's orphan GC. So there is almost no "during" to sample, and
whatever this clause protects against lands **after the reply**, while a
million orphaned subkeys are collected in the background.

A probe that stops when `DEL` returns cannot reach that by construction. The
original could not have failed, whatever the engine did afterwards.

## The fix

A **second connection** reads an unrelated key continuously, across the delete
and on through a settle window, and the assertion is a **p99 over that window**
rather than a single reading.

`SETTLE_S = 3.0` is not a claim that orphan GC completes in three seconds. It
is the window this drill observes, and the drill says so rather than implying
coverage it does not have.

## What did not change

**The O(1) half is untouched and was always the solid one.** It is a ratio
against a hash with 100x fewer fields, floored so two sub-millisecond numbers
cannot produce a noise ratio, with a corpus positive control requiring the big
build to take materially longer than the small one — so a bug creating two
tiny hashes cannot make it trivially true.

## The second positive control, and why it is not optional

A p99 of three samples is not a p99. Without a floor, the new assertion passes
*most loudly* when the reader thread died in its first millisecond — the shape
of every check that certifies by measuring nothing, which is most of
`docs/field-notes.md`.

`MIN_SAMPLES = 200` is deliberately far below what a working reader produces in
three seconds (thousands), so it fails on a broken reader rather than on a slow
machine.

## Related

- ops OPS-0037 — "could not measure" must never be recorded as "measured nothing"; a probe that cannot observe its window is that rule broken at the instrument
- ops `docs/roadmap.md` M1 — the exit clause this measures, and the audit that flagged it
