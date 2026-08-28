# BUG-0067 — the tier-down fix never reached the path it was written for

**Component:** `s3-accelerator`, S3A adoption path 1 (`FlintStreamFactory`)
**Found:** 2026-08-27, by the suite built for BUG-0066, on its first run.

## Symptom

A Flint tier that is **down when the job starts** fails the job outright on
adoption path 1. `FileSystem.get()` throws
`AnnotatedConnectException: Connection refused` out of `bind()`, before a single
byte is read.

This is ADR-0023 **D12.9** — bounded degradation, "the property that decides
deployability" — absent on the path the preflight script recommends first. A
cache being unavailable should make reads slower, never make them fail.

## The wrong conclusion drawn first

That this was a defect in the new test: pointing a probe at
`redis://127.0.0.1:1` and getting a connection error looks like the probe's own
fault, and the obvious repair is to make the probe tolerate it.

It is the product. **BUG-0058 is this exact bug**, found and fixed weeks
earlier — "a tier that is already down fails the job at filesystem init". The
fix went into `TierSupport`, which paths 2 and 3 build through and path 1 does
not. `TierDownSuite`, the ten-check suite that exists specifically for this
property, also builds through `TierSupport`. **So the gate for the property and
the fix for the property covered the same two paths, and neither covered the
third.**

## Root cause

`FlintStreamFactory.bind()` called `redis.connect(...)` eagerly and used
`conn.async()` directly. `TierSupport` instead catches the refusal and falls
back to `LazyTierCommands` — a proxy that redials at a rate limit and rejects
commands until it succeeds, so every rejected command degrades to the origin.

Path 1 builds its client directly rather than through `TierSupport`, which is
the same structural split that produced BUG-0066 in the same class. Two
adoption paths with two construction mechanisms means every fix has to be
landed twice, and nothing says when only one landing happened.

## The fix

`bind()` now does what `TierSupport` does: dial once, and on a `RuntimeException`
fall back to `LazyTierCommands.install(redis, reconnectMs)`, logging a warning
that names the tier. `serviceStop` closes the lazy connection too, which is null
until a redial succeeds. Adds `fs.s3a.flint.tier.reconnect.ms`, which this path
had no way to set.

## The check that now holds it

`ConfigReachSuite`'s `TIER_URI` probe: with the tier URI pointed at a closed
port, the read must **return correct bytes** and must cache nothing in the real
tier. The second half is the control — it proves the configured URI was the one
used, rather than the probe having quietly fallen back to the default tier and
succeeded for the wrong reason.

## What this says about the class of bug

Two bugs in one class in one afternoon, both "the fix landed on one path and
the suite for it built through the same helper the fix went into". A suite that
constructs its subject the way the fixed path constructs it will follow the fix
around and never test the path that was missed. **Where two adoption paths build
the same client differently, a property suite has to enter through both doors,
or it is testing the helper rather than the product.**

`TierDownSuite` still only covers paths 2 and 3. Path 1's tier-down behaviour is
covered by the single probe above, which is bind-time only — a tier that dies
*mid-job* on path 1 is still untested.

## A separable finding from the same hunt: one gate stage measures a stand-in

`ResilienceSpike` is gated as **"tier killed mid-job (4 checks)"**. It does not
use `FlintObjectClient`. It defines its own `ResilientObjectClient implements
ObjectClient` inside the spike file and wires AAL over that.

So the stage proves *a spike-local reimplementation* of the degradation logic
survives a tier being killed. It says nothing about whether the product's client
does. It reads, in the gate's output, exactly like coverage of the product.

This is the same root as the bug above, one turn further: not "the suite entered
through the door the fix went through", but "the suite built its own door". It is
how the mid-job property could look covered on every path while being covered on
none of them.

**Now genuinely covered:** `TierDownSuite` gains a path-1 arm that opens a real
`FileSystem` through `fs.s3a.input.stream.type=custom`, reads with the tier up
(armed: the tier answered), kills the tier with the filesystem still open, and
requires the next read to return correct bytes *and* to have reached the origin —
the second half being what separates degrading from serving something stale. It
passes, so the mid-job half of D12.9 was correct on path 1 all along; it was
simply never asserted there.

**Still open, separably:** whether `ResilienceSpike` should keep its own client
at all. A spike that predates the product and is now gated as a product test is
worth either pointing at `FlintObjectClient` or renaming so its four checks stop
reading as coverage they do not provide.

