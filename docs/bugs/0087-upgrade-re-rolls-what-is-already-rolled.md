# BUG-0087 — `upgrade` re-rolls seats already on the target, and fails over healthy pairs to do it

**Status:** OPEN.
**Area:** `crates/flint-ctl`, `fn upgrade`.
**Severity:** medium. Nothing is corrupted; the cost is an unnecessary write
interruption per pair, taken at exactly the moment an operator is recovering
from a roll that already went wrong.

## What happens

`upgrade` rolls every seat unconditionally. There is no check anywhere in it
that a seat is already running the target build:

- the canary is rolled whether or not it is already on the target;
- the replica loop rolls every replica;
- the master phase is `for (i, pair) in inv.pairs.iter().enumerate()` and
  performs a **fenced controlled failover on every pair**, with no build
  comparison first.

On a fresh roll that is exactly right. On a **partially rolled** fleet it is
not, and a partially rolled fleet is precisely when someone re-runs the
command.

## Why it matters now

A roll that dies half-way leaves the fleet on two builds. The obvious repair —
the one an operator reaches for at 02:00, and the one
[ADR-0036](../../../flint-cache-ops/docs/adr/0036-the-record-a-stalled-roll-leaves-behind.md)
assumed when it said the action "is one `flintctl upgrade` away" — is to run
the upgrade again. That claim is wrong and this is why: re-running it

1. kills and respawns seats that were already converged, and
2. fails over **every** pair again, including pairs already wholly on the
   target.

Each failover is a real write interruption. Re-running to move two straggling
seats can cost a failover on every pair in the fleet.

## What it should do

Skip what is already there. A seat reporting the target build needs no roll,
and a pair whose master is already on the target needs no failover. That
makes `upgrade` **idempotent**, which is the property the recovery case
actually wants: run it again and it does only what is left.

Two details that are not obvious and should be decided rather than
discovered:

- **A master that is behind still needs a failover** — a master cannot be
  warm-restarted onto a new build without promoting its replica first. So the
  saving is per-pair, not global, and a roll that stopped before the master
  phase legitimately fails over every pair.
- **"Already on the target" must be read from a live stamp**, not from the
  inventory or from what the last roll believed. `FLINTINFO build:` is the
  only source that survives a driver dying, and BUG-0083 is the reminder that
  a failed READ of it must not be treated as a mismatch.

## Found by

Building ADR-0036's acting half in the ops repo, and checking the ADR's own
claim that finishing a stalled roll was one command away before writing code
that depended on it.
