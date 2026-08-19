# BUG-0023: the chain traversal loses a link across a master kill — but only inside a full gate (OPEN)

Status: OPEN, found 2026-08-18 · Severity: **high if real** — the assertion is
a durability claim, and it has fired once against five clean solo runs, so the
open question is whether the product or the gate environment is at fault

## Symptom

Full gate at `c1371d9`, `chaos` step:

    chaos-chain: 200000 elements, 12 kills, driver=harness
    built 200000 links in 3.2s; waiting for replica to catch up
      [integrity] post-build: master=Some(13594) replica=Some(13594)
      hop 2631: killed MASTER, promoted; new master :6331
      [integrity] master=Some(13594) replica=Some(13594)
      hop 7691: killed REPLICA
      [integrity] master=Some(13594) replica=Some(13594)

    thread 'main' panicked at crates/flint-chaos/src/bin/chain.rs:192:25:
    BROKEN CHAIN at key0013595 (hop 13594): still nil after 55 retries over 3s
    on master :6331 — a truly lost link

The key is one past the integrity marker **both nodes agreed on**, and it was
still nil after 3 seconds of retries against the promoted master.

**The ledger half of the same run passed cleanly**: 12 kills, 0 corruption, 0
time-travel, 0 cross-key, final walk 300 present / 0 missing-or-regressed. Two
adjacent probes of the same behaviour disagreed in one run, on one box, under
one load.

## Reproduction: it does not, outside a gate

Five valid solo runs of `chaos_drill.sh`, same branch, idle box:

    TALLY  passed=5  reproduced=0  failed-other=0  refused=0

Positive-controlled — each run reached the failing configuration:
`driver=harness`, 200000 links, `PASS: walked 200000 links end-to-end through
7 master + 5 replica kills`. So the silence is a result, not an absence of
measurement.

(An earlier attempt reported `reproduced=2` from a loop that counted non-zero
exit as reproduction. Both were `fleet_guard` **refusals** caused by a foreign
`flint-agent` on the box — the drill never ran. Any tally here is three-way for
that reason.)

## What is ruled OUT

**Port collision.** The obvious theory, and it does not survive:

- `chaos_drill.sh` is the **only** drill declaring anything in 6330-6337, and
  every 63xx port is claimed exactly once across `tools/*_drill.sh`.
- The two drills invisible to the preflight (BUG-0020) use 6410 and none.
- The collisions in OPS-0005 (7402, 7411, 7412, 9466) are cross-repo or
  internal to `flint-cache`; none of those drills run in a public gate.

**A leaked seat from `restart`.** `chaos` declares 6330-6337 and calls
`fleet_guard`; restart's leaked seat was on 6410, outside that scope, so the
guard correctly did not refuse and chaos ran clean into its iterations. Disjoint
by port, so it could compete for CPU but not collide.

**Load alone.** A competing `cargo` build was running for part of that gate, so
contention is real — but the same gate's `chaos_unreadable` PASSED under the
same load, and BUG-0014's table shows loaded passes too. Load is not
sufficient.

## What survives

**It has only ever fired inside a full gate.** That is where, not why. It
points at drill-to-drill interaction or leftover state rather than at the write
path, and it is the **same signature BUG-0014 carries** — 21 runs outside a
gate, including 12 with injected replication lag, never fired that one either.

Two independent assertions, in different binaries, both firing only under a
full gate and never standalone, is a stronger signal than either alone. It is
also an argument that they may share a cause that is *environmental*, even
though the assertions themselves are about durability.

## Evidence

The failing log is preserved at `chaos-FAIL-c1371d9-brokenchain.log` (92
lines). It survived only because it was copied out by hand: per BUG-0021 the
gate writes one log per STEP, so the next run would have overwritten it — and
the natural response to an intermittent failure is to re-run.

## Next step — the probe is landed and verified (2026-08-19)

`crates/flint-chaos/src/bin/chain.rs` now dumps, at the instant the walk gives
up on a link, exactly what this section asked for:

    [lost-link] key=key0013595 hop=13594 retries=55
    [lost-link] master  :6331 role=master epoch=(0,2) latest_seq=... last_applied=... acked_seq=... seq_lag=... live_replicas=... dbsize=...
    [lost-link] replica :6330 role=replica epoch=(0,2) ...
    [lost-link] direct GET on master  :6331 -> ABSENT
    [lost-link] direct GET on replica :6330 -> PRESENT (10 bytes)
    [lost-link] verdict: PROMOTION LOSS — replicated to the other member but missing on the promoted master

The direct GET on each member is the discriminator. It separates three causes
the bare panic cannot:

| master | other member | verdict |
|---|---|---|
| PRESENT | any | **READ PATH** — the walk's reads were going somewhere else |
| ABSENT | PRESENT | **PROMOTION LOSS** — replicated, then lost by the promoted node |
| ABSENT | ABSENT | **NEVER LANDED** — lost at or before build/replication |

The first row is not in this file's original request, and it is there because
of BUG-0014: that bug's sharpest constraint is that iteration 1 has *never*
failed, so its assertion only ever fires on the operation that **follows a
harness promotion**. A stale-master resolution is therefore a live hypothesis
for both, and a dump that could not distinguish it would have handed back an
answer to half the question.

Every outcome is three-way — `PRESENT` / `ABSENT` / `UNREACHABLE`. A member
that cannot be reached must not render as a missing key, or the dump commits
the same error it exists to diagnose.

### The probe checks itself on every run

A dump that fires on ~7% of gate runs and has never reproduced standalone
cannot be left unexercised — a renamed FLINTINFO field would turn the one log
that matters into a page of `<absent>`, discovered weeks later. So before any
kill, on the healthy just-built cluster, `verify_probe` runs:

- **negative control first**: `key0000000` is never written (the chain starts
  at 1), so if the probe calls it PRESENT it is hallucinating and every later
  ABSENT is worthless;
- **then the positive control**: `key0000001` always exists, so if the probe
  calls it ABSENT it cannot see keys at all and would blame the product for
  its own blindness;
- **then both members' FLINTINFO** must yield all seven fields, or the run
  aborts saying the dump would print nothing usable.

It prints `[probe] self-check ok: ...` on success rather than staying silent,
because a self-check that only speaks on failure is indistinguishable from one
that never ran — which is how the first verification of this probe was caught
reading a stale binary that predated it.

Verified on a live pair: both verdict arms render, all seven fields resolve on
master and replica, and the self-check passes. What remains is for a real gate
to fire it.

## Related

- BUG-0014 — same gate-only signature, different assertion and binary
- BUG-0020, BUG-0021 — the drill-hygiene and evidence-retention gaps this
  investigation ran into
- OPS-0005 — the port collisions, ruled out above as a cause of this one
