# BUG-0121 — the direct-path RTO measured the SSH hop and called it recovery

Status: **the reporting is FIXED; the M2 exit verdict it affects is OPEN** —
see "What this does not settle"
Found: 2026-09-07, investigating the M2 soak's RTO tail
Component: `crates/flint-chaos/src/main.rs`, `writer.rs`

## The finding

The M2 soak's 800-kill run failed its exit on RTO: **worst 6002 ms against a
3000 ms budget**, 4 of 401 master kills over. Asked to find why those
promotions were slow.

They were not. Across all 401 master kills in that run:

| | p50 | p90 | p99 | max |
|---|---|---|---|---|
| reported RTO | 572 ms | 629 ms | 2207 ms | **6002 ms** |
| kill dispatch (`kill_ms` → SIGKILL returning) | **712 ms** | 755 ms | 2123 ms | 3984 ms |
| anchored to `dead_us` instead | −144 ms | −100 ms | 6 ms | 2857 ms |

**The dispatch p50 exceeds the reported RTO p50.** The direct-path RTO is
`first post-outage ack − kill_ms`, and `kill_ms` is stamped *before* the kill
is dispatched — on a multi-host fleet that is a master-discovery round trip
plus an SSH hop. So the figure is dispatch plus recovery with nothing
separating them, and it is dominated by the former.

## Neither stamp is the death, and that is the part worth keeping

The obvious repair — anchor to `dead_us` — is wrong, and the data says so
before any argument does: it yields **negative** recovery times, p50 −144 ms.
That is not a bug in the arithmetic. `dead_us` is stamped when the SIGKILL
**returns**, which is after the signal landed, so recovery can genuinely
complete before it.

So:

- `kill_ms` precedes dispatch — too early, inflates by the whole hop.
- `dead_us` follows the kill's return — too late, and goes negative.
- The death is somewhere inside a ~712 ms window and **these stamps cannot
  resolve it**.

Same anchor family as BUG-0120, which was the loss DEPTH; this is the RTO. One
harness, two headline numbers, both measured from a stamp that is not the
event.

## The right number already existed and was thrown away

`max_stall_ms` is the worst gap between consecutive ACKS. It needs no kill
stamp, and it is exactly what a client experienced. The **edge** path already
reports it as its RTO, for a reason stated in the source: through the proxy the
client never sees an error, so calling it RTO would overstate the outage.

On the direct path it is computed identically — the writer updates it on every
ack whenever a kill is armed, not gated on the edge — and then **never
surfaced**. The soak runs direct. So every run has measured the honest number
and discarded it.

## Fixed

The per-iteration line and the summary now carry both components:

    ... max_hold_ms=1 dispatch=34ms client_stall=25ms]

    of which kill DISPATCH (kill_ms to the SIGKILL returning): p50 …ms
    client-observed outage (worst gap between ACKS, no kill stamp): p50 …ms
    NOTE: the death is somewhere inside that dispatch window …

Verified locally, where dispatch is small and the decomposition is still
visible: `RTO 42ms` against `dispatch=34ms client_stall=25ms`. The reported RTO
is unchanged, deliberately — see below.

## What this does not settle

**Whether batch 3 actually breached, and therefore whether M2's exit was
failed by the product or by the instrument.** The stall was measured on that
run and not printed, and the log carries only aggregates, so it cannot be
recovered. A re-run on this build settles it in one pass.

**And the criterion is not mine to change.** The exit says "RTO ≤ 3 s in every
run". The honest client-observed figure is the stall, and judging the exit on
the dispatch-inflated number is judging it partly on SSH latency — but moving a
milestone's measurement is a decision, not a fix, so the reported RTO and the
assert are untouched here. What changed is that a reader can now see what the
number is made of.

**Do not read the local figures as the fleet's.** Locally `kill_master_hot`
signals a child process directly and dispatch is ~34 ms; the 712 ms p50 is a
property of killing over SSH across hosts.
