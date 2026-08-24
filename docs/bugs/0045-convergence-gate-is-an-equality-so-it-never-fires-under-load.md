# BUG-0045: the convergence gate is an equality, so it never fires under write load

Status: OPEN, found 2026-08-24 · Severity: HIGH — a pair under sustained write
load can lose its master and REFUSE to promote a healthy, in-sync replica,
staying write-dead until a human intervenes. A fix exists and is stranded on
an unmerged branch; see BUG-0044 for how that happened.

## The defect, in one line

`crates/flint-controller/src/main.rs:635`:

    let legit_converged = legit.live_replicas >= 1 && legit.seq_lag == Some(0);

`seq_lag == Some(0)` is an equality against a quantity that is only zero when
nothing is being written.

## Why that is not pedantry

Measured on a live 5-host fleet on 2026-08-15: with a continuous writer through
the proxy edge, the master's `seq_lag` sampled **82-151 across ten consecutive
1-second samples and was never once 0**.

So under load `legit_converged` is permanently false, and everything it gates
freezes at the instant sustained writing begins:

  - `converged_ever` (main.rs:~697) — never armed if load precedes the first
    quiet moment;
  - `last_converged` (main.rs:707) — stops advancing;
  - `last_insync` (main.rs:712) — the lineage memory the degraded-window path
    falls back on at main.rs:820.

Then main.rs:800 does the damage:

    if !self.converged_ever || self.last_converged.elapsed() > cfg.max_stale {

A frozen `last_converged` ages past `max_stale`, the controller decides its
view is too stale to act on, and refuses. Observed: **a master killed five
seconds into a burst, then 137 consecutive refusal ticks against a healthy
in-sync replica, write-dead until someone ran `flintctl start`.**

The failure mode is the worst shape available: the pair is fine, the replica is
fine, the controller can see both, and it declines to act — so nothing alarms
on a crash and the cluster simply stops accepting writes.

## Same fact, already handled correctly elsewhere

`roll_node` in flintctl already states it: *"under LIVE write load seq_lag
hovers above zero by design"*. One code path treats a positive `seq_lag` as
normal, another treats it as never-converged. The disagreement is the bug.

## The fix, which is written

flint `643d12e` — "controller: a convergence gate written as an equality never
fires under load", on `origin/phase1-drills`, never merged:

  - `Node::converged()` accepts `seq_lag == 0` **or** lag inside the healthy
    band, where the band is the seat's OWN `lag_soft_ms` read from the
    FLINTINFO the controller already parses — not a second copy of flintctl's
    hardcoded 500. Below the soft cap the master is not even delaying writes,
    so replication is keeping up by the write path's own definition, and an
    operator retuning the cap through FLINTCONFIG retunes this with it. A seat
    too old to report the field falls back to the shipped 500.
  - Adds `tools/loaded_promote_drill.sh`, which is the regression test: a
    promotion demanded *while writes are in flight*, which no existing drill
    does.

249 lines in the controller plus the 150-line drill.

## Note on the second line of main.rs:304

    .find(|n| n.reachable && n.epoch == top && n.live_replicas >= 1 && n.seq_lag == Some(0))

The same equality appears in the legitimate-master search. Whether it has the
same consequence is NOT established here and must be checked as part of
landing the fix, not assumed from the shape.

## How this was found

Auditing what else was stranded on the branch carrying BUG-0044's disk-guard
fix. The subject line alone was not evidence — several branch commits describe
failure classes main has since fixed independently, so each needs its defect
checked against main's code. This one was checked: the equality is on main at
line 635, and the gate it feeds is on main at line 800.
