# BUG-0126: the replica stall was a flag, not a stall, on the only fleet that runs it

**Status:** FIXED (the false attribution). The provocation itself remains
unimplemented on a real fleet — named at the bottom, not quietly left.

## Symptom

`soak-20260908T185044Z` was run with `--stall-replica-ms 1800`, the first
batch to ask for the RPO bound to be provoked rather than waited for. Its log
reports:

```
NOTE: loss depth 0 under a 1800ms replica stall, with 572 write(s) shed: the
master refused writes it could not replicate rather than acking them.
```

No stall was applied. The replica was never stopped for 1800 ms, or for any
interval. The 572 shed writes were natural lag, which is precisely the
condition the flag was added to stop depending on.

## Root cause

Three lines, in three files, none wrong on its own.

`crates/flint-chaos/src/cluster.rs`:

```rust
pub fn stall_replica(&self, on: bool) -> bool {
    match self {
        Target::Local { cluster, .. } => { signal_by_port(...); true }
        Target::Attached(_) => false,
    }
}
```

`crates/flint-chaos/src/main.rs`, the call site:

```rust
if stall_replica_ms > 0 && cluster.stall_replica(true) {
```

`&&` short-circuits, so on an attached fleet the sleep and the un-stall are
both skipped and nothing is printed. And the report picks its wording from the
flag rather than from what happened:

```rust
let how = if stall_replica_ms > 0 {
    format!("under a {stall_replica_ms}ms replica stall")
} else { "under natural lag, ...".to_string() };
```

`packaging/aws/chaos-cluster/run.sh:960` runs `flint-chaos --inventory`, and
`--inventory` is what selects `Target::Attached`. **Every multi-host run is
attached.** So the flag works on exactly the topology the soak never uses, and
is inert on the only one it does.

## The part worth keeping

The verdict was not wrong; the attribution was. `rpo_exercise()` keys on
`throttled > 0` — evidence that the master shed writes — not on the flag, so
`rpo_bound_exercised: true` in the ledger stands. The bound was exercised and
did hold. It just held by luck, under natural lag, in a run whose whole
purpose was to stop relying on luck.

That is the shape of it: **a feature added to remove a dependency on chance,
which silently depended on chance.** BUG-0120 introduced `--stall-replica-ms`
because "a clean durability block that proved nothing" is not evidence; the
flag was exercised against `Target::Local`, where it works, and shipped into a
harness that only ever constructs `Target::Attached`.

The reason it survived a full batch is that a no-op is invisible. A stall that
failed would have thrown; a stall that returns `false` reads as "nothing to
do", and the only line that would have contradicted it was generated from the
flag we passed instead of from the fleet we ran.

## The wrong conclusion drawn first

That the ledger row needed its `rpo_bound_exercised` corrected. It does not —
that field was already derived from evidence, having been fixed for this exact
reason one commit earlier. What needs correcting is `stall_replica_ms: 1800`,
which records a request, and the log sentence, which reports it as an outcome.

## The check that now holds it

`--stall-replica-ms` against an attached fleet is now **refused at startup**
rather than ignored: a run that cannot do the thing it was asked to do should
not spend two hours producing a record that says it did. `Target::Local` is
unaffected. Test: `a_stall_request_is_refused_when_it_cannot_be_honoured`.

The check's own first run was a flake: both tests wrote one inventory path
keyed on `process::id()`, which is identical for both, and cargo runs them in
parallel -- the first run failed and the identical second run passed. Fixed
before it was believed, because a test that clears on retry is worse than one
that fails.

Refusing is deliberately harsher than warning. The failure mode here was not
that someone missed a warning — there was no warning — but that the run
completed and wrote a plausible sentence into a ledger that the milestone
count is computed from. A batch lost to a hard error costs an afternoon; a
batch that lies costs the credibility of every row beside it.

## Still missing, and not fixed here

There is no way to provoke replica lag on a real fleet. `Attached` reaches
seats through `flintctl` (`self.ctl(&["kill-node", addr])`), and `flintctl`
has no signal subcommand — `host-kill-pidfile`, `host-stop-seat`,
`host-stop-all`, and nothing that sends `SIGSTOP`. Implementing it means a new
`host-signal` primitive plus routing, which is a feature and is not smuggled
in under a bug fix.

Until then the RPO bound on a multi-host fleet is measured only when the
replica happens to fall behind, which is the state BUG-0120 opened. The
difference is that it now says so.
