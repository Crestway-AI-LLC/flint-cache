# BUG-0145: adding two `cp` lines renames the running seat, and nothing notices (FIXED 2026-09-15)

Status: **FIXED 2026-09-15** — the refusal; in-place growth itself remains
unsupported and unbuilt · found 2026-09-14 · Severity: **medium** — needs a
hand-edit to reach, and the hand-edit is the only interface there is: `flintctl` has no
verb for growing a control plane, so editing the inventory is how you would
do it. The edit looks additive. It is not: it silently changes the identity
of the seat that is already running.

## The mechanism

Both of the CP's derived identifiers switch form on the **count**, not on the
seat:

```rust
fn cp_seat_name(inv: &Inventory, i: usize) -> String {
    if inv.cp.len() == 1 { "cp".to_string() } else { format!("cp-n{}", i + 1) }
}
fn cp_seat_state(inv: &Inventory, i: usize) -> String {
    if inv.cp.len() == 1 { format!("{d}/cp-state") } else { format!("{d}/cp-state-n{}", i + 1) }
}
```

So a fleet running one CP seat has a process whose pidfile is `cp.pid` and
whose argv carries `--state /var/lib/flint/cp-state`. Add two `cp` lines —
the count assert allows exactly 1 or 3, so 3 is the intended target — and
**the same running process is now called `cp-n1`, with a state dir of
`cp-state-n1`.** Nothing moved; the names moved underneath it.

## What happens next

`launch` decides whether a CP seat is already up with

```rust
seat_alive(&cp_runner(inv, i), "flint-controlplane", &cp_seat_state(inv, i))
```

and `pids_in_ps` matches the ident as a **whole token** — deliberately, and
`cp_seat_name`'s own comment records why: *"a three-seat CP runs with
`--state <statedir>/cp-state-n1`, so `<statedir>/cp-state` matches none of
them"*. The match is exact in both directions, so after the edit the check
looks for `cp-state-n1` and the live seat carries `cp-state`. **The running
control plane is invisible to the code deciding whether to start one.**

A duplicate is therefore spawned on port 7500. It cannot bind, and
[BUG-0144](0144-host-spawn-reports-success-and-takes-the-pidfile-without-checking.md)
means that failure is reported as a successful start and the pidfile is
overwritten with the dead child's pid. If instead the original had exited
first, the replacement starts against an **empty state dir** — a control
plane with no Raft state, on a fleet whose ownership truth lives there
(Option B, ADR-0018).

## The documented boot script routes straight into it

`docs/self-hosting.md` ships this:

```sh
if [ -e /var/lib/flint/cp-state ] || [ -e /var/lib/flint/cp-state-n1 ]; then
  "$CTL" -f "$INV" start          # already bootstrapped: idempotent boot
else
  "$CTL" -f "$INV" bootstrap
fi
```

It knows both spellings exist, and it is right to: on a grown inventory
`cp-state` is present, so it takes the `start` branch, which is the correct
branch — the fleet **is** bootstrapped. `start` is then the thing that cannot
see seat 0. So the failure is reachable by rebooting a box whose inventory was
edited, with no operator command at all.

## What it is not

Not reachable by any shipped verb: there is no `cp-grow`, `add-cp` or
equivalent, and no drill grows a CP. Not a defect in the naming scheme
either — distinguishing three seats requires distinct names, and a one-seat
fleet predates them. The defect is that **the identity of a running seat is
derived from a file that can change while it runs, and nothing compares the
two.**

## What would fix it

Stated, not chosen:

1. **Make the name independent of the count** — `cp-n1` always, including for
   a single seat. Correct going forward and a migration for every existing
   single-seat fleet, whose live pidfile and state dir are named the old way.
2. **Refuse the mismatch**: when a CP seat's expected state dir is absent but
   the other spelling is present on that host, stop and say so. Narrow, no
   migration, and it turns a silent duplicate into a message naming the
   edit that caused it.
3. **A `cp-grow` verb** that does the rename as a deliberate step. The most
   work, and the only one that makes growing supported rather than merely
   safe.

(2) is the smallest thing that removes the silent failure, and does not
foreclose (1) or (3).

## CORRECTION, same day: the rename is a symptom, and growth is not possible at all

The section above treats the changing name as the defect. Reading
`cp_seat_args` settles what it actually is, and the answer improves the bug:

```rust
if inv.cp.len() > 1 {
    args.extend(["--raft".into(), "--node-id".into(), (i + 1).to_string(), ...]);
}
```

There is **no `else`**. A single-seat control plane runs with no `--raft`, no
`--node-id` and no `--peers` — it is not a one-member Raft group that could
accept joiners, it is a different mode. So `cp-state` and `cp-state-n1` are
not two spellings of one thing; they hold **two different state formats**,
and naming them apart is correct.

**Which means growing a single-seat CP in place is not a thing that can
work**, naming aside. The defect is therefore not the rename. It is that
**nothing refuses the transition.** Editing one `cp` line into three passes
the count assert (1 or 3, and 3 is what you now have), and every downstream
step then behaves as described above: the running seat is invisible, a
duplicate is spawned on its port, and BUG-0144 reports that as success.

The two outcomes are worth separating, because they differ in cost:

- **The old seat is still alive.** The duplicate cannot bind, is reported as
  started, and the pidfile ends up naming a dead child. Recoverable by hand,
  once someone works out what happened.
- **The old seat had exited** (a reboot, which is exactly the path
  `boot.sh` takes). Three raft seats start with empty state, and the fleet's
  ownership truth — Option B commits cutovers to the CP — is left orphaned in
  `cp-state` while an empty Raft group takes over. That is the expensive one.

**So of the three fixes listed above, (1) is now wrong** and should not be
done: naming a single seat `cp-n1` would hide a real difference in what the
directory contains. **(2) is right, and its message can be much more useful
than a mismatch report** — the condition is knowable exactly, so it can say
*a single-seat control plane cannot be grown in place; its state is not Raft
state*, rather than *expected cp-state-n1, found cp-state*. (3) remains the
only route to actually supporting growth, and would have to migrate the state,
not just the name.

Still not reproduced. Read from `cp_seat_args`, `cp_seat_name`,
`cp_seat_state`, `launch`'s `seat_alive` call and `pids_in_ps`'s token
matching.

## What was done, 2026-09-15: (2), and only (2)

Jeff's call, after the correction above ruled (1) out and left (3) as work
nobody has asked for. `launch` now refuses the transition before anything is
spawned:

```
flintctl: a single-seat control plane cannot be grown in place; its state is
  not Raft state.
  <statedir>/cp-state exists (single-seat format) and the inventory now names
  3 cp seats.
  ...
```

**At the top of `launch`, which is the one function both `bootstrap` and
`start` pass through** — so the reboot path is covered, and that is the branch
that mattered: `boot.sh` takes it, and there the old seat is already gone, so
three Raft seats would come up empty while the fleet's ownership truth stayed
orphaned in `cp-state`.

**Before any spawn**, because after the first one the pidfile damage is done.

**The reverse is refused too.** A Raft statedir under a one-seat inventory is
the same asymmetry pointing the other way, and costs one more branch.

**Both directories present is deliberately allowed.** That is a half-finished
migration someone is in the middle of; guessing which half is live would be a
worse answer than letting them proceed. The drill asserts that, so it stays a
decision rather than becoming an oversight.

### The check

`tools/cp_growth_drill.sh`, registered in `CORE`. Six arms, and only two of
them are the refusal — the other four are the bring-ups that must NOT be
blocked, since a wrong refusal here stops every new Raft fleet and every
reboot.

Its first arm is a control on the drill itself. The first version of this test
asserted on output that never got past the `disposable on` provenance gate:
two arms "passed" against a refusal about build provenance rather than about
CP growth. The control now fails the drill if that gate is what answered.

Mutation-verified: with the refusal removed, the grow arm fails and names it.

## Not established

- ~~Whether a single-seat CP can in fact be grown to three at all.~~
  **Answered in the correction above: it cannot.** `cp_seat_args` passes
  `--raft` only when there is more than one seat, with no `else`, so the two
  state dirs hold different formats and growth is a mode change rather than a
  membership one.
- Not reproduced. The chain is read from `cp_seat_name`, `cp_seat_state`,
  `launch`'s `seat_alive` call and `pids_in_ps`'s token matching, each of
  which is quoted above; no fleet was grown to watch it happen.
