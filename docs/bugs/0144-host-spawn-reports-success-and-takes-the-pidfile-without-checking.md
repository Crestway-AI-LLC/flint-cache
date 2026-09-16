# BUG-0144: `host-spawn` reports success, and takes the pidfile, without checking the child survived (FIXED 2026-09-15)

Status: **FIXED 2026-09-15**, found 2026-09-14 · Severity: **medium** — not reachable
through any caller that honours the primitive's contract, and reachable
through a race that ops has filed (OPS-0250). What it leaves behind is worse
than the wasted spawn: **a dead pid in the pidfile of a live seat**, after
which every stop or kill routed through that pidfile aims at a corpse while
the real process keeps serving.

## What it does

```rust
"host-spawn" => {
    ...
    local_spawn_env(&statedir, &bins, &name, &bin, &args, &envs);
    println!("pid recorded in {statedir}/pids/{name}.pid");
    std::process::exit(0)
}
```

Nothing sits between the spawn and the success. `local_spawn_env` panics
only if the **exec** fails — a missing binary — and not if the process it
started exits a moment later, so a seat that cannot bind because its port is
already held is reported as started. And it writes the pidfile first:

```rust
std::fs::write(format!("{statedir}/pids/{name}.pid"), child.id().to_string())
```

before anything knows that child will live.

## Why this is a real state and not a hypothetical

`host-spawn` is unconditional **by design**, and that design is sound on its
own terms: `roll_node` stops the seat before spawning it, `upgrade` stops
each seat first, and `start` treats a seat with a live process as STARTING
and leaves it alone (`#139`). The contract is *the caller has ensured this
seat is down*. The primitive simply has no defence when that is false.

It is false during the race ops filed as **OPS-0250**: the box's own
`flint-supervise` timer and a remote agent's `restart-node` both repaired the
same seat within 1.4 seconds on 2026-09-14, and nothing interlocks them —
`supervise`'s guard is `pgrep -xc flintctl`, which counts local processes
while the agent works over ssh in three short-lived sessions. Land the
supervise tick in the 211 ms between the agent's `host-mark-reseed` and its
`host-spawn` and the contract is broken: supervise's copy is serving, the
agent's copy dies on bind, and the pidfile ends up naming the dead one.

That is the failure `cp_seat_name` was written for, reached by another road —
its comment records the cost as *"every stop/kill through that pidfile aims
at a corpse"*, and the abort reads *"port still bound after the process was
gone"*, which is exactly what it looks like when you kill the wrong thing.

## Found by reading, not by running

This answers a question OPS-0250 raised and listed as unmeasured: what
`host-spawn` does when the port is already held. A fleet experiment was
being designed for it; thirty lines of the primitive answered it, and the
answer was the worse of the two branches.

## The fix, and why it is not applied here

**Refuse to overwrite a pidfile that names a live process belonging to this
seat.** A live pid at spawn time is never legitimate under the contract
above, so refusing costs nothing that works today and turns the race's
outcome from a corrupted pidfile into a loud failure.

Two things make it more than a two-line change, which is why it is filed
rather than done:

- **A bare liveness check is not enough.** A stale pid can be recycled by an
  unrelated process, which is why `pids_matching` matches the ident as a
  whole token rather than trusting a number. The guard has to ask *is a
  process with this pid AND this seat's ident alive*, and `local_spawn_env`
  is not given the ident — `host-spawn` never receives one.

  **Measured rather than estimated, because the first version of this bullet
  said "a signature change across every spawn call site" and that conflates
  two numbers.** `local_spawn_env` itself has **two** call sites: the local
  branch of `spawn_env`, and the `host-spawn` handler. But the ident is
  *caller* knowledge — `seat_alive`'s callers pass `cp_seat_state(inv, i)` or
  the node's data dir — so threading it means touching `spawn_env`/`spawn`'s
  **fourteen** callers, not two.

  The cheaper alternatives were considered and do not work. Matching on `bin`
  alone is what pid reuse defeats, and a box running many `flint-server`
  processes is exactly where reuse lands on another one. Matching on the seat
  NAME, which `local_spawn_env` already has, fails because the name is not
  reliably in the argv: a node's is (`--data-dir …/node-7002`), the CP's is
  not (`cp-n1` is not a substring of `cp-state-n1`), and a proxy's is not
  (`proxy-7379` versus `--port 7379`).
- **This is the path every roll takes.** A wrong refusal here fails an
  upgrade mid-fleet, which is worse than the race it prevents.

## Not established

- **Whether `flint-server` exits on a bind conflict quickly enough** to lose
  the race in the direction described, or whether it retries. The primitive
  is read; the server's bind path is not.
~~Whether any drill spawns over a deliberately-live seat and would be broken
by the refusal.~~ **Checked: none does, and one asserts the opposite.**
`start_guard_drill.sh` freezes a replica with SIGSTOP to make the window
deterministic and then requires that `start` leave it alone — it fails on
*"start respawned a seat whose process was alive"*, on *"start WIPED the data
dir of a live seat"*, and on a duplicate process on the port. So the refusal
proposed above is a second line of defence for an invariant this repo already
holds and tests (`docs/bugs/0004`), not a new rule that existing drills would
trip over. That removes one of the two objections to doing it; the signature
change across every spawn call site remains.

## Fixed 2026-09-15 — and the fourteen callers were not needed

**The ident is not caller knowledge.** This file's own objection was that a
sound guard needs *pid AND ident*, that `local_spawn_env` is never given an
ident, and that supplying one means threading it through `spawn`/`spawn_env`'s
**fourteen** call sites. That framing is what kept the bug filed rather than
fixed, and it was wrong in one specific way: `local_spawn_env` already receives
`statedir`, `bins`, `name`, `bin` and `args`, and **`{bins}/{bin}` plus `args`
IS the seat's identity** — it is exactly the command line a previous spawn of
this same seat would be running under. So the check reads
`{statedir}/pids/{name}.pid` and asks for that pid's argv. **No signature
changed and no call site was touched.**

That also answers the objections this file raised against the cheaper
alternatives, which were both about an identity weaker than the question:
matching on `bin` alone is what pid reuse defeats, and matching on the seat NAME
fails because the name is not reliably in the argv (`cp-n1` is not a substring
of `cp-state-n1`). `argv[0]` has neither problem — it is `{bins}/{bin}`, the
binary out of this install, and it is what the seat is about to be exec'd as.

**And it is argv[0], not the whole argv, which is a correction to the first
version of this fix.** Requiring the ARGUMENTS to match too reads like the
stronger check and is the weaker one: it would have allowed exactly the
collision OPS-0250 recorded, because the two actors repairing `node-7002`
composed different argv for it — `--journal 0.0.0.0:7500` against `--journal
172.31.64.94:7500` — having read two inventories that disagree (BUG-0138). A
whole-argv check calls that a different seat and waves it through. Nothing
legitimate reaches this primitive with a live process in the pidfile whatever
its arguments, because `roll-node` and `upgrade` stop the seat first, `start`
leaves a live one alone and `launch` skips one already up; so the arguments are
DIAGNOSIS, not permission, and the refusal prints both lists and says when they
diverge. A stale pid, or one the kernel has recycled onto an unrelated process,
still does not match and still does not refuse — each has its own control in
the drill.

## The refusal alone does not close the race — OPS-0250's half

Two actors that both CHECK before either WRITES still both pass. So the same
change puts an exclusive lock on the seat, held across check-spawn-pidfile-write
in `local_spawn_env` and across the whole of `local_stop_seat` — the two
functions where `start`/`host-spawn` and `stop`/`host-stop-seat` respectively
converge, so one implementation covers a local actor and one arriving over ssh.

- **`flock(2)`, not a pid in a file**, because the kernel releases it when the
  process exits: a flintctl killed mid-repair must not wedge the seat it was
  repairing. The pid written into the lock file is diagnostic only, and the
  comment on the type says so, so that nobody later turns it into the lock.
- **Per seat, not per statedir.** The race is two actors on ONE seat; a
  statedir-wide lock would serialise a bootstrap whose seats do not contend.
- **`std::fs::File::try_lock`, so no new dependency.** The plan for this change
  assumed `libc`, which `flint-chaos`, `flint-storage` and `flint-server`
  already carry; it is not needed. `File::try_lock` has been stable since Rust
  1.89 and the toolchain pin is 1.98, so the lock costs no new edge, no
  `Cargo.lock` change and no licence review. Checked by compiling it, not by
  remembering the signature — it returns `Result<(), TryLockError>`, not the
  `io::Result<bool>` this was first written against.
- **Failing open, deliberately, when the lock cannot EXIST** (no directory, a
  filesystem without flock): this is a second line of defence on the path every
  roll walks. A holder that will not let go inside 60s is the opposite case and
  refuses — that is another actor actively working the seat.
- **What is NOT locked, with the reason.** `local_stop_all` kills every pidfile
  in the directory and the seven `kill_pidfile` calls that bypass `stop_seat`
  are untouched: neither participates in the check-then-write sequence this
  closes, and widening the lock to them is a change to `stop`'s behaviour that
  wants its own argument.

## The question this raised, and how it was kept falsifiable

`ps -o args=` is a FORMATTED column and I could not establish from the outside
whether it truncates on the gate box. That mattered a great deal to the first
version of this fix, where a truncated argv could never match and the guard
would have silently stopped guarding — a check that cannot fail, which is the
worst outcome available here and the one no gate notices. Keying the identity on
`argv[0]` removes that failure entirely, because argv[0] is at the FRONT of
whatever a truncating reader returns.

What truncation could still do is make the message LIE: a cut `running` list
compares unequal to `wanted`, and the refusal would then announce an argument
divergence that is not there and send the reader to BUG-0138 for a fault that
does not exist. So `pid_argv` still reads `/proc/<pid>/cmdline` first — the
whole argv, NUL-separated, with no width to be cut to — and falls back to `ps`
only on a host without procfs, which today means this laptop. The drill gives a
seat a data dir long enough to push its argv past 300 characters and requires an
IDENTICAL duplicate to be refused **without** that claim, so a truncating reader
fails the drill rather than quietly misdirecting every operator who hits the
refusal.

## Drill: `tools/spawn_duplicate_drill.sh`

Eight arms, of which **three require a spawn to be ALLOWED** — a clean
statedir, a pidfile naming a dead pid, and a pidfile naming a live `/bin/sleep`
that is not this seat. A guard that refused everything would pass an arm that
only looks for a non-zero exit, and those three are what make the refusal arms
mean something. Of the refusals, one is the identical duplicate and one is the
duplicate whose ARGUMENTS differ — the shape that actually happened, and the
one the first version of this fix would have allowed. The lock is tested deterministically rather than by racing two
spawns and hoping they overlap: an external holder takes the seat's lock with
`fcntl.flock`, and `host-spawn` must be observed with no pidfile written and
still running two seconds later, then must complete once the lock is released.
`host-stop-seat` gets the same arm, because a stop landing inside someone
else's spawn reaches the same corrupted state from the other side.

## Still not established, and now it does not matter

Whether `flint-server` exits on a bind conflict quickly enough to lose the race
in the direction described. It was the open question here; the fix does not
depend on the answer, because the guard refuses before either copy is started
rather than adjudicating which one loses.
