# BUG-0144: `host-spawn` reports success, and takes the pidfile, without checking the child survived (OPEN)

Status: **OPEN**, found 2026-09-14 · Severity: **medium** — not reachable
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
  is not given the ident — `host-spawn` never receives one. Adding it is a
  signature change across every spawn call site.
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
