# BUG-0166: the workspace stopped building at its own declared MSRV, and only a CI leg nobody reads said so (FIXED 2026-09-17)

Status: **FIXED 2026-09-17**; the raised floor **ratified by Jeff 2026-09-18**
on the question as put — keep 1.89, or revert to 1.88 and rewrite the lock.
Found while reading CI after an unrelated push · Severity: **low for the fleet, medium for the claim** — nothing
we ship is affected (the bundle is 14 binaries built at 1.98), but README and
`docs/self-hosting.md` both told a self-hoster that Rust 1.88 was enough, and
for two days it was not.

## Measured

`.github/workflows/msrv.yml` has failed on **every push since `89e3f476`**
(2026-09-16 01:11Z) — the last green run in a 30-run window. Today's four
pushes, mine and the peer's, each show `msrv failure` beside `ci success` and
`gate success`:

```
3d0badd9 failure | BUG-0159: the write-up and index record the fix ...
d8263eb3 failure | BUG-0163: rewind snapshots are never pruned (OPEN)
c9c1dc5c failure | BUG-0165: expand_fill waits for READY ...
2097d766 failure | BUG-0160: the Raft control plane refuses ...
75c5d44f failure | BUG-0161: record the placement measured on a kept gate box
```

The error, four times over:

```
error[E0658]: use of unstable library feature `file_lock`
error: could not compile `flint-ctl` (bin "flintctl") due to 4 previous errors
```

`File::try_lock` was stabilised in **1.89**. `Cargo.toml` declared
`rust-version = "1.88"`.

## Where it came from

`8d7c052` — "BUG-0144 + OPS-0250: a spawn refuses a live copy of its seat,
under a seat lock" — introduced `lock_seat`, which takes `flock(2)` through
`File::try_lock` so the lock belongs to the open file description. That is the
right primitive for the job, and the job (two `flintctl` processes not spawning
one seat twice) is a correctness fix worth keeping.

## Why every gate stayed green while CI was red

**The gate's default run excludes the `msrv` stage on purpose.** `gates.sh`
says so at the top: *"default: every stage but msrv"*, because
`.github/workflows/msrv.yml` already runs it per push. So the split is by
design — and the half that runs is the half nobody reads, while the half
everybody reads cannot see the problem. Eleven gate-box runs today, 161 steps
each, all green, none of them able to catch this.

That is the same shape as BUG-0158 one turn of the screw further out: not a
check that fails, a check whose **result nobody looks at**.

## The fix: raise the declaration to what is true

`rust-version = "1.89"`, with README:370 and `docs/self-hosting.md`:53 raised
with it — the `gates.sh` assertion that every Rust-version claim agrees with
`Cargo.toml` is what makes that a single edit rather than a hunt.

**Why not rewrite the lock instead.** The alternative the workflow names is
"the code stops using newer APIs". `File::try_lock` is the correct primitive
here and the lock it implements is a fix for a real double-spawn; replacing it
with a hand-rolled lockfile would trade a correctness fix for a version number.
1.89 shipped in August 2025, so the floor is still over a year old.

**What this costs a reader**: anyone pinned to exactly 1.88 can no longer build
from source. Nobody is known to be; the released bundle is unaffected.

## What it does NOT fix

**Nothing here makes anyone read the msrv leg.** The leg was red for two days
across five pushes by two sessions, and what found it was me reading CI for an
unrelated reason. A gate that runs somewhere nobody looks is a check with a
delivery problem, and this bug does not solve that — it just pays off the debt
it produced.
