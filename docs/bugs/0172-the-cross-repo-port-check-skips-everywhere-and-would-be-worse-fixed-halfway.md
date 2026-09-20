# BUG-0172: the cross-repo port check skips everywhere, and would be worse fixed halfway (FIXED 2026-09-20)

Status: **FIXED 2026-09-20**, the mirror of ops OPS-0281, assigned by Jeff the
same day · Severity: **no false failure — total silent loss of coverage, and a
trap laid for whoever fixed it.**

## The defect

`assert_no_cross_repo_ports` compares this repo's drill port claims against the
ops repo's, so the two suites cannot `-9` each other's seats on a machine
running both. It reached the ops repo as `${FLINT_OPS:-../flint-cache}` and read
`$ops/tools` — the working tree.

**It has been skipping on every worktree.** Every change here is made in a
linked worktree on the external SSD, where `../flint-cache` is nothing, so the
check printed

    SKIP: no ops checkout at ../flint-cache — cross-repo port overlap NOT checked

and compared nothing. Honest about it, and still no coverage — from the side
that BUG-0156 had already once widened for reading too little.

## The trap, which is the actual finding

**Fixing the resolution alone would have been worse than the skip.**

The repo already contains the better resolver, two functions above this one:
`assert_sibling_lock_path_is_pinned` resolves via `git rev-parse
--git-common-dir` to the primary worktree, with an override and a guard against
an empty substitution yielding `/..`. Its comment says exactly why — *"a plain
`..` default would SKIP on the one machine that HAS a checkout."* Copying that
resolution across is the obvious fix and takes ten minutes.

Measured before doing it:

| ops checkout the resolution finds | behind | overlap from its **tree** | from its **`origin/main`** |
|---|---|---|---|
| `~/dev/flint-cache` | **1295** | **7461 7462 7463** | *(none)* |

**Those three ports are ops OPS-0271**, *"a fleet drill and a public drill
claimed the same three ports"*, **fixed on 2026-09-19** by moving the fleet
drill to 7485-7488. Confirmed against the ops ref today: 0 drills there claim
7461-3, and 1 claims 7485-8.

So a resolution-only fix turns a silent gap into a **red public gate reporting a
bug that was closed two days earlier**, blamed on the other repository. The
order of the two halves is not a preference.

## The fix

Both halves, together:

- **Resolve** like the flint-kv guard already does — `FLINT_OPS` if set,
  otherwise the primary worktree's sibling, so it stops skipping on the machines
  that actually have both repos.
- **Read the ref.** The ops `tools/*_drill.sh` are materialised from
  `origin/main` into a temp dir and the existing `drill_declared_ports` is
  pointed at it, so `tools/lib/drill-ports.sh` is unchanged and both sides are
  still scanned by one library (BUG-0156's requirement).
- Working tree only when there is no ref, and the provenance is printed either
  way. No checkout at all still skips and says so.

It now compares **160 ops ports against this repo's 542** where it previously
compared nothing.

## Verification

Four layouts: `FLINT_OPS` unset, the 1295-behind checkout, a current ops
worktree, and a path that does not exist. The first three read `origin/main` and
report no collision; the fourth skips and says so.

Three controls, on a fixture ops repo built with a real `refs/remotes/origin/main`:

1. the read prefers `origin/main` over the working tree;
2. **a port claimed only in the ops working tree is NOT reported** — the
   7461-7463 shape, encoded so the trap above cannot be re-laid;
3. the same port **on the ref** still is — coverage kept, not traded for silence.

## Two bugs this found in itself

**The ref branch returned the temp root instead of its `tools/`**, while the
tree branch returned `$d/tools`. The two branches disagreed, so the ref path
handed `drill_declared_ports` a directory with no drills in it and the check
reported *"no port is claimed"* against an **empty set** — a false pass produced
by the fix. Control 3 is what caught it; without a coverage-kept arm this would
have shipped looking green.

**A probe of the new function run under zsh reported zero ops ports**, because
`for f in $listed` relies on the shell's word-splitting and zsh does not do it.
`gates.sh` is bash, so the shipped path was correct — but a function whose
answer depends on which shell sourced it is one measurement away from a
confident wrong number, and it nearly became one here. Now a `while read` loop,
verified identical under both shells: 121 files, 160 ports.

## What is NOT claimed

- **Not that a collision exists today.** There is none, checked from the ref in
  both directions. What changes is that the check can now see one.
- **Not that the other sibling reader was wrong.**
  `assert_sibling_lock_path_is_pinned` reads flint-kv's `tools/drill_lib.sh`
  from a tree, and that is the model this fix copied rather than a defect: it
  asserts a *mechanism is still taken*, which is a property of the code that
  runs, not of a ref.
