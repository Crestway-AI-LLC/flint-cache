# BUG-0117 — the duplicate-seat guard was inert on a multi-seat control plane (FIXED 2026-09-06)

**Status: FIXED 2026-09-06** · Severity: medium — no fleet has been affected,
because no fleet has ever run more than one CP seat off a laptop. Found while
scoping the roadmap's remaining multi-node-CP work.

## The guard, and what it is for

`launch` will not spawn a seat that is already there. A dial is not enough to
decide that: a Raft seat replaying its log answers nothing until it is ready, so
`PING` failing means "not serving", not "absent". The second check is a process
probe, and the comment at that call site says exactly what it is buying:

> spawning beside it gives the duplicate a lost port race and a clobbered
> pidfile — after which every stop aims at a corpse.

## The defect

    seat_alive(&runner_for(inv, seat), "flint-controlplane", &format!("{d}/cp-state"))

One literal, for every seat `i`. And `pids_in_ps` matches an ident as a **whole
token**:

    args.contains(bin) && args.split_whitespace().any(|t| t == ident)

A three-seat CP spawns with `--state <statedir>/cp-state-n1`, `-n2`, `-n3` —
`cp_seat_state` exists to say so, and `cp_seat_args` was spelling the same rule
a second time. So the ident `<statedir>/cp-state` matches **none of the three**,
and the probe answers *no process running* for three live seats.

**That is the dangerous direction.** Every caller reads `false` as "spawn one".
So the guard against duplicating a still-replaying seat was inert on precisely
the topology it was written for — the single-seat CP, where it works, is the one
that cannot have this problem.

## Why nothing caught it

- **One seat spells the dir `cp-state`.** `cp_seat_state` returns the bare form
  when `inv.cp.len() == 1`, so the literal is correct there, and every inventory
  under `packaging/` renders exactly one `cp` line.
- **The three-seat drills never call `start`.** `ctl_cpha` and `cpha_roll`
  bootstrap three seats, kill the leader and assert a mutation still lands —
  they never ask `flintctl` to bring a seat back, which is where the probe runs.
- **The window is narrow on loopback.** CP spawn to first PONG was measured at
  23–64 ms; a duplicate needs the probe to run inside that window. A seat
  replaying a real log on a real host is a much wider target.

## The fix

`cp_seat_state(inv, i)` at both call sites — the respawn decision and the
timeout diagnostic, which was reporting "process absent" on the same wrong
ident. One seat still spells it `cp-state`, so the single-node path is
unchanged.

And `cp_seat_args` now calls `cp_seat_state` instead of repeating its
`if len == 1` rule. The two had to agree exactly — the probe matches the
`--state` token the spawn writes — and the way two copies of a rule stop
agreeing is one of them being edited. Same argument `cp_seat_name` already
makes one field over, where two spellings of a pidfile name meant a roll
stopping a seat it never started.

## Controls

Three unit tests, each built from a parsed inventory rather than a hand-made
`Inventory`, so they exercise the parser a fleet uses:

| test | asserts |
|---|---|
| `the_old_literal_ident_matches_no_seat_of_a_three_seat_cp` | the literal finds **zero** of three live seats; each per-seat ident finds exactly one |
| `one_seat_still_matches_and_the_two_spellings_agree` | the single-seat path is untouched |
| `spawn_args_and_the_probe_spell_the_state_dir_identically` | `--state` as spawned equals the ident as probed, for 1 and 3 seats |

The third was **confirmed by reverting the fix**: with `cp_seat_args` putting
the literal back, it fails and the other two still pass.

## What these tests do NOT cover, stated rather than implied

They pin the token semantics and the two spellings. **They do not cover the
call site**: nothing here would fail if `launch` went back to passing a literal,
because a unit test cannot easily observe `launch` deciding to spawn.

That guard is a fleet-level one — a duplicate CP process is directly countable
on the host — and the cross-host control-plane exercise now counts them.

**CORRECTED the same day, once that exercise existed: counting them is not the
same as catching this.** `packaging/aws/cp-quorum/run.sh` (ops) kills a CP seat,
runs `start`, and asserts the host still has exactly the seats it should. It
passed — and it would have passed on the unfixed code too, because the seat it
kills is **genuinely dead**, and there the broken probe and the fixed one give
the same answer: absent, spawn, correct.

The harm needs a seat that is **up but not answering** — a Raft seat replaying
its log, which is the whole reason the probe exists beside the `PING`. Nothing
in the harness opens that window, and nothing yet does.

So the honest coverage is: the token semantics and the two spellings are pinned
by unit tests, the ordinary restart path is guarded on a real two-host fleet,
and **the defect's own condition has never been reproduced**. It was found by
reading and fixed by construction. Recorded this way because the first version
of this section pointed at an exercise as if it would close the gap, and it does
not.
