# BUG-0014: chaos_unreadable fails an acked write on a REPLICA kill (OPEN)

Status: OPEN · First fired 2026-08-11, not 2026-08-18 · Severity: high if real
— the oracle is asserting the durability claim, so either the claim broke or
the oracle is crying wolf, and both are worth an hour

**Numbering, because git log points the wrong way.** Commit `6613c15`, which
fixed the other three gate failures, calls this one "BUG-0012" in its message.
It is BUG-0014; the number was reassigned before filing, because 0012 was
already the WAL-retention livelock. That commit carries the same correction as
a git note (`git log --notes`), but a fresh clone does not fetch notes, so the
correction lives here too. Do not follow the 0012 reference.

## The measured behaviour: it fires ~7% of gate runs, and has for eight days

Every `gate` workflow run on `main` was read, and each was scored by whether
`chaos_unreadable` produced a `PASS` or `FAIL` line — not by whether the run
was red, which is a different question and the source of two wrong conclusions
below.

| | |
|---|---|
| gate runs where the drill actually ran | **57** (2026-08-10 → 2026-08-18) |
| runs where it FAILED | **4** |
| **measured failure rate** | **7.0%** |
| gate runs excluded because the drill did not run at all | 34 (predate the drill) |

The four firings:

| When | Run | Commit under test | Other drills failing |
|---|---|---|---|
| 2026-08-11T20:17Z | `31532171671` | `209a67ff` | none — it was the only failure |
| 2026-08-16T02:16Z | `31921536272` | `056e5930` | none — it was the only failure |
| 2026-08-18T03:08Z | `32094370382` | `0946d1a5` | coproc_vec, coproc_vec_tls, reseed |
| 2026-08-18T06:36Z | `32107656681` | `a7b742f4` | coproc_vec, coproc_vec_tls, reseed |

Green streaks between them, in order: **17, 13, 16, 2, 5**.

**That series is the whole finding.** Three separate streaks of 13+ green gate
runs have already happened, and every one of them ended in another failure. The
current streak is 5 — the shortest of the long ones. Nothing has stopped.

## There is no "it stopped failing" transition, and two of us went looking for one

Both investigations on 2026-08-18 tried to explain when the bug stopped firing.
It never stopped, so both explanations were built on a boundary that does not
exist. The two errors are different and worth keeping separately, because they
are the same mistake reached by different routes.

**Error 1 — dating a commit with the wrong field.** `0a763ff` was proposed as
the fix, on the grounds that every failure predates it. It was dated with `%ad`
(author date, 2026-08-17T22:59Z). Its commit date is 2026-08-18T15:22Z, 562
minutes later. Rebasing preserves the author date and rewrites the commit date,
and this repo rebases to land — so `%ad` is *systematically* wrong for "when
did this land", not occasionally wrong. Use `%cd`, or order by what each CI run
actually built.

**Error 2 — checking only the runs adjacent to the hypothesis.** The refutation
of error 1 was that `chaos_unreadable` passed at 05:12Z and 06:43Z, ten hours
before `0a763ff` landed. Both true. But only the runs on either side of the
proposed boundary were checked, and **the drill failed again at 06:36Z, between
them.** That failure was sitting in the run list the whole time. It was missed
because the search was for "is there a green after the boundary", which the data
answered, rather than "when did it last fail", which the data also answered and
nobody asked. A two-hour bisect window was then handed to a peer on that basis
and had to be withdrawn.

**The shared shape:** "every failure predates the fix" is TRUE and is the half
that misleads; "the greens begin at the fix" is FALSE and is the half that had
to hold. A necessary condition confirmed and read as a sufficient one — the
same family as a check that can only rule out being treated as though it
selected in. The test that catches it: **ask which observation would be
IMPOSSIBLE if the cause were wrong.** For `0a763ff`, none would be. That is
consistency, not support.

**Consequence for the record:** this bug is not fixed, was never fixed, and the
window between 03:08Z and 05:12Z contains only two commits, both of which touch
`README.md` and nothing else. There was never any code in it to bisect.

## What the base rate does to the rest of this write-up

At p = 0.07 per run, most of the negative evidence collected against this bug
is worth far less than it looked:

| Evidence | P(that outcome \| bug unfixed) |
|---|---|
| 6 raw-binary runs, no firing | 0.65 |
| 12 lag-sweep runs, no firing | 0.42 |
| **21 local runs, no firing** | **0.22** |
| 5 consecutive CI greens (current streak) | 0.70 |

**None of those is surprising under "nothing was fixed."** Specifically:

- **"It has only ever fired inside a full gate" is not established.** All four
  firings are gate runs and 21 local runs stayed silent — but 21 silent runs
  happen 22% of the time at this rate even if the gate makes no difference
  whatsoever. The observation is real; the inference from it is not supported.
  Distinguishing "the gate is required" from "the gate is where the runs are"
  needs ~42 local runs at zero, not 21.
- **Ruling this bug out at 95% confidence takes 42 consecutive green runs.**
  Five are in hand. Any future "the fix worked" claim should state its streak
  against that number.
- The lag sweep's result still stands, because it rests on a *positive*
  control rather than on silence — see below.

## Both causal hypotheses are eliminated by date

The previous revision named two candidates. The 2026-08-11 firing kills both,
because it predates each of them:

- **`f9782c4` (warm rejoin) — ELIMINATED.** It landed 2026-08-16T07:29Z. The
  drill had already failed on 08-11 and again on 08-16T02:16Z, both before it.
  This was the leading hypothesis and it cannot be the cause.
- **`056e5930` (#190, mark-for-rewind instead of wiping) — ELIMINATED.** It
  landed 2026-08-15T19:16Z, four days after the first firing.

**The claim that produced them was itself wrong.** The previous revision said
"first red gate run is `31933995660` (2026-08-16T07:29Z)" and derived
`f9782c4` as the commit between it and the last green. That run's
`chaos_unreadable` **PASSED** — the run was red on `reseed`. Reading a red gate
as a red drill is what put a five-day-old bug's origin on the wrong commit.

**BUG-0007 is also not it.** Same assertion text, but resolved 2026-08-07 by
`2aa573e`, four days before this drill's first firing, and in a different drill
(`chaos_drill.sh` at seed 7). An incomplete fix is not excluded, but nothing
here suggests one.

## Symptom

`tools/chaos_unreadable_drill.sh`:

    iter 1: pair 0: killed REPLICA; zero acked loss verified
    iter 2: pair 0: killed MASTER (writes in flight); RTO 22ms harness-promoted,
            not RTO [kill_ms=... max_hold_ms=2]; acked keys regressed: 20
            (all within the 1000ms cap); 3 key(s) unreadable after retries —
            retired from the ledger, NOT judged as loss
    thread 'main' panicked at crates/flint-chaos/src/main.rs:808:25:
    iter 3: REPLICA kill lost acked write at key3121: 18195 < 23739
    FAIL: scenario not exercised — need a MASTER kill followed by a REPLICA
          kill, got: REPLICA MASTER

The reported FAIL is downstream noise: iters 2 and 3 ARE master-then-replica,
but iter 3 panicked before it could print its kill line, so the drill's
positive control saw a truncated sequence. **The failure to investigate is the
panic, not the sequence check.**

**All four firings were recovered from CI artifacts** (`gate-logs`, uploaded by
`gate.yml` on every run, 14-day retention). An earlier revision of this file
claimed they were unrecoverable; that was wrong — see the retraction in
BUG-0021. The four are the same assertion: line 640 on the 2026-08-11 build and
line 808 today, both the `got >= *last_acked` assert.

## The four failures against three passes — the comparison that narrows it

Every run has the identical kill order (fixed seed): REPLICA, MASTER, REPLICA.

| Run | iter 2 regression | iter 3 outcome | gap |
|---|---|---|---|
| `31532171671` | 12 | **FAIL** key901: 30563 < 33119 | 2,556 |
| `31921536272` | 32 | **FAIL** key418: 25687 < 33542 | 7,855 |
| `32094370382` | 20 | **FAIL** key3121: 18195 < 23739 | 5,544 |
| `32107656681` | **8** | **FAIL** key539: 31237 < 32008 | 771 |
| `32102006432` | **54** | PASS — zero acked loss verified | — |
| `32108137975` | 37 | PASS — zero acked loss verified | — |
| `32181754512` | 25 | PASS — zero acked loss verified | — |

**Two findings, both from this table.**

**1. The master kill's regression does not predict the replica kill's failure,
and points the wrong way.** The smallest regression (8) failed; the largest
(54) passed. This is independent CI confirmation of the injected-lag result —
which forced regression to 1688-2343 and never fired — and it is stronger,
because it rests on observed failures rather than on silence. **Replication lag
is not the knob.**

**2. ITERATION 1 HAS NEVER FAILED.** In all seven logs, and every other log
read, `iter 1: pair 0: killed REPLICA; zero acked loss verified` passes. Iter 1
and iter 3 are the *same operation* — kill the replica, assert every acked key
still reads back. The assertion only ever fails on the replica kill that
**follows a master kill and a harness promotion.**

That is the sharpest constraint this bug has. A defect in the replica-kill
durability path should be able to fire on iter 1; this one cannot. It does not
*select* a cause — evidence that eliminates one candidate does not select
another — but it is consistent with the harness resolving a stale master after
promotion (the #126 class on the other side of the same branch) and
inconsistent with the replica-kill path itself being at fault.

**What it does not establish:** which of the two remains. The probe landed in
`581e074` prints `read_via` and the resolved address at the panic, which is
exactly the discriminator; it has not yet fired.

## Established

- The drill runs `flint-chaos --port-base 6346 --iterations 3 --keys 4000
  --seed 1 --mode mixed --inject-unreadable N`. There is **no `--edge`**, so
  `shared.edge` is None and the post-kill read at `main.rs:797-801` takes
  `cluster.master_client()` — "the port the harness KNOWS is master".
- The assertion is `got >= last_acked` against the ledger snapshot. It read
  18195 where 23739 had been acked: **5,544 behind**, on key3121.
- Iteration 2 (the MASTER kill immediately before) tolerated a regression of
  20 keys as being inside the 1000 ms cap. Iteration 3's gap is two orders of
  magnitude deeper, so "the same settling window, one iteration later" does
  not obviously cover it.
- The drill script itself was last modified 2026-08-14 (`c539a0d`) and before
  that 2026-08-06 (`ad2288e`) — it was unchanged across the 08-11 firing, and
  `209a67ff` (the commit under test then) touched `gates.sh`,
  `ctl_error_drill.sh` and `ns_escape_drill.sh`, none of the chaos path.

## Explicitly ruled out

**Not the proxy's routing.** The first hypothesis was that a read through the
edge landed on a node that had been demoted and was mid-re-seed — the shape
`main.rs:790-796` describes as the original #126 symptom. That cannot be it:
this drill passes no `--edge`, so the read never touches a proxy. Recorded
because it is the natural first guess and it costs an hour to re-derive.

**Replication lag is not the knob.** Promoted to its own section below, now
that CI evidence agrees with the injected-lag result from the other direction.

## Assumed, NOT established

With `f9782c4` eliminated and iteration 1 never failing, what remains splits
into two candidates that the code reading and the CI evidence disagree about.
Both are recorded because neither is settled.

**(a) The harness dials the pre-promotion seat.** This is the #126 class on the
other side of the same branch. **The code argues against it:** both promotion
paths assign the field synchronously before returning — `cluster.rs:1338`
(`self.master_port = self.replica_port`) and `cluster.rs:1406`
(`self.master_port = survivor`) — and `master_client()` reads that field at
`:378`. There is no visible window between promotion and the next read. It is
not ruled out, but nothing proposes a mechanism for it.

**(b) The harness dials the RIGHT seat, before that seat serves what the ledger
believes was acked.** This is a different failure from (a) and the code reading
above does not touch it. The promoted seat is the former replica; the writes in
question were acked by the seat that has just been killed. Iteration 2 already
observes exactly this — `acked keys regressed: N` — and forgives it as inside
the 1000 ms cap. The open question is whether the ledger snapshot iteration 3
asserts against still carries pre-regression values for those keys, so the
replica kill is blamed for a shortfall the master kill created and pardoned.

**Why (b) is not simply BUG-0007 returning.** BUG-0007's mechanism predicts that
more unreplicated tail fires it more often. Two independent lines say otherwise
— see below — so if (b) is right, the size of the regression is not the trigger
and something else selects which runs fail.

**The discriminator exists and has not yet fired.** The probe in `581e074`
prints, at the panic, both what `master_client()` resolved to and that node's
own role/epoch/seq_lag. (a) predicts a resolution that is not the promoted seat.
(b) predicts the promoted seat, with its own view showing it behind the ledger.

## ESTABLISHED: replication lag is not the knob

This one now has evidence from two independent directions and should not be
re-opened without new data:

- **Injected lag, 12 runs.** `--stall-replica-ms` (`main.rs:124`, applied
  `:382-389`, SIGSTOP on the replica) drove acked-key regression to
  **1688 / 2310 / 2343 / 2006** — roughly a hundred times the failures'
  tail — and the replica-kill assertion stayed silent in every run. The
  injector is positively controlled, so the silence is a result.
- **Observed CI failures, 7 runs.** The smallest regression (**8**) FAILED and
  the largest (**54**) PASSED.

One line rests on silence under forced lag, the other on which real runs went
red. They agree, and they agree against the hypothesis.

## Where to start

The instrumentation for this landed in `581e074` and is in the binary
(`strings target/release/flint-chaos | grep -c 'BUG-0014 DIAGNOSTIC'` → 1). It
has not yet fired, which at 5 runs means nothing either way.

1. **Do not chase a bisect window.** There is no transition to explain. The
   next firing is the next piece of evidence, and it arrives roughly once every
   14 gate runs.
2. At the panic, print which ADDRESS `master_client()` resolved to and that
   node's `FLINTINFO` role/epoch/last_applied. If it is not the promoted seat,
   this is an oracle bug and the fix belongs beside #126's.
3. If it IS the promoted seat, compare its `last_applied` to the ledger and
   walk back to whether the rejoin admitted a cursor it should have refused.
4. **Pull the artifact, do not re-run.** `gh run download <id> -n gate-logs`
   retrieves the failing drill's full log for 14 days after the run. Reading
   the artifact was worth more than every local reproduction attempt combined,
   and it was available the whole time.

**Run it alone.** Five drills were invalidated during BUG-0011's investigation
by a manual reproduction left running in another shell; `fleet_guard` refused
them correctly and its message was the diagnosis.

## Related

- `crates/flint-chaos/src/main.rs:790-801` — #126's fix and the comment that
  names this exact symptom on the direct path
- BUG-0021 — gate log retention; contains the retraction of the claim that
  these firings were unrecoverable
- BUG-0007 — resolved, same assertion text, different drill; eliminated by date
- BUG-0011 — the other open drill defect, and the run-it-alone rule
