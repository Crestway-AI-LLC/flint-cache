# BUG-0014: chaos_unreadable fails an acked write on a REPLICA kill (OPEN)

Status: OPEN, instrument FIXED 2026-08-22 · hypothesis (a) ELIMINATED 2026-08-22 by the first firing of the probe; (b) still unresolved, but the probe can now state which it is · First fired 2026-08-11, not 2026-08-18 · Severity: high if real
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

The firings (the rate table above is scoped to the first window; the last
row postdates it):

| When | Run | Commit under test | Other drills failing |
|---|---|---|---|
| 2026-08-11T20:17Z | `31532171671` | `209a67ff` | none — it was the only failure |
| 2026-08-16T02:16Z | `31921536272` | `056e5930` | none — it was the only failure |
| 2026-08-18T03:08Z | `32094370382` | `0946d1a5` | coproc_vec, coproc_vec_tls, reseed |
| 2026-08-18T06:36Z | `32107656681` | `a7b742f4` | coproc_vec, coproc_vec_tls, reseed |
| 2026-08-21T01:06Z | `32435117583` | `c9487f3` | none — it was the only failure |

Green streaks between them, in order: **17, 13, 16, 2, 5, 21**.

**That series is the whole finding.** Three separate streaks of 13+ green gate
runs have already happened, and every one of them ended in another failure. The
current streak was 5 when this was written; it reached **21** — the longest
yet — and then ended the same way on 2026-08-21. Nothing has stopped.

## Fifth firing, 2026-08-21 — the longest green streak yet, ended the same way

Run `32435117583`, commit `c9487f3`, during the rc.60 release cut. Twenty-one
consecutive green gate runs preceded it, beating the previous best of 17.

    iter 3: REPLICA kill lost acked write at key954: 19573 < 24264
    entries_above_got=[(seq=24264 sent_before_prev_kill=false)]

`sent_before_prev_kill=false`, so by this file's own decision rule the write
was served by the CURRENT master and lost — the "real" branch, not the
harness-ledger branch.

**The surface message was a consequence, not the cause,** and is worth
recording because it will mislead the next reader:

    FAIL: scenario not exercised — need a MASTER kill followed by a REPLICA
    kill, got: REPLICA MASTER
          the seed's kill order changed; pick a seed that yields MASTER then REPLICA

Nothing about the seed changed. The chaos run PANICKED at iteration 3, which
truncated the observed kill sequence to the two completed iterations, and the
scenario assertion then failed on the truncated list. Reading that message at
face value sends you to look for RNG drift that is not there.

**Attribution, because the commit it landed on invites the wrong one.** The
diff from the last green gate (`096aa63`) to `c9487f3` is two
`#[allow(clippy::result_large_err)]` attributes, their comments, and a
markdown file — no executable change whatsoever. The one change in this
stretch that could plausibly affect behaviour (the RESP3 map-decode rewrite)
is in `6ff8ca8`, two commits earlier, and that commit's own gate was green.
A re-run of `32435117583` at the identical sha passed.

Failure rate including this firing: **5 of ~78 runs, ~6.4%** — consistent
with the 7.0% measured over the first window, which is the point: it has not
drifted, improved, or gone away in ten days.

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

## The probe would have printed NOTHING — fixed 2026-08-18

Before the next firing could pay out, the drill would have thrown the payout
away. `chaos_unreadable_drill.sh` captured the harness's stdout to `$D/out.log`
and printed it as:

    sed 's/^/  | /' "$D/out.log" | grep -E "iter |unreadable|PASS|FAIL|panick"

with `rm -rf "$D"` on the trap. The grep is line-based and the BUG-0014
diagnostic is a seven-line panic message. Run the real format through the real
pipeline and only two lines survive — the `panicked at` header and the `iter N`
assertion. Everything that decides the question is dropped:

    DROPPED  == BUG-0014 DIAGNOSTIC ==
    DROPPED  read_via: direct (cluster.master_client)
    DROPPED  master:   127.0.0.1:6347 role=master epoch=3 last_applied=...
    DROPPED  prev_master_kill dead_ms: ...
    DROPPED  ledger last_acked=... entries_above_got=[... sent_before_prev_kill=true]
    DROPPED  READ IT SO: ...

**That is exactly the two-line shape every recovered CI log shows**, which had
been read as "the panic is terse". It was not; the drill was filtering it. The
probe, the `read_via` branch recorder and the `sent_before_prev_kill` flags —
the whole apparatus for separating a harness bug from a durability regression —
would have produced nothing, in CI and locally alike, and the unfiltered copy
is deleted when the drill exits.

Fixed: on a non-zero return the drill now prints the log entire, unfiltered.
The filtered view is kept for a passing run, where it is a readability choice
rather than a loss. **This drill was the only one of 108 that filtered a
captured log this way**, so the fix is local, not a sweep.

## FALSIFIED: the excluded-key mechanism, measured 2026-08-18

A candidate mechanism for (b): the master-kill prune at `:743` runs *inside* the
loop over the iteration-2 snapshot, so a key excluded from that snapshot
(`last_acked == 0`) is never pruned. If it later acquires an ack predating the
kill, iteration 3 asserts it against a seat that never had it. The two snapshot
filters are identical (`:547`, `:789`), so membership turns purely on timing —
and unlike regression volume, a single straggler would explain why the smallest
regression failed and the largest passed.

**Measured directly.** Instrumented the iteration-2 snapshot to count ledger
entries with `last_acked == 0`:

| configuration | in snapshot | EXCLUDED |
|---|---|---|
| `--keys 4000` (the drill's own), 5 runs | 4000 | **0, every run** |
| `--keys 50000` | 37988 | 0 |
| `--keys 200000` (positive control) | 69552 | **2** |

**The counter is not vacuous** — `writer.rs:295` creates the entry with
`or_default()` at write-issue time and only `record_ack` (`:315`) sets
`last_acked`, so the excluded population is exactly the keys whose FIRST ack is
in flight. At 200k keys that population is non-empty and the counter moved,
which is the positive control; at the drill's 4000 keys every key is
first-acked within the opening moments of the run, thousands of writes before
iteration 2.

**So the mechanism cannot fire at the configuration this bug actually runs.**
Recorded with its positive control because a zero from an instrument that
cannot move would have looked identical.

## 2026-08-22 — THE DISCRIMINATOR FIRED, and its verdict is not usable

It fired on the fifth occurrence and the output has been sitting in a GitHub
artifact since 2026-08-21. Nobody looked, for a reason worth recording: **the
run was re-run and the second attempt passed, so `gh run list` reports that run
as `success`.** The failure is only visible as `attempt=1`, and its artifact is
a second, earlier `gate-logs` entry on the same run id. A scan for failed gate
runs does not find it.

From `chaos-chaos_unreadable.log` in artifact 9431051446 (run 32435117583,
attempt 1, `c9487f3`):

    iter 3: REPLICA kill lost acked write at key954: 19573 < 24264
    == BUG-0014 DIAGNOSTIC ==
    read_via: direct (cluster.master_client)
    master:   local port 6351 role=master role_epoch=(0,2) seq_lag=43 live_replicas=1
    prev_master_kill dead_ms: 1787276126708
    ledger last_acked=24264 entries_above_got=[(seq=24264 sent=1787276126708
                            at=1787276126708 sent_before_prev_kill=false)]

**Hypothesis (a) is dead.** `read_via: direct` and the resolved node reports
`role=master role_epoch=(0,2)` — the harness dialled the promoted seat, not a
pre-promotion one. The code reading was right.

**But the verdict on (b) rests on a single entry at the clock's resolution limit** (and see the correction below, which narrows this). The probe's rule is: all
`sent_before_prev_kill=false` means "served by the CURRENT master and lost ->
real". The single offending entry has

    sent      = 1787276126708
    dead_ms   = 1787276126708

**the same millisecond**, and the test at `main.rs:945` is `sent <
last_dead_ms` — strict. So "sent in the same millisecond the previous master
died" is silently classified as "sent after it", and the probe reports a real
durability regression on the strength of a strict inequality between two equal
numbers.

At 1 ms granularity that write is genuinely unclassifiable: it may have been
acked by the old master microseconds before it died, which is the BUG-0007
ledger class this probe exists to separate out, or by the new one after, which
is a durability regression. **The instrument cannot tell, and says it can.**

### Correction, same day: the strict `<` is a deliberate convention, not an oversight

The paragraph above called the comparison a boundary artifact without reading
why it is written that way. It is documented, and the reasoning is sound
(`main.rs:478`):

    Two clocks on purpose. `kill_ms` is armed BEFORE the kill and times the
    outage from the writer's vantage. `dead_ms` is stamped AFTER the SIGKILL
    landed and is the only boundary the LEDGER may use: in the gap between the
    two — an epoch read plus a pkill spawn, tens of ms on a busy box — the old
    master is alive and still acking. Judging those acks as "sent after the
    kill, so the new master's" left the ledger claiming values the survivor
    never had, and the NEXT replica kill reported them as data loss.

So `sent >= dead_ms` means "the new master's write", and `main.rs:772` skips
exactly those entries for the same reason. The probe is consistent with the
codebase, and a write classified that way, if missing, IS a real loss. On that
reading the verdict of "real" is defensible rather than meaningless.

**What survives the correction is narrower and still worth acting on.** Because
`dead_ms` is stamped after the kill, the death instant lies at or before it —
so a send stamped in the SAME truncated millisecond may have preceded the death
and been served by the old master. The classification is probably right and is
not certain, and here the entire verdict rested on **one** entry sitting exactly
on that truncation boundary. "Probably a durability regression, on a single
sample at the one point the clock cannot resolve" is not a conclusion to act on,
and it is not the same statement as "unusable".

The remedy is unchanged and cheaper than the argument: microsecond stamps make
the comparison decidable at the only point where it is contested, and an
explicit AMBIGUOUS state for `sent == dead_ms` stops a tie from being reported
as either answer. Nothing about the convention needs to change.

### What this needs before the next firing

1. **A third outcome.** `sent == dead_ms` is neither `true` nor `false` for this
   purpose; it is AMBIGUOUS and must print as such. The current two-valued
   output converts a boundary into the more serious of the two answers.
2. **Finer timestamps.** Both values come from millisecond clocks. Microseconds
   would make the comparison meaningful at the only point where it is contested
   — a kill and an in-flight write land in the same millisecond routinely.
3. Only then is the "real vs harness bug" question answerable from this probe.

Filed rather than fixed here because changing the probe changes what the next
firing reports, and the next firing is roughly 14 gate runs away — the change
should be deliberate, not folded into a bug-reading pass.

### The reading rule that produced this

The probe was written to be read by rule, and the rule was followed: "All false
-> real." Following it here would have published a durability regression from a
tie. That is the day's recurring shape once more — a check that cannot answer
producing output indistinguishable from an answer — this time inside the
instrument built to settle the question.

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

## The instrument is fixed; the verdict is not yet re-taken

Implemented 2026-08-22. Both halves of the remedy the correction above asked
for, and nothing else — the convention itself was sound and is unchanged.

**1. The send stamp is now microseconds.** `writer::now_us` joins `now_ms`
rather than replacing it, because the two answer different questions: the send
stamp settles an ORDERING against the death instant, while the RPO bound is a
claim about milliseconds and reads better in them. `KeyLedger::acked_at` is
now `(seq, sent µs, acked ms)`, deliberately mixed and documented as such.
`kill_master_hot` and `Cluster::last_kill_dead_us` return microseconds too.

The rename from `dead_ms` to `dead_us` was not cosmetic: it turned three
missed call sites into compile errors. Changing the unit while keeping the
name would have left all three comparing microseconds against milliseconds,
silently, by a factor of a thousand.

**2. The tie has its own answer.** `oracle::classify_send` returns
`MaybeOldMaster` / `NewMaster` / `Ambiguous`, and both call sites — the loss
loop and the failure dump — now go through it. They used to spell the
judgement out separately and disagreed at the boundary, which is why the dump
could print `sent_before_prev_kill=false` for a tie, indistinguishable from a
write provably sent after the kill. That is the reading which made this bug
look decided.

An ambiguous entry is counted in `ambiguous_at_boundary` and excluded from
`beyond_cap` and `deepest_loss_ms` alike. It is not scored conservatively; it
is not scored. A measurement taken on an unattributable write is a number with
no referent, and averaging it into a bound corrupts the bound.

### What was verified, and what that does not cover

Both new assertions were mutation-tested — the only check that separates a
test from a decoration:

- reverting `classify_send` to the pre-fix `>=` reading (tie -> `NewMaster`)
  fails `a_tie_is_neither_master_and_must_not_be_silently_assigned`.
- rebuilding `now_us` on `as_millis() * 1000` — a microsecond clock in name
  only — fails `the_microsecond_clock_actually_resolves_below_a_millisecond`.
  Without that control the whole change could have been a no-op on a coarse
  platform clock while reading exactly like a fix.

`chaos_unreadable` and `chaos` both pass locally, and the drill log now
carries `dead_us` beside `kill_ms`, with the two differing by the ~16ms arming
gap the comment predicts.

**The original verdict has NOT been re-taken.** This run reported
`ambiguous_at_boundary` of zero and no regression, but the failure it was
built to adjudicate has not fired since — it is roughly fourteen gate runs
away by its historical rate. What changed is that when it next fires, the
report will say which of the three it is instead of choosing for the reader.
The bug stays OPEN until then, and closing it on the absence of a firing would
repeat the error this entry exists to record.

## Related

- `crates/flint-chaos/src/main.rs:790-801` — #126's fix and the comment that
  names this exact symptom on the direct path
- BUG-0021 — gate log retention; contains the retraction of the claim that
  these firings were unrecoverable
- BUG-0007 — resolved, same assertion text, different drill; eliminated by date
- BUG-0011 — the other open drill defect, and the run-it-alone rule
