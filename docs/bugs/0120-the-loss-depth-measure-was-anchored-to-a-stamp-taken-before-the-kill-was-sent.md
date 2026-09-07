# BUG-0120 — the loss-depth measure was anchored to a stamp taken before the kill was sent

Status: OPEN (instrument fixed; 56 of the 80 writes in question are settled by
hand from the fixed reading, 24 are not — see "The verdict on the 80")
Found: 2026-09-06, verifying the M2 failover soak's batch-3 result
Component: `crates/flint-chaos/src/main.rs`

## The question that exposed it

The M2 soak's 800-kill run (`soak-20260907T043640Z`, rc.69, 5 hosts, 401 master
kills, 12,476,306 writes) reported **80 acked keys regressed**. The run was
already FAIL on RTO — worst 6002ms against the 3000ms exit budget — so the
durability line was the thing to settle next: were those 80 within the async
replication contract?

The run's own output says they were. Each of the three lossy iterations prints:

    acked keys regressed: 24 (all within the 1000ms cap)

**That parenthetical is a constant.** It is a literal in the format string,
printed unconditionally beside the count. It renders identically if every one of
the 80 is an hour past the cap. Nothing behind it is evaluated. That is the
defect that nearly ended the investigation — the number was read, the annotation
believed, and the result almost written up as "80 regressed, all annotated as
within the contract".

## The actual defect

Discount the constant and two measured quantities remain, both zero on this run:

- `beyond_cap` — lost entries acked early enough that replication was obliged to
  have carried them: `at <= must_have_replicated_by`, where
  `must_have_replicated_by = kill_ms - (lag_hard_ms + rpo_margin_ms)` = kill_ms - 1500ms.
- `deepest_loss_ms` — `max(kill_ms.saturating_sub(at))` over lost entries, printed
  as `deepest acked-write loss: 0ms before the kill`.

**Both are anchored to `kill_ms`, and `kill_ms` is not the death.** It is stamped
at `main.rs:509`, twelve lines before `cluster.kill_master_hot()` at :521. On the
multi-host path — `Target::Attached`, what a real fleet uses — that call makes a
master-discovery round trip (`a.master()`), *then* an SSH kill, and only then
stamps `dead_us`. The true death lies in `(kill_ms, dead_us]`, and on this run
that window was not small:

| iter | RTO | keys lost | kill_ms → dead_us |
|---|---|---|---|
| 603 | 6002ms | 24 | **3304ms** |
| 610 | 3464ms | 29 | 720ms |
| 645 | 3568ms | 27 | 711ms |

A write acked inside that window was served by a master still alive. Its `at`
exceeds `kill_ms`, so `kill_ms.saturating_sub(at)` **saturates to 0** — it cannot
raise a `max` however deep it truly was — and `at <= kill_ms - 1500` is false, so
it never trips `beyond_cap`. On iter 603 the blind window is **3304ms, 3.3x the
1000ms cap**.

So `deepest_loss_ms: 0` was not a measurement that came back clean. It is the
saturation floor. And the summary then states the affirmative:

    NOTE: loss depth 0 means replication kept up throughout — the RPO bound was
    not exercised by this run (try --stall-replica-ms)

That is the strongest claim in the output, printed on a run that lost 80 acked
keys, and it rests entirely on the anchor.

## The verdict on the 80

The reading is recoverable by hand, because the population was NOT empty. Entries
whose send stamp ties the death stamp are excluded from both measures on purpose
(BUG-0014: a measurement on an unattributable write is "a number with no
referent"), and the run reports **`boundary ties (send == death stamp): 2`** —
two entries, run-wide. Everything else was measured.

That makes the chain tight:

1. `deepest_loss_ms` is a run-wide max over every lost entry, and it is **0**.
   With at most 2 entries excluded, essentially every lost write had
   `at >= kill_ms` — acked at or after the harness's pre-kill stamp.
2. The true death is at or before `dead_us`. So each lost write's true depth,
   `death - at`, is **bounded above by that iteration's `kill_ms → dead_us`
   window**.
3. Apply the bound per iteration:
   - **iter 610 — 720ms window, 29 keys.** Depth ≤ 720ms < 1000ms cap. **Within.**
   - **iter 645 — 711ms window, 27 keys.** Depth ≤ 711ms < 1000ms cap. **Within.**
   - **iter 603 — 3304ms window, 24 keys.** Depth ≤ 3304ms, which does not
     exclude a breach. **Not settled.**

**56 of the 80 are within the cap even on the strict age reading the product does
not owe; 24 cannot be judged from what the run recorded.** At most 2 of the 80
carry the ambiguity caveat, since only 2 entries run-wide were excluded.

Two further facts belong with that verdict. The run's `final walk: 300 present, 0
missing-or-regressed` — nothing was missing at the end. And loss tracks slow
failover: the three lossy kills are the three worst RTOs in 401 (6002, 3568,
3464ms) and 398 kills lost nothing — though the 4th-worst, iter 732 at 3240ms,
lost nothing either, so slow does not imply lossy and the sample is three.

## Why the age check was gone in the first place, and why that was right

The assertion that would have caught this was removed deliberately, and the
removal is well argued in a comment that stays: the product bounds the **volume**
at risk, not the **age** — past the cap the master stops accepting, so at most one
cap-window's worth is ever unreplicated, while an already-acked write ages without
limit behind a stalled replica. An age assertion fails honest runs for a promise
nothing makes, and it did (seed 42, a natural 3160ms stall).

Nothing here reopens that. Removing the *verdict* was correct. What went wrong is
that the *measurement* kept underneath it — explicitly, "the depth is still
MEASURED and reported every run, so a real regression remains visible" — is
anchored so that it cannot see the window where loss actually happens. The comment
promised a residual capability the code did not have.

## The fix

Three changes, all in the instrument; no product code is touched.

1. **Anchor both measures to `dead_us`**, not `kill_ms`. `dead_us` is stamped just
   after the SIGKILL returns, so death ≤ dead_us and the depth becomes an
   OVER-estimate. That is the correct direction for a durability measure: it may
   report a loss as deeper than it was, but it cannot report a deep loss as
   shallow. `beyond_cap` is counted rather than asserted, so a conservative
   over-count costs nothing. This is exactly the reading used above by hand, and
   on this run it would have reported ~3304ms on iter 603 instead of 0 — the run
   would have diagnosed itself.
2. **Report the per-iteration depth** on the loss line, replacing the constant
   parenthetical. The count is worth printing; the annotation claimed a property
   nothing evaluated.
3. **Name the anchor in the summary**, so the reported depth says which instant it
   is measured from and cannot be re-read as distance from the harness's stamp.

## Two more instances, found by sweeping for the shape

Grepping the repo for consumers of the changed strings turned up the same defect
twice more, both fixed here:

- **`tools/lag_cap_drill.sh`** printed `no shed write was ever counted as acked
  (acked regressions: ${LOST:-0}, all within the cap)` and asserted neither
  half. The sentence rendered identically whatever `$LOST` held; and the
  `${LOST:-0}` default meant that if the summary line ever moved, the failed
  `sed` would print a clean **zero** rather than admitting it read nothing —
  the vacuous-check shape this drill exists to catch elsewhere. Its PASS line
  made a third unasserted claim, "nothing shed was mistaken for data loss".
  Replaced with two capability asserts (both counters must actually parse) and
  a measured statement; the PASS now claims only the oracle verdict it really
  checks.
- **`crates/flint-chaos/src/cluster.rs`** carried the right instinct about the
  wrong case. Its comment says every recorded run reported `deepest acked-write
  loss: 0ms`, that this is not evidence of correctness, and that the cause is
  loopback replication acking in ~0.2ms — "including a 7-host run over a real
  network". That explains the LOCAL zeros. The multi-host run it cites as its
  strongest case is exactly where the anchor bug applies instead: there the
  harness did create the condition and the instrument could not see it.
  Corrected in place rather than deleted, since the local reasoning still holds.

The sweep is worth repeating whenever an output string changes: `grep` for the
literal, and read every consumer for a claim next to the number rather than only
for a parse that might break.

## What this does not settle

**Iter 603's 24 keys stay open**, and the fix cannot be applied retroactively —
the log carries aggregates, not per-entry `at` values, so batch 3 cannot be
re-judged. Settling them needs a re-run on the fixed instrument, worth pairing
with `--stall-replica-ms`: this run never exercised the RPO bound on purpose, and
a bound tested only by accident was tested three times in 401 kills.

The volume bound — the one the product actually promises — is still not asserted
anywhere. It needs the observed write rate and is tracked separately.

## Why nothing caught it

The anchor is correct on the LOCAL path. `Target::Local` kills a child process
directly, so `kill_ms` and `dead_us` are microseconds apart and the distinction
does not exist. Every chaos drill in CI is local. The gap opens only on
`Target::Attached`, where a discovery round trip and an SSH hop sit between the
two stamps — the multi-host fleet, which is exactly where the durability claim
matters and the one configuration no drill runs.

Same shape as BUG-0117 and the M4 remote runner before it: a check sound on the
topology it is tested on and inert on the topology it is for.
