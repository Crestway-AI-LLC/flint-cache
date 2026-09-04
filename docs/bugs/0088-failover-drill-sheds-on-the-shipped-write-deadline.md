# BUG-0088: `failover` fails when one write in 20 000 projects past the SHIPPED write deadline (OPEN)

Status: OPEN 2026-09-03 · Severity: medium — the drill is intermittent, but the
threshold it crosses is the product default, not a test setting

## Symptom

Two gate runs on `main`, two days apart, failed identically:

    == loading 20000 keys
    THROTTLED write would wait ~2017ms, past --write-deadline-ms 2000, retry with backoff
    errors: 1, replies: 20000
    GATES FAILED: failover

| run | commit | projected wait | over the deadline by |
|---|---|---|---|
| `33667073809` (2026-09-02) | `422a1bde` | **2017 ms** | 0.85% |
| `33800799780` (2026-09-03) | `ae51c612` | **2033 ms** | 1.65% |

One write out of twenty thousand, in both. This is a tail event landing about
one percent past a threshold — which is the whole finding.

## The 2000 ms is the SHIPPED DEFAULT, not the drill's choice

`tools/failover_drill.sh` never passes `--write-deadline-ms`, so the seat uses
`DEFAULT_WRITE_DEADLINE_MS` — **2 000**, `crates/flint-server/src/main.rs:1000`.
So the number being crossed here is the one every deployment runs unless an
operator overrides it.

And the comment that sets it predicts something very different from what was
measured (`main.rs:994`):

> (sub-millisecond at any sane concurrency), far below any client's timeout

The observation is **2017 ms on plain `ubuntu-latest` with `FLINT_GATE_JOBS: 4`**
— a 20 000-key load against a two-seat fleet on loopback. Whatever "sane
concurrency" excludes, it does not exclude the gate's own failover drill. That
comment also already asks to be revisited "against a measured p99 curve"; no such
curve exists, and this is the first evidence about where the tail actually sits.

## What this is NOT

**Not BUG-0035**, which is the LAG CAP shedding (`writes_shed_lag`, driven by
`lag-soft-ms`/`lag-hard-ms`). This is the write-deadline path
(`main.rs:3993`), which projects a wait and refuses before queueing. Same
`-THROTTLED` reply to a client, different gate, different knob.

It is nonetheless a counter-example to the framing in BUG-0035's
"LINUX does not shed at any burst" and "resolved for loopback" sections: this is
Linux, on loopback, shedding. Those sections were measuring the lag cap and are
not wrong about it — but "Linux does not shed" is the sentence a reader carries
away, and it is now false for at least one shed path.

**Not caused by the commit it fired on.** `ae51c612` is a warm-list and gate-check
change that touches no write path; `422a1bde` fired the same way two days
earlier. The base rate exonerates both.

## Established

- The signature is reproducible in shape across two independent runs, differing
  only in the projected wait (2017 / 2033 ms).
- The drill uses the shipped default deadline.
- `errors: 1, replies: 20000` in both — the failure is one write, not a stalled
  fleet.
- Runner is `ubuntu-latest`, `FLINT_GATE_JOBS: 4` (`.github/workflows/gate.yml:61,143`).

## NOT established

- **Whether this is new.** Of the 60 failed gate runs on `main` since 2026-08-16,
  **18 were examined** (the 10 since 08-28 and an 8-run sample from 08-27/28).
  `failover` failed 3 times, all on 09-02 and 09-03; two carry this shed and the
  third does not (`errors: 0`, and it also failed `roll_shed`). The other 42 are
  unexamined and logs before ~08-20 have aged out. An onset is suggested, not
  demonstrated.
- **The distribution.** Two samples exist, both above the line, because *the only
  time this number is recorded is when it exceeds the deadline.* Whether passing
  runs sit at 200 ms or 1 990 ms is unknown, and those are very different bugs.

## Where to start, and it is the instrument first

**Record the maximum projected wait on every run, not only when it exceeds.**
Today the drill is a threshold with an invisible distribution behind it: a pass
says "under 2000" and nothing more, so there is no way to tell a comfortable
margin from a run that missed by a millisecond, and no way to see the margin
eroding. One line on the pass path turns 60 future gate runs into a p99 curve
for free — the same gap, and the same fix, as BUG-0014's tie counter that
printed only when non-zero.

Only with that curve is the second question answerable: whether the default
belongs at 2000 ms, whether the drill's load is unrepresentative, or whether
something in the write path regressed in early September.

**Do not raise the deadline to make it green.** The threshold is the product's
promise to a client; moving it because a test crosses it converts a measurement
into a silence.

## 2026-09-03, same day — the instrument landed, and the first numbers are in

`write_wait_peak_ms` is now a high-water mark in `FLINTINFO`, beside the
instantaneous `write_wait_est_ms` it completes, and `failover_drill.sh` prints
it against the deadline on **every** run:

    == write-wait peak 29ms of 2000ms deadline (0 = no write projected a measurable wait)

**The first draft of this was wrong and running it is what showed that.** It
floored the recording at HALF the deadline, reasoning that the ordinary write
path should gain no atomic and only the approach to a refusal mattered. Run
locally it reported a flat **0** — technically true, useless in practice: no
gradient means no trend, so a future 2017 ms outlier would have had nothing to
be compared against, which is the entire purpose. The floor is now `est_ms > 0`,
which keeps the same property (the estimate is `inflight x service_us / 1000`,
so a quiet path yields 0 by integer division and takes no atomic) while
recording everything with anything to see.

**A first data point, and it is a wide gap.** This laptop peaks at **18-29 ms**
against the 2000 ms deadline across runs. CI reached **2017 ms**. Whatever
`ubuntu-latest` at `FLINT_GATE_JOBS: 4` is doing to this drill, it is roughly
two orders of magnitude away from an unloaded machine, which makes "the drill's
load is unrepresentative" much less likely than "four drills on a shared runner
contend hard enough to matter" — and that is now a measurable claim rather than
a guess.

The drill FAILS rather than reporting a comfortable zero if the field cannot be
read; mutation-tested by renaming the field, which produces the refusal and not
a `0`. Same rule as everywhere else here: cannot look is not absent.

**What to do with it:** the next several gate runs each contribute a peak. Once
there are enough, the question the default was always supposed to be checked
against — where p99 actually sits — is answerable from the gate log alone, with
no new harness and no spend.

## 2026-09-03, later — the first CI peaks, and the failure is a SPIKE not a drift

The instrument reached CI and reports on every run. First three, read from
`gate-logs-drills` artifacts:

| run | commit | peak |
|---|---|---|
| `33804748860` | `b439cf0` | 557 ms |
| `33806219239` | `3fd00e4` | 124 ms |
| `33806874986` | `07d7aea` | 495 ms |

Against the 2000 ms deadline, and against **18-29 ms** on an unloaded laptop.
So `ubuntu-latest` at `FLINT_GATE_JOBS: 4` runs this drill 20-30x closer to the
line than a quiet machine, and varies 4.5x between consecutive runs.

**The shape of the failure changes with this.** The two refusals were 2017 ms
and 2033 ms — roughly **4x the highest passing peak yet seen**. That is not a
distribution drifting up until it touches a threshold; it is an episodic spike
well outside the observed range. The comparison is legitimate across the
instrument boundary because both numbers are the same quantity: the refusal
message prints `est_ms`, and the peak is the high-water mark of `est_ms`.

**Stated with its sample size:** n=3 passing peaks. Three samples bound the
centre of a distribution, not its tail, so "4x beyond anything seen" is
suggestive and not yet a claim about how often a 2000 ms excursion happens.
More samples accrue from ordinary gate runs at no cost, which is the point of
having made it continuous.

### The refusal now names which term spiked

`est_ms` is `inflight x service_us`, and the factors fail for opposite reasons:
high inflight is more offered load than the node can take; high service time is
the node itself having got slow — compaction, a stalled disk, a descheduled
runner. The product cannot distinguish them and they need different responses,
so the message now carries both:

    THROTTLED write would wait ~2017ms (inflight 12 x service 168000us),
    past --write-deadline-ms 2000, retry with backoff

Nothing matched on the old text but its own definition, and the `THROTTLED`
prefix and `--write-deadline-ms` anchor are unchanged, so a client
prefix-matching the error class is unaffected.

### What this says about BUG-0064

That file's working theory is steady contention from the parallel gate. **Steady
contention predicts the whole distribution shifting toward the line**, and what
is measured is a tight-ish band at 124-557 ms with excursions to 2017 ms. A
spike four times beyond the band is a poor fit for "four drills share four
cores" and a better one for something episodic on the runner. Not decisive at
n=3, and the terms above are what would settle it: the next refusal will say
whether the spike was queue depth or service time.

## 2026-09-03, n=7 — CORRECTION: it is a tail, not a spike

The section above read three peaks (124, 495, 557 ms) and concluded the 2017 ms
refusals were "roughly 4x the highest passing peak yet seen... not a
distribution drifting up until it touches a threshold; an episodic spike well
outside the observed range." **That is withdrawn.** It was stated at n=3 with
the caveat that three samples bound a centre and not a tail, and four more
samples are what the caveat predicted.

| commit | result | peak (ms) |
|---|---|---|
| 3fd00e4 | pass | 124 |
| b7b74a9 | pass | 324 |
| 07d7aea | pass | 495 |
| b439cf0 | pass | 557 |
| d13169c | failed elsewhere | 763 |
| bbed94c | pass | 840 |
| 4aa5c56 | pass | **1322** |

Seven consecutive gate runs, median **557 ms**, max **1322 ms**, a **10.7x
spread** — and the maximum on a PASSING run is **66% of the 2000 ms deadline**.

Against that, the refusals at 2017 and 2033 ms are **1.5x the observed passing
maximum**, not 4x. That is the tail of a wide, heavy-looking distribution
reaching a threshold, which is the reading the first three samples ruled out and
should not have.

**What it means for the deadline.** 2000 ms was chosen where the comment beside
it predicts "sub-millisecond at any sane concurrency". On this runner the
routine maximum is already two thirds of the way to it, so the margin is not the
three orders of magnitude the comment implies but a factor of about 1.5. That is
a fact about the default, not only about the drill.

**And for BUG-0064:** a wide continuous distribution whose tail crosses a
threshold is a better fit for runner variability than for a discrete episodic
event, and it is what "steady contention" would look like once the distribution
is visible rather than inferred. It does not by itself separate contention from
a slow runner — that still needs the P=1 comparison, which is now a comparison
of distributions and no longer needs ~20 runs to be worth taking.

The instrument is doing what it was built for: four days of argument at n=0, a
wrong conclusion at n=3, a correction at n=7, all from runs that were happening
anyway.

## 2026-09-04 — a second drill, and it is the same term

`restart` refused a write on a **docs-only** commit (`36435a8`, one bug file,
36 insertions), so nothing in the change could have caused it:

    THROTTLED write would wait ~2011ms (inflight 30 x service 67038us)

**Queue depth 30** — an eighth of `failover`'s 256 — and **67 ms per write**,
seven to twenty times the worst `failover` service figure. The deadline is
crossed here almost entirely by service time, with a queue so shallow it could
not plausibly be blamed.

That matters for two reasons.

**It generalises the finding beyond one drill.** The `inflight` constant of 256
in `failover` is that drill's pipeline depth, so a sceptical reading of the
matched-arm result is that the whole effect is an artefact of one workload.
`restart` has a different shape, a different depth, and lands on the same term.

**And it came for free.** The terms live in the SERVER's refusal message, not in
`failover_drill.sh`, so every drill that crosses the deadline reports them
without being instrumented. The drill-side peak line covers `failover`; the
refusal covers all of them.

**Still not a distribution.** Two drills, three refusal samples between them.
What it rules out is "this is a `failover` artefact"; what it does not do is
bound how often 67 ms writes happen.

