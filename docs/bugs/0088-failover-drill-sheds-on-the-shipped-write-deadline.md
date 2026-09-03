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
