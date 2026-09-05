# BUG-0095: a field that reads `none` vanishes from Prometheus exactly when it matters (OPEN)

Status: **OPEN** (found 2026-09-05) · Severity: **medium** — no data is lost and
no node misbehaves, but five monitored signals go absent from the metrics
pipeline in precisely the state an operator built the alert for, and an absent
series renders as "no data", which most alert configurations treat as
not-firing.

## Symptom

`flint-exporter` emits every FLINTINFO field whose value parses as a number
(`crates/flint-exporter/src/main.rs`, `emit()`: `if let Ok(n) =
v.parse::<f64>()`). Anything else is skipped, silently and by design — that is
what keeps `role` and `build` out of the gauge space.

Five fields are NORMALLY numeric and render the word `none` in one state:

| field | reads `none` when | `flint-server/src/main.rs` |
|---|---|---|
| `lag_ms` | no live replica | :5546 |
| `seq_lag` | no live replica | :5529 |
| `acked_seq` | no live replica | :5543 |
| `cert_days_remaining` | the certificate cannot be read | :5631 |
| `disk_free_pct` | the filesystem cannot be read | :5658 |

So `flint_lag_ms` is present while replication is healthy and **gone when it is
not**. `flint_cert_days_remaining` is present while the certificate is readable
and gone when it is not. `flint_disk_free_pct` is present while the disk is
readable and gone when it is not.

`docs/self-hosting.md` names `flint_lag_ms` as an example metric to watch and
tells operators to alert on `disk_free_pct`.

## Root cause, and why `none` is not the mistake

`none` rather than `0` is deliberate and correct at the FLINTINFO layer. The
comment at :5526 says so — *"promotion-READINESS signal. `none` when no live
replica"* — and a zero lag against an absent replica must not render alike.
That is the same rule as `mem_src`, `disk_verdict` and
`collection_read_unmeasured`: never let "I could not look" render as "nothing
was wrong".

The defect is that the rule stops at the FLINTINFO boundary. Downstream, a
numeric filter converts *absent* into *absent series*, and a dashboard cannot
tell that from a node that is not being scraped at all. The unknown value was
protected from being read as zero and then lost the distinction anyway, one
layer further out.

## The repo already contains the other answer

`wal_headroom_seq` faces the identical question and answers it differently:

> `-1` for "no live replica", which is a different state from zero headroom
> and must not read as healthy.

That renders a NUMBER, so the exporter carries it, and `-1` is outside the
range of any real value, so it cannot be mistaken for one. The same file
therefore solves the same problem two ways, and only one of the two survives
the metrics pipeline.

## Candidate fixes

1. **Render `-1` where `none` is rendered now**, matching `wal_headroom_seq`.
   Consistent, and it needs no exporter change. It is a wire-format change for
   FLINTINFO consumers, so the blast radius has to be walked: the controller
   parses `lag_ms` with `v.parse().ok()` into `Option<u64>`
   (`flint-controller/src/main.rs:485`), and `"-1".parse::<u64>()` fails, so it
   would keep seeing `None` — correct, but by accident of the parse rather than
   by intent, which is worth making explicit rather than relying on.
   `flint-chaos/src/cluster.rs:1604` also reads `lag_ms:`.
2. **Have the exporter emit a presence gauge** (`flint_lag_ms_known 0|1`) and
   leave FLINTINFO alone. No wire-format change, but it puts per-field product
   knowledge in the exporter, which is deliberately lean.
3. **Document it and change nothing** — name, for each of the five, the numeric
   field an alert should actually use. `disk_free_pct` already has one
   (`disk_unknown_samples`); `lag_ms` has `live_replicas`, which is numeric and
   goes to 0 in the same state. The others have none.

(1) is the recommendation: it is the answer this codebase already chose once,
it needs no new concept, and (3) is worth doing regardless of which is picked.

## Why this is filed rather than fixed

Which of the three is right is a decision about the FLINTINFO contract, not an
inference from the defect. Recorded so the choice is one somebody made.

## How it was found

Enumerating the non-numeric FLINTINFO fields for a documentation sentence, by
diffing a live `FLINTINFO` against `parse::<f64>()` rather than reading the
format string. The `none` is in the binding, not the template, so reading the
format string does not show it — every one of these five looks numeric there.

## Related

- BUG-0079 — never assert a fact you failed to look up; this is the same rule
  losing its effect one layer downstream
- BUG-0060 — `collection_read_unmeasured` and `collection_read_mode`, added
  under the same rule; `collection_read_mode` is a string by nature and is
  documented as `FLINTINFO`-only
