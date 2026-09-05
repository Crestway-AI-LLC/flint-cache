# BUG-0095: a field that reads `none` vanishes from Prometheus exactly when it matters (FIXED)

Status: **FIXED 2026-09-05** (found the same day) · Severity: **medium** — no
data was lost and no node misbehaved, but five monitored signals went absent
from the metrics pipeline in precisely the state an operator built the alert
for, and an absent series renders as "no data", which most alert
configurations treat as not-firing. Jeff's call on the fix: candidate 1, the
sentinel.

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

## ~~Why this is filed rather than fixed~~ — decided, and fixed

Candidate 1. `-1` for the four unsigned fields, matching `wal_headroom_seq`,
plus candidate 3's documentation regardless.

**`cert_days_remaining` could not take `-1`, and that is the one place the
decision could not be applied literally.** Its real range INCLUDES -1: it is
`Option<i64>` and the function's own doc says "Negative once expired", so `-1`
means *expired between one and two days ago*. Using it would have replaced a
silence with a false statement, which is a worse version of the defect rather
than a fix for it. That field renders `flint_tls::CERT_DAYS_UNKNOWN`
(`-99999` — 273 years, outside any certificate) and keeps the property that
matters: an alert written the obvious way, `< 14`, still fires on it.

**The proxy and the control plane render `cert_days_remaining` too**, in
`PROXYSTATS`, `CPINFO` and the HA `CPINFO` — three more copies of the same
`none`, found while walking consumers rather than named in the original
report. All four now share the one constant.

**The blast radius held, and it is now load-bearing rather than lucky.** Every
consumer parses these into `Option<u64>` via `parse().ok()` —
`flint-controller`, `flint-chaos`, `flint-ctl` — so `-1` fails an unsigned
parse and still reads as unknown, exactly as `none` did. That is one type
change away from silently becoming a lag of minus one, which
`Node::converged` would read as BETTER than caught up, so
`the_unknown_sentinel_does_not_parse_as_a_lag` pins it: the parse loop was
split out of `observe` into `apply_flintinfo` for the purpose, and the test
carries a positive control that a caught-up body still reads as converged.

**One readability regression, fixed rather than accepted.** `flintctl status`
prints `seq_lag` in a table a person reads, and `seq_lag -1` looks like a
quantity. `human_unknown` turns the sentinel back into the word at the last
moment — the sentinel is a wire convention, and the CLI is not the wire.

## The guard

`tools/flintinfo_numeric_drill.sh`. **A node in its DEFAULT state is the worst
case**, which is why it costs one seat and no fleet: standalone, no replica,
no TLS. That single configuration exercises four of the five, and it is the
configuration all four were already broken in — the defect was reachable by
starting a node and looking.

It asserts three things, and the second is what makes it more than a
spell-check:

1. Every field is a number, unless its key is in a declared `STRINGS` list.
2. **Every key in `STRINGS` is present.** A check of the form "numeric unless
   exempt" is only as good as its exemptions, and a stale one silently
   re-permits the whole class.
3. The unknown states render the SENTINEL, not a healthy-looking zero — plus a
   positive control that `disk_free_pct`, whose unknown state is not reachable
   on this host, carries a real 0-100 reading, so a build that rendered `-1`
   everywhere fails.

Verified by mutation, twice. Reverting `lag_ms` to `"none"` fails check 1
naming the field. Reverting it to `"0"` — numeric, and therefore invisible to
check 1 — fails check 3, which is why check 3 exists. The proxy and control
plane are covered from `build_stamp_drill.sh`, which already stands both seats
up.

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
