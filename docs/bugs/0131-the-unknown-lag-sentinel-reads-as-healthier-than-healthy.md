# BUG-0131: the unknown-lag sentinel reads as healthier than healthy, and every replica flies it (OPEN)

Status: **OPEN**, found 2026-09-10 · Severity: **medium** — no node misbehaves
and no data is at risk. Two of the five signals BUG-0095 restored to the
metrics pipeline are restored in a form that reads *below every threshold an
operator would set*, and `docs/self-hosting.md` states the opposite in so many
words. Live since **rc.70**.

## What was observed

The operations session, comparing the playground either side of the rc.71 roll:
a **replica**'s `FLINTINFO seq_lag` read `none` under rc.69 and reads `-1`
under rc.71.

That is BUG-0095 working exactly as designed, and the tags say so:

```
v0.1.0-rc.67   does not contain f91ce69
v0.1.0-rc.69   does not contain f91ce69
v0.1.0-rc.70   CONTAINS f91ce69
v0.1.0-rc.71   CONTAINS f91ce69
```

`flintctl status` still prints the word, because `human_unknown` turns the
sentinel back at the last moment — so nothing on the human surface changed and
the question "was that deliberate?" has a clean yes.

Two things follow that BUG-0095 did not walk.

## Half 1 — the sentinel sits on the HEALTHY side of the threshold for the lag fields

BUG-0095 chose `-1` because it is outside the real range of the four unsigned
fields. Its guarantee is stated once, for the field that could not take `-1`:

> That field renders `flint_tls::CERT_DAYS_UNKNOWN` (`-99999` — 273 years,
> outside any certificate) and keeps the property that matters: an alert
> written the obvious way, `< 14`, still fires on it.

**That property holds where the obvious alert is a FLOOR and inverts where it
is a CEILING.** The five fields split cleanly and nobody wrote the split down:

| field | the obvious alert | sentinel | fires on the sentinel? |
|---|---|---|---|
| `cert_days_remaining` | `< 14` | `-99999` | **yes** |
| `disk_free_pct` | `< 10` | `-1` | **yes** |
| `lag_ms` | `> 5000` | `-1` | **no** |
| `seq_lag` | `> N` | `-1` | **no** |
| `acked_seq` | (not a threshold field) | `-1` | n/a |

`-1 > 5000` is false. So for the two lag fields the unknown state is not merely
missed by the obvious alert — it is the **minimum point on the chart**, below
every real reading the field can produce.

`docs/self-hosting.md` generalises the guarantee across the whole class, and
names a ceiling as its first example:

> So write the obvious alert (`flint_lag_ms > 5000`,
> `flint_cert_days_remaining < 14`) and the unknown states trip it.

For the second of those, true. For the first, false.

**Is this worse than the absent series it replaced?** Not uniformly, and the
honest answer needs both halves. BUG-0095's win is real: the series is now
present, so `flint_seq_lag == -1` is writable at all and a dashboard shows the
state instead of a gap. What it lost is the case it was arguing about — the
operator who writes the obvious threshold alert. Absent is "no data", which
most alerting treats as not-firing but which `absent()` can catch and which a
person reading a panel sees as a hole. `-1` under a ceiling alert is present,
plotted, and renders as the healthiest sample in the window. BUG-0095 refused
to trade a silence for a false statement in `cert_days_remaining` — "a worse
version of the defect rather than a fix for it" — and then made that trade in
`lag_ms` and `seq_lag` without noticing, because the field it reasoned about
was a floor.

## Half 2 — a replica flies the sentinel permanently, and `role` is not on the series

All three lag fields read `ReplHub`, which tracks the seat's **outbound**
replicas:

```rust
/// Freshest cursor among LIVE replicas — the promotion candidate's
/// cursor, and therefore the RPO reference. None = no live replica.
pub fn effective_acked(&self, now_ms: u64) -> Option<u64>
```

A replica has no outbound replicas. Nothing calls `record_ack` on it, the map
is empty for the whole life of the role, and `effective_acked` is structurally
`None`. So a **healthy, fully caught-up replica publishes `seq_lag -1`,
`acked_seq -1` and `lag_ms -1` forever** — not in a fault, not transiently, but
as its normal steady state.

Which means the alert that DOES work for half 1 — `flint_seq_lag == -1` — fires
on every replica in the fleet, permanently, from the moment rc.70 lands.

And it cannot be narrowed the documented way. `flint-exporter` puts `role` on
`flint_up` and `flint_build_info` and nowhere else; every per-field gauge
carries `instance` alone:

```rust
fields.push_str(&format!("{prefix}{key}{{instance=\"{instance}\"}} {n}\n"));
```

Excluding replicas therefore needs `and on(instance) flint_up{role="master"}`,
a join no document mentions and that nothing in the repo demonstrates.

**Under rc.69 this half was accidentally right.** The word `none` failed the
numeric filter, the series went absent on replicas, and absence is the correct
rendering for a field that does not apply — ADR-0018's rule, and the one
OPS-0213 adopted the same week: *export nothing rather than export "cannot
count" forever; absence means not applicable, a value means applicable*. rc.70
made the master case right and the replica case wrong, because one sentinel is
being asked to carry two different facts:

- on a **master**: the field applies and cannot be known — a widowed pair, the
  exact state the field exists to report.
- on a **replica**: the field does not apply and never will.

`-1` is the right rendering for one of those.

## Why nothing caught it

`tools/flintinfo_numeric_drill.sh` asserts all three read `-1`, and its header
is explicit about the configuration and why:

> **A node in its DEFAULT state is the worst case**, which is why it costs one
> seat and no fleet: standalone, no replica, no TLS.

That is the right economy for BUG-0095 and it is a **master** with no replica.
So the drill pins the sentinel in the one role where it means a fault, and
never runs the role where it means not-applicable. The assertion is correct;
its coverage is one of the two roles the field is rendered in.

The same shape as the drill BUG-0128 was found behind: a check that certified
the mechanism in the one configuration the live fleet does not have.

## The blast radius, walked by role this time

BUG-0095 walked consumers and found them all safe. That still holds, and the
reason is unchanged — every in-repo consumer parses to `Option<u64>`, and `-1`
fails an unsigned parse:

| consumer | reads | verdict |
|---|---|---|
| `flint-controller` `apply_flintinfo` | `seq_lag`, `lag_ms`, `lag_soft_ms` | safe; `Node` is built with `None` defaults and folded, so an ABSENT field behaves identically to an unparseable one — which also makes candidate 1 below safe to roll |
| `flint-chaos/src/cluster.rs` | `seq_lag`, `lag_ms` | safe; documents the replica row as `seq_lag:-1` already |
| `flint-ctl` | via `human_unknown` | safe, and the only surface that renders the word |
| ops `flint-agent` `world::node_from_info` | `seq_lag` | safe by parse; **its comment is stale** — it says the field "reads `none`", which has not been true since rc.70 |
| ops `flint-agent` `metrics.rs` | `n.seq_lag` | emits `flint_node_seq_lag` only when it parsed, so a replica gets no series — the rc.69 behaviour, and correct for a not-applicable field |

**No signed parser of these fields exists anywhere in either repo** (grepped
`i64`/`i32`/`as_i64`/`parse::<i` across both). The hazard BUG-0095 named — "one
type change away from silently becoming a lag of minus one" — is still one type
change away, and `the_unknown_sentinel_does_not_parse_as_a_lag` still pins it.

So the exposure is entirely in **what an operator writes against Prometheus**,
which is why the documentation half is the part fixed in this commit and the
rest is a decision.

## What is NOT established

- **No alert is known to have broken.** Neither repo ships alert rules; the
  guidance in `self-hosting.md` is prose. The claim here is that an operator
  following that prose gets a lag alert that is silent in the unknown state,
  not that one did.
- **The replica reading is the ops session's observation plus this code path.**
  I did not stand up a pair to see `acked_seq` and `lag_ms` render `-1` on a
  replica; `seq_lag` is what was observed on the playground, and the other two
  come from the same `ReplHub` in the same `map_or_else`. Standing up a pair
  would settle it in one command and has not been done.
- **Nothing here is evidence about BUG-0082's demotion fix**, which is in the
  same binary and still unexercised on the fleet.

## The decision, named rather than made

This is a FLINTINFO wire-format question one release after the last one, while
a roll is in flight. Recorded so it is picked deliberately:

1. **Omit the three master-side fields from a replica's FLINTINFO entirely.**
   ADR-0018's rule applied at the source: absence = not applicable, `-1` =
   applicable and unknown, and the two stop colliding. The controller tolerates
   a missing field identically to an unparseable one (it folds into `None`
   defaults, and its `loading` arm already documents that tolerance for rolling
   upgrades), so the risk is low — but it is a second wire change in two
   releases, and consumers outside these repos are not enumerable.
2. **Put `role` on the exporter's per-field gauges.** No wire change, and
   `flint_seq_lag{role="master"} == -1` becomes writable. But a label is part
   of a series' identity — the exporter's own comment refuses this for `up` for
   exactly that reason — so every one of these series would end and restart.
3. **Document both halves and change no code.** Cheapest, and it is what this
   commit does for the sentence that is actively wrong. It leaves an operator
   who writes `== -1` paging on healthy replicas.

(1) is the recommendation: it is the rule the repo already adopted twice
(ADR-0018, OPS-0213) and the only candidate that fixes the collision rather
than working around it. It is not taken here because a wire change mid-roll is
not a call to make in passing.

## Fixed in this commit

`docs/self-hosting.md` only, and only the part that is false: the sentence
promising that the obvious alert trips on the unknown state now says which
direction that holds in, and the replica's permanent sentinel is stated with
the join needed to exclude it. No behaviour changes.

## Related

- **BUG-0095** — the sentinel this refines; its range analysis was right and
  its direction analysis covered one field.
- **OPS-0134** — the same sentinel on `cert_days_remaining`, reading as an
  urgent repair to the operations agent on a plaintext seat. Filed as "Jeff's
  call" and still open. **Its scope line is now stale**: it records
  *"Production unaffected — f91ce69 is in no release tag (merge-base against
  rc.67/rc.69)"*, and f91ce69 ships in rc.70 and rc.71 with rc.71 on the
  playground.
- **OPS-0213** / **ADR-0018** — absence means not applicable; a value means
  applicable. Half 2 is that rule broken at the source instead of downstream.
- **BUG-0079** — never assert a fact you failed to look up. The doc sentence
  asserts a property of five fields that was verified on one.

## How it was found

Checking a peer's report that a wire value had changed, rather than accepting
"harmless, it still parses to `None`". It is harmless to every consumer in the
repo; the question nobody had asked was what it renders on the role that flies
it permanently, and what an alert written against it does with a negative
number.
