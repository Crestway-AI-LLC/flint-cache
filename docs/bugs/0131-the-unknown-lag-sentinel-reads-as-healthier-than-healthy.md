# BUG-0131: the unknown-lag sentinel reads as healthier than healthy, and every replica flies it (FIXED 2026-09-10)

Status: **FIXED 2026-09-10** (found the same day) · Severity: **medium** — no
node misbehaves and no data is at risk. Two of the five signals BUG-0095 restored to the
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

- **No alert is known to have broken, and the shipped ones are not exposed.**
  The ops repo *does* ship alarms — six, created by
  `packaging/aws/ops-box/alarms.sh` — but every one is in the `FlintOps`
  CloudWatch namespace on agent-level metrics (`Pages`, `PrimaryHealthy`,
  `OverdueIncidents`, `TriageUnjudged`, `WatchLaneHealthy`,
  `OpsRosterMismatch`); none reads `seq_lag`, `lag_ms` or `acked_seq`. The
  agent's own two consumers *do* act on the field and are conservative by
  construction: `insight.rs` sorts an unknown lag LAST when choosing a
  promotion candidate (`unwrap_or(u64::MAX)`), and `tier2.rs` gates on
  `== 0`, which an unknown can never satisfy. So the exposure really is
  operator-written PromQL alone — the claim here is that an operator
  following `self-hosting.md`'s prose gets a lag alert silent in the unknown
  state, not that one did.
- **The replica reading started as the ops session's observation plus this
  code path** — `seq_lag` is what was seen on the playground, and the other two
  come from the same `ReplHub` in the same `map_or_else`. **Since settled by
  standing up a pair**: mutant 1 below, on a real replica, reports
  `a replica still renders acked_seq:-1`, so all three were confirmed, not
  inferred.
- **No fleet has run the fix.** It is verified on a gate box and by mutation;
  the playground is on rc.71, which does not carry it. rc.72 is the first
  release that would, and that is a release-cut decision, not this file's.
- **Nothing here is evidence about BUG-0082's demotion fix**, which is in the
  same binary and still unexercised on the fleet.

## Fixed 2026-09-10 — candidate (1), Jeff's call

Three candidates were recorded rather than taken, because a FLINTINFO
wire-format change one release after the last one, with a roll in flight, is
not a decision to make in passing:

1. **Omit the three master-side fields from a replica entirely.** ADR-0018 at
   the source: absence = not applicable, `-1` = applicable and unknown.
2. Put `role` on the exporter's per-field gauges. No wire change, but a label
   is part of a series' identity, so every one of these series would end and
   restart — which the exporter already refuses to do for `up`.
3. Document both halves and change no code.

**(1) was chosen.** `master_side_fields` returns the three as two spliceable
fragments, empty on a replica. Two fragments because the fields are not
adjacent in the template — `acked_seq`/`seq_lag` follow `last_applied`,
`lag_ms` follows `live_replicas` — and each terminates itself with CRLF so an
empty one leaves no stray blank line.

**The condition is "not a replication source", not "is a replica"** —
`read_only && live_replicas == 0`. The first cut keyed on the role alone, on
the reasoning that a liveness window would make the fields' *presence* flap.
**That was wrong, and the gate caught it**: see below.

**`live_replicas` is master-side too and deliberately NOT omitted.** On a
replica it renders a true `0` rather than a sentinel, so it states a fact
instead of claiming an unknown. Omitting it would also have broken the two
drills below for no gain.

### The CLI keeps its column, and that is not cosmetic

`flintctl status` prints every pair member through one fixed-width row, and
**two drills parse that row positionally** — `$10 seq_lag, $12 live_replicas`
in `failover_bystander_drill.sh` and `failover_churn_drill.sh`. An absent field
rendered blank under `{lag:<5}`, which collapses under awk's field splitting:
`$10` shifts onto `live_replicas` and `$12` disappears. So the CLI now has
**three** renderings, not two — a number; `none`, which is `human_unknown`
turning the sentinel back for a widowed master; and **`n/a`** for a replica,
which does not render the field at all. A column that disappears is not a
narrower column.

`failover_churn_drill.sh:218` happens to filter `$4=="master"` before reading
`$10`, so it was never exposed; `failover_bystander_drill.sh:134` matches by
ADDRESS and is. Both are safe with the column held.

### The guard, in the file whose economy hid this

`tools/flintinfo_numeric_drill.sh` gains a second seat. Its header argues **"A
node in its DEFAULT state is the worst case ... one seat and no fleet:
standalone, no replica, no TLS"** — correct for BUG-0095, and a **master** with
no replica, so the sentinel was only ever pinned in the role where it means a
fault. The new phase attaches a replica on 6429 and asserts the three are
ABSENT, behind three positive controls, because *"the key is absent"* is the
assertion that passes most loudly on an empty body, a truncated reply, or a
seat that is not a replica at all:

- the seat reports `role:replica`;
- it still carries `latest_seq`, `last_applied`, `live_replicas`, `uptime_ms`,
  `disk_free_pct` — so a truncated body fails rather than passes;
- and **the master, now with a live replica, still renders all three**, with
  `live_replicas == 1` asserted first so a replica that never attached cannot
  make the widowed reading look like the healthy one.

**And a FOURTH arm, added after the gate refuted the first cut**: demote the
master while its replica is attached, and require that it *still* renders all
three. It asserts `role:replica` and `live_replicas == 1` first, so it cannot
pass against a build that never demoted or a replica that detached — without
those it would go green against the very code it exists to catch.

So the drill now covers all four states the fields have: standalone master
(sentinel), plain replica (absent), master with a live replica (real numbers),
and demoted master mid-drain (real numbers).

**Verified by mutation, three times, and each mutant dies to a different
phase:**

| mutant | dies to |
|---|---|
| the omission never fires — the bug itself | the NEW replica phase: *"a replica still renders acked_seq:-1"* |
| the fields are dropped EVERYWHERE — BUG-0095 reopened | **BUG-0095's ORIGINAL phase**: *"acked_seq= on a node with no live replica; expected -1"* |
| keyed on the role alone — the first cut | the NEW demote arm: *"a DEMOTED master with a live replica omits acked_seq"* |

Two are worth noting. The second says the pre-existing check is what defends
against this fix reopening the bug it builds on. The third says this drill now
catches, in forty seconds, what previously surfaced as a 30-second panic deep
inside a rolling upgrade in a different drill.

Seven unit tests cover `master_side_fields` directly, including that a
caught-up master reports `0` rather than the sentinel, that `seq_lag` is the
DIFFERENCE and not the cursor, and that a read-only *source* with no ack yet
renders the sentinel rather than nothing — the collapse pointing the other way.

**The drill also gained a cleanup trap**, which is not incidental: every `fail`
exits immediately, so a red run left its seats behind and `fleet_guard` then
refused the NEXT run with *"this box already has Flint processes outside …"*,
all orphans at `ppid 1`. That reads as a broken environment rather than as the
previous failure's litter. Found by mutation-testing this very file — two of
three mutants left a seat and blocked the next one — and verified by running a
mutant and confirming no seat and no data directory survive, while the logs do.

### The first cut was keyed on the role, and every rolling upgrade broke

Recorded because the reasoning was stated confidently and was wrong, and
because what refuted it is a topology I had explicitly checked for and
concluded did not exist.

`flintsync` carries no read-only guard, so a node pointed at a replica IS
served and registers in that replica's hub. I looked for somewhere that
builds such a chain, found that `flintctl` starts `pair[0]` bare and gives
the rest `--replica-of pair[0]` and that the control plane registers PAIRS,
and wrote the boundary down as "not a topology this system builds".

**It is built on every roll.** `controlled_failover` demotes the old master
and then polls **that seat's** `seq_lag` until it reads 0 — only the demoted
seat knows how far its replica has drained — so for the length of the drain a
seat is read-only *with a live replica still attached and still acking*. I
had walked `flintctl`'s spawn paths and the FLINTSYNC handler, and not the
demote path, where a seat becomes a replica while keeping its replicas.

Keyed on the role, that seat rendered nothing, the drain loop never saw
`Some("0")`, and after 30 seconds:

```
thread 'main' panicked at crates/flint-ctl/src/main.rs:6028:9:
replica never drained the demoted master 127.0.0.1:6502's tail
```

Caught by `build_read_failure` on the gate — everything else in `check` and
all 140 drills passed. **The lesson is not "check more call sites"**; it is
that "is a replica" and "has no replicas" are different predicates, and the
fields are about the second. A master with zero replicas is still never
omitted, because *that* is the widowed state BUG-0095 exists to keep visible.

The presence of these fields can therefore change on one seat, at a demotion.
That is a real transition and not a flapping series: a pair replica never
acquires an outbound replica, and a master never loses the fields at all.

Consumers re-walked: the controller folds a MISSING field into the same `None`
as an unparseable one (`Node` is built with defaults and folded, and its
`loading` arm already documents that tolerance for rolling upgrades);
`flintctl`'s reconverge path reads the MASTER and already renders an absent
field as `?`; the chaos harness reads `master_info` and prints `<absent>`; the
ops agent's `info_field` yields `None`. `cold_start_roles_drill` polls
`seq_lag` on port 7403, which is the seat that ACCEPTS the seed writes — a
master. `controlled_failover` reads it off a seat that is read-only *and a
replication source*, which is the case the final condition is built around.
No consumer needed changing.

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
