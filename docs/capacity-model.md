# Flint capacity model, v1

> Sizing the open Flint data plane. The numbers and rules here apply to any
> deployment; where this doc mentions "the agent" computing fills and
> firing expansion triggers, that automation is the Crestway managed plane
> — self-hosters read the same fill fractions off `FLINTINFO`/`sst_bytes`
> and apply the same rules by hand (see [self-hosting.md](self-hosting.md)).

Answers two operator questions from measured system boundaries:

1. **How many groups should one Flint cluster contain?**
2. **When should a cluster add more nodes?**

Every number below is either measured in this repo (source cited) or a
stated design constant. When a bound is revised, revise it here — this
document is the input the expansion triggers and the cluster-sizing
defaults are derived from.

## Measured boundaries (the raw material)

| Boundary | Value | Source |
|---|---|---|
| Per-node throughput | ~100K ops/s at p99 < 1 ms | M0 exit bench, i4i-class |
| Cold NVMe read | p50 223 µs / p99 775 µs | EC2 i4i.2xlarge 100 GB run |
| Client-observed failover outage | ~472 ms (proxy-absorbed) | m3_exit_drill |
| Replication push ceiling | 4 MB per drain cycle (~GB/s-class link, self-pacing) | REPL_TAIL_BUDGET_BYTES + chain drill |
| Proxy admission cap | 1,024 concurrent conns (default, tunable) | --max-conns |
| Slot space | 16,384 slots, range-partitioned per pair | flint-slot / range map |
| Controller probe cost | ~1–2 ms per healthy node; **800 ms timeout per dark node** | controller `call` timeouts |
| Controller cadence | poll 200 ms × confirm 3 ⇒ detection ≈ 0.6 s | controller defaults |
| Tenant cap (v0 scope) | ~50 GB per tenant | roadmap scope discipline |

## Question 1 — groups per cluster

The architecture was built so most components DON'T bound group count:

- **Control plane:** holds level-1 intent only (registry + range map;
  ADR-0004). Registry writes are per-tenant-onboarding, not per-group-load;
  CPWATCH fan-out scales with *proxies*, and push suppression drops no-op
  pushes. Not the binding constraint.
- **Proxy:** routing is an in-memory range lookup; table size is trivial at
  any plausible group count. Broadcast commands (DBSIZE/FLUSHALL) cost
  O(groups) per call — linear and rare. Not binding.
- **Fleet journal / agent bookkeeping:** event volume scales with incident
  rate, not group count. Not binding.

Two constraints actually bind:

**(a) Supervision sweep vs. detection SLO — the sharp one.** A controller
probes every node of every supervised pair each tick. Healthy probes cost
~1–2 ms; a DARK node costs the full 800 ms timeout. Detection latency is
`confirm × max(poll, sweep_time)`, so the sweep must stay under the poll
interval **under failure**, not just in the happy path:

    sweep ≈ healthy_nodes × 2 ms + dark_nodes × 800 ms

With poll = 200 ms, a single controller absorbs ~64 pairs (128 nodes,
~256 ms healthy sweep — already at budget) and exactly ZERO margin for a
correlated failure: one dark AZ-worth of nodes multiplies the sweep by
seconds and detection stalls exactly when it matters. Controllers are
stateless and arbitrate via epoch fencing (proven in the concurrent-
controller drill), so the answer is sharding, not bigger controllers:

> **Rule: ≤ 32 pairs per controller shard** (≈70 ms healthy sweep, so even
> 8 simultaneous dark nodes keep detection inside 3× poll). Run one shard
> per ~32 pairs; shards are just controllers with disjoint `--pairs` lists.

**(b) Slot granularity — the smooth ceiling.** Ranges partition 16,384
slots. Rebalancing granularity degrades as slots-per-pair shrinks; below
~64 slots/pair the planner cannot spread heat smoothly (one hot slot is
>1.5% of the pair). That caps a cluster at **256 pairs** outright.

**v1 recommendation:** size a cluster at **up to 64 pairs (128 data
nodes) across 2 controller shards**, hard ceiling 256 pairs. Beyond that,
run another cluster: the marginal cost of a second cluster (one more CP
quorum + proxy subset) is small, and blast radius, upgrade batches, and
capacity math all stay human-sized. Revisit when a real fleet measures
the supervision sweep at scale.

## Question 2 — when to add nodes (the live trigger)

Fill is the driver; everything else (ops/s, connections) alarms first
through the exporter and is solved by *rebalancing*, not expansion. The
storage signal is `FLINTINFO sst_bytes` (live SST bytes — reclaimable
space excluded), tracked per pair by the agent when told the deployment's
per-node capacity (`--node-capacity-bytes` / inventory `capacity` line).

Thresholds, and why:

- **70% fill ⇒ expand.** Expansion is cheap but *drainage is not
  instant*: a pair joins unranged, then rebalancing ships slots at a
  bounded pace (REPL budget, one-slot cutovers). 70% leaves room for the
  drain to complete before the pressure region.
- **80% is the planning line.** The agent computes days-to-80% from the
  observed growth rate (linear over its history window) and quotes it in
  the recommendation — the difference between "expand this sprint" and
  "expand today".
- **90% ⇒ page.** Past comfortable-drain territory: RocksDB compaction
  needs working space, and a compaction stall on a full disk is a
  latency incident. Human decides whether to expand + throttle intake or
  emergency-migrate the whale tenant (CPSETSUBSET isolation).

The loop is closed through the same machinery as everything else:

    agent sweep → CapacityPressure insight (fill ≥ 70%)
      → ExpandCluster recommendation (evidence: fill %, ETA; command:
        flintctl expand …)  [≥90% escalates to PageHuman]
      → human runs the command; the new pair joins UNRANGED
      → controller rebalancing drains the pressured pair
      → fill drops below threshold → the standing condition clears

Exporter series backing it: `flint_node_sst_bytes{node,pair}`,
`flint_pair_sst_bytes{pair}`, `flint_pair_capacity_bytes{pair}`,
`flint_pair_days_to_80{pair}`, `flint_insight{kind="capacity_pressure"}`.

## Scaling out: adding shards and moving slots

A cluster grows by adding pairs. `flintctl expand <master>,<replica>` joins a
pair **unranged** — it owns no slots and takes no traffic until slots move to
it. Two mechanisms move slots onto the new capacity; both drive the same
epoch-fenced `FLINTMIGRATEIN` cutover (a slot is served throughout, and the
control plane records the new owner via `CPSETSLOT` at commit — no acked
write is lost).

**1. Automatic rebalancing (policy-driven).** With `--rebalance-deadband
<frac>` the controller observes each pair's load every cycle, plans the
minimum set of moves to bring the group within the deadband, and — with
`--rebalance-execute` — ships them a few slots per cycle, re-planning from
fresh observations each time (convergence by small steps; the deadband stops
the loop at balance). *What "load" means is a policy*, selected with
`--balance-policy`:

  - **`size` (default, open stack).** Balance by data: a pair's load is the
    sum of its per-slot key counts (`FLINTSLOTSTATS`; bytes later). This is
    what "70% fill ⇒ expand → controller drains the pressured pair" above
    uses. It is the right default — the pressure signal that triggers
    expansion is a *capacity* signal.
  - **`traffic` (Crestway managed plane).** Balance by request rate
    (ops/second per slot) rather than bytes, so a small-but-hot slot range is
    spread across pairs even when every pair is well under its size budget.
    It plugs into the same `BalancePolicy` seam without changing the planner;
    it needs per-slot ops metering the open stack does not emit, which is why
    it ships with the managed service rather than the open repo.

An unknown `--balance-policy` name is a startup error, not a silent fallback.

**2. Operator-directed move.** When an operator wants a *specific* slot range
on a *specific* destination — draining a noisy-neighbor range, pre-placing a
known-hot tenant, or staging a planned migration — `flintctl migrate-slots
<ns> <lo-hi> <src-pair> <dest-pair>` moves exactly that range, one slot at a
time, and commits ownership to the CP. This is manual and outside the
deadband loop: it does exactly what is asked, nothing more. Verified by
`tools/migrate_slots_drill.sh` (range moves, CP records the new owner, keys
intact, zero acked-write loss across the cutover).

**Non-storage expansion signals** (rebalance-or-expand judgment calls,
watched on the same dashboard): sustained per-node ops near the 100K
ceiling with balanced slots (expand); proxy `active` conns pinned at the
admission cap fleet-wide (add proxies, not pairs); replication lag
persistently near the cap under normal load (the pair is write-saturated:
expand and drain its hottest slots first).
