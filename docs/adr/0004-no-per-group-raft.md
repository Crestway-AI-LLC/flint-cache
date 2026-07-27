# ADR-0004: No per-group Raft — stateless controllers + epoch fencing

Status: accepted — July 2026
Supersedes the "group meta trio (Raft ×3)" in design.md §2.3 (v1 draft).

## Context

The v1 design gave each shard group a 3-node Raft trio owning failover:
failure detection, promotions, fencing-epoch issuance, leases, slot→pair
assignment. While building M2 we implemented the decision logic first as a
standalone process (flint-controller) to be wrapped in Raft later, and found
the wrapper adds no correctness:

- Every structural action (promote, demote, slot claim) is serialized by
  **epochs in the data nodes' own manifests** — an action at an equal or
  lower epoch is rejected (`-FENCED`). The data plane is the arbiter.
- The controller is **discovery-based and stateless**: each tick it
  re-derives the truth (who is master, at what epoch, is the replica
  converged) by polling the nodes. There is no coordinator state that can
  diverge from reality, and any number of controllers may run concurrently —
  they observe the same reality, reach the same conclusions, and duplicates
  are fenced. Proven by drill: 3 concurrent controllers, exactly one
  effective promotion; a lone survivor still recovers the next failure.
- The dangerous split-brain window — a partitioned master still taking
  writes — is closed by **push leases** (self-fence on TTL when no
  controller can reach the node). No coordinator of any kind can close this
  window, because by definition it cannot reach the partitioned node.

## Decision

**Zero per-group Raft.** Failover, fencing, zombie demotion, spare respawn,
and (later) intra-group rebalancing are performed by stateless, redundant
controllers whose actions are epoch-fenced by per-node manifests.

**One narrow global Raft remains** (control plane, unchanged): durable
consensus only for intent that cannot be re-derived by observing nodes —
in-flight migration state machines, tenant→group placement, spare-pool
allocation. Low-churn, never on the data path or in per-shard failover.

## Liveness and safety analysis

**Deadlock: structurally impossible.** No controller holds anything while
waiting on anything; actions are one-shot fenced writes. Epochs are
monotonic, so colliding controllers cannot cycle — every effective action
strictly increases the max epoch, and all controllers reconverge on the same
observed state within a tick. The failure modes are deliberate *stalls*:
no reachable survivor, or the degraded-window guard (survivor not recently
observed converged → refuse and page). A Raft coordinator would add a stall
class of its own — quorum loss halts all decisions even with a healthy
survivor available — whereas any single live stateless controller suffices.

**Split-brain: bounded transients only, converging by rule.**
1. *Partitioned old master*: keeps accepting writes for at most the lease
   TTL, then self-fences. Writes in that window are lost on rejoin — inside
   the product's time-boxed-loss contract for cache failover. Raft would not
   shrink this window; only self-fencing can. Edge case: a partition that
   strands the master *with* a controller keeps the lease renewed
   indefinitely — closed on the master side by min-replicas-to-write (an
   isolated master loses its replica links and sheds writes within the
   liveness window), which also makes controller count and placement pure
   availability choices with no safety weight.
2. *Two controllers promoting different survivors* (multi-replica case):
   both promotions can land in different per-node manifests. Next tick,
   every controller applies the same rule — legitimate master = highest
   epoch; all other claimants are demoted as zombies — so the transient
   resolves in ~1 poll interval, and the loser's writes fall under the same
   loss contract. Raft would serialize this away, but the client-visible
   contract is identical because replication is async either way.

**Invariant this rests on (protect in review):** every structural change is
(a) epoch-fenced by the manifests it affects and (b) idempotent under
re-observation — and every structural *state*, including in-flight ones,
is observable from manifests. Any new structural operation (migration
cutover, spare assignment) must satisfy all three before it ships.

## Two-level metadata is preserved

The trio's second job — being level 2 of the metadata hierarchy, so the
global plane stores only coarse slot→group ranges and never absorbs
intra-group churn — survives the removal; only its mechanism changes:

- **Truth**: intra-group slot→pair lives in the data nodes' own manifests
  (SlotClaim + slot-epoch), durable and fenced, sharded across the nodes it
  describes. No quorum needed for the mapping to exist.
- **Serving**: the group routing table is *derived* state — any controller
  computes it by observing manifests, versions it by max epoch, and
  publishes it; proxies cache it. Authoritative correction sits below even
  that: a node answers `-MOVED` from its own manifest for a slot it does
  not own, so stale caches self-correct (the Redis Cluster pattern, which
  routes with zero consensus — ours is stronger: durable fenced manifests,
  not gossip).
- **In-flight intra-group migration intent** is made observable rather than
  Raft-held: the protocol records its phase in the participating pairs'
  manifests (destination claims the slot at a higher epoch in `IMPORTING`
  state; source marks `MIGRATING` and serves until handoff), so a crashed
  controller re-observes the phase and resumes or rolls back.

Consequently nothing intra-group ever rises to the global layer: its Raft
holds *inter*-group intent only (migrations across groups, tenant
placement, spare allocation), exactly as in the two-level design.

**Carried-forward requirements:**
1. Promotion at equal epochs on two nodes is a tie the "highest epoch wins"
   rule cannot break. The multi-replica controller must apply a
   deterministic tie-break (equal epoch → lowest address wins) identically
   in leader selection and zombie fencing.
2. The rebalancing planner must be deterministic with hysteresis (same
   inputs → same plan; act only beyond a deadband). Fencing prevents
   conflicting moves; only planner determinism prevents oscillation between
   concurrent controllers. If chaos testing still shows thrash, restrict
   planning (not execution) to the lease-holding controller — a lock, not
   consensus.

## Consequences

- No consensus library in the group path; no quorum to operate, page on, or
  cold-rebuild. "Trio rebuild" ceases to exist as a procedure — controllers
  are stateless, so recovery is "start one anywhere."
- Debuggability needs are met without a consensus log: the epoch sequence is
  already a total order of structural decisions; decisions are emitted to
  the fleet journal (`flint-journal`) keyed by epoch; any controller can
  serve the derived cluster view. If interleaved multi-controller logs prove
  confusing in practice, the escape hatch is lease-based leader election
  among controllers (a lock, not consensus) — fencing remains the safety.
- The global control plane keeps openraft for its narrow durable-intent job.
