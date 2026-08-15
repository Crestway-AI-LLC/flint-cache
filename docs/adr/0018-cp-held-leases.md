# ADR-0018: The write lease is held at the control plane, not the controller

Status: accepted — supersedes the lease half of ADR-0004 (epoch fencing is
unchanged)

## Context

ADR-0004 closed the dangerous split-brain window — a partitioned master still
acking writes after a successor is promoted — with **push leases**: the
controller renews each master's lease every tick, and a master whose lease
expires self-fences to read-only.

Five consecutive scale runs (runs 8–12, 2026-08-14) demonstrated the cost of
anchoring that lease to the controller. The controller is ONE process with no
quorum under it, and the lease design converts every mode of its silence into
a fleet-wide write outage:

- A >TTL stall self-fences every master at once (#168). Fixed by recovery-on-
  resume — which only works if the controller resumes.
- The recovery predicate could not fire for a replica-less lineage holder
  (#171). Fixed with remembered observations — which only helps if the
  controller is running its loop.
- Run 12: the controller went silent AFTER a clean bring-up (cause still under
  investigation, #172). Its CP registration went ~50s stale, the decoupled
  renewers stopped renewing — correctly, per their own design — and all eight
  nodes fenced. Every pair had a healthy, in-sync member. There was nothing to
  protect against: **promotion is performed only by controllers, so when no
  controller is alive there is no successor to split-brain with.** The fence
  fired precisely when it protects nothing, and, the controller being the
  recovery mechanism too, the outage was permanent.

Each bug got a correct local fix, and the shape repeated, because the shape is
structural: **the component holding the fence is the least available component
in the system.** The fix that ends the class is moving the fence, not
hardening its holder again.

The control plane is the natural holder. It is 3-seat Raft (#148), file-durable
per seat, already dialled by every node (`--journal`), already the registry
proxies trust, and already told about promotions (`CPPROMOTED`) — today as a
deliberately non-durable hint.

## Decision

1. **Masters renew their own lease against the CP.** A serving master calls
   `CPLEASE <self-addr>` every `ttl/3` on the same mTLS channel the journal
   uses (the TTL itself is the node's own `--lease-ttl-ms`). The reply is one
   of:
   - `OK` — the CP's master-of-record for that pair is this address,
     or the pair has no record yet (adoption; see 4).
   - `-SUPERSEDED <successor>` — a promotion has been recorded over this
     node. The node fences **immediately**, not at TTL.
   The server's lease watchdog is unchanged: deadline in the future or fence.
   Only the renewer moves — from a thread in the controller to a loop in the
   master itself.

2. **Promotion writes the fencing record FIRST.** The promotion path becomes:
   decide → `CPFENCE <pair-idx> <survivor>` (durable, Raft-committed, version
   bump wakes watching proxies — subsuming CPPROMOTED's hint role) →
   `FLINTPROMOTE` the survivor. A promotion that cannot commit its record does
   not happen. This inverts ADR-0004's "failover must not depend on the CP",
   deliberately: a promotion without a fencing record is exactly the
   split-brain reopened, and the CP's quorum is the most available thing in
   the fleet to depend on.

3. **The controller stops renewing leases entirely.** The per-pair renewer
   threads (#168's decoupling) are deleted, not moved. The controller's job
   contracts to what ADR-0004 said it was: observe, decide, fence, promote.
   Controller death, stall, deploy, or crash-loop now costs FAILOVER
   CAPABILITY while it lasts — new failures stand un-failed-over — and
   nothing else. A healthy fleet keeps serving indefinitely.

4. **Adoption bootstraps the record.** A `CPLEASE` from a member of a pair
   with no master-of-record adopts that member as the record (gen 0, logged).
   Only serving masters renew, so in a converged pair exactly one node ever
   asks. This lets existing fleets roll onto D with no inventory change.

5. **Membership guard.** `CPLEASE`/`CPFENCE` addresses must belong to the
   pair the CP already tracks (`CPSETPAIR` keeps it current through
   swap/expand). A stray or replayed address is refused.

## Safety analysis

The TTL stays seconds-scale but the DEFAULT moves from 3000 to 5000 ms,
and the reason is a measurement, not taste. The lease TTL bounds the
window in which a partitioned old master can still ack writes after its
successor is promoted, and the chaos ledger's acked-loss oracle measures
that window — that pressure holds it DOWN. What pushes it up is new to
self-renewal: a routine CP leader election leaves the lease path dark for
~2.4 s measured (follower heartbeat timeout + vote + first served
renewal), a window the old controller-pushed renewals never had inside
their tolerance. At TTL 3000 / renew ttl/3, a leader kill fenced a
healthy fleet about as often as not (found by ctl_cpha the moment nodes
carried leases); at 5000 the tolerance is ~3.3 s with fast retry filling
the tail. The reachable-superseded fence is unaffected: it still fires
within one renewal interval (~ttl/3), so the LONG bound applies only to a
master partitioned from the CP entirely — ADR-0004's own worst case, two
seconds wider.

- *Old master partitioned from the CP*: cannot renew; fences ≤ TTL after its
  last renewal. The controller (which by definition can reach the CP if it
  promoted — CPFENCE succeeded) has already fenced the lineage. Window ≤ TTL,
  identical to ADR-0004.
- *Old master reachable*: its next renewal returns `-SUPERSEDED`; it fences
  within one renewal interval (≤ ttl/3) — FASTER than today's expiry-only
  path.
- *Manual/raw FLINTPROMOTE without CPFENCE* (a human with valkey-cli): the
  promoted node's renewals return `-SUPERSEDED` against the stale record, and
  it re-fences within a renewal interval. Un-recorded promotions do not
  stick. This is a feature: `flintctl failover` (and the controller) are the
  supported paths and both write the record.
- *CP quorum lost*: masters cannot renew and fence ≤ TTL. This is the new
  dependency, accepted with eyes open: nobody can be promoted while the CP is
  down either (CPFENCE cannot commit), so a fenced fleet under CP loss is not
  losing a race to a successor — and a 3-seat Raft CP going dark is a
  categorically rarer event than a single controller process going quiet,
  which is the event measured five runs in a row.

## Overload isolation: the lease path must not queue behind CP work

Putting leases on the CP creates a new way to lose them: a renewal that sits
in a queue behind an expensive CP operation — snapshot serialization for many
watchers, a Raft commit, a journal burst — is indistinguishable from a
partition once the delay crosses TTL. CP OVERLOAD must not be able to fence
the fleet, or this ADR has only relocated the bug it fixes.

So the lease path is segregated from the rest of the CP's work:

- **Its own state, its own lock.** The master-of-record table lives in a
  dedicated structure with its own lock, not inside the big `State` mutex.
  `CPLEASE` touches only that table — never the registry, never the snapshot,
  never the journal. A renewal cannot contend with a snapshot being serialized
  or a commit being fsynced, because they share nothing.
- **Renewals are reads.** `CPLEASE` allocates nothing durable and never
  commits; only `CPFENCE` and first-touch adoption write. The steady-state
  lease load is a hash lookup per master per `ttl/3` — it scales with PAIRS,
  not keys or traffic (512 pairs ≈ 500 tiny reads/s, each answerable in
  microseconds once off the shared lock).
- **Measured, not assumed.** The CP tracks and exposes lease-path latency
  (`lease_p99_us` in CPINFO), and the drill for this ADR hammers the CP with
  watchers and snapshot churn while asserting renewal p99 stays well inside
  `ttl/3`. If contention ever appears despite the split lock, the escalation
  path is a dedicated listener for the lease class — a separate accept queue,
  not just a separate lock — and the gauge is what tells us it is needed.

## What this dissolves

#168's recovery machinery, #171's remembered-lineage escape, and #172's blast
radius all exist because controller silence fences healthy masters. Under D,
controller silence fences nothing. The #171 remembered-lineage logic is kept
— it still recovers a pair whose master genuinely DIED replica-less — but its
self-fence trigger disappears. `controller_stall_drill` inverts: the drill now
asserts the fleet does NOT fence during the stall and keeps serving writes,
with the positive control moved to `CPFENCE` (a recorded promotion must still
fence the old master within a renewal interval).

## Consequences

- The data plane's write availability now depends on CP quorum (after TTL).
  Accepted per the safety analysis. The CP was already the auth/routing
  registry; this makes its availability tier explicit.
- Lease traffic: one small read-only command per master per ttl/3 (~1s).
  Renewals do not commit; only CPFENCE and adoption write.
- `CPPROMOTED` is subsumed by `CPFENCE` (which bumps the version and wakes
  proxies) and is removed from the promotion path; the command remains,
  deprecated, for one release.
- flintctl: `lease-ttl-ms` moves from the controller's args to the servers';
  `flintctl failover` gains the CPFENCE step. Inventory syntax is unchanged.
- Runbook: "controller down" stops being a fleet emergency. "CP quorum lost"
  gains a hard deadline (TTL) and pages accordingly.
