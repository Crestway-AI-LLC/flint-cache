# BUG-0003: drills sharing a port adopt and kill each other's seats (RESOLVED)

Status: RESOLVED 2026-08-06 · Found by a guard added in the fleet repo · Severity: medium (spurious gate failures)

## Symptom
Two consecutive runs of the fleet gate failed on two different drills —
`saas_fulfillment`, then `traffic_rebalance` — each of which passed when run
alone. In the public repo, 23 ports were claimed by two or more drills
across 14 pairs and one three-way, without having visibly bitten yet.

## Root cause
`tools/lib/fleet.sh` decides seat ownership by scope directory OR declared
port. A port claimed by two drills therefore makes each of them treat the
other's processes as its own: `fleet_guard` waves them through and
`fleet_kill` sends them `-9`.

In a serial suite the usual symptom is milder and more confusing than that
implies — a seat from the previous drill that has not finished dying still
holds the port, and the next drill's control plane times out waiting to
bind, reporting whatever product-shaped failure that causes downstream.

## Fix
Disjoint blocks, allocated by script rather than by eye: read every
`fleet_init` in both repos plus the default cluster ports, treat that union
as taken, hand out from a free range, rewrite with digit-boundary matching.

Doing it by hand is not safe. The fleet repo's copy of the guard below
fired on a block chosen by eyeballing adjacent numbers, within an hour of
the guard being added, against its own author.

## The check that holds it
`assert_no_port_overlap` in `tools/gates.sh`, beside `assert_no_default_ports`.
Both are properties of the SET of drills that no single drill can check
about itself.

## Postscript: the guard was fooled for a week by ports nothing declared

Reopened and fixed 2026-08-06, same day. The block allocation above was
correct and the guard did work — on the ports it could see. `flint-chaos`
bound 6460/6461/6470+n/7690 as constants in Rust, so the seven drills that
drive it bound a cluster's worth of ports they never declared. Invisible to
`assert_no_port_overlap`, which reads `fleet_init` lines and nothing else.

That is how `tenant_quota_drill`'s control plane came to share 7690 with the
chaos proxy with both guards green, and it is why an exemption
(`CHAOS_PORTS="6460 6470 7690"`) had to be carved into the guard to keep it
from failing on a conflict it could not otherwise express.

The fix was to make the ports an argument — `--port-base`, an 8-port block
per drill (`cluster::SPAN`) — so the exemption could be deleted rather than
maintained.

**Two things nearly shipped half-fixed, and both are the same mistake.**

1. `next_replica_port` stayed hardcoded at `6470 + next_id`. The base moved
   the two seats that exist before the first kill and nothing else, so every
   replacement replica — which is most of what a chaos run is — still walked
   up from 6470, unbounded. Measured: a 12-second run with `--port-base 6390`
   bound 6472, which `reseed_drill` declares. The pool is now base-relative
   AND bounded, because a drill can only declare a bounded set; it cycles
   within `SPAN` rather than climbing, skipping any slot not yet free.

2. Compatibility wrappers `bootstrap()` / `bootstrap_controlled()` kept the
   old 6460 default "for callers with no reason to choose". Three of this
   crate's four binaries — `chain`, `hotkey`, `proxy_chaos` — took that
   default and went on binding the hardcoded block while the drills driving
   them declared a different one. The wrappers are deleted: every caller
   must name its block, because a default is an invitation to stay invisible.

Both were found by measurement, not review — sampling `lsof` for every port
bound across a whole run and diffing against the declared block. Neither
would have been caught by the drills passing, because they did pass.

## The check that holds THAT
Not a script — arithmetic. Every port the harness binds is derived from
`base` and wraps within `SPAN`, so staying inside the declared block is a
property of the code rather than of anyone remembering. The gate's
exemption is gone, which is the observable part: a collision anywhere in
the set is now expressible, and fails.
