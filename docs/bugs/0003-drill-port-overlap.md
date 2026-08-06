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
