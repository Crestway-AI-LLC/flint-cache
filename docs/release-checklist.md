# Release checklist

The pre-release ritual, in order. CI enforces the first block; the drill
and chaos blocks are deliberately manual (minutes-long, randomized) and
MUST be run before tagging.

## 1. Gates (CI order — fmt first)

    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace
    cargo clippy --workspace --all-targets --features flint-server/rocks -- -D warnings
    cargo test --workspace --features flint-server/rocks

## 2. Conformance — three targets, all 100%

Against a local Valkey (the oracle), Flint mem, and Flint rocks:

    ./target/release/flint-conformance --target 127.0.0.1:<port>

If the oracle disagrees with Flint, Flint is wrong. If a corpus case
disagrees with the oracle, the case is wrong.

## 3. Core drills (each prints PASS or exits nonzero)

    tools/restart_drill.sh            # warm restart, data intact
    tools/repl_drill.sh               # replication parity + lag
    tools/failover_drill.sh           # epoch-fenced promotion, zombie fenced
    tools/proxy_drill.sh              # one endpoint across migration+failover
    tools/slot_migrate_drill.sh       # bulk + tail slot move
    tools/slot_map_drill.sh           # CP slot-ownership truth, cold proxy
    tools/rebalance_execute_drill.sh  # planner + executor to convergence
    tools/tenant_quota_drill.sh       # rate + storage quota enforcement
    tools/token_rotation_drill.sh     # dual-version token roll, zero downtime
    tools/cert_reload_fleet_drill.sh  # leaf rotation under live traffic
    tools/controlplane_ha_drill.sh    # Raft CP: election, failover, watch push
    tools/federation_plumbing_drill.sh

## 4. Chaos (the honesty step)

    tools/chaos_drill.sh              # random kills vs the ledger oracle
    tools/proxy_chaos_drill.sh        # same, full client->proxy->node path

Both must report ALL SEEDS PASSED with zero corruption / time-travel /
cross-key / acked-loss anomalies.

## 5. Tag

Tag only after 1-4 are green in one working tree with no uncommitted
changes. The tag message names the conformance count and the drill set
run.
