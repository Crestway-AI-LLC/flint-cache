# Release checklist

The pre-release ritual, in order. CI enforces the first block; the drill
and chaos blocks are deliberately manual (minutes-long, randomized) and
MUST be run before tagging.

**Run it, don't retype it:**

    tools/gates.sh

That script IS sections 1-4 below, in this order, and it keeps every step's
output under `/tmp/flint-gates` so a failure can be read instead of
reproduced. This document stays as the explanation of why each step exists
and what it is allowed to be red about; the script is what actually runs.
Retyping the list is how a step silently leaves the gate.

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
    tools/decommission_drill.sh       # graceful failover + single-node retire, guarded
    tools/config_file_drill.sh        # config-file tunables + hot reload (no restart)
    tools/federation_plumbing_drill.sh
    tools/disk_pressure_drill.sh      # host out of room: shed, serve, self-recover
    tools/ctl_error_drill.sh          # a refused command reports, never panics
    tools/client_compat_drill.sh      # redis-py + node-redis, both on RESP3
    tools/proxy_registry_drill.sh     # stray registrations cannot strand a tenant
    tools/reseed_drill.sh             # a replica outside the WAL re-seeds, warm restart does not
    tools/attached_chaos_drill.sh     # chaos through the OPERATOR path, fleet's own controller

## 3b. Integrity — the cluster agrees with itself

Every operation that changes topology now ends in `flintctl verify`
automatically, so a bootstrap or expand that produced an inconsistent
cluster FAILS rather than reporting success. Confirm it independently on a
live cluster before tagging:

    flintctl -f cluster.flint verify --probe <tenant>:<token>

It reconciles three separate beliefs about the fleet — the control plane's
registry, each node's own manifest, and the proxy's actual behaviour — and
the disagreements are the interesting failures. Two shipped bugs went
undetected precisely because each component was internally consistent and
every drill was green: fan-out kept addressing a master that had been dead
since the last failover, and the proxy rejected inline commands while
`--pipe` reported success.

`--probe` is what exercises the data plane; without it the structural
checks still run but the ones that catch a stale routing table cannot, and
it says SKIPPED rather than implying a clean bill.

## 4. Chaos (the honesty step)

    tools/chaos_drill.sh              # random kills vs the ledger oracle
    tools/proxy_chaos_drill.sh        # same, full client->proxy->node path

Both must report ALL SEEDS PASSED with zero corruption / time-travel /
cross-key / acked-loss anomalies.

**Known limit, stated so nobody mistakes the coverage.** Both chaos drills
kill PROCESSES on one host. They cannot produce a network partition, host
loss, cross-AZ latency, or a single host running out of disk — the faults
that a multi-machine cluster has and a single box does not. That is the
weakest useful form of chaos, and it still found two serious bugs during
the 8-pair EC2 run (docs/bench/scale-8-pairs.md).

## 4b. Multi-host chaos (fleet repo, costs money, one command)

    packaging/aws/chaos-cluster/run.sh --tag <tag>

Provisions throwaway EC2 hosts, stages the REAL release bundle, bootstraps
across them, runs the same ledger oracle with kills routed through flintctl
to the owning host, verifies, and destroys everything. Not in CI — that
needs credentials and a cost budget — but it is one command and it tears
down on every exit path, including Ctrl-C, and the hosts self-terminate on
their own TTL if the driving side dies.

This is what covers the faults section 4 cannot: a real network between the
pair members. It still does not cover partitions, host loss or a single host
filling its disk; those remain untested.

## 5. Tag

Tag only after 1-4 are green in one working tree with no uncommitted
changes. The tag message names the conformance count and the drill set
run. If the release BREAKS the on-disk format, say `format-break` in
the tag annotation — the pipeline records it in the manifest, and
`flintctl upgrade --manifest` refuses the canary fast path for it
(a format break cannot roll back; it ships via the migration runbook).

## 6. What the tag triggers (fleet releases)

Pushing the tag to the fleet repo runs the release pipeline: tests on
both workspaces against the exact bits shipped, then ONE artifact —
the 14-binary Linux bundle plus `manifest.json` (version, bundle URL,
sha256, format_break, public commit) attached to a GitHub release.
Deploying is then one command (or the ops portal's Canary-upgrade
button): download, verify the sha256, unpack into the inventory's bins
dir, and

    flintctl -f <inventory> upgrade --manifest manifest.json --version-tag <tag>

— canary replica first, soak against the fleet journal, remaining
replicas, masters last via controlled failover; any unexpected journal
transition aborts the roll (already-upgraded nodes stay: roll forward).
A HOTFIX is the same pipeline and the same command with a shorter
`--soak-ms` — never a separate untested path.
