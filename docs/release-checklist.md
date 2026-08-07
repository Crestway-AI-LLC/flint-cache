# Release checklist

The pre-release ritual, in order. **CI now enforces all four blocks on
every pull request** (`.github/workflows/gate.yml` runs
`tools/gates.sh conformance drills chaos`; `ci.yml` runs the `check`
stage). Running it yourself before tagging is still the rule — a tag is
cut from a working tree, not from a green pull request — but the drills
and chaos are no longer a thing anyone has to remember.

Two CI-specific notes:

- **`FLINT_GATE_STRICT=1` makes a skipped drill a failed one.** Locally a
  skip is right (no `mkfs.ext4` on macOS). In CI it is not: a drill that
  skipped reads identically to one that passed, so a forgotten dependency
  would silently delete coverage from every future run.
- **The conformance oracle is pinned to Valkey 9.1.0** and built from
  source, because Valkey forked after Ubuntu noble's package freeze and
  is not installable with apt there. Pinned rather than latest: a
  floating oracle turns a Valkey release into a mysterious Flint
  conformance failure.

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
    cargo clippy --workspace --all-targets --features flint-server/rocks,flint-backup/rocks -- -D warnings
    cargo test --workspace --features flint-server/rocks,flint-backup/rocks

## 2. Conformance — three targets, all 100%

Against a local Valkey (the oracle), Flint mem, and Flint rocks:

    ./target/release/flint-conformance --target 127.0.0.1:<port>

If the oracle disagrees with Flint, Flint is wrong. If a corpus case
disagrees with the oracle, the case is wrong.

## 3. Core drills (each prints PASS or exits nonzero)

    tools/gates.sh drills

**The list lives in `CORE` at the top of `tools/gates.sh`, and nowhere
else.** This section used to enumerate the drills with a one-line gloss
each, which is exactly the shape the top of this file warns about: the
enumeration went stale at 23 while the gate grew to 39, so a reader
checking what the suite covers would have undercounted it by 40%. Adding a
drill to `CORE` is what puts it in the gate; there is no second list to
keep in step.

Each drill's own header says what it proves and which incident it was
written for — `head -20 tools/<name>_drill.sh` is the gloss this section
used to carry, kept next to the code it describes.

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

## 6. What the tag builds (fleet releases)

The tag does not TRIGGER anything: the release is built by running
`packaging/aws/release-box/run.sh --version <tag>` in the fleet repo,
which brings up an Amazon Linux 2023 EC2 box, builds both workspaces
there, and tears it down.

It is a script rather than a CI job on purpose. The fleet runs AL2023,
and a bundle linked against the Ubuntu runner's newer glibc will not
start on it — so the build has to happen on the target OS either way.
Making it a script one person runs also removes a hosted CI provider
from the critical path of shipping a fix, which stopped being
hypothetical when an Actions outage blocked rc.32 mid-release.

It produces ONE artifact — the 14-binary Linux bundle plus
`manifest.json` (version, bundle URL, sha256, format_break, public
commit) — attached to a GitHub release on the fleet repo and mirrored
to the public repo, with the same checks the CI job used to run: tests
on both workspaces against the exact bits shipped, and an assert that
the build stamp LANDED, by asking `flint-server --build-version` and
`flintctl --build-version` rather than trusting the build.
Deploying is then one command (or the ops portal's Canary-upgrade
button): download, verify the sha256, unpack into the inventory's bins
dir, and

    flintctl -f <inventory> upgrade --manifest manifest.json --version-tag <tag>

— canary replica first, soak against the fleet journal, remaining
replicas, masters last via controlled failover; any unexpected journal
transition aborts the roll (already-upgraded nodes stay: roll forward).
A HOTFIX is the same pipeline and the same command with a shorter
`--soak-ms` — never a separate untested path.
