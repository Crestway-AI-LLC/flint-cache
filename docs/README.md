# Flint docs

Start here if you are deploying Flint:

- `self-hosting.md` — the operator's guide: prerequisites, the inventory,
  running it as a service, sizing, monitoring, tenants, rotation.
- `capacity-model.md` — how much disk and how many nodes for a working set.
- `tenant-guide.md` — what to hand an application team connecting to it.
- `failover.md` — the failure model: planned handoff, crash, partition,
  and why split-brain is impossible.

Reference:

- `command-support.md` — the supported command matrix, semantics notes,
  and what is excluded by design.
- `release-checklist.md` — the pre-release ritual: gates, conformance,
  drills, chaos.
- `runbooks/ca-rotation.md` — the one certificate operation that is a
  supervised runbook rather than automation.
- `architecture.md` — the three planes, and a normal write/read traced
  end to end through the code.
- `adr/` — architecture decision records for the open stack (numbered
  globally; gaps in the sequence are Crestway-internal records about the
  managed-service plane, referenced from code comments by number).
- `bugs/` — notable root-caused bugs, kept as regression documentation.
- `retry-safety.md` — which commands are safe to retry, and why.

Some code comments also reference `docs/design.md` and the raw benchmark
data under `docs/bench/` — both are Crestway-internal working documents;
the public conclusions live in the ADRs (e.g. ADR-0003 for the storage
baseline numbers).
