# ADR-0003: RocksDB via rust-rocksdb as the v0/v1 storage engine

Status: accepted · Date: 2026-07-11

## Context
The engine needs an LSM with CFs, WAL tailing, compaction filters,
checkpoints, and range deletes, at NVMe latencies. Building a native Rust
LSM first would delay everything behind unproven plumbing.

## Decision
Proceed on RocksDB through rust-rocksdb 0.24. Basis:
- Coverage audit 10/10 (commit 0e44483) — every design-critical API works;
  one defect (DBWALIterator skips its starting batch) neutralized by the
  sequence-idempotent tailer contract encoded in the audit test.
- Decision benchmarks on i4i.2xlarge NVMe (docs/bench/2026-07-11) clear the
  product contract: sync-ack p50 325 µs / p99 0.7 ms; beyond-RAM read
  p99 229 µs; read p99.9 ~1 ms even under full manual compaction.

## Consequences
- Baseline config to codify in flint-storage: raised max_open_files,
  compaction rate limiter, group-commit WAL cadence (fsync p99.9 ~6 ms is
  the tail to tune down); measure auto-compaction under sustained churn.
- The native-LSM question is deferred until production profiles exist
  (revisit at M2 exit with real workload data), behind the `Kv` seam.

## Alternatives considered
Native Rust LSM now (delays every milestone behind the riskiest work);
C++ shim over RocksDB directly (needed only if rust-rocksdb gaps had been
found — they weren't).
