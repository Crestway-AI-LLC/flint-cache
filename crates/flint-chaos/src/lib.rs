//! Shared chaos-test infrastructure (see `cluster`). Workloads live in the
//! binaries: `flint-chaos` (random-write KV oracle) and `chain` (linked-list
//! traversal under failover).
pub mod cluster;
