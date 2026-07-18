// SPDX-License-Identifier: Elastic-2.0
//! Shared chaos-test infrastructure (see `cluster`). Workloads live in the
//! binaries: `flint-chaos` (random-write KV oracle direct to the node),
//! `proxy_chaos` (the same oracle through the proxy — client→proxy→node),
//! and `chain` (linked-list traversal under failover).
pub mod cluster;
pub mod oracle;
