//! flint-conformance: the compatibility oracle.
//!
//! Runs Redis/Valkey conformance tests against an arbitrary RESP endpoint
//! and reports pass rates per command family. This harness gates every
//! milestone: the oracle runs before the features exist.
//!
//! M0 scope: harness that (1) spawns a target server, (2) drives the Valkey
//! test suite (vendored as a git submodule later) or its own table-driven
//! cases, (3) emits a machine-readable pass-rate report for CI trend lines.

fn main() {
    println!("flint-conformance 0.0.1 (harness lands in M0)");
}
