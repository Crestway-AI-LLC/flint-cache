// SPDX-License-Identifier: Elastic-2.0
//! Make the release tag part of the build's cache key.
//!
//! `version()` reads `FLINT_RELEASE_TAG` with `option_env!`, resolved at
//! COMPILE time. Cargo keys its cache on sources and flags, not on the
//! ambient environment, so without this a rebuild in a warm target directory
//! would keep whatever tag was present at the FIRST compile and ship a binary
//! that confidently misreports its own version.
fn main() {
    println!("cargo:rerun-if-env-changed=FLINT_RELEASE_TAG");
}
