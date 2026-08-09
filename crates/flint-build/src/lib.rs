// SPDX-License-Identifier: Elastic-2.0
//! What this binary IS — one definition, shared by every Flint binary.
//!
//! The stamp used to be a property of whoever LAUNCHED a process: flint-server
//! read `FLINT_BUILD_VERSION` at runtime and only `flintctl upgrade` set it, so
//! `start`, `restart-node` and systemd all produced nodes reporting `0.0.1` out
//! of good release binaries, and `flintctl verify` failed "single build across
//! the fleet" on a fleet where every file was identical.
//!
//! It lives in its own crate rather than being copied into each binary because
//! today already demonstrated what happens when one fact has several
//! definitions: a proxy's identity was computed in three places, two of them
//! agreed with each other and not with the third, and a healthy 7-host cluster
//! failed its own verify. `option_env!` resolves where it is WRITTEN, so
//! putting it here also means the tag is captured exactly once.

/// Precedence, and the ORDER is the point:
///
///   1. `FLINT_RELEASE_TAG`, baked in at COMPILE time by the release build.
///   2. `FLINT_BUILD_VERSION` from the environment, for unstamped builds.
///   3. The caller's crate version.
///
/// The baked value deliberately WINS over the environment, which is what makes
/// `upgrade`'s post-roll build assertion mean something: flintctl used to set
/// the variable and then verify the value it had just set, so the check passed
/// even when the staged binary was still the old one.
pub fn version(crate_version: &str) -> String {
    resolve(
        option_env!("FLINT_RELEASE_TAG"),
        std::env::var("FLINT_BUILD_VERSION").ok(),
        crate_version,
    )
}

/// The precedence itself, separated from where the three values come from —
/// `option_env!` resolves at compile time, so a test of `version()` could only
/// ever observe the one combination this build happened to get, which is
/// precisely the case that was wrong.
pub fn resolve(baked: Option<&str>, env: Option<String>, crate_version: &str) -> String {
    baked
        .map(str::to_string)
        .or(env)
        .unwrap_or_else(|| crate_version.to_string())
}

/// Is this binary a RELEASE, as opposed to a developer or CI-artifact build?
///
/// The distinction is load-bearing, not cosmetic: a non-release binary is
/// refused the fleet-mutating verbs unless the inventory declares itself
/// disposable, so a source build cannot roll the playground. That guard is
/// only possible because a binary can now say what it is.
///
/// Deliberately strict — a release tag is `v<major>.<minor>.<patch>` with an
/// optional suffix (`v0.1.0-rc.23`). Anything else, including the crate-version
/// fallback `0.0.1` and the dev stamp `0.0.0-dev+<sha>`, is not a release. The
/// failure that matters is calling a dev build a release, so the shape has to
/// be opted INTO rather than merely not matched.
pub fn is_release(v: &str) -> bool {
    let Some(rest) = v.strip_prefix('v') else {
        return false;
    };
    let core = rest.split(['-', '+']).next().unwrap_or("");
    let parts: Vec<&str> = core.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
}

/// How a reported build string goes out on the RESP WIRE, in `HELLO`'s
/// `version` field.
///
/// A separate form from [`display`] because the reader is a program, not a
/// person. Real Redis answers `7.2.4` there, and client libraries feature-gate
/// on it by parsing it as a version number — so the leading `v` of a release
/// tag is dropped, and the operator-facing `unstamped` rewrite is NOT applied:
/// a word where a number belongs is worse to a parser than an honest `0.0.1`.
///
/// This existing at all is the fix for a real defect. Both `HELLO` handlers
/// passed `env!("CARGO_PKG_VERSION")` — the workspace version, literally
/// `0.0.1` — so on a fleet where `flintctl status`, `CPINFO`, `PROXYSTATS` and
/// `--build-version` all correctly said `v0.1.0-rc.37`, the ONE version string
/// a client library ever reads said `0.0.1`. ADR-0014 D1 shipped every
/// operator-facing stamp and missed the only client-facing one; it was found
/// by speaking RESP to the playground edge from outside, which is the only
/// vantage point that can see it.
///
/// Everything that is not a release tag passes through verbatim, for the same
/// reason [`display`] does: an operator who rolled `--version-tag v2` must see
/// `2`, not a value invented here.
pub fn wire(reported: &str) -> &str {
    reported.strip_prefix('v').unwrap_or(reported)
}

/// How a reported build string should be SHOWN to an operator.
///
/// Exactly ONE value is rewritten: the crate-version fallback that a
/// from-source build reports when nothing stamped it. `0.0.1` reads like a
/// real (if oddly old) release to someone who has just cloned a tagged
/// repository, and saying `unstamped` is the difference between a version
/// and an admission that there isn't one.
///
/// Everything else passes through VERBATIM, and that is the load-bearing
/// half. The first version of this rewrote anything that was not a release
/// tag, which swallowed `upgrade --version-tag v2` into "unstamped" — so the
/// status column an operator reads to confirm a roll actually took could no
/// longer tell two builds apart, and it disagreed with the build label the
/// exporter publishes for the same node. A display rule that hides the fact
/// it is displaying is worse than the confusion it set out to fix.
pub fn display<'a>(reported: &'a str, crate_version: &str) -> &'a str {
    if reported == crate_version {
        "unstamped"
    } else {
        reported
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_crate_fallback_admits_it_is_not_a_version() {
        assert_eq!(display("0.0.1", "0.0.1"), "unstamped");
    }

    /// The regression that shipped in rc.29 and was caught by the fleet
    /// repo's canary drill: an operator-chosen build number is not a release
    /// tag, and hiding it breaks the one column that confirms an upgrade.
    #[test]
    fn an_operator_chosen_build_number_survives_display() {
        for v in ["v2", "build-1234", "2026-08-04.3", "v0.1.0-rc.29"] {
            assert_eq!(display(v, "0.0.1"), v, "{v} must be shown as itself");
        }
    }

    /// The defect this function fixes: the wire form must be the BUILD, not
    /// the crate version that `HELLO` used to hardcode.
    #[test]
    fn the_wire_form_carries_the_release_without_its_v() {
        assert_eq!(wire("v0.1.0-rc.37"), "0.1.0-rc.37");
        assert_ne!(wire(&resolve(Some("v0.1.0-rc.37"), None, "0.0.1")), "0.0.1");
    }

    /// A client library parses this field. `unstamped` would not parse, so
    /// the operator-facing rewrite must NOT leak onto the wire.
    #[test]
    fn the_wire_form_never_says_unstamped() {
        assert_eq!(wire("0.0.1"), "0.0.1");
        assert_eq!(display("0.0.1", "0.0.1"), "unstamped");
    }

    #[test]
    fn a_wire_version_is_stripped_at_most_once_and_only_at_the_front() {
        assert_eq!(wire("0.0.0-dev+117cd15"), "0.0.0-dev+117cd15");
        assert_eq!(wire("v2"), "2");
        assert_eq!(wire("build-1234"), "build-1234");
        assert_eq!(wire("vv1.0.0"), "v1.0.0");
        assert_eq!(wire(""), "");
    }

    #[test]
    fn the_dev_channel_stamp_already_names_itself() {
        assert_eq!(display("0.0.0-dev+117cd15", "0.0.1"), "0.0.0-dev+117cd15");
    }

    #[test]
    fn a_baked_tag_outranks_whatever_the_launcher_says() {
        assert_eq!(
            resolve(
                Some("v0.1.0-rc.23"),
                Some("v0.0.9-whatever".into()),
                "0.0.1"
            ),
            "v0.1.0-rc.23"
        );
    }

    #[test]
    fn a_launcher_cannot_make_a_binary_claim_a_version_it_is_not() {
        let claimed = resolve(Some("v0.1.0-rc.23"), Some("v0.1.0-rc.24".into()), "0.0.1");
        assert_ne!(claimed, "v0.1.0-rc.24");
    }

    #[test]
    fn without_a_baked_tag_the_environment_still_works() {
        assert_eq!(
            resolve(None, Some("v0.1.0-rc.22".into()), "0.0.1"),
            "v0.1.0-rc.22"
        );
        assert_eq!(resolve(None, None, "0.0.1"), "0.0.1");
    }

    #[test]
    fn release_tags_are_recognised() {
        for v in ["v0.1.0", "v0.1.0-rc.23", "v1.2.3", "v10.0.4-beta"] {
            assert!(is_release(v), "{v} should be a release");
        }
    }

    /// The direction that matters. A dev build wrongly classed as a release
    /// would be handed the fleet-mutating verbs against a live cluster, so
    /// every non-release shape is pinned here rather than left to inference.
    #[test]
    fn everything_that_is_not_a_release_tag_is_refused() {
        for v in [
            "0.0.1",             // the crate-version fallback
            "0.0.0-dev+117cd15", // the dev stamp
            "v0.1",              // not three components
            "v0.1.0.1",          // four
            "vX.Y.Z",            // not numeric
            "0.1.0",             // no leading v
            "",                  // nothing at all
            "release",           // wishful
            "v..",               // empty components
        ] {
            assert!(!is_release(v), "{v:?} must NOT count as a release");
        }
    }
}
