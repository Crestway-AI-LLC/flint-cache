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

#[cfg(test)]
mod tests {
    use super::*;

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
