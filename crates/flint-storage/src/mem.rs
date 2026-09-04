// SPDX-License-Identifier: Elastic-2.0
//! What the NODE has, as opposed to what one unit of work is allowed.
//!
//! BUG-0060 is an audit whose question is "where is a limit missing that would
//! let a node crash rather than refuse", and its answer names two candidate
//! fixes: a node-level budget the per-unit limits draw against, or a per-unit
//! limit *derived from the node's memory and the connection count* rather than
//! chosen as a constant. **Both are impossible while the seat cannot read its
//! own memory**, which it could not until this module. `disk.rs` is the model
//! and it is deliberately followed: read the real thing, report it, and let
//! policy live somewhere else.
//!
//! THIS MODULE SETS NO POLICY AND SHEDS NOTHING. It is measurement, so that the
//! budget can later be argued from a number instead of a guess. `disk.rs` earned
//! the same separation the hard way -- its own comment records that a total of
//! zero must read as *100% free* precisely so a failed measurement can never
//! shed writes by itself.

/// Node memory, in bytes. `avail` is what the OS believes is obtainable without
/// swapping -- not `free`, which excludes reclaimable page cache and reads far
/// too low on any machine that has done work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Usage {
    pub avail_bytes: u64,
    pub total_bytes: u64,
}

impl Usage {
    /// Available as a percentage of total, 0..=100.
    ///
    /// A total of zero returns 100, the same convention as [`crate::disk`]: an
    /// unreadable measurement must look like *abundance* rather than pressure,
    /// so that whatever consumes this later cannot be pushed into refusing by
    /// the mere absence of a reading.
    pub fn avail_pct(&self) -> u64 {
        if self.total_bytes == 0 {
            return 100;
        }
        // 128-bit, NOT `saturating_mul(100)`. Saturating clamps the NUMERATOR
        // and then divides, so on a machine with more than u64::MAX/100 bytes
        // the ratio silently becomes 1 instead of 100 -- a wrong answer dressed
        // as an overflow guard. The unit test for it failed on the first run,
        // which is the only reason this is not still `saturating_mul`.
        let pct = u128::from(self.avail_bytes) * 100 / u128::from(self.total_bytes);
        u64::try_from(pct).unwrap_or(100)
    }
}

/// Read the node's memory, or `None` where it cannot be read.
///
/// `None` is a THIRD STATE and callers must keep it distinct from zero: no
/// memory available and no reading are opposite facts, and collapsing them is
/// the failure this codebase names "cannot look is not absent" (ADR-0028).
///
/// Linux only, via `/proc/meminfo`. macOS is deliberately `None` rather than a
/// figure derived from `vm_stat`: the fleet runs Linux, and a locally-invented
/// number would be compared against real ones later and believed.
pub fn sample() -> Option<Usage> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let mut avail_kb: Option<u64> = None;
    let mut total_kb: Option<u64> = None;
    for line in text.lines() {
        let Some((key, rest)) = line.split_once(':') else {
            continue;
        };
        // "MemTotal:       16384000 kB" -- the unit is always kB in meminfo,
        // and is checked rather than assumed: a field that ever reports another
        // unit must not be read as kB and silently scaled by 1024.
        let mut parts = rest.split_whitespace();
        let Some(value) = parts.next().and_then(|v| v.parse::<u64>().ok()) else {
            continue;
        };
        if parts.next() != Some("kB") {
            continue;
        }
        match key {
            "MemAvailable" => avail_kb = Some(value),
            "MemTotal" => total_kb = Some(value),
            _ => {}
        }
    }
    Some(Usage {
        avail_bytes: avail_kb?.saturating_mul(1024),
        total_bytes: total_kb?.saturating_mul(1024),
    })
}

#[cfg(test)]
mod tests {
    use super::Usage;

    #[test]
    fn a_total_of_zero_reads_as_abundant_not_starved() {
        let u = Usage {
            avail_bytes: 0,
            total_bytes: 0,
        };
        assert_eq!(
            u.avail_pct(),
            100,
            "an unreadable total must not look like pressure"
        );
    }

    #[test]
    fn percentages_are_computed_from_the_pair() {
        assert_eq!(
            Usage {
                avail_bytes: 4,
                total_bytes: 16
            }
            .avail_pct(),
            25
        );
        assert_eq!(
            Usage {
                avail_bytes: 16,
                total_bytes: 16
            }
            .avail_pct(),
            100
        );
    }

    /// A machine with more memory than `u64::MAX / 100` would overflow a naive
    /// `avail * 100`. Saturating is the guard; this pins it.
    #[test]
    fn a_huge_total_does_not_overflow_the_percentage() {
        let u = Usage {
            avail_bytes: u64::MAX,
            total_bytes: u64::MAX,
        };
        assert_eq!(u.avail_pct(), 100);
    }
}
