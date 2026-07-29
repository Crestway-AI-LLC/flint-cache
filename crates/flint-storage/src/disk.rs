// SPDX-License-Identifier: Elastic-2.0
//! How much room is left on the filesystem holding the data directory.
//!
//! The node needs this because nothing else caps it. Per-tenant storage
//! quotas bound each namespace, but the sum of quotas is *meant* to exceed
//! the disk — that oversubscription is the packing economics — so the host
//! filling up is a normal consequence of the business model rather than an
//! operator error.
//!
//! And an LSM does not degrade gracefully when it runs out. Compaction
//! writes the new SSTs before dropping the old ones, so it needs headroom
//! and stops making progress well before the last byte is gone. Worse, the
//! obvious cure is a trap: freeing space means deleting, a delete is a
//! WRITE (a tombstone), and actually reclaiming the bytes needs the
//! compaction that has no room to run. A node allowed to fill completely
//! can be unable to dig itself out.
//!
//! So the point of measuring is to stop early, while there is still enough
//! room to delete and compact — not to report the disaster after it lands.

use std::path::Path;

/// A filesystem's capacity, as of one sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Usage {
    pub free_bytes: u64,
    pub total_bytes: u64,
}

impl Usage {
    /// Free space as a percentage of total, 0..=100. A total of zero (an
    /// unreadable or exotic filesystem) reads as 100% free, so a failed
    /// measurement never sheds writes on its own — see [`sample`].
    pub fn free_pct(&self) -> u64 {
        if self.total_bytes == 0 {
            return 100;
        }
        self.free_bytes.saturating_mul(100) / self.total_bytes
    }
}

/// Sample the filesystem containing `path`.
///
/// Uses the space available to an UNPRIVILEGED writer (`f_bavail`), not the
/// raw free count: most filesystems reserve a slice for root, and a server
/// that counted it would believe in room it cannot use.
///
/// Returns `None` when the filesystem cannot be interrogated. Callers must
/// treat that as "unknown", never as "full" — a syscall failing is not
/// evidence about disk space, and shedding writes because a stat failed
/// would turn a monitoring gap into an outage.
#[cfg(unix)]
pub fn sample(path: &Path) -> Option<Usage> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: `statvfs` fills the struct we hand it and reads only the
    // NUL-terminated path we just built; both live for the call.
    let stat = unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) != 0 {
            return None;
        }
        stat
    };
    // `f_frsize` is the fragment size that block counts are denominated in.
    // Some platforms report 0; fall back to f_bsize rather than compute a
    // confident zero.
    let unit = if stat.f_frsize > 0 {
        stat.f_frsize as u64
    } else {
        stat.f_bsize as u64
    };
    Some(Usage {
        free_bytes: (stat.f_bavail as u64).saturating_mul(unit),
        total_bytes: (stat.f_blocks as u64).saturating_mul(unit),
    })
}

#[cfg(not(unix))]
pub fn sample(_path: &Path) -> Option<Usage> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_a_real_filesystem() {
        let u = sample(Path::new(".")).expect("the working directory is on a filesystem");
        assert!(u.total_bytes > 0, "total should be positive");
        assert!(
            u.free_bytes <= u.total_bytes,
            "free {} exceeds total {}",
            u.free_bytes,
            u.total_bytes
        );
        assert!(u.free_pct() <= 100);
    }

    #[test]
    fn a_path_that_does_not_exist_is_unknown_not_full() {
        // The distinction the gate depends on: no answer must never be
        // mistaken for "no space".
        assert_eq!(sample(Path::new("/definitely/not/here/at/all")), None);
    }

    #[test]
    fn free_pct_handles_the_degenerate_total() {
        let u = Usage {
            free_bytes: 0,
            total_bytes: 0,
        };
        assert_eq!(u.free_pct(), 100, "an unmeasurable filesystem is not full");
        let half = Usage {
            free_bytes: 50,
            total_bytes: 100,
        };
        assert_eq!(half.free_pct(), 50);
    }
}
