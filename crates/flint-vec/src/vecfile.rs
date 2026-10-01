// SPDX-License-Identifier: Elastic-2.0
//! The local file of full vectors a quantized set re-ranks from (ADR-0049 D2).
//!
//! Fixed-size records of `dim` little-endian float32s, record `i` holding the
//! vector of graph slot `i`, so a read is one positioned read of `dim × 4`
//! bytes and a reused slot (BUG-0197) is overwritten in place. The file is
//! DERIVED, like the graph: the binary deletes every one at startup, and the
//! cold-start rebuild writes them again from the durable rows. Nothing in it is
//! ever the only copy, so nothing here is fsynced.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

/// The extension of a vector file, which the binary sweeps from `--vec-dir`
/// at startup.
pub const EXTENSION: &str = "vecs";

pub struct VecFile {
    file: File,
    path: PathBuf,
    dim: usize,
}

impl VecFile {
    /// Create (or truncate) the file at `path` for vectors of `dim`.
    pub fn create(path: &Path, dim: usize) -> io::Result<VecFile> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(VecFile {
            file,
            path: path.to_path_buf(),
            dim,
        })
    }

    /// A file that cannot be written: what a failed disk looks like to
    /// [`VecFile::put`], for tests of the fallback.
    #[cfg(test)]
    pub fn read_only(path: &Path, dim: usize) -> io::Result<VecFile> {
        File::create(path)?;
        Ok(VecFile {
            file: File::open(path)?,
            path: path.to_path_buf(),
            dim,
        })
    }

    fn offset(&self, slot: u32) -> u64 {
        slot as u64 * (self.dim as u64 * 4)
    }

    /// Write slot `slot`'s vector, appending or overwriting.
    pub fn put(&self, slot: u32, v: &[f32]) -> io::Result<()> {
        debug_assert_eq!(v.len(), self.dim);
        let mut buf = Vec::with_capacity(v.len() * 4);
        for x in v {
            buf.extend_from_slice(&x.to_le_bytes());
        }
        self.file.write_all_at(&buf, self.offset(slot))
    }

    /// Read several slots. Each is hinted to the kernel first, so on a cold
    /// file the device serves them together rather than one round trip after
    /// another: the re-rank's reads, "issued in parallel" (ADR-0049 D2).
    ///
    /// Measured by `reads_cold_and_warm`, 40 random records per query, on
    /// macOS: cold, 3,406 µs one after another against 636 µs hinted on the
    /// internal SSD, and 16,248 against 2,704 on a USB one; warm, 25 against
    /// 40. On Linux (a c7i's EBS root volume) `posix_fadvise` halved it: 18,397
    /// cold against 8,299, and 26 against 30 warm. An earlier probe on macOS
    /// tried eight threads instead: about 650 and 2,600 cold, and 75 warm, the
    /// threads' own start-up cost.
    pub fn get_many(&self, slots: &[u32]) -> io::Result<Vec<Vec<f32>>> {
        if slots.len() > 1 {
            for &s in slots {
                self.prefetch(s);
            }
        }
        slots.iter().map(|&s| self.get(s)).collect()
    }

    /// Ask the kernel to start reading `slot` into the page cache, without
    /// waiting. Only a hint: a kernel that ignores it costs a syscall.
    fn prefetch(&self, slot: u32) {
        let (off, len) = (self.offset(slot), self.dim * 4);
        #[cfg(target_os = "linux")]
        // SAFETY: posix_fadvise reads only its integer arguments.
        unsafe {
            libc::posix_fadvise(
                self.file.as_raw_fd(),
                off as libc::off_t,
                len as libc::off_t,
                libc::POSIX_FADV_WILLNEED,
            );
        }
        #[cfg(target_os = "macos")]
        // SAFETY: F_RDADVISE reads the radvisory it is passed, which lives
        // across the call.
        unsafe {
            let ra = libc::radvisory {
                ra_offset: off as libc::off_t,
                ra_count: len as libc::c_int,
            };
            libc::fcntl(self.file.as_raw_fd(), libc::F_RDADVISE, &ra);
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let _ = (off, len);
    }

    /// Read slot `slot`'s vector.
    pub fn get(&self, slot: u32) -> io::Result<Vec<f32>> {
        let mut buf = vec![0u8; self.dim * 4];
        self.file.read_exact_at(&mut buf, self.offset(slot))?;
        let (words, _) = buf.as_chunks::<4>();
        Ok(words.iter().map(|w| f32::from_le_bytes(*w)).collect())
    }

    /// Bytes on disk, for `VEC.INFO`.
    pub fn len_bytes(&self) -> u64 {
        self.file.metadata().map(|m| m.len()).unwrap_or(0)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// A set's file goes when the set does, which today is only in tests; the
/// binary's startup sweep covers a process that dies holding one.
impl Drop for VecFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drop `f`'s pages from the page cache, so the next reads go to the
    /// device. Linux: write back, then `POSIX_FADV_DONTNEED`. macOS:
    /// `msync(MS_INVALIDATE)` over a mapping of it.
    fn evict(f: &VecFile, len: usize) {
        f.file.sync_data().expect("sync");
        #[cfg(target_os = "linux")]
        // SAFETY: posix_fadvise reads only its integer arguments.
        unsafe {
            libc::posix_fadvise(f.file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }
        #[cfg(target_os = "macos")]
        // SAFETY: a read-only shared mapping of a file this test owns,
        // unmapped before it returns.
        unsafe {
            let fd = f.file.as_raw_fd();
            let m = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            );
            assert!(m != libc::MAP_FAILED);
            libc::msync(m, len, libc::MS_INVALIDATE);
            libc::munmap(m, len);
        }
        let _ = len;
    }

    #[test]
    fn records_round_trip_and_overwrite_in_place() {
        let d = std::env::temp_dir().join(format!("flint-vecfile-{}", std::process::id()));
        std::fs::create_dir_all(&d).expect("dir");
        let f = VecFile::create(&d.join("0.vecs"), 3).expect("create");
        f.put(0, &[1.0, 2.0, 3.0]).expect("put");
        f.put(2, &[7.0, 8.0, 9.5]).expect("put");
        f.put(0, &[-1.0, 0.5, 1e-7]).expect("put");
        assert_eq!(f.get(0).expect("get"), vec![-1.0, 0.5, 1e-7]);
        assert_eq!(
            f.get_many(&[2, 0]).expect("get_many"),
            vec![vec![7.0, 8.0, 9.5], vec![-1.0, 0.5, 1e-7]]
        );
        assert_eq!(f.len_bytes(), 3 * 3 * 4);
        let path = f.path().to_path_buf();
        drop(f);
        assert!(!path.exists(), "a file goes with its set");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// The measurement behind `get_many`, not a pass/fail: reads of 40 random
    /// records per query from a 100,000-record file, one after another and
    /// hinted, warm and with the file evicted before each query. Run it where
    /// the file will live:
    ///
    /// ```text
    /// VECFILE_DIR=/mnt/nvme cargo test --release -p flint-vec \
    ///     reads_cold_and_warm -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore]
    fn reads_cold_and_warm() {
        let dir = std::env::var("VECFILE_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        let (n, dim) = (100_000u32, 128usize);
        let f = VecFile::create(&dir.join("reads-probe.vecs"), dim).expect("create");
        let v: Vec<f32> = (0..dim).map(|i| i as f32).collect();
        for s in 0..n {
            f.put(s, &v).expect("put");
        }
        let mut x = 777u64;
        let queries: Vec<Vec<u32>> = (0..200)
            .map(|_| {
                (0..40)
                    .map(|_| {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        (x % n as u64) as u32
                    })
                    .collect()
            })
            .collect();
        for cold in [false, true] {
            for hinted in [false, true] {
                let mut spent = std::time::Duration::ZERO;
                for q in &queries {
                    if cold {
                        evict(&f, n as usize * dim * 4);
                    }
                    let t = std::time::Instant::now();
                    if hinted {
                        f.get_many(q).expect("read");
                    } else {
                        for &s in q {
                            f.get(s).expect("read");
                        }
                    }
                    spent += t.elapsed();
                }
                eprintln!(
                    "{} cold={cold} hinted={hinted}: {:.0} us per 40 reads",
                    dir.display(),
                    spent.as_secs_f64() * 1e6 / queries.len() as f64
                );
            }
        }
    }
}
