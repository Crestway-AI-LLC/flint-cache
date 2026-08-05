// SPDX-License-Identifier: Elastic-2.0
//! Where a backup set's bytes live.
//!
//! One trait, so the format never learns which store it is on. `LocalDir`
//! ships here and is not a toy: it is the on-prem evaluator's store, the
//! drill's store, and the staging area a restore streams from when the
//! object store is the thing that is unreachable. The S3-compatible
//! implementation is a second impl of this trait in the seat, where its SDK
//! and its credentials stay contained (ADR-0011 D8).
//!
//! The trait is deliberately small and deliberately STREAMING. A backup set
//! is a handful of small text objects and a long tail of SSTs whose size is
//! bounded only by someone's compaction settings, so an interface that hands
//! back `Vec<u8>` would work in every test and fail on the first real
//! cluster. `read`/`write` exist only for the manifest, and say so.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

pub trait ObjectStore {
    /// Stream `key` out. The reader is the only way to touch a large
    /// object, and verification uses it so checksumming never buffers.
    fn open(&self, key: &str) -> io::Result<Box<dyn Read>>;

    /// Upload the file at `from` under `key`.
    fn put_file(&self, key: &str, from: &Path) -> io::Result<()>;

    /// Every key under `prefix`, in sorted order, relative to the store
    /// root. Sorted because an unsorted listing makes a diff between what
    /// the manifest claims and what the store holds depend on the store's
    /// iteration order.
    fn list(&self, prefix: &str) -> io::Result<Vec<String>>;

    /// Small whole objects: the manifest, and nothing else. Anything that
    /// can grow with the dataset must use `open`/`put_file`.
    fn read(&self, key: &str) -> io::Result<Vec<u8>>;
    fn write(&self, key: &str, bytes: &[u8]) -> io::Result<()>;

    /// Remove one object. Absent is SUCCESS: retention pruning retries
    /// after partial failures, and a delete that errors on already-gone
    /// turns every retry into a false alarm.
    fn delete(&self, key: &str) -> io::Result<()>;
}

/// A backup set in a directory. Also the shape every other store is
/// described against: keys are `/`-joined, never absolute, never `..`.
pub struct LocalDir {
    root: PathBuf,
}

impl LocalDir {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Resolve `key` under the root, refusing anything that could escape it.
    ///
    /// A key comes out of a MANIFEST, and a manifest comes out of the object
    /// store — which during a restore is the least trusted input in the
    /// system, since restoring is what you do after something has gone
    /// wrong. `../../etc/whatever` in an object line must be a refusal, not
    /// a write.
    fn path(&self, key: &str) -> io::Result<PathBuf> {
        if key.is_empty()
            || key.starts_with('/')
            || key
                .split('/')
                .any(|c| c == ".." || c == "." || c.is_empty())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsafe object key {key:?}"),
            ));
        }
        Ok(self.root.join(key))
    }
}

impl ObjectStore for LocalDir {
    fn open(&self, key: &str) -> io::Result<Box<dyn Read>> {
        Ok(Box::new(std::fs::File::open(self.path(key)?)?))
    }

    fn put_file(&self, key: &str, from: &Path) -> io::Result<()> {
        let dest = self.path(key)?;
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Write-then-rename, the same discipline every manifest in this
        // project uses: a crash mid-copy leaves no half object that a later
        // verify would have to distinguish from a corrupt one.
        let tmp = dest.with_extension("part");
        std::fs::copy(from, &tmp)?;
        std::fs::rename(&tmp, &dest)
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let mut out = Vec::new();
        walk(&self.root, &self.root, &mut out)?;
        out.retain(|k| k.starts_with(prefix));
        out.sort();
        Ok(out)
    }

    fn read(&self, key: &str) -> io::Result<Vec<u8>> {
        std::fs::read(self.path(key)?)
    }

    fn write(&self, key: &str, bytes: &[u8]) -> io::Result<()> {
        let dest = self.path(key)?;
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = dest.with_extension("part");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(bytes)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &dest)
    }

    fn delete(&self, key: &str) -> io::Result<()> {
        let path = self.path(key)?;
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        }
        // Sweep now-empty parents up to the root: an object store has no
        // directories, so a LocalDir that leaves husks behind makes the
        // two stores disagree about what a pruned set looks like — and
        // leaks one empty dir per retention cycle forever (#123's shape).
        let mut dir = path.parent().map(|p| p.to_path_buf());
        while let Some(d) = dir {
            if d == self.root || std::fs::remove_dir(&d).is_err() {
                break; // non-empty or root: stop, both are fine
            }
            dir = d.parent().map(|p| p.to_path_buf());
        }
        Ok(())
    }
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) -> io::Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        // An absent root lists empty rather than erroring: "no backups yet"
        // and "the bucket is unreachable" must not look the same, and only
        // the second is an error.
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            walk(root, &path, out)?;
        } else if let Ok(rel) = path.strip_prefix(root) {
            out.push(rel.to_string_lossy().replace('\\', "/"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Tmp(PathBuf);
    impl Tmp {
        fn new(tag: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let p = std::env::temp_dir().join(format!(
                "flint-backup-store-{tag}-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).expect("temp dir");
            Self(p)
        }
    }
    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn keys_that_could_escape_the_root_are_refused() {
        let t = Tmp::new("escape");
        let s = LocalDir::new(&t.0);
        for bad in ["../outside", "a/../../outside", "/etc/passwd", "", "a//b"] {
            assert!(
                s.write(bad, b"x").is_err(),
                "{bad:?} should be refused — a manifest is untrusted input"
            );
        }
        // The negative control: an ordinary nested key still works, so the
        // check is rejecting traversal rather than rejecting everything.
        s.write("pairs/0/000009.sst", b"x").expect("normal key");
    }

    #[test]
    fn listing_is_sorted_recursive_and_empty_when_absent() {
        let t = Tmp::new("list");
        let s = LocalDir::new(t.0.join("never-created"));
        assert!(s.list("").expect("absent root lists empty").is_empty());

        let s = LocalDir::new(&t.0);
        for k in ["pairs/1/b", "pairs/0/a", "manifest"] {
            s.write(k, b"x").expect("write");
        }
        assert_eq!(
            s.list("").expect("list"),
            ["manifest", "pairs/0/a", "pairs/1/b"]
        );
        assert_eq!(s.list("pairs/0").expect("list"), ["pairs/0/a"]);
    }

    #[test]
    fn a_put_file_is_readable_back_byte_for_byte() {
        let t = Tmp::new("put");
        let src = t.0.join("src.bin");
        std::fs::write(&src, b"payload").expect("src");
        let s = LocalDir::new(t.0.join("store"));
        s.put_file("pairs/0/x.sst", &src).expect("put");
        let mut got = Vec::new();
        s.open("pairs/0/x.sst")
            .expect("open")
            .read_to_end(&mut got)
            .expect("read");
        assert_eq!(got, b"payload");
        // No `.part` left behind — a leftover would be listed as an object
        // the manifest never mentions, which verify reports as tampering.
        assert_eq!(s.list("").expect("list"), ["pairs/0/x.sst"]);
    }
}
