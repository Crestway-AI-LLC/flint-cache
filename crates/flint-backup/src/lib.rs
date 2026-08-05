// SPDX-License-Identifier: Elastic-2.0
//! The backup ARTIFACT — ADR-0011.
//!
//! What a backup set contains, and whether it is intact. Nothing here talks
//! to a cluster or to an object store's API; the seat does that. This crate
//! exists so the format has exactly one implementation, because a format
//! with two has two.
//!
//! ## The layout
//!
//! ```text
//! <set>/manifest              the index — the only object read whole
//! <set>/cp-state              the control plane's state file
//! <set>/pairs/<n>/<file>      pair n's master checkpoint, flat
//! ```
//!
//! ## Why the manifest is the trust boundary
//!
//! Restore verifies every checksum before it touches anything (D2). That is
//! not belt-and-braces: a backup is read at the worst moment, from storage
//! nobody has looked at since it was written, and a silently truncated SST
//! restores into a cluster that serves wrong answers instead of failing.
//! So the manifest is authoritative in BOTH directions — an object it does
//! not list is as much a defect as one it lists and cannot find. A set that
//! has grown an extra file has been tampered with or written by something
//! that does not understand the format, and either way it is not restorable.

pub mod store;

use std::fmt;
use store::ObjectStore;

/// The manifest's format version, and the whole of D7's mechanism.
///
/// A restoring binary that does not recognise the version REFUSES and names
/// the runbook. A format break cannot be rolled back, so it must not be
/// silently rolled forward by a restore either — the failure mode being
/// avoided is a new binary reading an old set's rows with new meanings and
/// producing a cluster that looks fine.
pub const FORMAT: u32 = 1;

/// The manifest object's key within a set. Fixed, because finding the index
/// must not itself require an index.
pub const MANIFEST_KEY: &str = "manifest";

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// The set was produced by a format this binary does not understand.
    Format {
        found: u32,
        supported: u32,
    },
    /// The manifest itself is unreadable or self-inconsistent.
    Malformed(String),
    /// An object's bytes do not match what the manifest recorded.
    Checksum {
        key: String,
        expected: String,
        found: String,
    },
    /// The manifest lists an object the store does not have.
    Missing {
        key: String,
    },
    /// The store holds an object the manifest does not list.
    Unlisted {
        key: String,
    },
    Io(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Format { found, supported } => write!(
                f,
                "backup format {found} is not readable by this build (supports {supported}); \
                 see docs/runbooks/backup-format-migration.md — a format break is not \
                 rolled forward by a restore"
            ),
            Error::Malformed(what) => write!(f, "malformed manifest: {what}"),
            Error::Checksum {
                key,
                expected,
                found,
            } => write!(
                f,
                "object {key} is corrupt: manifest records sha256 {expected}, store holds {found}"
            ),
            Error::Missing { key } => {
                write!(f, "object {key} is listed in the manifest but absent")
            }
            Error::Unlisted { key } => write!(
                f,
                "object {key} is present but not listed in the manifest — the set has been \
                 written to by something that does not understand the format"
            ),
            Error::Io(e) => write!(f, "{e}"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

/// One pair's contribution, and the facts a restore needs to reason about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairRecord {
    pub index: usize,
    /// The master the checkpoint was cut on — provenance, not a routing
    /// instruction. Restore places by CURRENT slot ownership (D5), never by
    /// the topology recorded here, because slots move.
    pub master: String,
    /// The role epoch observed on that master. Recorded so a restore can say
    /// which lineage the data descends from; the epoch is NOT restored, it
    /// is scrubbed (D4).
    pub epoch: String,
    /// The engine sequence the checkpoint captured — the one number that
    /// orders two backups of the same pair unambiguously when their wall
    /// clocks disagree.
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRecord {
    pub key: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Manifest {
    pub id: String,
    pub started_ms: u64,
    pub finished_ms: u64,
    /// The release that produced the set. Not the same question as `FORMAT`:
    /// the format says whether it can be read, this says what to blame.
    pub release: String,
    /// Which control-plane seat the captured `cp-state` came from. Recorded
    /// because on a multi-seat CP the file is Raft-replicated and any seat's
    /// copy is legitimate — so "which one" is a fact a later investigation
    /// needs and cannot recover.
    pub cp_source: String,
    pub pairs: Vec<PairRecord>,
    pub objects: Vec<ObjectRecord>,
}

impl Manifest {
    /// Line-oriented text, matching every other manifest in this project.
    ///
    /// Not JSON, for one reason that matters: this file is read by a human
    /// during an incident, possibly with `head`, on a box where the tooling
    /// is whatever the AMI shipped.
    pub fn render(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!("format {FORMAT}\n"));
        s.push_str(&format!("id {}\n", self.id));
        s.push_str(&format!("started {}\n", self.started_ms));
        s.push_str(&format!("finished {}\n", self.finished_ms));
        s.push_str(&format!("release {}\n", self.release));
        s.push_str(&format!("cp-source {}\n", self.cp_source));
        for p in &self.pairs {
            s.push_str(&format!(
                "pair {} {} {} {}\n",
                p.index, p.master, p.epoch, p.seq
            ));
        }
        for o in &self.objects {
            s.push_str(&format!("object {} {} {}\n", o.key, o.bytes, o.sha256));
        }
        s
    }

    pub fn parse(raw: &str) -> Result<Self, Error> {
        let mut m = Manifest::default();
        let mut format_seen = None;
        for (n, line) in raw.lines().enumerate() {
            let line = line.trim_end();
            if line.is_empty() {
                continue;
            }
            let bad = |what: &str| Error::Malformed(format!("line {}: {what}", n + 1));
            let mut f = line.split(' ');
            let key = f.next().unwrap_or_default();
            match key {
                "format" => {
                    let v: u32 = f
                        .next()
                        .and_then(|v| v.parse().ok())
                        .ok_or_else(|| bad("format takes a number"))?;
                    // Checked here rather than by the caller: every path into
                    // a manifest goes through parse, so a version gate
                    // anywhere else is a version gate with a way around it.
                    if v != FORMAT {
                        return Err(Error::Format {
                            found: v,
                            supported: FORMAT,
                        });
                    }
                    format_seen = Some(v);
                }
                "id" => m.id = f.next().unwrap_or_default().to_string(),
                "started" => {
                    m.started_ms = f
                        .next()
                        .and_then(|v| v.parse().ok())
                        .ok_or_else(|| bad("started takes a millisecond clock"))?
                }
                "finished" => {
                    m.finished_ms = f
                        .next()
                        .and_then(|v| v.parse().ok())
                        .ok_or_else(|| bad("finished takes a millisecond clock"))?
                }
                "release" => m.release = f.next().unwrap_or_default().to_string(),
                "cp-source" => m.cp_source = f.next().unwrap_or_default().to_string(),
                "pair" => {
                    let (Some(index), Some(master), Some(epoch), Some(seq)) =
                        (f.next(), f.next(), f.next(), f.next())
                    else {
                        return Err(bad("pair takes <index> <master> <epoch> <seq>"));
                    };
                    m.pairs.push(PairRecord {
                        index: index.parse().map_err(|_| bad("pair index"))?,
                        master: master.to_string(),
                        epoch: epoch.to_string(),
                        seq: seq.parse().map_err(|_| bad("pair seq"))?,
                    });
                }
                "object" => {
                    let (Some(key), Some(bytes), Some(sha)) = (f.next(), f.next(), f.next()) else {
                        return Err(bad("object takes <key> <bytes> <sha256>"));
                    };
                    m.objects.push(ObjectRecord {
                        key: key.to_string(),
                        bytes: bytes.parse().map_err(|_| bad("object size"))?,
                        sha256: sha.to_string(),
                    });
                }
                // Fail closed. An unknown key inside a version this binary
                // claims to support means the writer knew something the
                // reader does not, and restoring while ignoring it is how a
                // set gets restored minus the part that mattered.
                other => return Err(bad(&format!("unknown key {other:?}"))),
            }
        }
        if format_seen.is_none() {
            return Err(Error::Malformed(
                "no format line — refusing to guess the version".into(),
            ));
        }
        if m.id.is_empty() {
            return Err(Error::Malformed("no id".into()));
        }
        Ok(m)
    }
}

/// What a verification found. Reported rather than returned as a bare bool
/// because "how much did we check" is itself the finding: a verify that
/// silently examined nothing passes.
#[derive(Debug, PartialEq, Eq)]
pub struct Report {
    pub id: String,
    pub objects: usize,
    pub bytes: u64,
}

/// Re-hash every object in the set and compare against the manifest.
///
/// Both directions (see the module note): a listed object that is absent, a
/// present object that is unlisted, and any checksum mismatch are each a
/// refusal. Streaming throughout — an SST is not read into memory to be
/// hashed.
pub fn verify(store: &dyn ObjectStore, manifest: &Manifest) -> Result<Report, Error> {
    let mut bytes = 0u64;
    for o in &manifest.objects {
        let mut r = match store.open(&o.key) {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::Missing { key: o.key.clone() });
            }
            Err(e) => return Err(e.into()),
        };
        let found = flint_tls::sha256_stream_hex(&mut r)?;
        if found != o.sha256 {
            return Err(Error::Checksum {
                key: o.key.clone(),
                expected: o.sha256.clone(),
                found,
            });
        }
        bytes += o.bytes;
    }
    // The other direction. A set that has grown a file was not written by
    // this format, and "extra data we did not check" is not a state a
    // restore may proceed from.
    let listed: std::collections::HashSet<&str> =
        manifest.objects.iter().map(|o| o.key.as_str()).collect();
    for key in store.list("")? {
        if key != MANIFEST_KEY && !listed.contains(key.as_str()) {
            return Err(Error::Unlisted { key });
        }
    }
    Ok(Report {
        id: manifest.id.clone(),
        objects: manifest.objects.len(),
        bytes,
    })
}

/// Read and verify a set in one step — the only entry point a restore should
/// use, so that "load the manifest" and "check the manifest" cannot be
/// separated by a caller in a hurry.
pub fn load_verified(store: &dyn ObjectStore) -> Result<(Manifest, Report), Error> {
    let raw = store.read(MANIFEST_KEY)?;
    let manifest = Manifest::parse(&String::from_utf8_lossy(&raw))?;
    let report = verify(store, &manifest)?;
    Ok((manifest, report))
}

#[cfg(test)]
mod tests {
    use super::store::{LocalDir, ObjectStore};
    use super::*;

    struct Tmp(std::path::PathBuf);
    impl Tmp {
        fn new(tag: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let p = std::env::temp_dir().join(format!(
                "flint-backup-{tag}-{}-{}",
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

    /// A complete, intact set — and the capability assert every corruption
    /// test below depends on. Without it, "verify refused" proves nothing,
    /// because a verify that refuses everything would pass them all.
    fn good_set(dir: &std::path::Path) -> (LocalDir, Manifest) {
        let store = LocalDir::new(dir);
        let payloads = [
            ("cp-state", b"pairs 1\n".to_vec()),
            ("pairs/0/000009.sst", vec![7u8; 4096]),
            ("pairs/0/CURRENT", b"MANIFEST-000005\n".to_vec()),
        ];
        let mut objects = Vec::new();
        for (key, bytes) in &payloads {
            store.write(key, bytes).expect("write object");
            objects.push(ObjectRecord {
                key: (*key).to_string(),
                bytes: bytes.len() as u64,
                sha256: flint_tls::sha256_hex(bytes),
            });
        }
        let manifest = Manifest {
            id: "backup-1785900000000".into(),
            started_ms: 1785900000000,
            finished_ms: 1785900007000,
            release: "v0.1.0-rc.16".into(),
            cp_source: "10.0.0.1:7500".into(),
            pairs: vec![PairRecord {
                index: 0,
                master: "10.0.0.2:7001".into(),
                epoch: "(1,3)".into(),
                seq: 20002,
            }],
            objects,
        };
        store
            .write(MANIFEST_KEY, manifest.render().as_bytes())
            .expect("write manifest");
        (store, manifest)
    }

    #[test]
    fn an_intact_set_verifies_and_reports_what_it_checked() {
        let t = Tmp::new("intact");
        let (store, manifest) = good_set(&t.0);
        let report = verify(&store, &manifest).expect("intact set must verify");
        assert_eq!(report.objects, 3);
        assert_eq!(report.bytes, 8 + 4096 + 16);
    }

    #[test]
    fn the_manifest_round_trips_through_its_own_text() {
        let t = Tmp::new("roundtrip");
        let (_, manifest) = good_set(&t.0);
        assert_eq!(
            Manifest::parse(&manifest.render()).expect("parse"),
            manifest
        );
    }

    #[test]
    fn one_flipped_byte_is_refused() {
        let t = Tmp::new("flip");
        let (store, manifest) = good_set(&t.0);
        // The corruption that motivates checksums at all: same length, one
        // bit different. A size check alone would pass this.
        let mut sst = std::fs::read(t.0.join("pairs/0/000009.sst")).expect("read");
        sst[2000] ^= 0x01;
        std::fs::write(t.0.join("pairs/0/000009.sst"), &sst).expect("write");
        match verify(&store, &manifest) {
            Err(Error::Checksum { key, .. }) => assert_eq!(key, "pairs/0/000009.sst"),
            other => panic!("a flipped byte must be refused, got {other:?}"),
        }
    }

    #[test]
    fn a_truncated_object_is_refused() {
        let t = Tmp::new("truncate");
        let (store, manifest) = good_set(&t.0);
        std::fs::write(t.0.join("pairs/0/000009.sst"), vec![7u8; 4000]).expect("truncate");
        assert!(matches!(
            verify(&store, &manifest),
            Err(Error::Checksum { .. })
        ));
    }

    #[test]
    fn a_missing_object_is_refused() {
        let t = Tmp::new("missing");
        let (store, manifest) = good_set(&t.0);
        std::fs::remove_file(t.0.join("pairs/0/CURRENT")).expect("remove");
        match verify(&store, &manifest) {
            Err(Error::Missing { key }) => assert_eq!(key, "pairs/0/CURRENT"),
            other => panic!("a missing object must be refused, got {other:?}"),
        }
    }

    #[test]
    fn an_object_the_manifest_never_listed_is_refused() {
        let t = Tmp::new("unlisted");
        let (store, manifest) = good_set(&t.0);
        // The direction a checksum loop alone cannot see: every listed
        // object is perfect, and the set still must not be restored.
        store
            .write("pairs/0/000011.sst", b"from somewhere else")
            .expect("write");
        match verify(&store, &manifest) {
            Err(Error::Unlisted { key }) => assert_eq!(key, "pairs/0/000011.sst"),
            other => panic!("an unlisted object must be refused, got {other:?}"),
        }
    }

    #[test]
    fn a_future_format_refuses_and_names_the_runbook() {
        let raw = format!("format {}\nid x\n", FORMAT + 1);
        match Manifest::parse(&raw) {
            Err(e @ Error::Format { .. }) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("runbook") || msg.contains("migration"),
                    "{msg}"
                );
            }
            other => panic!("a future format must refuse, got {other:?}"),
        }
    }

    #[test]
    fn a_manifest_key_this_build_does_not_know_is_refused() {
        // Fail closed within a supported version: the writer knew something
        // the reader does not, and ignoring it restores the set minus
        // whatever that was.
        let raw = format!("format {FORMAT}\nid x\nencryption aes256\n");
        assert!(matches!(Manifest::parse(&raw), Err(Error::Malformed(_))));
    }

    #[test]
    fn a_manifest_without_a_format_line_is_refused() {
        assert!(matches!(
            Manifest::parse("id x\nstarted 1\n"),
            Err(Error::Malformed(_))
        ));
    }

    #[test]
    fn load_verified_is_the_only_door_and_it_checks() {
        let t = Tmp::new("door");
        let (store, _) = good_set(&t.0);
        let (m, r) = load_verified(&store).expect("intact");
        assert_eq!(m.pairs[0].seq, 20002);
        assert_eq!(r.objects, 3);

        let mut sst = std::fs::read(t.0.join("pairs/0/000009.sst")).expect("read");
        sst[10] ^= 0xff;
        std::fs::write(t.0.join("pairs/0/000009.sst"), &sst).expect("write");
        assert!(matches!(load_verified(&store), Err(Error::Checksum { .. })));
    }
}
