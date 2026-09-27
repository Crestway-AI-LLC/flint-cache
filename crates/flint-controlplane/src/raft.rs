// SPDX-License-Identifier: Elastic-2.0
//! openraft HA: the control plane's durable intent (tenant registry, proxy
//! fleet, group topology) replicated by Raft across a 3-node (→5 GA) quorum,
//! removing the single-node SPOF (design.md §2.2).
//!
//! Boundary (ADR-0004): this Raft holds ONLY coarse, low-churn intent that
//! cannot be re-derived by observing data nodes — never per-group failover
//! or slot→pair state, which stay observable in the data-node manifests.
//!
//! Storage: the state is tiny and low-churn, so the whole store is
//! serialized and atomically replaced (temp+fsync+rename) on every mutation
//! — no append-only log-file machinery. Implemented as the single v1
//! `RaftStorage` trait wrapped by `Adaptor`; the applied data is
//! `RegistryState`, so the admin mutation logic is unchanged and Raft just
//! replicates it.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Cursor, Write};
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::storage::{LogState, RaftStorage, Snapshot};
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend,
    RaftLogReader, RaftSnapshotBuilder, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::registry::{Mutation, RegistryState};

pub type NodeId = u64;
pub type Request = Mutation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub version: u64,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
);

type SErr = StorageError<NodeId>;

fn io(verb: ErrorVerb, e: impl std::error::Error + 'static) -> SErr {
    StorageIOError::new(ErrorSubject::Store, verb, AnyError::new(&e)).into()
}

fn atomic_write(path: &PathBuf, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

#[derive(Default, Serialize, Deserialize)]
struct Persisted {
    vote: Option<Vote<NodeId>>,
    last_purged: Option<LogId<NodeId>>,
    #[serde(default)]
    log: BTreeMap<u64, Entry<TypeConfig>>,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    registry: RegistryState,
    snapshot: Option<StoredSnapshot>,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

/// The whole control-plane store: one durable file, one lock.
pub struct Store {
    path: PathBuf,
    inner: Mutex<Persisted>,
}

impl Store {
    /// Open the store at `path`. No file is a new node. A file that cannot
    /// be read or will not parse stops the node (BUG-0191): it used to load
    /// as no vote, no log and no registry, and the node ran on as though it
    /// were new -- which is what a truncated write, or a record from a newer
    /// release read after a rollback, turned a real control plane into. The
    /// single-node loader (`State::load_or_new`) refuses for the same reason.
    pub fn open(path: PathBuf) -> Arc<Self> {
        let inner: Persisted = match std::fs::read(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Persisted::default(),
            Err(e) => panic!(
                "flint-controlplane: cannot read the Raft store {}: {e}\n  \
                 Refusing to start rather than run as a new, EMPTY node.",
                path.display()
            ),
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(p) => p,
                Err(e) => panic!(
                    "flint-controlplane: the Raft store {} will not parse: {e}\n  \
                     Refusing to start rather than run as a new, EMPTY node, which is \
                     what this used to do. A file a newer release wrote (a rollback) \
                     or a damaged one both land here. Restore it from a backup, or run \
                     the release that wrote it. Do not simply start this node on an \
                     empty store: a Raft node that forgets its vote can vote twice.",
                    path.display()
                ),
            },
        };
        Arc::new(Self {
            path,
            inner: Mutex::new(inner),
        })
    }

    /// Read-only copy of the applied registry (for serving snapshots to
    /// proxies and CPINFO on the leader).
    pub async fn registry(&self) -> RegistryState {
        self.inner.lock().await.registry.clone()
    }

    // `SErr` is openraft's `StorageError<NodeId>`, which every `RaftStorage`
    // method must return; all seven callers of this helper are in that impl.
    // Boxing here buys an allocation on the write path and unboxing at each
    // call site, and the large error crosses the trait boundary regardless.
    #[allow(clippy::result_large_err)]
    async fn flush(&self, p: &Persisted) -> Result<(), SErr> {
        let bytes = serde_json::to_vec(p).map_err(|e| io(ErrorVerb::Write, e))?;
        atomic_write(&self.path, &bytes).map_err(|e| io(ErrorVerb::Write, e))
    }
}

impl RaftLogReader<TypeConfig> for Arc<Store> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, SErr> {
        let p = self.inner.lock().await;
        Ok(p.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<Store> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, SErr> {
        let mut p = self.inner.lock().await;
        let data = serde_json::to_vec(&p.registry).map_err(|e| io(ErrorVerb::Read, e))?;
        let meta = SnapshotMeta {
            last_log_id: p.last_applied,
            last_membership: p.last_membership.clone(),
            snapshot_id: format!("{}", p.last_applied.map(|l| l.index).unwrap_or(0)),
        };
        p.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
        self.flush(&p).await?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStorage<TypeConfig> for Arc<Store> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, SErr> {
        let p = self.inner.lock().await;
        let last = p
            .log
            .iter()
            .next_back()
            .map(|(_, e)| e.log_id)
            .or(p.last_purged);
        Ok(LogState {
            last_purged_log_id: p.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), SErr> {
        let mut p = self.inner.lock().await;
        p.vote = Some(*vote);
        self.flush(&p).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, SErr> {
        Ok(self.inner.lock().await.vote)
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), SErr>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut p = self.inner.lock().await;
        for e in entries {
            p.log.insert(e.log_id.index, e);
        }
        self.flush(&p).await
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<NodeId>) -> Result<(), SErr> {
        let mut p = self.inner.lock().await;
        let _ = p.log.split_off(&log_id.index);
        self.flush(&p).await
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), SErr> {
        let mut p = self.inner.lock().await;
        p.last_purged = Some(log_id);
        p.log = p.log.split_off(&(log_id.index + 1));
        self.flush(&p).await
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), SErr> {
        let p = self.inner.lock().await;
        Ok((p.last_applied, p.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<Response>, SErr> {
        let mut p = self.inner.lock().await;
        let mut replies = Vec::with_capacity(entries.len());
        for entry in entries {
            p.last_applied = Some(entry.log_id);
            match &entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(mutation) => p.registry.apply(mutation.clone()),
                EntryPayload::Membership(m) => {
                    p.last_membership = StoredMembership::new(Some(entry.log_id), m.clone());
                }
            }
            replies.push(Response {
                version: p.registry.version,
            });
        }
        self.flush(&p).await?;
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, SErr> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), SErr> {
        let bytes = snapshot.into_inner();
        let registry: RegistryState =
            serde_json::from_slice(&bytes).map_err(|e| io(ErrorVerb::Read, e))?;
        let mut p = self.inner.lock().await;
        p.registry = registry;
        p.last_applied = meta.last_log_id;
        p.last_membership = meta.last_membership.clone();
        p.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        });
        self.flush(&p).await
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, SErr> {
        Ok(self.inner.lock().await.snapshot.clone().map(|s| Snapshot {
            meta: s.meta,
            snapshot: Box::new(Cursor::new(s.data)),
        }))
    }
}

#[cfg(test)]
mod open_tests {
    use super::*;

    /// A fresh directory per test, removed when dropped.
    struct Dir(PathBuf);
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn dir(tag: &str) -> Dir {
        let p = std::env::temp_dir().join(format!(
            "flint-raft-open-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).expect("temp dir");
        Dir(p)
    }

    fn version(store: &Store) -> u64 {
        store.inner.try_lock().expect("unlocked").registry.version
    }

    /// No file is a new node, and a file this build wrote loads as written.
    #[test]
    fn a_missing_store_is_a_new_node_and_a_written_one_loads() {
        let d = dir("roundtrip");
        let path = d.0.join("state");
        assert_eq!(
            version(&Store::open(path.clone())),
            0,
            "a missing file is a new node"
        );
        let mut p = Persisted::default();
        p.registry.version = 7;
        std::fs::write(&path, serde_json::to_vec(&p).expect("encode")).expect("write");
        assert_eq!(version(&Store::open(path)), 7);
    }

    /// BUG-0191: a store file that will not parse -- a truncated write, or
    /// one holding a record from a newer release after a rollback -- loaded
    /// as NO vote, NO log and NO registry, and the node ran on as though it
    /// were new. The single-node loader refuses to start on the same
    /// condition (`State::load_or_new`); so does this now.
    #[test]
    fn a_store_that_will_not_parse_stops_the_node_rather_than_loading_empty() {
        let d = dir("damaged");
        let path = d.0.join("state");
        let mut p = Persisted::default();
        p.registry.version = 7;
        let bytes = serde_json::to_vec(&p).expect("encode");
        for damaged in [
            &bytes[..bytes.len() / 2],
            b"{\"vote\":\"UnknownFromANewerRelease\"}",
        ] {
            std::fs::write(&path, damaged).expect("write");
            let opened = std::panic::catch_unwind(|| Store::open(path.clone()));
            assert!(
                opened.is_err(),
                "a store that will not parse loaded (as registry version {}) instead of stopping",
                opened.map(|s| version(&s)).unwrap_or_default()
            );
        }
    }
}
