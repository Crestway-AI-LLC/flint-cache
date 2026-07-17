//! The Raft-replicated registry state + its mutations (state-machine data).
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Mutation {
    AddProxy(String),
    AddPair {
        nodes: Vec<String>,
        /// Slot range (level-1 routing state); None = unranged/expansion.
        range: Option<(u16, u16)>,
    },
    /// Replace pair `idx`'s membership (node swap: a replacement node takes
    /// a dead member's seat; slot ranges are positional, so the pair id is
    /// the stable identity and membership floats).
    SetPair {
        idx: usize,
        nodes: Vec<String>,
    },
    AddTenant {
        name: String,
        token: String,
        ns: String,
        subset: Vec<String>,
    },
    SetSubset {
        name: String,
        subset: Vec<String>,
    },
    /// Dual-version rotation: current token becomes previous, `new` becomes
    /// current. Both authenticate until DropPrev — zero-downtime rotation.
    RotateToken {
        name: String,
        new: String,
    },
    /// Retire the previous token once it has drained.
    DropPrev {
        name: String,
    },
    /// Set a tenant's replica-read opt-in (ADR-0005 D7).
    SetReplicaReads {
        name: String,
        on: bool,
    },
    /// Set a tenant's proxy near-cache opt-in (ADR-0005 D6). Stale reads
    /// within the proxy cache TTL are allowed for this tenant.
    SetLocalCache {
        name: String,
        on: bool,
    },
    /// Set a tenant's quotas (M5): fleet ops/s and storage bytes; 0 =
    /// unlimited. Lowering max_bytes does NOT flip over_quota by itself —
    /// the metering loop owns that verdict.
    SetQuota {
        name: String,
        ops_per_sec: u64,
        max_bytes: u64,
    },
    /// The metering loop's storage verdict (M5): pushed to proxies as the
    /// 'q' flag; writes shed with -QUOTA while set.
    SetOverQuota {
        name: String,
        on: bool,
    },
}

pub use crate::tenant::Tenant;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegistryState {
    pub version: u64,
    pub proxies: Vec<String>,
    pub pairs: Vec<Vec<String>>,
    /// Slot range owned by pairs[i] (level-1 routing state); None =
    /// unranged (legacy) — proxies fall back to count-derived ranges.
    #[serde(default)]
    pub ranges: Vec<Option<(u16, u16)>>,
    pub tenants: BTreeMap<String, Tenant>,
}

/// FNV-1a seed for deterministic subset placement (not a security
/// boundary; tokens are). Shared with the single-node path.
fn fnv1a(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Deterministic shuffle-shard: k distinct proxies for `name` from a
/// sorted `fleet`, so any node computes the same subset without
/// coordination (design.md §2.1).
pub fn shuffle_shard(name: &str, fleet: &[String], k: usize) -> Vec<String> {
    let mut sorted: Vec<&String> = fleet.iter().collect();
    sorted.sort();
    let k = k.min(sorted.len());
    let mut picked = Vec::with_capacity(k);
    let mut seed = fnv1a(name.as_bytes());
    while picked.len() < k && !sorted.is_empty() {
        seed = seed
            .wrapping_add(0x9E3779B97F4A7C15)
            .wrapping_mul(0xBF58476D1CE4E5B9);
        let idx = (seed >> 33) as usize % sorted.len();
        picked.push(sorted.remove(idx).clone());
    }
    picked.sort();
    picked
}

impl RegistryState {
    pub fn apply(&mut self, m: Mutation) {
        self.version += 1;
        match m {
            Mutation::AddProxy(a) => {
                if !self.proxies.contains(&a) {
                    self.proxies.push(a);
                }
            }
            Mutation::AddPair { nodes, range } => {
                if !self.pairs.contains(&nodes) {
                    self.pairs.push(nodes);
                    while self.ranges.len() < self.pairs.len() - 1 {
                        self.ranges.push(None);
                    }
                    self.ranges.push(range);
                }
            }
            Mutation::SetPair { idx, nodes } => {
                if let Some(p) = self.pairs.get_mut(idx) {
                    *p = nodes;
                }
            }
            Mutation::AddTenant {
                name,
                token,
                ns,
                subset,
            } => {
                self.tenants.insert(
                    name.clone(),
                    Tenant {
                        name,
                        token,
                        ns,
                        subset,
                        prev_token: None,
                        replica_reads: false,
                        local_cache: false,
                        ops_per_sec: 0,
                        max_bytes: 0,
                        over_quota: false,
                    },
                );
            }
            Mutation::SetSubset { name, subset } => {
                if let Some(t) = self.tenants.get_mut(&name) {
                    t.subset = subset;
                }
            }
            Mutation::RotateToken { name, new } => {
                if let Some(t) = self.tenants.get_mut(&name) {
                    t.prev_token = Some(std::mem::replace(&mut t.token, new));
                }
            }
            Mutation::DropPrev { name } => {
                if let Some(t) = self.tenants.get_mut(&name) {
                    t.prev_token = None;
                }
            }
            Mutation::SetReplicaReads { name, on } => {
                if let Some(t) = self.tenants.get_mut(&name) {
                    t.replica_reads = on;
                }
            }
            Mutation::SetLocalCache { name, on } => {
                if let Some(t) = self.tenants.get_mut(&name) {
                    t.local_cache = on;
                }
            }
            Mutation::SetQuota {
                name,
                ops_per_sec,
                max_bytes,
            } => {
                if let Some(t) = self.tenants.get_mut(&name) {
                    t.ops_per_sec = ops_per_sec;
                    t.max_bytes = max_bytes;
                }
            }
            Mutation::SetOverQuota { name, on } => {
                if let Some(t) = self.tenants.get_mut(&name) {
                    t.over_quota = on;
                }
            }
        }
    }

    /// The snapshot a given proxy should see: shared pair topology + ONLY
    /// the tenants whose subset includes it (the sub-group boundary).
    pub fn snapshot_for(&self, proxy: &str) -> (u64, String, String) {
        let (pairs, tenants) =
            crate::tenant::snapshot_for(&self.pairs, &self.ranges, self.tenants.values(), proxy);
        (self.version, pairs, tenants)
    }
}
