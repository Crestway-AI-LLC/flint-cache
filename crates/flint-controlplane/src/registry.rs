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
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Tenant {
    pub name: String,
    pub token: String,
    pub ns: String,
    pub subset: Vec<String>,
    /// Previous token during a rotation; both it and `token` auth to `ns`
    /// until dropped. None outside a rotation window.
    #[serde(default)]
    pub prev_token: Option<String>,
}

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
        }
    }

    /// The snapshot a given proxy should see: shared pair topology + ONLY
    /// the tenants whose subset includes it (the sub-group boundary).
    pub fn snapshot_for(&self, proxy: &str) -> (u64, String, String) {
        let pairs = self
            .pairs
            .iter()
            .enumerate()
            .map(|(i, p)| match self.ranges.get(i).copied().flatten() {
                Some((a, b)) => format!("{}|{a}-{b}", p.join(",")),
                None => p.join(","),
            })
            .collect::<Vec<_>>()
            .join(";");
        let tenants = self
            .tenants
            .values()
            .filter(|t| t.subset.iter().any(|s| s == proxy))
            .flat_map(|t| {
                let mut v = vec![format!("{}={}", t.token, t.ns)];
                if let Some(p) = &t.prev_token {
                    v.push(format!("{}={}", p, t.ns));
                }
                v
            })
            .collect::<Vec<_>>()
            .join(",");
        (self.version, pairs, tenants)
    }
}
