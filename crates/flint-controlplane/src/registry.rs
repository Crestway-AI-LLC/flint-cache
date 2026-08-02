// SPDX-License-Identifier: Elastic-2.0
//! The Raft-replicated registry state + its mutations (state-machine data).
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Mutation {
    /// The controller promoted `addr`. Carries no routing authority: it
    /// bumps a generation so the next snapshot wakes every watching proxy
    /// and tells it which pair to re-probe. See tenant::promote_hint.
    Promoted {
        addr: String,
    },
    AddProxy(String),
    /// Retire a proxy registration.
    ///
    /// Registrations were append-only, so a proxy that changed identity —
    /// bind address one bootstrap, advertise name the next — left BOTH rows
    /// standing, and nothing distinguished the live one. Tenant placement
    /// shuffle-shards across this list, so a new tenant could be assigned to
    /// a name no running proxy answers to and get -WRONGPASS from the edge
    /// with a correct token. Observed on the playground.
    DelProxy(String),
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
    /// Remove a tenant: the record (auth revoked on the next push) and its
    /// namespace's slot-map exception rows. Data wipe is the caller's
    /// follow-up (`flintctl tenant remove`) — the CP holds no data.
    DelTenant {
        name: String,
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
    /// Set a tenant's federation flag (ADR-0007, plumbing only today).
    SetFederated {
        name: String,
        on: bool,
    },
    /// Set a tenant's async write-queue opt-in (ADR-0005 D4).
    SetAsyncWrites {
        name: String,
        on: bool,
    },
    /// Record slot-ownership truth at cutover (Option B).
    SetSlotOwner {
        ns: String,
        slot: u16,
        pair: u16,
    },
    /// Retire an exception row (a move back; interior hits split).
    ClearSlotOwner {
        ns: String,
        slot: u16,
    },
    /// The consolidation sweep: merge adjacent runs, drop rows redundant
    /// against the default ranges. Deterministic (pure function of state),
    /// so it is safe as a replicated mutation.
    ConsolidateSlots,
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
    /// Set the fleet admin token pair directly (ADR-0006 D4). The CP command
    /// layer computes current/previous; the mutation just stores what it is
    /// told, so the Raft log is the single source of truth.
    SetAdmin {
        token: Option<String>,
        prev: Option<String>,
    },
}

pub use crate::tenant::Tenant;

/// Accept both the run form (4-tuple) and the legacy single form
/// (3-tuple, widened to a run) when loading Raft snapshots.
fn runs_compat<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Vec<crate::tenant::SlotRun>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Row {
        Run((String, u16, u16, u16)),
        Single((String, u16, u16)),
    }
    let rows: Vec<Row> = Vec::deserialize(d)?;
    Ok(rows
        .into_iter()
        .map(|r| match r {
            Row::Run(t) => t,
            Row::Single((ns, slot, pair)) => (ns, slot, slot, pair),
        })
        .collect())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegistryState {
    /// Slot-ownership exception RUNS (Option B + consolidation):
    /// (ns, lo, hi, pair_idx). Serde: pre-Option-B snapshots load with
    /// none; pre-consolidation 3-tuple singles load as width-1 runs.
    #[serde(default, deserialize_with = "runs_compat")]
    pub exceptions: Vec<crate::tenant::SlotRun>,
    pub version: u64,
    pub proxies: Vec<String>,
    pub pairs: Vec<Vec<String>>,
    /// Slot range owned by pairs[i] (level-1 routing state); None =
    /// unranged (legacy) — proxies fall back to count-derived ranges.
    #[serde(default)]
    pub ranges: Vec<Option<(u16, u16)>>,
    pub tenants: BTreeMap<String, Tenant>,
    /// Fleet admin (operator) token, current + previous (ADR-0006 D4).
    /// Plaintext (the agent retrieves it; proxies get only the digest).
    /// Rafted like every other registry fact.
    #[serde(default)]
    pub admin_token: Option<String>,
    #[serde(default)]
    pub admin_prev: Option<String>,
    /// Last promotion reported by the controller: (addr, generation).
    /// Deliberately NOT persisted across a CP restart — it is a live wakeup,
    /// not a durable fact (tenant::promote_hint explains why that is safe).
    #[serde(skip)]
    pub promoted: Option<(String, u64)>,
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
            Mutation::Promoted { addr } => {
                // Monotonic per CP process. Compared for INEQUALITY by the
                // proxy, never for ordering, so a restart resetting it to 1
                // costs one extra probe rather than going permanently quiet.
                let next = self.promoted.as_ref().map_or(1, |(_, g)| g + 1);
                self.promoted = Some((addr, next));
            }
            Mutation::AddProxy(a) => {
                if !self.proxies.contains(&a) {
                    self.proxies.push(a);
                }
            }
            Mutation::DelProxy(a) => {
                self.proxies.retain(|p| p != &a);
                // A retired proxy must not linger in any tenant's subset:
                // leaving it there is the same trap one level down, and the
                // tenant would keep a placement slot pointing at nothing.
                for t in self.tenants.values_mut() {
                    t.subset.retain(|p| p != &a);
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
                        federated: false,
                        async_writes: false,
                        ops_per_sec: 0,
                        max_bytes: 0,
                        over_quota: false,
                    },
                );
            }
            Mutation::DelTenant { name } => {
                if let Some(t) = self.tenants.remove(&name) {
                    let ns = t.ns;
                    self.exceptions.retain(|(e_ns, _, _, _)| e_ns != &ns);
                }
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
            Mutation::SetFederated { name, on } => {
                if let Some(t) = self.tenants.get_mut(&name) {
                    t.federated = on;
                }
            }
            Mutation::SetAsyncWrites { name, on } => {
                if let Some(t) = self.tenants.get_mut(&name) {
                    t.async_writes = on;
                }
            }
            Mutation::SetSlotOwner { ns, slot, pair } => {
                let n = self.pairs.len();
                crate::tenant::set_slot_owner(
                    &mut self.exceptions,
                    &ns,
                    slot,
                    pair,
                    &self.ranges,
                    n,
                );
            }
            Mutation::ClearSlotOwner { ns, slot } => {
                crate::tenant::clear_slot_owner(&mut self.exceptions, &ns, slot);
            }
            Mutation::ConsolidateSlots => {
                let n = self.pairs.len();
                crate::tenant::normalize(&mut self.exceptions, &self.ranges, n);
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
            Mutation::SetAdmin { token, prev } => {
                self.admin_token = token;
                self.admin_prev = prev;
            }
        }
    }

    /// The snapshot a given proxy should see: shared pair topology + ONLY
    /// the tenants whose subset includes it (the sub-group boundary).
    pub fn snapshot_for(&self, proxy: &str) -> (u64, String, String, String, String, String) {
        let (pairs, tenants) =
            crate::tenant::snapshot_for(&self.pairs, &self.ranges, self.tenants.values(), proxy);
        let d = |t: &Option<String>| {
            t.as_deref()
                .map(|s| flint_tls::sha256_hex(s.as_bytes()))
                .unwrap_or_else(|| "-".into())
        };
        let admin = format!("{},{}", d(&self.admin_token), d(&self.admin_prev));
        (
            self.version,
            pairs,
            tenants,
            admin,
            crate::tenant::exceptions_spec_for(&self.exceptions, self.tenants.values(), proxy),
            crate::tenant::promote_hint(&self.promoted),
        )
    }
}

#[cfg(test)]
mod promote_tests {
    use super::*;

    fn reg() -> RegistryState {
        RegistryState {
            pairs: vec![vec!["a:1".into(), "b:1".into()]],
            ..Default::default()
        }
    }

    /// The view `watch()` compares for delta suppression.
    fn pushed_view(
        t: &(u64, String, String, String, String, String),
    ) -> (String, String, String, String, String) {
        (
            t.1.clone(),
            t.2.clone(),
            t.3.clone(),
            t.4.clone(),
            t.5.clone(),
        )
    }

    /// THE trap this feature had to avoid. A promotion changes nothing else a
    /// proxy can see — same pairs, same tenants, same admin, same exceptions
    /// — so if the hint were left out of the pushed view, `watch()` would
    /// suppress the push as "view unchanged" and the notice would never
    /// reach anyone. The bug would present as "the hint never arrives",
    /// pointing at the network rather than at the four-field tuple that
    /// discarded it.
    #[test]
    fn a_promotion_changes_the_pushed_view_so_suppression_cannot_eat_it() {
        let mut r = reg();
        let before = r.snapshot_for("p1");
        r.apply(Mutation::Promoted { addr: "b:1".into() });
        let after = r.snapshot_for("p1");
        assert_ne!(
            pushed_view(&before),
            pushed_view(&after),
            "a promotion must change the pushed view or delta suppression eats it"
        );
        assert_eq!(
            before.1, after.1,
            "a promotion must not disturb the pair topology"
        );
        assert_eq!(
            before.2, after.2,
            "a promotion must not disturb the tenant table"
        );
    }

    #[test]
    fn generation_advances_per_promotion_so_repeats_are_distinguishable() {
        let mut r = reg();
        r.apply(Mutation::Promoted { addr: "b:1".into() });
        let first = r.snapshot_for("p1").5;
        // The SAME address promoted again is a genuinely new event (promote
        // b, fail back to a, promote b again). Without the generation the
        // two render identically and the second is silently dropped.
        r.apply(Mutation::Promoted { addr: "b:1".into() });
        let second = r.snapshot_for("p1").5;
        assert_ne!(
            first, second,
            "re-promoting the same node must still be visible"
        );
        assert_eq!(first, "b:1|1");
        assert_eq!(second, "b:1|2");
    }

    #[test]
    fn no_promotion_renders_an_empty_hint() {
        assert_eq!(reg().snapshot_for("p1").5, "");
    }

    /// The hint must not survive a restart — it is a live wakeup, not a
    /// fact. If it were persisted, a proxy would re-probe on every reconnect
    /// for a promotion that happened days ago.
    #[test]
    fn the_hint_is_not_persisted() {
        let mut r = reg();
        r.apply(Mutation::Promoted { addr: "b:1".into() });
        let json = serde_json::to_string(&r).expect("serialize");
        let back: RegistryState = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(
            back.promoted, None,
            "the promotion hint must not round-trip through persistence"
        );
    }
}
