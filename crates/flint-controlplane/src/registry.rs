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
    /// ADR-0018: commit `addr` as its pair's master-of-record BEFORE it is
    /// promoted. The superseded master's next CPLEASE renewal trips over
    /// this record and fences immediately. Also refreshes the promoted
    /// hint, so the commit itself wakes watching proxies (subsuming
    /// CPPROMOTED on the promotion path).
    Fence {
        addr: String,
    },
    /// First CPLEASE from a member of a pair with no master-of-record
    /// adopts that member (gen 0). Only serving masters renew, so exactly
    /// one node per converged pair ever asks.
    LeaseAdopt {
        addr: String,
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
    /// Set the fleet admin token pair directly (ADR-0006 D4). The CP command
    /// layer computes current/previous; the mutation just stores what it is
    /// told, so the Raft log is the single source of truth.
    SetAdmin {
        token: Option<String>,
        prev: Option<String>,
    },
    /// Register (or replace) a co-processor command family (ADR-0010 D1):
    /// `prefix` (e.g. "VEC.") routes to `endpoints`. Global, not per-tenant —
    /// the CP is the fleet's single source for the family route table, which
    /// proxies otherwise only get from the static `--families` flag.
    SetFamily {
        prefix: String,
        endpoints: Vec<String>,
    },
    /// Retire a family registration. Emitting the now-smaller table (possibly
    /// empty) is how a proxy learns to stop routing that prefix.
    ClearFamily {
        prefix: String,
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
    /// Co-processor command families (ADR-0010 D1): prefix -> endpoints.
    /// Global (not shuffle-sharded like tenants); serialized into snapshot
    /// element 7 for every proxy. `#[serde(default)]` so a pre-family Raft
    /// snapshot / log loads with an empty table.
    #[serde(default)]
    pub families: BTreeMap<String, Vec<String>>,
    /// Master-of-record write leases (ADR-0018): (pair members, master,
    /// fencing generation). Rafted — a CP that forgot a fencing record
    /// would let a healed old master adopt itself back while its successor
    /// serves. `#[serde(default)]` so pre-lease snapshots load empty.
    #[serde(default)]
    pub leases: Vec<(Vec<String>, String, u64)>,
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
                //
                // AND THE HOLE IS REFILLED (ADR-0030). Removing without
                // replacing made fleet membership reach subsets in ONE
                // direction -- the one that degrades. A tenant that lost a
                // proxy ran narrower for good, repeated retirements walked it
                // down to an empty subset, and an empty subset is a tenant
                // answering -WRONGPASS everywhere: an outage reached by
                // attrition rather than by any decision about that tenant.
                // Nothing re-widened it but an operator noticing and running
                // CPSETSUBSET.
                //
                // THE TARGET IS THE SUBSET'S OWN LENGTH BEFORE THE REMOVAL,
                // and deliberately not a stored k -- there is no stored k, and
                // the current width is the better answer anyway: an operator
                // who widened this tenant by hand for whale isolation keeps
                // that width instead of being silently reset to the default.
                //
                // EXISTING MEMBERS ARE NEVER MOVED. Only the hole is filled,
                // so a retirement costs the connections on the retired proxy
                // and no others. Re-sharding the whole subset would preserve
                // the same isolation property and move live connections for
                // tenants that had nothing wrong with them.
                crate::tenant::refill_after_retire(
                    &self.proxies,
                    self.tenants.values_mut(),
                    &a,
                    shuffle_shard,
                );
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
                    // The OLD membership is the only handle on this pair's
                    // lease row once the vector is overwritten, so take it
                    // before replacing (BUG-0151).
                    let old = std::mem::replace(p, nodes.clone());
                    crate::tenant::repoint_lease_row(&mut self.leases, &old, &nodes);
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
            Mutation::Fence { addr } => {
                // Membership is checked by the serving handler; apply must
                // stay total, so an addr outside every pair is a no-op
                // rather than a divergence.
                if let Some(members) = self.pairs.iter().find(|p| p.contains(&addr)).cloned() {
                    // BUG-0150: this resolved the row by member-vector EQUALITY
                    // while ha.rs's CPLEASE renewal reads it by containment --
                    // the exact asymmetry BUG-0065 closed in the single-node
                    // path and never here. `members` is recomputed from
                    // `self.pairs`, so any membership change (CPSETPAIR) left
                    // the existing row unequal and pushed a SECOND row for the
                    // same pair; the renewal's containment find then returned
                    // whichever came first.
                    match crate::tenant::lease_row_index(&self.leases, &addr) {
                        Some(i) => {
                            self.leases[i].1 = addr.clone();
                            self.leases[i].2 += 1;
                        }
                        None => self.leases.push((members, addr.clone(), 1)),
                    }
                    // The version bump at the top of apply() plus this hint
                    // is exactly what CPPROMOTED did — the wake now rides
                    // the durable fencing commit instead of trailing it.
                    let next = self.promoted.as_ref().map_or(1, |(_, g)| g + 1);
                    self.promoted = Some((addr, next));
                }
            }
            Mutation::LeaseAdopt { addr } => {
                // Same one key as Fence above and as the renewal read (BUG-0150).
                if let Some(members) = self.pairs.iter().find(|p| p.contains(&addr)).cloned()
                    && crate::tenant::lease_row_index(&self.leases, &addr).is_none()
                {
                    self.leases.push((members, addr, 0));
                }
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
            Mutation::SetFamily { prefix, endpoints } => {
                self.families.insert(prefix, endpoints);
            }
            Mutation::ClearFamily { prefix } => {
                self.families.remove(&prefix);
            }
        }
    }

    /// Render the family route table into snapshot element 7's wire grammar
    /// (`PREFIX=addr,addr;PREFIX=addr`), the same the proxy's `parse_families`
    /// reads and the `--families` flag uses. `BTreeMap` iteration is ordered,
    /// so the string is deterministic — required for the watch loop's
    /// delta-suppression to compare views correctly.
    pub fn families_spec(&self) -> String {
        crate::tenant::families_spec(&self.families)
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

#[cfg(test)]
mod family_tests {
    use super::*;

    /// The SAME trap as the promotion hint, one field over (ADR-0010 D1).
    /// families is GLOBAL and lives OUTSIDE snapshot_for's tuple, so the watch
    /// loops must fold `families_spec()` into the view they compare — or a
    /// family-only change is suppressed and "the co-processor route never
    /// arrived" looks like a network fault. This asserts the field the view
    /// must include actually moves on a family mutation.
    #[test]
    fn a_family_change_moves_the_families_spec_so_suppression_cannot_eat_it() {
        let mut r = RegistryState::default();
        assert_eq!(r.families_spec(), "", "no families -> empty element 7");
        let before = r.families_spec();

        r.apply(Mutation::SetFamily {
            prefix: "VEC.".into(),
            endpoints: vec!["c:9".into()],
        });
        assert_eq!(r.families_spec(), "VEC.=c:9");
        assert_ne!(
            before,
            r.families_spec(),
            "a family registration must change the pushed families view"
        );

        // A second family: ordered, ';'-joined (BTreeMap determinism).
        r.apply(Mutation::SetFamily {
            prefix: "JSON.".into(),
            endpoints: vec!["d:1".into(), "d:2".into()],
        });
        assert_eq!(r.families_spec(), "JSON.=d:1,d:2;VEC.=c:9");

        // Clearing the LAST family must render empty (NOT absent) so the proxy
        // learns to drop it — the whole reason element 7 is always emitted.
        r.apply(Mutation::ClearFamily {
            prefix: "VEC.".into(),
        });
        r.apply(Mutation::ClearFamily {
            prefix: "JSON.".into(),
        });
        assert_eq!(r.families_spec(), "", "clearing all families -> empty spec");
    }

    /// #[serde(default)] lets a pre-family Raft snapshot / log load clean.
    #[test]
    fn families_default_when_absent_from_serialized_state() {
        let json = r#"{"version":3,"proxies":[],"pairs":[],"tenants":{}}"#;
        let r: RegistryState = serde_json::from_str(json).expect("deserialize pre-family state");
        assert!(r.families.is_empty());
        assert_eq!(r.families_spec(), "");
    }
}

/// BUG-0150: the lease-row key, on the RAFT path. `main.rs` was fixed for
/// BUG-0065 and this file was not, so these are the states the equality key
/// used to reach and cannot any more. The last one is the control: a key
/// widened into "always find something" would satisfy the first two.
#[cfg(test)]
mod bug_0150_lease_key_tests {
    use super::*;

    fn pair_of(r: &mut RegistryState, nodes: &[&str]) {
        r.apply(Mutation::AddPair {
            nodes: nodes.iter().map(|s| s.to_string()).collect(),
            range: None,
        });
    }

    /// THE defect, behaviourally. TWO ROWS FOR ONE PAIR is the state BUG-0065
    /// was about, and it is reachable here because the raft `AddPair` had no
    /// canonicalisation: `a,b` and `b,a` both registered, so a row could be
    /// keyed on one vector while the fence recomputes the other.
    ///
    /// Keyed by member-vector EQUALITY the fence cannot see that row, pushes a
    /// SECOND one, and the renewal -- which reads by containment -- returns
    /// whichever comes first. The fence writes one row and the renewal reads
    /// the other: a freshly promoted master told it was superseded by the peer
    /// it just replaced.
    ///
    /// Keyed by containment, both land on the same row whatever the table
    /// holds, and a duplicate is merely stale instead of contradictory.
    #[test]
    fn a_fence_updates_the_same_row_the_renewal_reads() {
        let mut r = RegistryState::default();
        pair_of(&mut r, &["a:1", "b:2"]);
        // A row keyed on the OTHER ordering -- what an un-canonicalised
        // registration used to leave behind. b:2 is the master on record.
        r.leases = vec![(
            vec!["b:2".to_string(), "a:1".to_string()],
            "b:2".to_string(),
            3,
        )];

        r.apply(Mutation::Fence {
            addr: "a:1".to_string(),
        });

        assert_eq!(
            r.leases.len(),
            1,
            "the fence must UPDATE the row the renewal will read, not add a \
             second one beside it: {:?}",
            r.leases
        );
        let i = crate::tenant::lease_row_index(&r.leases, "a:1").expect("a row for a:1");
        assert_eq!(
            r.leases[i].1, "a:1",
            "the row the renewal reads must name the freshly fenced master"
        );
        assert_eq!(
            r.leases[i].2, 4,
            "the generation advances on the row it found"
        );
    }

    /// The other half of BUG-0065's fix, which was also missing here: the raft
    /// handler now sorts before proposing, so `a,b` and `b,a` are one pair to
    /// apply's `contains` dedupe as well as to every containment check. This
    /// asserts what apply() does with the canonical form the handler sends.
    #[test]
    fn a_reordered_pair_does_not_register_twice() {
        let mut r = RegistryState::default();
        let mut ab = vec!["a:1".to_string(), "b:2".to_string()];
        let mut ba = vec!["b:2".to_string(), "a:1".to_string()];
        ab.sort();
        ba.sort();
        pair_of(&mut r, &["a:1", "b:2"]);
        r.apply(Mutation::AddPair {
            nodes: ba.clone(),
            range: None,
        });
        assert_eq!(
            r.pairs.len(),
            1,
            "canonicalised, the reordered pair is the same pair: {:?}",
            r.pairs
        );
        assert_eq!(ab, ba);
    }

    /// A lease row is found by CONTAINMENT, and the demoted peer must still
    /// read as superseded -- the assertion that would fail if the key were
    /// widened into "always answer OK".
    #[test]
    fn the_demoted_peer_still_resolves_to_the_row_naming_the_new_master() {
        let mut r = RegistryState::default();
        pair_of(&mut r, &["a:1", "b:2"]);
        r.apply(Mutation::Fence {
            addr: "a:1".to_string(),
        });
        let i = crate::tenant::lease_row_index(&r.leases, "b:2").expect("row via the peer");
        assert_eq!(r.leases[i].1, "a:1", "b:2 must not read itself as master");
    }
}

/// BUG-0151 on the RAFT path, which `lease_after_repoint_drill` cannot reach:
/// it drives a single-node control plane, and this is the other implementation.
#[cfg(test)]
mod bug_0151_repoint_tests {
    use super::*;

    #[test]
    fn a_repoint_moves_the_lease_row_so_the_next_fence_finds_it() {
        let mut r = RegistryState::default();
        r.apply(Mutation::AddPair {
            nodes: vec!["a:1".to_string(), "b:2".to_string()],
            range: None,
        });
        r.apply(Mutation::LeaseAdopt {
            addr: "a:1".to_string(),
        });
        // b:2 is replaced by c:3, then c:3 is promoted -- replace a failed
        // replica, then lose the master.
        r.apply(Mutation::SetPair {
            idx: 0,
            nodes: vec!["a:1".to_string(), "c:3".to_string()],
        });
        r.apply(Mutation::Fence {
            addr: "c:3".to_string(),
        });

        assert_eq!(
            r.leases.len(),
            1,
            "one pair, one row: a fence that could not find the repointed row \
             pushed a second one and both answered OK: {:?}",
            r.leases
        );
        // The displaced incumbent is STILL a member, so it still resolves a
        // row -- and that row must name the new master, or CPLEASE hands it an
        // OK it has no right to.
        let i = crate::tenant::lease_row_index(&r.leases, "a:1")
            .expect("a:1 is still a member and must resolve a row");
        assert_eq!(
            r.leases[i].1, "c:3",
            "the displaced incumbent must read the fenced master, not itself"
        );
    }

    /// The control: repointing must not invent a row for a pair that never
    /// held one, which would make every pair look leased.
    #[test]
    fn a_pair_that_never_held_a_lease_gains_no_row_from_a_repoint() {
        let mut r = RegistryState::default();
        r.apply(Mutation::AddPair {
            nodes: vec!["a:1".to_string(), "b:2".to_string()],
            range: None,
        });
        r.apply(Mutation::SetPair {
            idx: 0,
            nodes: vec!["a:1".to_string(), "c:3".to_string()],
        });
        assert!(r.leases.is_empty(), "{:?}", r.leases);
    }
}

/// ADR-0030's refill. Every test here is a state the ratchet used to reach
/// and cannot any more; the LAST one is the control, because a refill that
/// fires unconditionally would satisfy all the others and is a different bug.
#[cfg(test)]
mod adr_0030_refill_tests {
    use super::*;

    fn fleet(n: usize) -> RegistryState {
        let mut r = RegistryState::default();
        for i in 0..n {
            r.apply(Mutation::AddProxy(format!("10.0.0.{i}:7379")));
        }
        r
    }

    fn with_tenant(r: &mut RegistryState, name: &str, k: usize) {
        let subset = shuffle_shard(name, &r.proxies, k);
        r.apply(Mutation::AddTenant {
            name: name.to_string(),
            token: format!("tok-{name}"),
            ns: name.to_string(),
            subset,
        });
    }

    fn subset(r: &RegistryState, name: &str) -> Vec<String> {
        r.tenants
            .get(name)
            .map(|t| t.subset.clone())
            .unwrap_or_default()
    }

    #[test]
    fn a_retirement_refills_the_hole_it_makes() {
        let mut r = fleet(4);
        with_tenant(&mut r, "acme", 2);
        let before = subset(&r, "acme");
        assert_eq!(before.len(), 2);
        r.apply(Mutation::DelProxy(before[0].clone()));
        let after = subset(&r, "acme");
        assert_eq!(
            after.len(),
            2,
            "the tenant must come back to its width: {after:?}"
        );
        assert!(
            !after.contains(&before[0]),
            "the retired proxy is still placed"
        );
        assert!(
            after.contains(&before[1]),
            "the surviving member was moved, and need not have been"
        );
    }

    /// The width that is restored is the tenant's OWN, not the default: an
    /// operator who widened a whale by hand must not be silently narrowed.
    #[test]
    fn an_operator_widened_tenant_keeps_its_own_width() {
        let mut r = fleet(5);
        with_tenant(&mut r, "whale", 4);
        let before = subset(&r, "whale");
        assert_eq!(before.len(), 4);
        r.apply(Mutation::DelProxy(before[2].clone()));
        assert_eq!(
            subset(&r, "whale").len(),
            4,
            "a hand-widened tenant was reset toward the default"
        );
    }

    /// The terminus the ratchet used to reach. Retiring every proxy a tenant
    /// holds, one at a time, must not walk it down to -WRONGPASS while the
    /// fleet still has proxies to serve from.
    #[test]
    fn repeated_retirements_do_not_walk_a_tenant_to_empty() {
        let mut r = fleet(5);
        with_tenant(&mut r, "acme", 2);
        for _ in 0..3 {
            let s = subset(&r, "acme");
            assert_eq!(s.len(), 2, "narrowed mid-sequence: {s:?}");
            r.apply(Mutation::DelProxy(s[0].clone()));
        }
        let end = subset(&r, "acme");
        assert_eq!(
            end.len(),
            2,
            "walked down to {end:?} with proxies still in the fleet"
        );
    }

    /// Refill SPREADS. Without the shuffle-shard step every repaired tenant
    /// lands on whichever proxy sorts first, which is the isolation property
    /// shuffle-sharding exists for, inverted.
    #[test]
    fn refill_does_not_pile_every_tenant_onto_one_survivor() {
        let mut r = fleet(6);
        for n in ["a", "b", "c", "d", "e", "f", "g", "h"] {
            with_tenant(&mut r, n, 2);
        }
        let victim = r.proxies[0].clone();
        r.apply(Mutation::DelProxy(victim.clone()));
        let mut counts = std::collections::HashMap::new();
        for t in r.tenants.values() {
            assert!(!t.subset.contains(&victim));
            // WIDTH FIRST, and it is what makes this test die to a refill that
            // never runs. Without it the spread assertion holds vacuously --
            // no tenant gains anything, so nothing piles anywhere, and the
            // mutant reaches the same answer by a different path.
            assert_eq!(
                t.subset.len(),
                2,
                "{} was left narrow: {:?}",
                t.name,
                t.subset
            );
            for p in &t.subset {
                *counts.entry(p.clone()).or_insert(0usize) += 1;
            }
        }
        let max = counts.values().copied().max().unwrap_or(0);
        let total: usize = counts.values().sum();
        assert!(max < total, "every tenant landed on one proxy: {counts:?}");
    }

    /// A fleet smaller than the width leaves the tenant short, correctly --
    /// there is nothing to fill from, and inventing a duplicate would be worse.
    #[test]
    fn a_fleet_too_small_leaves_it_short_rather_than_duplicating() {
        let mut r = fleet(2);
        with_tenant(&mut r, "acme", 2);
        r.apply(Mutation::DelProxy(r.proxies[0].clone()));
        let s = subset(&r, "acme");
        assert_eq!(s.len(), 1, "expected one, got {s:?}");
        let mut uniq = s.clone();
        uniq.dedup();
        assert_eq!(uniq.len(), s.len(), "the refill duplicated a proxy: {s:?}");
    }

    /// THE CONTROL. Everything above is satisfied by a refill that runs on
    /// every mutation and re-widens tenants nobody touched. Retiring a proxy
    /// NO tenant holds must leave every subset byte-identical.
    #[test]
    fn retiring_an_unused_proxy_changes_no_subset() {
        let mut r = fleet(4);
        with_tenant(&mut r, "acme", 2);
        let before = subset(&r, "acme");
        let spare = r
            .proxies
            .iter()
            .find(|p| !before.contains(p))
            .expect("a spare")
            .clone();
        r.apply(Mutation::DelProxy(spare));
        assert_eq!(
            subset(&r, "acme"),
            before,
            "an unrelated retirement moved a tenant"
        );
    }
}
