// SPDX-License-Identifier: Elastic-2.0
//! The ONE Tenant type and the ONE snapshot encoding, shared by both
//! control-plane modes (simple `state.rs` and Raft `registry.rs`).
//!
//! History note (why this module exists): the two modes used to carry
//! duplicate Tenant structs and duplicate snapshot renderers, and every
//! tenant-flag addition (D7's '#r', D6's '#c') meant editing the same
//! logic in four files. The proxy parses ONE wire format; it must be
//! produced by ONE function.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Tenant {
    pub name: String,
    pub token: String,
    pub ns: String,
    /// The shuffle-shard subset of proxies serving this tenant. Empty means
    /// "not yet assigned" (no proxies registered at add time).
    pub subset: Vec<String>,
    /// Previous token during a rotation; both it and `token` auth to `ns`
    /// until dropped. None outside a rotation window.
    #[serde(default)]
    pub prev_token: Option<String>,
    /// Replica-read opt-in (ADR-0005 D7): proxy may fan reads across the
    /// pair's replicas; writes stay on the master. Tenant's explicit choice.
    #[serde(default)]
    pub replica_reads: bool,
    /// Proxy near-cache opt-in (ADR-0005 D6): the proxy may answer this
    /// tenant's GETs from its short-TTL local cache — an allowed-staleness
    /// contract, the tenant's explicit choice.
    #[serde(default)]
    pub local_cache: bool,
    /// Fleet-wide ops/s quota (M5); 0 = unlimited. Pushed to each proxy
    /// PRE-DIVIDED by the tenant's subset size, so per-proxy token buckets
    /// enforce the fleet budget without cross-proxy coordination.
    #[serde(default)]
    pub ops_per_sec: u64,
    /// Storage-bytes quota (M5); 0 = unlimited. Enforced SOFTLY: the agent
    /// meters usage, the CP flips `over_quota`, proxies shed WRITES with
    /// -QUOTA (reads always served — a full tenant can read its data out).
    #[serde(default)]
    pub max_bytes: u64,
    /// The storage-quota verdict (set by the metering loop, not directly by
    /// an operator). Rides the snapshot as the 'q' flag.
    #[serde(default)]
    pub over_quota: bool,
    /// Federation flag (ADR-0007, plumbing only today): this tenant's slot
    /// space may span member clusters, served by a dedicated proxy group.
    /// Rides the snapshot as the 'f' flag; routing semantics arrive with
    /// the fleet-map work.
    #[serde(default)]
    pub federated: bool,
    /// Async write-queue opt-in (ADR-0005 D4): the proxy pins this tenant's
    /// backend connections with the async handshake flag, so batchable
    /// string/counter writes coalesce through the node's write queue
    /// (ack-after-apply — added write latency, never staleness). Rides the
    /// snapshot as the 'a' flag; operator-set (hot-key write mitigation).
    #[serde(default)]
    pub async_writes: bool,
}

impl Tenant {
    /// The snapshot flag suffix the proxy parses: "#<flags>[@<rate>]" —
    /// flags 'r' (replica reads), 'c' (near-cache), 'q' (over storage
    /// quota), 'f' (federated, ADR-0007), 'a' (async write queue, ADR-0005
    /// D4); `rate` = this tenant's PER-PROXY
    /// ops/s share (present only
    /// when a rate quota is set). THE single producer of this encoding —
    /// see the proxy's `apply_snapshot` for the single consumer.
    pub fn flags_suffix(&self) -> String {
        let mut flags = String::new();
        if self.replica_reads {
            flags.push('r');
        }
        if self.local_cache {
            flags.push('c');
        }
        if self.over_quota {
            flags.push('q');
        }
        if self.federated {
            flags.push('f');
        }
        if self.async_writes {
            flags.push('a');
        }
        let rate = self.per_proxy_rate();
        match (flags.is_empty(), rate) {
            (true, 0) => String::new(),
            (false, 0) => format!("#{flags}"),
            // A rate needs the '#' anchor even with no flags set.
            (_, r) => format!("#{flags}@{r}"),
        }
    }

    /// The tenant's fleet ops/s quota divided across its proxy subset
    /// (ceiling, so the fleet sum never rounds below the granted budget).
    /// 0 = unlimited.
    pub fn per_proxy_rate(&self) -> u64 {
        if self.ops_per_sec == 0 {
            return 0;
        }
        let n = self.subset.len().max(1) as u64;
        self.ops_per_sec.div_ceil(n)
    }
}

/// Render the snapshot a given proxy should see: the shared pair topology
/// ("a,b" or "a,b|start-end", ';'-joined) plus ONLY the tenants whose
/// subset includes it ("token=ns[#flags]", ','-joined; a rotating tenant
/// contributes its previous token too). The subset filter is the
/// blast-radius/security boundary: a proxy never holds tokens it does not
/// serve.
pub fn snapshot_for<'a>(
    pairs: &[Vec<String>],
    ranges: &[Option<(u16, u16)>],
    tenants: impl Iterator<Item = &'a Tenant>,
    proxy: &str,
) -> (String, String) {
    let pairs_spec = pairs
        .iter()
        .enumerate()
        .map(|(i, p)| match ranges.get(i).copied().flatten() {
            Some((a, b)) => format!("{}|{a}-{b}", p.join(",")),
            None => p.join(","),
        })
        .collect::<Vec<_>>()
        .join(";");
    let tenants_spec = tenants
        .filter(|t| t.subset.iter().any(|s| s == proxy))
        .flat_map(|t| {
            let suffix = t.flags_suffix();
            let mut v = vec![format!("{}={}{suffix}", t.token, t.ns)];
            if let Some(p) = &t.prev_token {
                v.push(format!("{}={}{suffix}", p, t.ns));
            }
            v
        })
        .collect::<Vec<_>>()
        .join(",");
    (pairs_spec, tenants_spec)
}

/// A slot-ownership exception RUN: `(ns, lo, hi, pair_idx)` — ownership of
/// `ns`'s slots `lo..=hi` diverges from the default range to `pair_idx`.
/// Adjacent single-slot commits compress into runs (the consolidation op),
/// so the table is sized by fragmentation SHAPE, not migrated-slot count.
pub type SlotRun = (String, u16, u16, u16);

/// Record `(ns, slot) -> pair` into `runs`: carve the slot out of any run
/// covering it (splitting an interior hit), insert, merge with adjacent
/// same-ns same-pair neighbors, and RETIRE anything redundant against the
/// default ranges (an exception that agrees with the default is not an
/// exception — this is also how a committed move-back self-retires without
/// the CPCLEARSLOT sharp edge).
pub fn set_slot_owner(
    runs: &mut Vec<SlotRun>,
    ns: &str,
    slot: u16,
    pair: u16,
    ranges: &[Option<(u16, u16)>],
    pair_count: usize,
) {
    clear_slot_owner(runs, ns, slot);
    runs.push((ns.to_string(), slot, slot, pair));
    normalize(runs, ranges, pair_count);
}

/// Remove `slot` from any run covering it (splitting when interior).
/// Returns whether anything covered it.
pub fn clear_slot_owner(runs: &mut Vec<SlotRun>, ns: &str, slot: u16) -> bool {
    let mut hit = false;
    let mut out: Vec<SlotRun> = Vec::with_capacity(runs.len() + 1);
    for (n, lo, hi, p) in runs.drain(..) {
        if n != ns || slot < lo || slot > hi {
            out.push((n, lo, hi, p));
            continue;
        }
        hit = true;
        if lo < slot {
            out.push((n.clone(), lo, slot - 1, p));
        }
        if hi > slot {
            out.push((n, slot + 1, hi, p));
        }
    }
    *runs = out;
    hit
}

/// The consolidation sweep (also applied on every mutation): sort, merge
/// adjacent same-ns same-pair runs, and drop runs made entirely of slots
/// whose default owner already IS the run's pair.
pub fn normalize(runs: &mut Vec<SlotRun>, ranges: &[Option<(u16, u16)>], pair_count: usize) {
    runs.sort();
    let mut out: Vec<SlotRun> = Vec::with_capacity(runs.len());
    for (n, lo, hi, p) in runs.drain(..) {
        if let Some((ln, _, lhi, lp)) = out.last_mut()
            && *ln == n
            && *lp == p
            && *lhi as u32 + 1 == lo as u32
        {
            *lhi = hi;
            continue;
        }
        out.push((n, lo, hi, p));
    }
    out.retain(|(_, lo, hi, p)| {
        !(*lo..=*hi).all(|s| flint_slot::default_pair(s, ranges, pair_count) == Some(*p as usize))
    });
    *runs = out;
}

/// Render the exception table for the snapshot's 6th frame element:
/// Render the promotion HINT a proxy receives: `"<addr>|<generation>"`, or
/// empty when no promotion has ever been reported.
///
/// A HINT, NOT AUTHORITY. The address names the node the controller just
/// promoted, but the proxy does not route on it — it re-probes that pair and
/// believes whoever answers as master, exactly as it does when a backend
/// dies. Authority stays with the epoch-fenced nodes, so a stale, delayed or
/// simply wrong hint costs one probe and cannot misroute a write. What the
/// hint buys is only WHEN the probe happens: immediately, instead of when
/// some client's request next fails.
///
/// The generation exists because the address alone is not distinguishing:
/// promote A, fail back to A, and two real events render identically. It is
/// compared for INEQUALITY rather than ordering, so a CP restart that resets
/// it (the hint is deliberately not persisted — it is a live wakeup, not a
/// fact worth surviving) still triggers one harmless re-probe instead of
/// going permanently quiet against a proxy that remembers a higher number.
///
/// Lives here, called by BOTH the single-node `State` and the Raft
/// `RegistryState`, because those two render snapshots separately and a
/// second copy of this is how one deployment mode silently loses the
/// feature.
pub fn promote_hint(promoted: &Option<(String, u64)>) -> String {
    match promoted {
        Some((addr, generation)) => format!("{addr}|{generation}"),
        None => String::new(),
    }
}

/// Render the co-processor family route table (ADR-0010 D1) into snapshot
/// element 7's wire grammar `PREFIX=addr,addr;PREFIX=addr` — the ONE format
/// the proxy's `parse_families` reads, produced by the ONE function (this
/// module's whole reason to exist), shared by both CP modes. Ordered input
/// (a `BTreeMap`) in, deterministic string out, so the watch loops can
/// compare it for delta-suppression. Families with no endpoints are still
/// emitted (`PREFIX=`): a registered-but-unreachable family answers
/// `-COPROCUNAVAIL`, which is not the same as an unregistered one.
pub fn families_spec(families: &std::collections::BTreeMap<String, Vec<String>>) -> String {
    families
        .iter()
        .map(|(prefix, addrs)| format!("{prefix}={}", addrs.join(",")))
        .collect::<Vec<_>>()
        .join(";")
}

/// `ns:slot:pair` for single-slot runs, `ns:lo-hi:pair` for wider ones,
/// joined by ';' (empty when none).
///
/// FILTERED like the tenant table: a proxy receives only rows for
/// namespaces of tenants whose subset includes it — the same blast-radius
/// boundary ("a proxy never holds facts about tenants it does not
/// serve"), and it keeps each proxy's map sized by ITS tenants'
/// fragmentation, not the fleet's.
pub fn exceptions_spec_for<'a>(
    exceptions: &[SlotRun],
    tenants: impl Iterator<Item = &'a Tenant>,
    proxy: &str,
) -> String {
    let served: std::collections::HashSet<&str> = tenants
        .filter(|t| t.subset.iter().any(|s| s == proxy))
        .map(|t| t.ns.as_str())
        .collect();
    exceptions
        .iter()
        .filter(|(ns, _, _, _)| served.contains(ns.as_str()))
        .map(|(ns, lo, hi, pair)| {
            if lo == hi {
                format!("{ns}:{lo}:{pair}")
            } else {
                format!("{ns}:{lo}-{hi}:{pair}")
            }
        })
        .collect::<Vec<_>>()
        .join(";")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_encoding_for_both_modes() {
        let mk = |name: &str, subset: Vec<&str>, rr: bool, lc: bool| Tenant {
            name: name.into(),
            token: format!("tok-{name}"),
            ns: name.into(),
            subset: subset.into_iter().map(String::from).collect(),
            replica_reads: rr,
            local_cache: lc,
            ..Tenant::default()
        };
        let tenants = [
            mk("plain", vec!["p1"], false, false),
            mk("reads", vec!["p1"], true, false),
            mk("cache", vec!["p1"], false, true),
            mk("both", vec!["p1"], true, true),
            mk("elsewhere", vec!["p2"], true, true),
        ];
        let pairs = vec![vec!["a".to_string(), "b".to_string()]];
        let ranges = vec![Some((0u16, 16383u16))];
        let (ps, ts) = snapshot_for(&pairs, &ranges, tenants.iter(), "p1");
        assert_eq!(ps, "a,b|0-16383");
        assert_eq!(
            ts,
            "tok-plain=plain,tok-reads=reads#r,tok-cache=cache#c,tok-both=both#rc"
        );
        // Subset filtering: p2's tenant never reaches p1.
        assert!(!ts.contains("elsewhere"));
        // Quotas (M5): the rate is divided across the subset (ceiling), the
        // over-quota verdict is the 'q' flag, and a rate with no flags
        // still gets its '#' anchor.
        let mut quota = mk("quota", vec!["p1"], true, false);
        quota.subset = vec!["p1".into(), "p2".into(), "p3".into()];
        quota.ops_per_sec = 1000;
        quota.over_quota = true;
        let (_, ts) = snapshot_for(&pairs, &ranges, [quota].iter(), "p1");
        assert_eq!(ts, "tok-quota=quota#rq@334");
        let mut bare = mk("bare", vec!["p1"], false, false);
        bare.ops_per_sec = 50;
        let (_, ts) = snapshot_for(&pairs, &ranges, [bare].iter(), "p1");
        assert_eq!(ts, "tok-bare=bare#@50");
        // Rotation: the previous token rides with the same flags.
        let mut rot = mk("rot", vec!["p1"], true, false);
        rot.prev_token = Some("old-tok".into());
        let (_, ts) = snapshot_for(&pairs, &ranges, [rot].iter(), "p1");
        assert_eq!(ts, "tok-rot=rot#r,old-tok=rot#r");
    }
}
