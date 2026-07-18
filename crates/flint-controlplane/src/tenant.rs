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
}

impl Tenant {
    /// The snapshot flag suffix the proxy parses: "#<flags>[@<rate>]" —
    /// flags 'r' (replica reads), 'c' (near-cache), 'q' (over storage
    /// quota); `rate` = this tenant's PER-PROXY ops/s share (present only
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
