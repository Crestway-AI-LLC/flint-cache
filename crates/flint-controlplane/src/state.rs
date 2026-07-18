//! Control-plane state: the durable intent nothing else can re-derive
//! (design.md §2.2) — tenant registry (token, namespace, proxy subset),
//! proxy fleet, group topology — plus a monotonic version stamped on every
//! mutation, which is what the snapshot-push protocol keys on.
//!
//! v1 durability: whole-state serialize -> temp file -> fsync -> rename on
//! every mutation. The state is tiny and low-churn by design, so atomic
//! full-file replacement is simpler and no less correct than a log. The
//! openraft ×3 deployment is the HA follow-on; the serialized form is the
//! future Raft snapshot format.
//!
//! Serialized format (line-based, space-delimited — namespaces and addrs
//! are validated space-free):
//!   version <n>
//!   proxy <addr>
//!   pair <addr,addr[,addr]>
//!   tenant <name> <token> <ns> <proxyaddr,proxyaddr|->

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;

pub use crate::tenant::Tenant;

#[derive(Debug, Default)]
pub struct State {
    pub version: u64,
    pub proxies: Vec<String>,
    pub pairs: Vec<Vec<String>>,
    /// Slot range owned by pairs[i] (level-1 routing state, design.md §2.2).
    /// None = unranged (legacy registries / static mode): proxies fall back
    /// to count-derived ranges. An EXPANSION pair joins with an empty range
    /// (None here is "unranged", (0,0)-style empty is expressed by simply
    /// never covering a slot): it owns nothing until migration moves slots,
    /// so adding capacity can never re-route data that has not moved.
    pub ranges: Vec<Option<(u16, u16)>>,
    /// Keyed by tenant name (BTreeMap: deterministic serialization order).
    pub tenants: BTreeMap<String, Tenant>,
    /// Fleet operator (admin) token, CURRENT (ADR-0006 D4). Stored PLAINTEXT
    /// — unlike tenant tokens it is RETRIEVED by our own components (the
    /// agent presents it to proxies over their token-auth front door), so it
    /// cannot be one-way hashed; at rest it relies on volume encryption. The
    /// DIGEST is what the proxies receive (they verify, never retrieve).
    pub admin_token: Option<String>,
    /// Previous admin token during a rotation window (both valid until the
    /// agent's drop-on-adoption retires it).
    pub admin_prev: Option<String>,
    path: Option<PathBuf>,
}

/// FNV-1a — a stable, dependency-free seed for deterministic subset
/// assignment. NOT a security boundary (tokens are); just placement.
fn fnv1a(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Deterministic shuffle-shard: pick `k` distinct proxies for `name` from
/// `fleet` (sorted first, so the choice is independent of registration
/// order). Same tenant + same fleet => same subset on every node that ever
/// computes it — no coordination needed for agreement.
pub fn shuffle_shard(name: &str, fleet: &[String], k: usize) -> Vec<String> {
    let mut sorted: Vec<&String> = fleet.iter().collect();
    sorted.sort();
    let k = k.min(sorted.len());
    let mut picked = Vec::with_capacity(k);
    let mut seed = fnv1a(name.as_bytes());
    while picked.len() < k && !sorted.is_empty() {
        // Splitmix-style step; uniform enough for placement.
        seed = seed
            .wrapping_add(0x9E3779B97F4A7C15)
            .wrapping_mul(0xBF58476D1CE4E5B9);
        let idx = (seed >> 33) as usize % sorted.len();
        picked.push(sorted.remove(idx).clone());
    }
    picked.sort();
    picked
}

/// Parse "a-b" into a slot range; "-" (or anything malformed) is None.
pub fn parse_range(raw: &str) -> Option<(u16, u16)> {
    let (a, b) = raw.split_once('-')?;
    Some((a.trim().parse().ok()?, b.trim().parse().ok()?))
}

/// Render the tenant->subset mapping as DNS zone A records: each tenant
/// name resolves to ONLY its shuffle-shard subset's hosts. This is the
/// publication format (design.md §2.1) — pushing it to a DNS provider is an
/// integration on top. Proxies share one standard port in production, so A
/// records suffice; per-member ports would be SRV, deliberately out of v1.
pub fn dns_zone<'a>(
    suffix: &str,
    tenants: impl Iterator<Item = (&'a str, &'a Vec<String>)>,
) -> String {
    let mut out = String::new();
    for (name, subset) in tenants {
        for member in subset {
            let host = member.split(':').next().unwrap_or(member);
            out.push_str(&format!("{name}.{suffix}. 30 IN A {host}\n"));
        }
    }
    out
}

impl State {
    pub fn load_or_new(path: PathBuf) -> Self {
        let mut s = State {
            path: Some(path.clone()),
            ..Default::default()
        };
        let Ok(raw) = std::fs::read_to_string(&path) else {
            return s;
        };
        for line in raw.lines() {
            let mut parts = line.split(' ');
            match parts.next() {
                Some("version") => {
                    s.version = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
                }
                Some("proxy") => {
                    if let Some(a) = parts.next() {
                        s.proxies.push(a.to_string());
                    }
                }
                Some("pair") => {
                    if let Some(nodes) = parts.next() {
                        s.pairs.push(nodes.split(',').map(String::from).collect());
                        s.ranges.push(parts.next().and_then(parse_range));
                    }
                }
                Some("tenant") => {
                    if let (Some(name), Some(token), Some(ns), Some(subset)) =
                        (parts.next(), parts.next(), parts.next(), parts.next())
                    {
                        let subset = if subset == "-" {
                            Vec::new()
                        } else {
                            subset.split(',').map(String::from).collect()
                        };
                        let prev_token = parts
                            .next()
                            .and_then(|p| if p == "-" { None } else { Some(p.to_string()) });
                        // ADR-0006 D1 migration: a plaintext-era token (not
                        // a 64-hex digest) is hashed ON LOAD — one-way, once.
                        let digestify = |t: String| {
                            if t.len() == 64 && t.bytes().all(|b| b.is_ascii_hexdigit()) {
                                t
                            } else {
                                flint_tls::sha256_hex(t.as_bytes())
                            }
                        };
                        let replica_reads = parts.next() == Some("1");
                        let local_cache = parts.next() == Some("1");
                        let ops_per_sec = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
                        let max_bytes = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
                        let over_quota = parts.next() == Some("1");
                        s.tenants.insert(
                            name.to_string(),
                            Tenant {
                                name: name.to_string(),
                                token: digestify(token.to_string()),
                                ns: ns.to_string(),
                                subset,
                                prev_token: prev_token.map(digestify),
                                replica_reads,
                                local_cache,
                                ops_per_sec,
                                max_bytes,
                                over_quota,
                            },
                        );
                    }
                }
                Some("admin") => {
                    s.admin_token = parts.next().filter(|v| *v != "-").map(String::from);
                    s.admin_prev = parts.next().filter(|v| *v != "-").map(String::from);
                }
                _ => {}
            }
        }
        s
    }

    fn serialize(&self) -> String {
        let mut out = format!("version {}\n", self.version);
        if self.admin_token.is_some() || self.admin_prev.is_some() {
            out.push_str(&format!(
                "admin {} {}\n",
                self.admin_token.as_deref().unwrap_or("-"),
                self.admin_prev.as_deref().unwrap_or("-")
            ));
        }
        for p in &self.proxies {
            out.push_str(&format!("proxy {p}\n"));
        }
        for (i, pair) in self.pairs.iter().enumerate() {
            let range = match self.ranges.get(i).copied().flatten() {
                Some((a, b)) => format!("{a}-{b}"),
                None => "-".to_string(),
            };
            out.push_str(&format!("pair {} {range}\n", pair.join(",")));
        }
        for t in self.tenants.values() {
            let subset = if t.subset.is_empty() {
                "-".to_string()
            } else {
                t.subset.join(",")
            };
            out.push_str(&format!(
                "tenant {} {} {} {subset} {} {} {} {} {} {}\n",
                t.name,
                t.token,
                t.ns,
                t.prev_token.as_deref().unwrap_or("-"),
                t.replica_reads as u8,
                t.local_cache as u8,
                t.ops_per_sec,
                t.max_bytes,
                t.over_quota as u8,
                subset = subset
            ));
        }
        out
    }

    /// Bump the version and persist atomically (temp + fsync + rename).
    /// Every mutation goes through here — an un-persisted version can never
    /// be observed by a subscriber.
    pub fn commit(&mut self) -> std::io::Result<u64> {
        self.version += 1;
        if let Some(path) = &self.path {
            let tmp = path.with_extension("tmp");
            {
                let mut f = std::fs::File::create(&tmp)?;
                f.write_all(self.serialize().as_bytes())?;
                f.sync_all()?;
            }
            std::fs::rename(&tmp, path)?;
        }
        Ok(self.version)
    }

    /// The snapshot a given proxy should see: the shared pair topology plus
    /// ONLY the tenants whose subset includes it (or unassigned-subset
    /// tenants never — no subset, no service). This filtering is the
    /// blast-radius/security boundary: a proxy never holds tokens it does
    /// not serve.
    /// The snapshot a given proxy should see (shared renderer — the subset
    /// filter is the blast-radius/security boundary: a proxy never holds
    /// tokens it does not serve).
    pub fn snapshot_for(&self, proxy: &str) -> (u64, String, String, String) {
        let (pairs, tenants) =
            crate::tenant::snapshot_for(&self.pairs, &self.ranges, self.tenants.values(), proxy);
        (self.version, pairs, tenants, self.admin_digests())
    }

    /// The admin field the proxy receives: "curdigest,prevdigest" (either
    /// "-"). DIGESTS — the proxy verifies a presented admin token by hash,
    /// exactly like tenant tokens (D1); it never holds the plaintext.
    pub fn admin_digests(&self) -> String {
        let d = |t: &Option<String>| {
            t.as_deref()
                .map(|s| flint_tls::sha256_hex(s.as_bytes()))
                .unwrap_or_else(|| "-".into())
        };
        format!("{},{}", d(&self.admin_token), d(&self.admin_prev))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fleet(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("10.0.0.{i}:7000")).collect()
    }

    #[test]
    fn shuffle_shard_is_deterministic_and_order_independent() {
        let f1 = fleet(10);
        let mut f2 = fleet(10);
        f2.reverse();
        let a = shuffle_shard("tenant-a", &f1, 3);
        let b = shuffle_shard("tenant-a", &f2, 3);
        assert_eq!(a, b, "same tenant + same fleet => same subset");
        assert_eq!(a.len(), 3);
        // Distinct members.
        let mut d = a.clone();
        d.dedup();
        assert_eq!(d.len(), 3);
        // Different tenants usually get different subsets (spot check).
        let c = shuffle_shard("tenant-b", &f1, 3);
        assert_ne!(a, c, "expected different subsets for different tenants");
    }

    #[test]
    fn shuffle_shard_clamps_k_to_fleet() {
        let f = fleet(2);
        assert_eq!(shuffle_shard("t", &f, 5).len(), 2);
        assert!(shuffle_shard("t", &[], 3).is_empty());
    }

    #[test]
    fn state_roundtrips_through_disk() {
        let dir = std::env::temp_dir().join(format!("flint-cp-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("state");
        let _ = std::fs::remove_file(&path);

        let mut s = State::load_or_new(path.clone());
        s.proxies.push("p1:7000".into());
        s.proxies.push("p2:7000".into());
        s.pairs.push(vec!["a:1".into(), "b:1".into()]);
        s.tenants.insert(
            "acme".into(),
            Tenant {
                name: "acme".into(),
                token: "tok-acme".into(),
                ns: "acme".into(),
                subset: vec!["p1:7000".into()],
                prev_token: None,
                replica_reads: false,
                local_cache: false,
                ops_per_sec: 0,
                max_bytes: 0,
                over_quota: false,
            },
        );
        let v = s.commit().expect("commit");
        assert_eq!(v, 1);

        let loaded = State::load_or_new(path.clone());
        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.proxies, s.proxies);
        assert_eq!(loaded.pairs, s.pairs);
        // ADR-0006 D1: this token was written PLAINTEXT (straight into the
        // struct, bypassing the CPADDTENANT digest boundary) — load MIGRATES
        // it to its SHA-256 digest, one-way. The digest form then roundtrips
        // unchanged (64-hex is recognized as already-digested).
        let expected_digest = flint_tls::sha256_hex(b"tok-acme");
        assert_eq!(
            loaded.tenants.get("acme").map(|t| t.token.clone()),
            Some(expected_digest.clone())
        );
        let reloaded = State::load_or_new(path.clone());
        assert_eq!(
            reloaded.tenants.get("acme").map(|t| t.token.clone()),
            Some(expected_digest)
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn snapshot_is_filtered_per_proxy() {
        let mut s = State::default();
        s.pairs.push(vec!["a:1".into(), "b:1".into()]);
        for (name, proxy) in [("t1", "p1"), ("t2", "p2"), ("t3", "p1")] {
            s.tenants.insert(
                name.into(),
                Tenant {
                    name: name.into(),
                    token: format!("tok-{name}"),
                    ns: name.into(),
                    subset: vec![proxy.into()],
                    prev_token: None,
                    replica_reads: false,
                    local_cache: false,
                    ops_per_sec: 0,
                    max_bytes: 0,
                    over_quota: false,
                },
            );
        }
        s.version = 7;
        let (v, pairs, tenants, _admin) = s.snapshot_for("p1");
        assert_eq!(v, 7);
        assert_eq!(pairs, "a:1,b:1");
        assert_eq!(tenants, "tok-t1=t1,tok-t3=t3");
        let (_, _, t2, _) = s.snapshot_for("p2");
        assert_eq!(t2, "tok-t2=t2", "p2 must not see p1's tokens");
    }
}
