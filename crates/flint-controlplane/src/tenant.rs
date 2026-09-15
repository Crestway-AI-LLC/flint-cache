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

/// A command-family prefix must survive BOTH serialization grammars unchanged:
/// the snapshot wire spec `PREFIX=addr,addr;PREFIX=addr` (so no `=` `;` `,`)
/// and the single-node persistence line `family PREFIX addr,addr`, which the
/// reload splits on spaces (so no ASCII whitespace). It must also be printable
/// ASCII — a control byte or DEL is not a real command prefix. Enforced at the
/// CPFAMILY handlers because a prefix that violates this round-trips one way in
/// and a different way out: registered as `VEC SET`, reloaded after a CP
/// restart as prefix `VEC` routing to a bogus endpoint `SET`, with no command
/// ever re-issued to reveal the drift. `is_ascii_graphic` is exactly the
/// 0x21..=0x7E printable-non-space range, so it already excludes space, tab,
/// newline, CR and NUL; the `matches!` adds the three wire delimiters.
pub fn valid_family_prefix(prefix: &str) -> bool {
    !prefix.is_empty()
        && prefix
            .bytes()
            .all(|b| b.is_ascii_graphic() && !matches!(b, b'=' | b';' | b','))
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

    #[test]
    fn a_family_prefix_that_breaks_a_grammar_is_rejected() {
        // Real prefixes: alphanumerics, the dot separator, underscores/dashes.
        for ok in ["VEC.", "JSON.", "BF.", "FT.", "TS.", "A", "X_Y-Z.1"] {
            assert!(valid_family_prefix(ok), "{ok:?} should be accepted");
        }
        // Empty, whitespace (breaks the single-node `family P addr` line), the
        // three wire delimiters, and control/DEL bytes are all rejected — each
        // would round-trip inconsistently through one of the two grammars.
        for bad in [
            "",        // never a valid prefix
            "VEC SET", // space → reload splits it, prefix becomes "VEC"
            "A\tB",    // tab is ASCII whitespace too
            "A\nB",    // newline drops the family entirely on reload
            "A=B",     // wire `PREFIX=addr` delimiter
            "A;B",     // wire family separator
            "A,B",     // wire endpoint separator
            "A\x01B",  // control byte
            "A\x7fB",  // DEL
        ] {
            assert!(!valid_family_prefix(bad), "{bad:?} should be rejected");
        }
    }
}

/// ADR-0030's refill, in ONE place because this control plane has two of
/// everything else.
///
/// `CPDELPROXY` is implemented twice — `ha.rs` proposes a `Mutation::DelProxy`
/// that lands in `registry::RegistryState::apply`, and `main.rs` mutates
/// `state::State` inline for the single-node path. The first version of this
/// fix went into the mutation only, so six unit tests passed and the live
/// control plane did nothing: `subset_ratchet_drill` runs single-node and is
/// what caught it. The tests exercised the TYPE; the drill exercised the
/// PRODUCT.
///
/// `shuffle` is passed in because that function is duplicated as well
/// (`state.rs` and `registry.rs` hold identical copies). Taking it as an
/// argument keeps the refill single even while its input is not; unifying the
/// two shuffles is a larger change and is filed rather than smuggled in here.
pub fn refill_after_retire<'a>(
    proxies: &[String],
    tenants: impl Iterator<Item = &'a mut Tenant>,
    retired: &str,
    shuffle: fn(&str, &[String], usize) -> Vec<String>,
) {
    for t in tenants {
        // The target is the subset's OWN width before the removal, not a
        // stored k -- there is none, and the current width is the better
        // answer anyway: an operator who widened a whale by hand keeps that
        // width instead of being reset to the default.
        let want = t.subset.len();
        t.subset.retain(|p| p != retired);
        if t.subset.len() == want {
            continue;
        }
        // The ideal placement over the fleet as it now stands. Taking the
        // members not already held keeps the shuffle-shard SPREAD -- without
        // it every repaired tenant lands on whichever proxy sorts first,
        // which is the isolation property inverted.
        for c in shuffle(&t.name, proxies, want) {
            if t.subset.len() >= want {
                break;
            }
            if !t.subset.contains(&c) {
                t.subset.push(c);
            }
        }
        // The ideal set can overlap what is already held, so widen the search
        // rather than leave a tenant short on a fleet that could cover it. A
        // fleet SMALLER than `want` leaves it short, correctly: padding with a
        // duplicate to reach the number would be worse.
        for c in proxies {
            if t.subset.len() >= want {
                break;
            }
            if !t.subset.contains(c) {
                t.subset.push(c.clone());
            }
        }
        t.subset.sort();
    }
}

/// THE one way a lease row is resolved, for BOTH control planes.
///
/// A row is `(pair members, master-of-record, generation)` and it is found by
/// MEMBERSHIP CONTAINMENT: the first row whose members include `addr`.
///
/// BUG-0065: the fence wrote by member-vector EQUALITY while the renewal read
/// by containment, so with two rows for one pair the fence updated one and the
/// renewal read the other -- and a freshly promoted master was told it had
/// been superseded by the peer it had just replaced. One key means the write
/// and the read cannot land on different rows whatever the table holds; a
/// duplicate becomes merely stale instead of contradictory.
///
/// BUG-0150: that fix, and the structural test that holds it shut, lived in
/// `main.rs` -- and the test read `include_str!("main.rs")`, so the raft state
/// machine kept resolving rows by equality with the forbidden literal sitting
/// in a file the guard did not open. Hence this function is here, where both
/// paths can reach it, rather than beside one of them.
pub fn lease_row_index(rows: &[(Vec<String>, String, u64)], addr: &str) -> Option<usize> {
    rows.iter()
        .position(|(m, _, _)| m.iter().any(|x| x == addr))
}

/// The `CPMYSTATUS` body (ADR-0014 D3), formatted once for both control planes.
///
/// BUG-0148: this verb was dispatched ONLY by the single-node control plane.
/// A tenant on a Raft control plane got `unknown command` for the one command
/// ADR-0014 gives them to ask about themselves, and `tenant_status_drill`
/// covers D3 on a single node, so nothing ever said so.
///
/// Hand-porting the body into `ha.rs` would have closed that gap and opened
/// the one BUG-0146 is about -- two copies of a format, kept in step by hand,
/// with the data agreeing so nothing reconciles the behaviour. So the body
/// lives here and `build` is passed in, for the same reason
/// [`refill_after_retire`] takes its shuffle: the caller owns what genuinely
/// differs between the two paths, this owns what must not differ at all.
pub fn my_status_body(t: &Tenant, usage_bytes: u64, build: &str) -> Vec<u8> {
    // `endpoint` is the tenant's OWN proxy subset -- what they already dial,
    // and what CPSNAPSHOT already tells them. Not a topology leak, and the
    // distinction ADR-0014 draws: their endpoint yes, node addresses and pair
    // layout no. Nothing here reads any tenant but this one.
    format!(
        "tenant:{}\r\nnamespace:{}\r\nendpoint:{}\r\n\
         quota_ops_per_sec:{}\r\nquota_max_bytes:{}\r\n\
         usage_bytes:{}\r\nover_quota:{}\r\n\
         replica_reads:{}\r\nlocal_cache:{}\r\nasync_writes:{}\r\n\
         federated:{}\r\nbuild:{}\r\n",
        t.name,
        t.ns,
        if t.subset.is_empty() {
            "-".to_string()
        } else {
            t.subset.join(",")
        },
        t.ops_per_sec,
        t.max_bytes,
        usage_bytes,
        t.over_quota as u8,
        t.replica_reads as u8,
        t.local_cache as u8,
        t.async_writes as u8,
        t.federated as u8,
        build,
    )
    .into_bytes()
}

#[cfg(test)]
mod my_status_tests {
    use super::*;

    fn t() -> Tenant {
        Tenant {
            name: "acme".into(),
            token: "sha256-of-the-real-token".into(),
            ns: "acme".into(),
            subset: vec!["10.0.0.1:9001".into(), "10.0.0.2:9001".into()],
            ops_per_sec: 5000,
            max_bytes: 1 << 30,
            over_quota: true,
            replica_reads: true,
            ..Tenant::default()
        }
    }

    #[test]
    fn it_reports_this_tenants_own_fields_and_no_others() {
        let body =
            String::from_utf8(my_status_body(&t(), 4242, "v0.2.17")).expect("the body is ASCII");
        for want in [
            "tenant:acme",
            "namespace:acme",
            "endpoint:10.0.0.1:9001,10.0.0.2:9001",
            "quota_ops_per_sec:5000",
            "quota_max_bytes:1073741824",
            "usage_bytes:4242",
            "over_quota:1",
            "replica_reads:1",
            "local_cache:0",
            "build:v0.2.17",
        ] {
            assert!(body.contains(want), "missing {want} in:\n{body}");
        }
        // The TOKEN DIGEST must never appear. It is on the struct, it is the
        // credential, and a formatter that reads `t` has it in hand.
        assert!(
            !body.contains("sha256-of-the-real-token"),
            "the token digest leaked:\n{body}"
        );
    }

    #[test]
    fn an_unplaced_tenant_reports_a_dash_rather_than_an_empty_field() {
        // A tenant whose subset is empty is DRAINED, and `endpoint:` with
        // nothing after it reads as a parse failure to whoever consumes this.
        let mut t = t();
        t.subset.clear();
        let body = String::from_utf8(my_status_body(&t, 0, "v0")).expect("the body is ASCII");
        assert!(body.contains("endpoint:-\r\n"), "{body}");
    }
}

#[cfg(test)]
mod refill_tests {
    use super::*;

    fn tenant(name: &str, subset: &[&str]) -> Tenant {
        Tenant {
            name: name.to_string(),
            subset: subset.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    /// THE TEST THAT WOULD HAVE CAUGHT THE REAL BUG. The refill is called from
    /// two places with two different `shuffle_shard`s -- `registry.rs` has one
    /// and `state.rs` has an identical copy -- and if those ever diverge the
    /// raft and single-node control planes place tenants differently while
    /// both look fine in isolation.
    ///
    /// Asserting they AGREE is the cheapest guard available without unifying
    /// them, which is a larger change filed separately.
    #[test]
    fn both_shuffles_refill_a_retirement_identically() {
        let proxies: Vec<String> = (0..5).map(|i| format!("10.0.0.{i}:7379")).collect();
        for name in ["acme", "globex", "initech", "umbrella"] {
            let mut a = tenant(name, &[&proxies[0], &proxies[1]]);
            let mut b = a.clone();
            let live: Vec<String> = proxies.iter().skip(1).cloned().collect();
            refill_after_retire(
                &live,
                std::iter::once(&mut a),
                &proxies[0],
                crate::registry::shuffle_shard,
            );
            refill_after_retire(
                &live,
                std::iter::once(&mut b),
                &proxies[0],
                crate::state::shuffle_shard,
            );
            assert_eq!(
                a.subset.len(),
                2,
                "{name} was left narrow by the registry shuffle"
            );
            assert_eq!(
                a.subset, b.subset,
                "the two control-plane paths placed {name} differently: {:?} vs {:?}",
                a.subset, b.subset
            );
        }
    }
}
