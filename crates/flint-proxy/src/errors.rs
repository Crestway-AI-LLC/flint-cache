// SPDX-License-Identifier: Elastic-2.0
//! Per-tenant error counts, by kind.
//!
//! `PROXYSTATS` already counts auth failures and connection sheds, but a
//! fleet-wide integer answers "are there errors" and never "whose, and
//! which" — and those are the only two questions worth asking during an
//! incident. One tenant hammering a moved slot and every tenant seeing
//! -TRYAGAIN look identical in a single counter, while being a support
//! reply and a fleet emergency respectively.
//!
//! Counted at the ONE place every command's reply passes through, next to
//! the latency histogram, rather than at the ~20 sites that construct an
//! error. A counter bolted onto each construction site is a counter that
//! silently stops covering the next one somebody adds.
//!
//! **The kind set is CLOSED.** Error text is attacker-influenced in the
//! general case and operator-influenced in all of them, so mapping it
//! straight into a Prometheus label would let one malformed command mint
//! a series that never goes away. Anything unrecognised lands in `other`,
//! which is itself the signal that this list needs an entry.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

/// The RESP error prefixes the proxy actually returns to a client, plus
/// `other`. Adding one here is deliberate: it is a new dashboard series
/// and a new thing an operator will ask about.
const KINDS: [&str; 9] = [
    "ERR",       // the catch-all RESP error: bad args, unknown command
    "NOAUTH",    // a command before AUTH
    "WRONGPASS", // AUTH with a token no tenant owns
    "MOVED",     // slot moved; a client that is not following redirects
    "TRYAGAIN",  // mid-migration bridge — transient by construction
    "QUOTA",     // over the tenant's configured cap
    "READONLY",  // a write that reached a replica
    "THROTTLED", // per-tenant rate limit (the command-level one)
    "other",     // unrecognised prefix: the "add a kind" signal
];

/// The kind for a reply, or None when the reply is not an error at all.
/// Takes the FIRST token: RESP errors are `-<CODE> <human text>` and only
/// the code is a category. The text is deliberately never read — it is
/// where the unbounded cardinality lives.
pub fn classify(msg: &str) -> &'static str {
    let code = msg.split(' ').next().unwrap_or("");
    KINDS
        .iter()
        .find(|k| **k != "other" && *k == &code)
        .copied()
        .unwrap_or("other")
}

#[derive(Default)]
pub struct ErrorCounts {
    /// ns -> kind -> count. A plain map behind an RwLock, matching
    /// `LatencyHistograms`: writes happen only on an ERROR reply, which on
    /// a healthy fleet is rare, so the contention argument that shapes the
    /// latency fast path does not apply here.
    tenants: RwLock<HashMap<Vec<u8>, HashMap<&'static str, u64>>>,
}

impl ErrorCounts {
    /// Record one error reply. `ns` is the authed namespace, or `-` for
    /// replies that happen before a tenant is bound — NOAUTH and WRONGPASS
    /// have no tenant by definition, and attributing them to the last
    /// authed one would blame a bystander.
    pub fn observe(&self, ns: Option<&[u8]>, kind: &'static str) {
        let key: &[u8] = ns.unwrap_or(b"-");
        if let Ok(mut map) = self.tenants.write() {
            *map.entry(key.to_vec())
                .or_default()
                .entry(kind)
                .or_insert(0) += 1;
        }
    }

    /// Drop counts for namespaces no longer in the tenant table, so a
    /// removed tenant's series stops being exported. Same contract as
    /// `LatencyHistograms::retain`. `-` is never dropped: it is not a
    /// tenant and nothing will ever "keep" it.
    pub fn retain(&self, keep: &HashSet<Vec<u8>>) {
        if let Ok(mut map) = self.tenants.write() {
            map.retain(|ns, _| ns == b"-" || keep.contains(ns));
        }
    }

    /// Report lines: `<ns> <kind> <count>`.
    ///
    /// `scope` = Some(ns): only that tenant's counts; None: everything.
    /// Same scoping contract as PROXYHOTKEYS and PROXYLATENCY, and for the
    /// same reason — a tenant's error profile is tenant data. It leaks
    /// their client's health and, via QUOTA, their commercial limits.
    pub fn report(&self, scope: Option<&[u8]>) -> String {
        let Ok(map) = self.tenants.read() else {
            return String::new();
        };
        let mut out = String::new();
        let mut rows: Vec<(&Vec<u8>, &HashMap<&'static str, u64>)> = map.iter().collect();
        // Sorted so the output is stable between scrapes; an exporter diff
        // that reorders every poll is unreadable.
        rows.sort_by(|a, b| a.0.cmp(b.0));
        for (ns, kinds) in rows {
            if scope.is_some_and(|s| s != ns.as_slice()) {
                continue;
            }
            let ns_s = String::from_utf8_lossy(ns);
            for k in KINDS {
                if let Some(n) = kinds.get(k) {
                    out.push_str(&format!("{ns_s} {k} {n}\r\n"));
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_takes_the_code_not_the_text() {
        assert_eq!(classify("NOAUTH Authentication required"), "NOAUTH");
        assert_eq!(classify("MOVED 1234 10.0.0.1:7001"), "MOVED");
        assert_eq!(classify("ERR unknown command 'BLAH'"), "ERR");
    }

    #[test]
    fn unknown_prefixes_collapse_rather_than_mint_series() {
        // The whole point of the closed set: a client can provoke odd
        // error text, and none of it may become a label value.
        assert_eq!(classify("WEIRDCODE something"), "other");
        assert_eq!(classify(""), "other");
        assert_eq!(classify("err lowercase is not the code"), "other");
    }

    #[test]
    fn pre_auth_errors_are_not_blamed_on_a_tenant() {
        let e = ErrorCounts::default();
        e.observe(None, "NOAUTH");
        e.observe(Some(b"acme"), "ERR");
        let all = e.report(None);
        assert!(all.contains("- NOAUTH 1"), "{all}");
        assert!(all.contains("acme ERR 1"), "{all}");
        // A tenant sees only its own.
        let scoped = e.report(Some(b"acme"));
        assert!(scoped.contains("acme ERR 1"));
        assert!(!scoped.contains("NOAUTH"), "{scoped}");
    }

    #[test]
    fn retain_drops_removed_tenants_but_keeps_the_unauthed_bucket() {
        let e = ErrorCounts::default();
        e.observe(Some(b"gone"), "ERR");
        e.observe(Some(b"stays"), "ERR");
        e.observe(None, "NOAUTH");
        let mut keep = HashSet::new();
        keep.insert(b"stays".to_vec());
        e.retain(&keep);
        let all = e.report(None);
        assert!(!all.contains("gone"), "{all}");
        assert!(all.contains("stays ERR 1"), "{all}");
        assert!(all.contains("- NOAUTH 1"), "{all}");
    }
}
