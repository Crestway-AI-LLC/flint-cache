//! Per-tenant latency histograms, measured AT THE PROXY (M4 "client-side
//! metrics", revised: we cannot put code in users' clients, so we measure at
//! the closest point we do control — one hop from the client). What a tenant
//! reads here is "your latency as the proxy saw it"; the difference between
//! this and their own client-side numbers is their network + the proxy's
//! accept queue, a decomposition support conversations actually need.
//!
//! Shape: per (namespace, read|write lane) — the same D1 classifier split
//! used everywhere else — a fixed-bucket cumulative histogram (Prometheus
//! convention), plus sum and count, so Grafana computes per-tenant
//! p50/p99 with histogram_quantile(). Buckets are atomics; the hot path
//! takes a read lock on the tenant map and does two relaxed increments.
//! Timing covers command handling INCLUDING cache hits (D6) and backend
//! retries — tenant-perceived latency, not backend service time.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Bucket upper bounds in MICROSECONDS, cumulative (Prometheus `le`).
/// Spans a cache hit (~100 µs) to a stalled failover retry (~5 s budget).
/// The exporter (flint-agent) renders these as seconds in `le` labels —
/// the two lists must stay in step (see agent metrics.rs LATENCY_LE).
pub const BUCKETS_US: [u64; 12] = [
    250,       // 0.25 ms — cache hit / same-host round trip
    500,       // 0.5 ms
    1_000,     // 1 ms — healthy backend read
    2_000,     // 2 ms
    5_000,     // 5 ms
    10_000,    // 10 ms
    25_000,    // 25 ms
    50_000,    // 50 ms — soft-lag delays land here
    100_000,   // 100 ms
    250_000,   // 250 ms — failover chase territory
    1_000_000, // 1 s
    5_000_000, // 5 s — the proxy retry budget; anything above is +Inf
];

/// One lane's histogram: cumulative-at-read (we store per-bucket counts and
/// cumulate when reporting), plus sum/count for averages and quantile math.
#[derive(Default)]
struct Lane {
    buckets: [AtomicU64; BUCKETS_US.len() + 1], // +1 = +Inf overflow
    sum_us: AtomicU64,
    count: AtomicU64,
}

impl Lane {
    fn observe(&self, us: u64) {
        let idx = BUCKETS_US
            .iter()
            .position(|&b| us <= b)
            .unwrap_or(BUCKETS_US.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// "count sum_us c0 c1 ... cN" with CUMULATIVE bucket counts (le shape).
    fn report(&self) -> String {
        let mut out = format!(
            "{} {}",
            self.count.load(Ordering::Relaxed),
            self.sum_us.load(Ordering::Relaxed)
        );
        let mut cum = 0u64;
        for b in &self.buckets {
            cum += b.load(Ordering::Relaxed);
            out.push(' ');
            out.push_str(&cum.to_string());
        }
        out
    }
}

#[derive(Default)]
struct TenantHist {
    read: Lane,
    write: Lane,
}

#[derive(Default)]
pub struct LatencyHistograms {
    tenants: RwLock<HashMap<Vec<u8>, TenantHist>>,
}

impl LatencyHistograms {
    pub fn observe(&self, ns: &[u8], is_write: bool, us: u64) {
        // Fast path: the tenant already has a histogram (everything after
        // its first command) — read lock + relaxed atomics only.
        if let Ok(map) = self.tenants.read()
            && let Some(h) = map.get(ns)
        {
            let lane = if is_write { &h.write } else { &h.read };
            lane.observe(us);
            return;
        }
        // First command ever for this namespace: create under the write
        // lock, then record. (Namespaces are CP-vetted tenant names, so
        // this map cannot be grown by unauthenticated garbage.)
        if let Ok(mut map) = self.tenants.write() {
            let h = map.entry(ns.to_vec()).or_default();
            let lane = if is_write { &h.write } else { &h.read };
            lane.observe(us);
        }
    }

    /// Drop histograms for namespaces no longer in the tenant table (a
    /// removed tenant's series would otherwise be exported forever).
    pub fn retain(&self, keep: &std::collections::HashSet<Vec<u8>>) {
        if let Ok(mut map) = self.tenants.write() {
            map.retain(|ns, _| keep.contains(ns));
        }
    }

    /// Report lines: `<ns> <read|write> <count> <sum_us> <cum buckets...>`.
    /// `scope` = Some(ns): only that tenant's lanes (the authed view);
    /// None: every tenant (the pre-auth operator view) — the same scoping
    /// contract as PROXYHOTKEYS (latency shapes are tenant data too: they
    /// leak workload patterns).
    pub fn report(&self, scope: Option<&[u8]>) -> String {
        let Ok(map) = self.tenants.read() else {
            return String::new();
        };
        let mut out = String::new();
        for (ns, h) in map.iter() {
            if scope.is_some_and(|s| s != ns.as_slice()) {
                continue;
            }
            let ns = String::from_utf8_lossy(ns);
            out.push_str(&format!("{ns} read {}\r\n", h.read.report()));
            out.push_str(&format!("{ns} write {}\r\n", h.write.report()));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_cumulate_and_scope() {
        let l = LatencyHistograms::default();
        l.observe(b"acme", false, 200); // <=250us
        l.observe(b"acme", false, 900); // <=1ms
        l.observe(b"acme", false, 9_000_000); // +Inf
        l.observe(b"acme", true, 3_000); // write lane, <=5ms
        l.observe(b"globex", false, 100);
        let r = l.report(Some(b"acme"));
        assert!(!r.contains("globex"), "scoped view leaked: {r}");
        let read_line = r
            .lines()
            .find(|l| l.starts_with("acme read"))
            .expect("acme read lane");
        let f: Vec<&str> = read_line.split_whitespace().collect();
        // ns kind count sum_us c(250us) c(500us) c(1ms) ... c(+inf)
        assert_eq!(f[2], "3", "count");
        assert_eq!(f[4], "1", "le 250us");
        assert_eq!(f[6], "2", "le 1ms cumulative");
        assert_eq!(*f.last().expect("buckets"), "3", "+Inf equals count");
        let write_line = r
            .lines()
            .find(|l| l.starts_with("acme write"))
            .expect("acme write lane");
        assert!(write_line.split_whitespace().nth(2) == Some("1"));
        // Operator view carries both tenants.
        let all = l.report(None);
        assert!(all.contains("acme read") && all.contains("globex read"));
    }
}
