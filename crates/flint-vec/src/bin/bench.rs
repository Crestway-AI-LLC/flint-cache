// SPDX-License-Identifier: Elastic-2.0
//! Flat-vs-HNSW measurement for flint-vec: index build time, end-to-end
//! `VEC.SEARCH` latency, and HNSW recall against the flat EXACT oracle, across
//! corpus sizes. This is the payoff of building flat first — flat's top-k IS
//! ground truth, so recall needs no external label set.
//!
//! It drives the real [`Store`] command path (`plan`/`commit`), so the search
//! latency includes query parsing, exactly as a served `VEC.SEARCH` would —
//! the honest end-to-end number, not raw algorithm time. Synthetic vectors from
//! a fixed-seed xorshift, so two runs on the same box are comparable.
//!
//! Usage: bench [--sizes 1000,10000,100000] [--dim 128] [--queries 200]
//!              [--k 10] [--ef 64] [--metric cosine|l2|ip]
//! Not wired into any gate — it allocates a corpus and takes seconds; run it by
//! hand when the index engine or its parameters change.

use flint_resp::Value;
use flint_vec::{Plan, Store};
use std::time::Instant;

/// Deterministic xorshift64 — a bench must be reproducible, and pulling in a
/// PRNG crate for uniform noise is not worth the dependency.
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    /// A float in [-1, 1).
    fn unit(&mut self) -> f32 {
        ((self.next_u64() >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    }
    fn vec(&mut self, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| self.unit()).collect()
    }
}

/// One synthetic vector. With no centroids it is uniform random — the
/// ANN-adversarial worst case (no structure, so true neighbours are barely
/// nearer than random). With centroids it is a random centroid plus small
/// noise: low intrinsic dimensionality, the way real embeddings actually sit
/// (they cluster by topic), where an approximate index earns its recall.
fn synth(rng: &mut Rng, dim: usize, centroids: &Option<Vec<Vec<f32>>>, spread: f32) -> Vec<f32> {
    match centroids {
        None => rng.vec(dim),
        Some(cs) => {
            let c = &cs[(rng.next_u64() as usize) % cs.len()];
            c.iter().map(|&x| x + rng.unit() * spread).collect()
        }
    }
}

/// `count` synthetic vectors, formatted for the command path.
fn corpus(
    rng: &mut Rng,
    count: usize,
    dim: usize,
    centroids: &Option<Vec<Vec<f32>>>,
    spread: f32,
) -> Vec<String> {
    (0..count)
        .map(|_| vec_str(&synth(rng, dim, centroids, spread)))
        .collect()
}

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

/// Comma-separated floats, the format `parse_vector` accepts and `VEC.GET`
/// emits — so a query string round-trips through the real command path.
fn vec_str(v: &[f32]) -> String {
    v.iter()
        .map(|f| f.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// Run a command through the two-phase path (commit a write, return a reply),
/// as the co-processor would against a store that never sheds.
fn exec(st: &mut Store, ns: &[u8], args: &[Vec<u8>]) -> Value {
    // The bench uses no TTLs, so a fixed t=0 clock is sufficient.
    match st.plan(ns, args, 0) {
        Plan::Reply(v) => v,
        Plan::Write { apply, ok, .. } => {
            st.commit(ns, apply);
            ok
        }
    }
}

/// The ids of a `VEC.SEARCH` reply, nearest first.
fn result_ids(v: &Value) -> Vec<Vec<u8>> {
    let mut ids = Vec::new();
    if let Value::Array(Some(rows)) = v {
        for row in rows {
            if let Value::Array(Some(pair)) = row
                && let Some(Value::Bulk(Some(id))) = pair.first()
            {
                ids.push(id.clone());
            }
        }
    }
    ids
}

/// p50/p99/mean of a latency sample, in microseconds.
fn stats(mut xs: Vec<u128>) -> (u128, u128, u128) {
    xs.sort_unstable();
    let n = xs.len().max(1);
    let p = |q: f64| xs[((q * (n as f64 - 1.0)).round() as usize).min(n - 1)];
    let mean = xs.iter().sum::<u128>() / n as u128;
    (p(0.50), p(0.99), mean)
}

/// Build a set of `kind` (flat|hnsw) holding `vecs`, timed. Returns build ms.
fn build(
    st: &mut Store,
    ns: &[u8],
    set: &str,
    kind: &str,
    dim: usize,
    metric: &str,
    vecs: &[String],
) -> u128 {
    let mut create = vec![
        b("VEC.CREATE"),
        b(set),
        b("DIM"),
        b(&dim.to_string()),
        b("METRIC"),
        b(metric),
    ];
    if kind == "hnsw" {
        create.push(b("INDEX"));
        create.push(b("hnsw"));
    }
    let t0 = Instant::now();
    exec(st, ns, &create);
    for (i, vs) in vecs.iter().enumerate() {
        exec(st, ns, &[b("VEC.SET"), b(set), b(&format!("v{i}")), b(vs)]);
    }
    t0.elapsed().as_millis()
}

/// Search `set` for each query, timing each server-side call. Returns
/// (per-query result-id lists, per-query latency µs).
fn search_all(
    st: &Store,
    ns: &[u8],
    set: &str,
    queries: &[String],
    k: usize,
    ef: usize,
) -> (Vec<Vec<Vec<u8>>>, Vec<u128>) {
    let (mut all_ids, mut lat) = (
        Vec::with_capacity(queries.len()),
        Vec::with_capacity(queries.len()),
    );
    let ef_s = ef.to_string();
    let k_s = k.to_string();
    for q in queries {
        // Pre-build the arg vector so only plan() (parse + search) is timed.
        let args = [b("VEC.SEARCH"), b(set), b(q), b(&k_s), b("EF"), b(&ef_s)];
        let t0 = Instant::now();
        let reply = st.plan(ns, &args, 0);
        lat.push(t0.elapsed().as_micros());
        let Plan::Reply(v) = reply else {
            unreachable!("SEARCH is a read")
        };
        all_ids.push(result_ids(&v));
    }
    (all_ids, lat)
}

/// recall@k of `approx` against the exact `oracle`, averaged over queries.
fn recall(oracle: &[Vec<Vec<u8>>], approx: &[Vec<Vec<u8>>], k: usize) -> f64 {
    let mut sum = 0.0;
    for (o, a) in oracle.iter().zip(approx) {
        let hits = a.iter().filter(|id| o.contains(id)).count();
        sum += hits as f64 / k as f64;
    }
    sum / oracle.len().max(1) as f64
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let sizes: Vec<usize> = arg(&a, "--sizes")
        .unwrap_or_else(|| "1000,10000,100000".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let dim: usize = arg(&a, "--dim").and_then(|s| s.parse().ok()).unwrap_or(128);
    let queries: usize = arg(&a, "--queries")
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let k: usize = arg(&a, "--k").and_then(|s| s.parse().ok()).unwrap_or(10);
    let ef: usize = arg(&a, "--ef").and_then(|s| s.parse().ok()).unwrap_or(64);
    let metric = arg(&a, "--metric").unwrap_or_else(|| "cosine".into());
    // --clusters 0 (default) = uniform random, the ANN worst case. --clusters C
    // = C-centroid structured data, the realistic-embedding case.
    let clusters: usize = arg(&a, "--clusters")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let spread: f32 = arg(&a, "--spread")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.10);
    let data = if clusters == 0 {
        "uniform-random (ANN worst case)".to_string()
    } else {
        format!("{clusters} clusters, spread {spread} (embedding-like)")
    };

    println!(
        "flint-vec bench — metric={metric} dim={dim} queries={queries} k={k} ef={ef}\n\
         data: {data}\n\
         (end-to-end VEC.SEARCH via the command path; flat top-{k} is the recall oracle)\n"
    );
    println!(
        "{:>8}  {:>9}  {:>9}  {:>22}  {:>22}  {:>9}",
        "N", "flat ms", "hnsw ms", "flat µs p50/p99/mean", "hnsw µs p50/p99/mean", "recall"
    );

    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for &n in &sizes {
        // Fresh store + fresh vectors per size (the rng advances, so sizes do
        // not share a prefix — each N is an independent corpus).
        let mut st = Store::new();
        let ns = b("bench");
        // Centroids drawn first (from the same stream) so data and queries of
        // this size share them — a query near a centroid has real neighbours.
        let centroids: Option<Vec<Vec<f32>>> =
            (clusters > 0).then(|| (0..clusters).map(|_| rng.vec(dim)).collect());
        let vecs = corpus(&mut rng, n, dim, &centroids, spread);
        let queries_s = corpus(&mut rng, queries, dim, &centroids, spread);

        let flat_ms = build(&mut st, &ns, "f", "flat", dim, &metric, &vecs);
        let hnsw_ms = build(&mut st, &ns, "h", "hnsw", dim, &metric, &vecs);

        let (oracle, flat_lat) = search_all(&st, &ns, "f", &queries_s, k, ef);
        let (approx, hnsw_lat) = search_all(&st, &ns, "h", &queries_s, k, ef);

        let (f50, f99, fm) = stats(flat_lat);
        let (h50, h99, hm) = stats(hnsw_lat);
        let r = recall(&oracle, &approx, k);
        println!(
            "{n:>8}  {flat_ms:>9}  {hnsw_ms:>9}  {:>22}  {:>22}  {r:>9.3}",
            format!("{f50}/{f99}/{fm}"),
            format!("{h50}/{h99}/{hm}"),
        );
    }

    // EF sweep at the largest corpus: recall and latency are a dial, and the
    // point of HNSW over flat is choosing where on it to sit.
    if let Some(&n) = sizes.iter().max() {
        let mut st = Store::new();
        let ns = b("sweep");
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        let centroids: Option<Vec<Vec<f32>>> =
            (clusters > 0).then(|| (0..clusters).map(|_| rng.vec(dim)).collect());
        let vecs = corpus(&mut rng, n, dim, &centroids, spread);
        let queries_s = corpus(&mut rng, queries, dim, &centroids, spread);
        build(&mut st, &ns, "f", "flat", dim, &metric, &vecs);
        build(&mut st, &ns, "h", "hnsw", dim, &metric, &vecs);
        let (oracle, _) = search_all(&st, &ns, "f", &queries_s, k, ef);

        println!("\nEF sweep @ N={n} (hnsw):");
        println!("{:>6}  {:>9}  {:>16}", "ef", "recall", "µs p50/p99/mean");
        for &e in &[16usize, 32, 64, 128, 256] {
            let (approx, lat) = search_all(&st, &ns, "h", &queries_s, k, e);
            let (p50, p99, mean) = stats(lat);
            let r = recall(&oracle, &approx, k);
            println!("{e:>6}  {r:>9.3}  {:>16}", format!("{p50}/{p99}/{mean}"));
        }
    }
}
