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
//!              [--quant sq8 [--rerank R] [--vec-dir D]]
//!                (ADR-0049: adds a quantized HNSW arm; with --vec-dir, a second
//!                 one whose full vectors are in a file in D, as step 2 serves)
//!        bench --memory [--sizes ...] [--dim ...] [--vec-dir D]
//!                (heap bytes each kind of set holds per vector, counted by
//!                 this binary's allocator, beside what the D4 meter charges)
//!        bench --data DIR --arm plain|<code>|<code>-disk [--n N] [--queries Q]
//!              [--k 10] [--metric cosine] [--vec-dir D] [--pq-train N]
//!              [--gt-only]
//!                (ADR-0049 verifications 1 and 2 on a real corpus: one set a
//!                 process, its RSS growth, and recall against brute force
//!                 across EF and RERANK; DIR holds base.fbin and query.fbin.
//!                 A code is sq8, bin or pq; `-disk` keeps the full vectors in
//!                 --vec-dir)
//! Not wired into any gate — it allocates a corpus and takes seconds; run it by
//! hand when the index engine or its parameters change.

use flint_resp::Value;
use flint_vec::{Plan, Store};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::time::Instant;

/// Heap bytes this process holds, for `--memory`: requested sizes, so not the
/// allocator's own rounding, and not what the OS reports, which on a host that
/// compresses or swaps memory says less than was allocated.
static HEAP: AtomicIsize = AtomicIsize::new(0);

struct Counting;

// SAFETY: every call is forwarded to `System` unchanged; this only counts.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        HEAP.fetch_add(l.size() as isize, Ordering::Relaxed);
        // SAFETY: the caller's contract, passed through.
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        HEAP.fetch_sub(l.size() as isize, Ordering::Relaxed);
        // SAFETY: the caller's contract, passed through.
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        HEAP.fetch_add(new as isize - l.size() as isize, Ordering::Relaxed);
        // SAFETY: the caller's contract, passed through.
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// `--memory`: for each kind of set, the heap held per vector after `n`
/// inserts, beside the D4 meter's charge for them.
fn memory(sizes: &[usize], dim: usize, metric: &str, vec_dir: Option<&std::path::Path>) {
    println!(
        "{:>8}  {:<22}  {:>14}  {:>14}",
        "N", "set", "heap B/vector", "meter B/vector"
    );
    let mut rng = Rng(0x5EED_0049);
    for &n in sizes {
        let vecs = corpus(&mut rng, n, dim, &None, 0.0);
        let mut arms = vec![
            ("flat", None, "flat"),
            ("hnsw", None, "hnsw"),
            ("hnsw", Some("sq8"), "hnsw sq8, RAM"),
        ];
        if vec_dir.is_some() {
            arms.push(("hnsw", Some("sq8"), "hnsw sq8, --vec-dir"));
        }
        for (kind, quant, label) in arms {
            let ns = b("mem");
            let before = HEAP.load(Ordering::Relaxed);
            let mut st = Store::new();
            if let (Some(d), true) = (vec_dir, label.ends_with("--vec-dir")) {
                std::fs::create_dir_all(d).expect("--vec-dir");
                st.set_vec_dir(d.to_path_buf());
            }
            build(&mut st, &ns, "s", kind, quant, dim, metric, &vecs);
            let held = HEAP.load(Ordering::Relaxed) - before;
            println!(
                "{n:>8}  {label:<22}  {:>14.0}  {:>14.0}",
                held as f64 / n as f64,
                st.ns_mem_bytes(&ns) as f64 / n as f64
            );
        }
    }
}

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
/// `quant` adds `QUANT <q>` (ADR-0049; hnsw only).
#[allow(clippy::too_many_arguments)]
fn build(
    st: &mut Store,
    ns: &[u8],
    set: &str,
    kind: &str,
    quant: Option<&str>,
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
    if let Some(q) = quant {
        create.push(b("QUANT"));
        create.push(b(q));
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
    rerank: usize,
) -> (Vec<Vec<Vec<u8>>>, Vec<u128>) {
    let (mut all_ids, mut lat) = (
        Vec::with_capacity(queries.len()),
        Vec::with_capacity(queries.len()),
    );
    let ef_s = ef.to_string();
    let k_s = k.to_string();
    let rr_s = rerank.to_string();
    for q in queries {
        // Pre-build the arg vector so only plan() (parse + search) is timed.
        let args = [
            b("VEC.SEARCH"),
            b(set),
            b(q),
            b(&k_s),
            b("EF"),
            b(&ef_s),
            b("RERANK"),
            b(&rr_s),
        ];
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
    if a.iter().any(|x| x == "--data") {
        real_data(&a);
        return;
    }
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
    let quant: Option<String> = arg(&a, "--quant");
    let vec_dir: Option<std::path::PathBuf> = arg(&a, "--vec-dir").map(Into::into);
    if a.iter().any(|x| x == "--memory") {
        memory(&sizes, dim, &metric, vec_dir.as_deref());
        return;
    }
    let rerank: usize = arg(&a, "--rerank")
        .and_then(|s| s.parse().ok())
        .unwrap_or(4 * k);
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

        let flat_ms = build(&mut st, &ns, "f", "flat", None, dim, &metric, &vecs);
        let hnsw_ms = build(&mut st, &ns, "h", "hnsw", None, dim, &metric, &vecs);

        let (oracle, flat_lat) = search_all(&st, &ns, "f", &queries_s, k, ef, k);
        let (approx, hnsw_lat) = search_all(&st, &ns, "h", &queries_s, k, ef, k);

        let (f50, f99, fm) = stats(flat_lat);
        let (h50, h99, hm) = stats(hnsw_lat);
        let r = recall(&oracle, &approx, k);
        println!(
            "{n:>8}  {flat_ms:>9}  {hnsw_ms:>9}  {:>22}  {:>22}  {r:>9.3}",
            format!("{f50}/{f99}/{fm}"),
            format!("{h50}/{h99}/{hm}"),
        );
        if let Some(q) = quant.as_deref() {
            // Each quantized arm in a Store of its own, so its D4 meter is
            // the set's alone.
            let mut arms = vec![(None, "vectors in RAM")];
            if let Some(d) = vec_dir.as_ref() {
                arms.push((Some(d), "vectors in --vec-dir"));
            }
            for (dir, label) in arms {
                let mut sq = Store::new();
                if let Some(d) = dir {
                    std::fs::create_dir_all(d).expect("--vec-dir");
                    sq.set_vec_dir(d.clone());
                }
                let q_ms = build(&mut sq, &ns, "q", "hnsw", Some(q), dim, &metric, &vecs);
                let (qids, q_lat) = search_all(&sq, &ns, "q", &queries_s, k, ef, rerank);
                let (q50, q99, qm) = stats(q_lat);
                let qr = recall(&oracle, &qids, k);
                println!(
                    "{:>8}  {:>9}  {q_ms:>9}  {:>22}  {:>22}  {qr:>9.3}   <- hnsw QUANT {q} RERANK {rerank}, {label}, meter {} MB",
                    "",
                    "",
                    "",
                    format!("{q50}/{q99}/{qm}"),
                    sq.ns_mem_bytes(&ns) / (1024 * 1024),
                );
            }
        }
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
        build(&mut st, &ns, "f", "flat", None, dim, &metric, &vecs);
        build(&mut st, &ns, "h", "hnsw", None, dim, &metric, &vecs);
        let (oracle, _) = search_all(&st, &ns, "f", &queries_s, k, ef, k);

        println!("\nEF sweep @ N={n} (hnsw):");
        println!("{:>6}  {:>9}  {:>16}", "ef", "recall", "µs p50/p99/mean");
        for &e in &[16usize, 32, 64, 128, 256] {
            let (approx, lat) = search_all(&st, &ns, "h", &queries_s, k, e, k);
            let (p50, p99, mean) = stats(lat);
            let r = recall(&oracle, &approx, k);
            println!("{e:>6}  {r:>9.3}  {:>16}", format!("{p50}/{p99}/{mean}"));
        }
    }
}

/// A float32 vector file mapped read-only: `u32` count, `u32` dim, then
/// count x dim little-endian floats (the big-ann-benchmarks `.fbin` layout).
/// Mapped rather than read so the arms run as separate processes share one
/// copy in the page cache, and so it shows as file-backed memory, not in the
/// anonymous RSS the measurement is about.
struct Fbin {
    map: *const u8,
    len: usize,
    n: usize,
    dim: usize,
}

// SAFETY: the mapping is read-only and lives as long as the process, so any
// thread may read it.
unsafe impl Sync for Fbin {}

impl Fbin {
    fn open(path: &std::path::Path) -> Fbin {
        use std::os::fd::AsRawFd;
        let f = std::fs::File::open(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let len = f.metadata().expect("metadata").len() as usize;
        // SAFETY: a read-only private mapping of a file this process opened,
        // kept for the life of the process and never written through.
        let map = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                f.as_raw_fd(),
                0,
            )
        };
        assert!(map != libc::MAP_FAILED, "mmap {}", path.display());
        let map = map as *const u8;
        // SAFETY: the mapping is at least the 8-byte header (checked below).
        let head = unsafe { std::slice::from_raw_parts(map, 8.min(len)) };
        assert_eq!(
            head.len(),
            8,
            "{} is shorter than its header",
            path.display()
        );
        let n = u32::from_le_bytes([head[0], head[1], head[2], head[3]]) as usize;
        let dim = u32::from_le_bytes([head[4], head[5], head[6], head[7]]) as usize;
        assert_eq!(
            len,
            8 + n * dim * 4,
            "{}: size disagrees with its header",
            path.display()
        );
        Fbin { map, len, n, dim }
    }

    fn row(&self, i: usize) -> &[f32] {
        assert!(i < self.n && 8 + (i + 1) * self.dim * 4 <= self.len);
        // SAFETY: within the mapping (asserted at open); offset 8 + 4k is
        // 4-byte aligned on a page-aligned mapping; the host is little-endian,
        // as every target this repo builds for is.
        unsafe {
            std::slice::from_raw_parts(self.map.add(8 + i * self.dim * 4) as *const f32, self.dim)
        }
    }
}

/// Anonymous resident memory of this process (Linux `RssAnon`), in bytes.
fn rss_anon() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = s.lines().find(|l| l.starts_with("RssAnon:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// Score where HIGHER is nearer, as flint-vec answers `VEC.SEARCH`.
fn exact_score(metric: &str, q: &[f32], qn: f32, v: &[f32], vn: f32) -> f32 {
    let dot: f32 = q.iter().zip(v).map(|(a, b)| a * b).sum();
    match metric {
        "l2" => -q.iter().zip(v).map(|(a, b)| (a - b) * (a - b)).sum::<f32>(),
        "ip" => dot,
        _ => {
            if qn == 0.0 || vn == 0.0 {
                0.0
            } else {
                dot / (qn * vn)
            }
        }
    }
}

/// The exact top-`k` ids of each query over the first `n` base rows, by
/// brute force on every core, cached in `dir` because it is the same for
/// every arm.
fn ground_truth(
    dir: &std::path::Path,
    base: &Fbin,
    n: usize,
    queries: &Fbin,
    nq: usize,
    k: usize,
    metric: &str,
) -> Vec<Vec<u32>> {
    let cache = dir.join(format!("gt-n{n}-q{nq}-k{k}-{metric}.ibin"));
    if let Ok(bytes) = std::fs::read(&cache)
        && bytes.len() == nq * k * 4
    {
        let ids: Vec<u32> = bytes
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        return ids.chunks(k).map(|c| c.to_vec()).collect();
    }
    let t0 = Instant::now();
    let norm = |v: &[f32]| v.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norms: Vec<f32> = (0..n).map(|i| norm(base.row(i))).collect();
    let threads = std::thread::available_parallelism().map_or(1, |p| p.get());
    let qids: Vec<usize> = (0..nq).collect();
    let mut gt = vec![Vec::new(); nq];
    std::thread::scope(|sc| {
        let handles: Vec<_> = qids
            .chunks(nq.div_ceil(threads))
            .map(|chunk| {
                let norms = &norms;
                sc.spawn(move || {
                    chunk
                        .iter()
                        .map(|&qi| {
                            let q = queries.row(qi);
                            let qn = norm(q);
                            let mut top: Vec<(f32, u32)> = Vec::with_capacity(k + 1);
                            for (i, &vn) in norms.iter().enumerate() {
                                let s = exact_score(metric, q, qn, base.row(i), vn);
                                if top.len() < k || s > top[top.len() - 1].0 {
                                    let at = top.partition_point(|&(t, _)| t >= s);
                                    top.insert(at, (s, i as u32));
                                    top.truncate(k);
                                }
                            }
                            (qi, top.into_iter().map(|(_, i)| i).collect::<Vec<u32>>())
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        for h in handles {
            for (qi, ids) in h.join().expect("ground-truth thread") {
                gt[qi] = ids;
            }
        }
    });
    let bytes: Vec<u8> = gt.iter().flatten().flat_map(|i| i.to_le_bytes()).collect();
    let _ = std::fs::write(&cache, bytes);
    eprintln!(
        "ground truth: {nq} queries over {n} in {:.0} s, {threads} threads",
        t0.elapsed().as_secs_f64()
    );
    gt
}

/// `--data`: ADR-0049 verifications 1 and 2 on a real corpus. One set a run
/// (`--arm`), so this process's RSS growth is that set's alone.
fn real_data(a: &[String]) {
    let dir = std::path::PathBuf::from(arg(a, "--data").expect("--data DIR"));
    let arm = arg(a, "--arm").unwrap_or_else(|| "plain".into());
    let k: usize = arg(a, "--k").and_then(|s| s.parse().ok()).unwrap_or(10);
    let metric = arg(a, "--metric").unwrap_or_else(|| "cosine".into());
    let base = Fbin::open(&dir.join("base.fbin"));
    let queries = Fbin::open(&dir.join("query.fbin"));
    assert_eq!(base.dim, queries.dim, "base and queries disagree on dim");
    let n: usize = arg(a, "--n")
        .and_then(|s| s.parse().ok())
        .unwrap_or(base.n)
        .min(base.n);
    let nq: usize = arg(a, "--queries")
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
        .min(queries.n);
    let dim = base.dim;
    let gt = ground_truth(&dir, &base, n, &queries, nq, k, &metric);
    if a.iter().any(|x| x == "--gt-only") {
        return;
    }

    let ns = b("real");
    let mut st = Store::new();
    let (code, disk) = match arm.strip_suffix("-disk") {
        Some(c) => (c, true),
        None => (arm.as_str(), false),
    };
    let quant = match code {
        "plain" if !disk => None,
        "sq8" | "bin" | "pq" => Some(code),
        _ => panic!("--arm plain|<code>|<code>-disk, the code sq8, bin or pq; not {arm}"),
    };
    if disk {
        let d = std::path::PathBuf::from(arg(a, "--vec-dir").expect("a -disk arm needs --vec-dir"));
        std::fs::create_dir_all(&d).expect("--vec-dir");
        st.set_vec_dir(d);
    }
    if let Some(t) = arg(a, "--pq-train").and_then(|s| s.parse().ok()) {
        st.set_pq_train(t);
    }
    let mut create = vec![
        b("VEC.CREATE"),
        b("s"),
        b("DIM"),
        b(&dim.to_string()),
        b("METRIC"),
        b(&metric),
        b("INDEX"),
        b("hnsw"),
    ];
    if let Some(q) = quant {
        create.extend([b("QUANT"), b(q)]);
    }
    exec(&mut st, &ns, &create);
    let (rss0, heap0) = (rss_anon(), HEAP.load(Ordering::Relaxed));
    let t0 = Instant::now();
    // The slowest single VEC.SET, and when: a PQ set trains inside one.
    let (mut slowest, mut slowest_at) = (0f64, 0usize);
    for i in 0..n {
        let t = Instant::now();
        exec(
            &mut st,
            &ns,
            &[
                b("VEC.SET"),
                b("s"),
                b(&i.to_string()),
                b(&vec_str(base.row(i))),
            ],
        );
        let took = t.elapsed().as_secs_f64();
        if took > slowest {
            (slowest, slowest_at) = (took, i + 1);
        }
        if (i + 1) % (n / 10).max(1) == 0 {
            eprintln!(
                "{arm}: {} of {n} in {:.0} s",
                i + 1,
                t0.elapsed().as_secs_f64()
            );
        }
    }
    let build = t0.elapsed().as_secs_f64();
    let heap = (HEAP.load(Ordering::Relaxed) - heap0) as f64 / n as f64;
    let rss = match (rss0, rss_anon()) {
        (Some(r0), Some(r1)) => format!("{:.0}", (r1 as f64 - r0 as f64) / n as f64),
        _ => "n/a".into(),
    };
    println!(
        "arm={arm} n={n} dim={dim} metric={metric} build={build:.0}s rss_B_per_vector={rss} heap_B_per_vector={heap:.0} meter_B_per_vector={:.0} slowest_set_ms={:.0} at={slowest_at}",
        st.ns_mem_bytes(&ns) as f64 / n as f64,
        slowest * 1e3
    );

    // Verification 2's depths, 2k to 10k, and 20k for the codes that may need
    // more than that.
    let reranks: Vec<usize> = match quant {
        None => vec![k],
        Some(_) => vec![2 * k, 4 * k, 10 * k, 20 * k],
    };
    for ef in [64usize, 128, 256] {
        for &rr in &reranks {
            let (mut hits, mut lat) = (0usize, Vec::with_capacity(nq));
            for (qi, truth) in gt.iter().enumerate().take(nq) {
                let args = [
                    b("VEC.SEARCH"),
                    b("s"),
                    b(&vec_str(queries.row(qi))),
                    b(&k.to_string()),
                    b("EF"),
                    b(&ef.to_string()),
                    b("RERANK"),
                    b(&rr.to_string()),
                ];
                let t = Instant::now();
                let reply = st.plan(&ns, &args, 0);
                lat.push(t.elapsed().as_micros());
                let Plan::Reply(v) = reply else {
                    panic!("a search is a reply")
                };
                hits += result_ids(&v)
                    .iter()
                    .filter_map(|id| std::str::from_utf8(id).ok()?.parse::<u32>().ok())
                    .filter(|id| truth.contains(id))
                    .count();
            }
            let (p50, p99, _) = stats(lat);
            println!(
                "arm={arm} n={n} ef={ef} rerank={rr} recall@{k}={:.4} p50_us={p50} p99_us={p99}",
                hits as f64 / (nq * k) as f64
            );
        }
    }
}
