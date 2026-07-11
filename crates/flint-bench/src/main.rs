//! flint-bench: the p99.9-under-adversity measurement instrument.
//!
//! Phases (run all, or pick with --phase):
//!   fill     — sequential load of the dataset (unsynced batches)
//!   mixed    — read/write mix at steady state
//!   compact  — the same mix while a full manual compaction runs
//!   ckpt     — the same mix while a checkpoint is created
//!   sync     — sync-write (fsync-before-ack) latency; RocksDB group-commits
//!              concurrent sync writers, so this measures the ack path
//!
//! Numbers on macOS are NOT representative (different fsync semantics,
//! desktop SSD); the decision run happens on an EC2 i4i/r7gd with local
//! NVMe. This binary is kept simple and deterministic so the same rig runs
//! in both places.
//!
//! Usage:
//!   flint-bench --dir /nvme/bench --keys 1000000 --value-size 512 \
//!               --threads 4 --secs 20 --phase all

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use rand::{Rng, SeedableRng, rngs::SmallRng};
use rocksdb::{DB, Options, WriteBatch, WriteOptions};

struct Config {
    dir: String,
    keys: u64,
    value_size: usize,
    threads: usize,
    secs: u64,
    read_pct: u32,
    phase: String,
}

fn arg<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::args()
        .skip_while(|a| a != name)
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let cfg = Config {
        dir: arg(
            "--dir",
            std::env::temp_dir()
                .join("flint-bench")
                .display()
                .to_string(),
        ),
        keys: arg("--keys", 1_000_000),
        value_size: arg("--value-size", 512),
        threads: arg("--threads", 4),
        secs: arg("--secs", 15),
        read_pct: arg("--read-pct", 90),
        phase: arg("--phase", "all".to_string()),
    };
    println!(
        "dir={} keys={} value={}B threads={} secs={} mix={}r/{}w phase={}",
        cfg.dir,
        cfg.keys,
        cfg.value_size,
        cfg.threads,
        cfg.secs,
        cfg.read_pct,
        100 - cfg.read_pct,
        cfg.phase
    );

    let _ = std::fs::remove_dir_all(&cfg.dir);
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.increase_parallelism(cfg.threads as i32);
    let db = Arc::new(DB::open(&opts, &cfg.dir).expect("open db"));

    let run = |name: &str| cfg.phase == "all" || cfg.phase == name;

    if run("fill") || cfg.phase == "all" {
        phase_fill(&db, &cfg);
    }
    if run("mixed") {
        phase_mixed(&db, &cfg, "mixed (steady state)", || {}, || {});
    }
    if run("compact") {
        let db2 = Arc::clone(&db);
        phase_mixed(
            &db,
            &cfg,
            "mixed + full compaction",
            move || db2.compact_range(None::<&[u8]>, None::<&[u8]>),
            || {},
        );
    }
    if run("ckpt") {
        let db2 = Arc::clone(&db);
        let ckpt_dir = format!("{}-ckpt", cfg.dir);
        let _ = std::fs::remove_dir_all(&ckpt_dir);
        phase_mixed(
            &db,
            &cfg,
            "mixed + checkpoint",
            move || {
                rocksdb::checkpoint::Checkpoint::new(&db2)
                    .expect("ckpt handle")
                    .create_checkpoint(&ckpt_dir)
                    .expect("create ckpt");
            },
            || {},
        );
    }
    if run("sync") {
        phase_sync(&db, &cfg);
    }
}

/// Incompressible pseudorandom value — constant-byte values compress ~15:1
/// and let the whole dataset hide in RAM, silently invalidating any
/// beyond-RAM read measurement.
fn rand_value(rng: &mut SmallRng, size: usize) -> Vec<u8> {
    let mut v = vec![0u8; size];
    rng.fill(&mut v[..]);
    v
}

fn key(i: u64) -> Vec<u8> {
    format!("key:{i:012}").into_bytes()
}

fn phase_fill(db: &Arc<DB>, cfg: &Config) {
    let started = Instant::now();
    let mut rng = SmallRng::seed_from_u64(7);
    let mut batch = WriteBatch::default();
    for i in 0..cfg.keys {
        batch.put(key(i), rand_value(&mut rng, cfg.value_size));
        if batch.len() >= 500 {
            db.write(std::mem::take(&mut batch)).expect("fill write");
        }
    }
    db.write(batch).expect("fill write");
    db.flush().expect("flush");
    let secs = started.elapsed().as_secs_f64();
    println!(
        "fill: {} keys in {:.1}s ({:.0} puts/s)\n",
        cfg.keys,
        secs,
        cfg.keys as f64 / secs
    );
}

/// Runs the read/write mix on worker threads while `disturbance` executes on
/// its own thread; reports separate read and write histograms.
fn phase_mixed(
    db: &Arc<DB>,
    cfg: &Config,
    label: &str,
    disturbance: impl FnOnce() + Send + 'static,
    _after: impl FnOnce(),
) {
    let stop = Arc::new(AtomicBool::new(false));
    let total_ops = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for t in 0..cfg.threads {
        let db = Arc::clone(db);
        let stop = Arc::clone(&stop);
        let total = Arc::clone(&total_ops);
        let (keys, vsize, read_pct) = (cfg.keys, cfg.value_size, cfg.read_pct);
        workers.push(std::thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(42 + t as u64);
            let mut reads = Histogram::<u64>::new(3).expect("hist");
            let mut writes = Histogram::<u64>::new(3).expect("hist");
            while !stop.load(Ordering::Relaxed) {
                let k = key(rng.random_range(0..keys));
                let is_read = rng.random_range(0..100) < read_pct;
                if is_read {
                    let t0 = Instant::now();
                    let _ = db.get(&k);
                    record(&mut reads, t0);
                } else {
                    let val = rand_value(&mut rng, vsize);
                    let t0 = Instant::now();
                    db.put(&k, &val).expect("put");
                    record(&mut writes, t0);
                }
                total.fetch_add(1, Ordering::Relaxed);
            }
            (reads, writes)
        }));
    }
    let started = Instant::now();
    let disturber = std::thread::spawn(disturbance);
    std::thread::sleep(Duration::from_secs(cfg.secs));
    stop.store(true, Ordering::Relaxed);
    let mut reads = Histogram::<u64>::new(3).expect("hist");
    let mut writes = Histogram::<u64>::new(3).expect("hist");
    for w in workers {
        let (r, wr) = w.join().expect("worker");
        reads.add(r).expect("merge");
        writes.add(wr).expect("merge");
    }
    let _ = disturber.join();
    let secs = started.elapsed().as_secs_f64();
    println!("{label}:");
    println!(
        "  throughput: {:.0} ops/s",
        total_ops.load(Ordering::Relaxed) as f64 / secs
    );
    report("reads ", &reads);
    report("writes", &writes);
    println!();
}

/// Sync-write ack latency: every write carries WriteOptions{sync=true};
/// concurrent writers exercise RocksDB's internal group commit.
fn phase_sync(db: &Arc<DB>, cfg: &Config) {
    let stop = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::new();
    for t in 0..cfg.threads {
        let db = Arc::clone(db);
        let stop = Arc::clone(&stop);
        let (keys, vsize) = (cfg.keys, cfg.value_size);
        workers.push(std::thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(1042 + t as u64);
            let mut hist = Histogram::<u64>::new(3).expect("hist");
            let mut wo = WriteOptions::default();
            wo.set_sync(true);
            while !stop.load(Ordering::Relaxed) {
                let k = key(rng.random_range(0..keys));
                let val = rand_value(&mut rng, vsize);
                let mut batch = WriteBatch::default();
                batch.put(&k, &val);
                let t0 = Instant::now();
                db.write_opt(batch, &wo).expect("sync write");
                record(&mut hist, t0);
            }
            hist
        }));
    }
    std::thread::sleep(Duration::from_secs(cfg.secs));
    stop.store(true, Ordering::Relaxed);
    let mut hist = Histogram::<u64>::new(3).expect("hist");
    let mut count = 0u64;
    for w in workers {
        let h = w.join().expect("worker");
        count += h.len();
        hist.add(h).expect("merge");
    }
    println!("sync writes (fsync-before-ack, {} threads):", cfg.threads);
    println!("  throughput: {:.0} acks/s", count as f64 / cfg.secs as f64);
    report("acks  ", &hist);
    println!();
}

fn record(hist: &mut Histogram<u64>, t0: Instant) {
    let us = t0.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
    let _ = hist.record(us.max(1));
}

fn report(label: &str, hist: &Histogram<u64>) {
    if hist.is_empty() {
        println!("  {label}: (no samples)");
        return;
    }
    println!(
        "  {label}: p50={}µs p99={}µs p99.9={}µs max={}µs (n={})",
        hist.value_at_quantile(0.50),
        hist.value_at_quantile(0.99),
        hist.value_at_quantile(0.999),
        hist.max(),
        hist.len()
    );
}
