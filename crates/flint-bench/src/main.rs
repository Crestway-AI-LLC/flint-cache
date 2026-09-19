// SPDX-License-Identifier: Elastic-2.0
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

/// RocksDB's own defaults, which are what an untuned L0 is compared against.
/// Named here so a report can say "reached 19 of 20" instead of a bare count.
const L0_SLOWDOWN_TRIGGER: u64 = 20;
const L0_STOP_TRIGGER: u64 = 36;

/// What counts as a throughput collapse, as a percentage of the first
/// quarter. Arbitrary, which is why every verdict prints it: a 25 GB local
/// fill measured 59% (INCONCLUSIVE here, FALSIFIED at a 65% line), and a
/// reader who disagrees has the full per-interval table to disagree with.
const COLLAPSE_PCT: f64 = 50.0;

/// One reading of what the engine was doing partway through a fill.
struct FillSample {
    at_secs: f64,
    interval_rate: f64,
    l0: Option<u64>,
    pending_bytes: Option<u64>,
    stopped: Option<u64>,
    delayed_rate: Option<u64>,
}

/// BUG-0013's instrument.
///
/// This used to print one number — total keys over total seconds — and that
/// is the shape that made the 3 GB run unscoreable. The bug asks whether a
/// bulk load builds a compaction backlog until RocksDB applies back-pressure,
/// and an aggregate cannot answer it: degradation WITHIN a pass is invisible,
/// and `write_stopped: 0` is indistinguishable from an instrument that was
/// never exercised.
///
/// So the fill samples as it runs. The property strings are the same ones
/// `flint-storage/src/rocks.rs` reads for FLINTINFO, deliberately, so a run
/// here and a run against a server are answering with the same numbers.
///
/// `None` is reported as `-` rather than 0. A zero from an engine that cannot
/// answer is worth nothing, which is the distinction BUG-0022 exists for.
fn sample_engine(db: &Arc<DB>, at_secs: f64, interval_rate: f64) -> FillSample {
    let prop = |name: &str| db.property_int_value(name).ok().flatten();
    FillSample {
        at_secs,
        interval_rate,
        l0: prop("rocksdb.num-files-at-level0"),
        pending_bytes: prop("rocksdb.estimate-pending-compaction-bytes"),
        stopped: prop("rocksdb.is-write-stopped"),
        delayed_rate: prop("rocksdb.actual-delayed-write-rate"),
    }
}

fn phase_fill(db: &Arc<DB>, cfg: &Config) {
    let started = Instant::now();
    let mut rng = SmallRng::seed_from_u64(7);
    let mut batch = WriteBatch::default();
    let mut samples: Vec<FillSample> = Vec::new();
    let mut last_at = 0.0f64;
    let mut last_keys = 0u64;

    for i in 0..cfg.keys {
        batch.put(key(i), rand_value(&mut rng, cfg.value_size));
        if batch.len() >= 500 {
            db.write(std::mem::take(&mut batch)).expect("fill write");
            // Time-based rather than key-based: under a stall the key rate is
            // exactly what collapses, so sampling every N keys would sample
            // the interesting region least often.
            let at = started.elapsed().as_secs_f64();
            if at - last_at >= 2.0 {
                let rate = (i - last_keys) as f64 / (at - last_at);
                samples.push(sample_engine(db, at, rate));
                last_at = at;
                last_keys = i;
            }
        }
    }
    db.write(batch).expect("fill write");
    db.flush().expect("flush");
    let secs = started.elapsed().as_secs_f64();
    if secs - last_at > 0.0 && cfg.keys > last_keys {
        let rate = (cfg.keys - last_keys) as f64 / (secs - last_at);
        samples.push(sample_engine(db, secs, rate));
    }

    println!(
        "fill: {} keys in {:.1}s ({:.0} puts/s overall)",
        cfg.keys,
        secs,
        cfg.keys as f64 / secs
    );
    report_fill(&samples);
}

/// Print the samples and score them, the three ways BUG-0013 requires.
///
/// PHASES ARE NEVER JOINED. The first automated verdict on this bug read
/// FALSIFIED because a script appended a refill's rates to the fill's and
/// compared the fill's opening against the refill's close — two workloads,
/// scored as one. This function is handed ONE phase's samples and has no way
/// to see another, which is the structural version of that lesson rather than
/// a comment asking the next person to be careful.
fn report_fill(samples: &[FillSample]) {
    if samples.len() < 4 {
        println!(
            "  (only {} sample(s) — too short to score; raise --keys)\n",
            samples.len()
        );
        return;
    }
    println!("  t(s)   interval/s     L0  pending MB  stopped  delayed/s");
    for s in samples {
        let fmt = |v: Option<u64>| v.map_or("-".to_string(), |x| x.to_string());
        println!(
            "  {:>5.1} {:>12.0} {:>6} {:>11} {:>8} {:>10}",
            s.at_secs,
            s.interval_rate,
            fmt(s.l0),
            s.pending_bytes
                .map_or("-".to_string(), |b| format!("{:.0}", b as f64 / 1e6)),
            fmt(s.stopped),
            fmt(s.delayed_rate),
        );
    }

    let quarter = samples.len() / 4;
    let mean = |w: &[FillSample]| w.iter().map(|s| s.interval_rate).sum::<f64>() / w.len() as f64;
    let first = mean(&samples[..quarter.max(1)]);
    let last = mean(&samples[samples.len() - quarter.max(1)..]);
    let stalled = samples
        .iter()
        .any(|s| s.stopped.unwrap_or(0) >= 1 || s.delayed_rate.unwrap_or(0) > 0);
    let max_l0 = samples.iter().filter_map(|s| s.l0).max();
    // Stated, not hidden: "collapsed" is a halving of the interval rate from
    // the first quarter to the last. A reader who disagrees with the threshold
    // has the whole table above to disagree with.
    let collapsed = last < first * (COLLAPSE_PCT / 100.0);

    println!(
        "\n  first-quarter {:.0}/s, last-quarter {:.0}/s ({:+.1}%)",
        first,
        last,
        (last - first) / first * 100.0
    );
    match max_l0 {
        Some(l0) => println!(
            "  max L0 {} of {} (slowdown) / {} (stop)",
            l0, L0_SLOWDOWN_TRIGGER, L0_STOP_TRIGGER
        ),
        None => println!("  max L0 -  (engine did not answer; a zero here would mean nothing)"),
    }

    // Only the middle verdict kills the hypothesis. INCONCLUSIVE is not a
    // near-miss: it means the instrument was never exercised, and a zero from
    // an instrument that never moved is not evidence in either direction.
    // FALSIFIED REQUIRES THE RUN TO HAVE ENTERED THE REGIME, and that clause
    // is not in the bug's three-way rule as written. BUG-0022 predicted the
    // failure -- "its three-way criterion collapses to 'hypothesis dead' on
    // every run" -- and this function reproduced it on its first outing: a
    // 3 GB fill dropped 55% from warm-up alone, never took L0 above 3 of 20,
    // and printed FALSIFIED. A collapse with L0 flat at the bottom says
    // nothing about back-pressure, because back-pressure was never in play;
    // it is a different workload from the 120 GB one the bug is about.
    //
    // So the hypothesis can only be killed by a run that actually pressured
    // compaction: L0 at least half way to the slowdown trigger, AND a
    // collapse, AND no stall. Anything else is INCONCLUSIVE, and the reason
    // says which kind, because "never collapsed" and "collapsed with no
    // backlog at all" are different facts about the run.
    let entered = max_l0.unwrap_or(0) >= L0_SLOWDOWN_TRIGGER / 2;

    // THE VERDICT STATES ITS OWN THRESHOLD, always. Two of the three outcomes
    // turn on one arbitrary number, and a run can land close to it: a local
    // 25 GB fill measured -40.9%, which is INCONCLUSIVE at 50% and FALSIFIED
    // at 35%. A verdict that hides the number it turned on invites a reader to
    // treat it as threshold-free, and this bug has already nearly published
    // one wrong verdict from an instrument nobody could see into.
    let ratio = last / first * 100.0;
    if stalled {
        println!("  VERDICT: CONFIRMED — the engine applied back-pressure during this pass\n");
    } else if collapsed && entered {
        println!(
            "  VERDICT: FALSIFIED — L0 reached the regime and throughput collapsed with no stall \
             signal, so back-pressure is not what capped it (last quarter {ratio:.0}% of first; \
             collapse threshold <{:.0}%)\n",
            COLLAPSE_PCT
        );
    } else if collapsed {
        println!(
            "  VERDICT: INCONCLUSIVE — throughput collapsed, but L0 never rose past {} of {}, so \
             compaction was never under pressure and this collapse is a different workload from \
             the one the hypothesis is about (last quarter {ratio:.0}% of first)\n",
            max_l0.unwrap_or(0),
            L0_SLOWDOWN_TRIGGER
        );
    } else {
        println!(
            "  VERDICT: INCONCLUSIVE — throughput never collapsed, so the stall regime was never \
             reached and the zeros above were never exercised (last quarter {ratio:.0}% of first; \
             collapse threshold <{:.0}%)\n",
            COLLAPSE_PCT
        );
    }
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
