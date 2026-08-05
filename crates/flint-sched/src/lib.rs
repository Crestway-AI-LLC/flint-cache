// SPDX-License-Identifier: Elastic-2.0
//! Recurring jobs, as a library — ADR-0011 D8's scheduler.
//!
//! A LIBRARY and not a seat, deliberately: a separate scheduler process is
//! a new failure domain that has to reach into other components to fire
//! them, and needs its own HA story before it protects anything. A library
//! gives one implementation of the parts that are actually hard and no new
//! process to keep alive. This fleet already hand-rolled three of these
//! (the controller's snapshot cadence, the agent's consolidation cron, the
//! GC sweeper); they are migration candidates once backup has proven this
//! one — not before, because an abstraction with one caller is a guess.
//!
//! ## The decisions, each of which is a bug when made the other way
//!
//! **Jobs run on ONE thread, serially.** A job structurally cannot overlap
//! itself — the property ADR-0011's verification item 8 demands, because a
//! backup checkpoint pins live SSTs against compaction reclaim, and two
//! overlapping backups pin twice and present as disk exhaustion on a
//! healthy cluster. Serial across jobs too: backup, verify and rehearsal
//! contend for the same disk and the same bucket, and interleaving them
//! buys nothing but the bill.
//!
//! **The next run is scheduled from COMPLETION, not from the nominal
//! slot.** A backup that ran long must not be followed by a burst of
//! catch-up backups — each would cut a fresh checkpoint of data the
//! previous one just shipped. Missed windows are dropped, visibly (the
//! stats say when the last run finished), never replayed.
//!
//! **Time is monotonic [`Instant`], never the wall clock.** An operator
//! fixing a host's clock must not fire every job at once (jump backward)
//! or starve them for hours (jump forward). Wall time appears only in
//! REPORTS, where humans need it.
//!
//! **Failure retries sooner, with exponential backoff, capped at the
//! interval.** A failed nightly backup that silently waits for tomorrow
//! night doubles the data-loss window; one that hammers a broken endpoint
//! every second is an outage amplifier. Backoff climbs from `retry_after`
//! and never exceeds `every`.
//!
//! **Jitter is added once, at startup.** Its purpose is to keep a fleet of
//! seats provisioned at the same minute from hitting the object store in
//! lockstep forever; per-run jitter would also wander the cadence for no
//! one's benefit.

use std::time::{Duration, Instant};

/// What one job reports about itself. Everything an exporter or a status
/// file needs; nothing here is required for scheduling itself.
#[derive(Debug, Clone, Default)]
pub struct JobStats {
    pub runs: u64,
    pub failures: u64,
    pub consecutive_failures: u32,
    /// The last outcome's message — `Ok(summary)` or `Err(error)`.
    pub last: Option<Result<String, String>>,
    /// Wall-clock ms of the last SUCCESSFUL completion. Wall time, because
    /// this line is read by humans and scraped into alerts; scheduling
    /// itself never touches it.
    pub last_ok_wall_ms: Option<u64>,
}

pub struct Job {
    pub name: String,
    /// Nominal cadence, measured from each run's completion.
    pub every: Duration,
    /// First-failure retry delay; doubles per consecutive failure, capped
    /// at `every`.
    pub retry_after: Duration,
    run: Box<dyn FnMut() -> Result<String, String> + Send>,
}

impl Job {
    pub fn new(
        name: impl Into<String>,
        every: Duration,
        retry_after: Duration,
        run: impl FnMut() -> Result<String, String> + Send + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            every,
            retry_after,
            run: Box::new(run),
        }
    }
}

struct Slot {
    job: Job,
    next: Instant,
    stats: JobStats,
}

pub struct Scheduler {
    slots: Vec<Slot>,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler {
    pub fn new() -> Self {
        Self { slots: Vec::new() }
    }

    /// Add a job. Its first run lands after `initial_delay` — the caller's
    /// startup jitter, decided once (see the module note).
    pub fn add(&mut self, job: Job, initial_delay: Duration) {
        self.slots.push(Slot {
            next: Instant::now() + initial_delay,
            job,
            stats: JobStats::default(),
        });
    }

    /// A deterministic-enough startup jitter in `[0, max)`, from the OS's
    /// randomly seeded hasher — no RNG dependency, no time arithmetic.
    pub fn startup_jitter(max: Duration) -> Duration {
        use std::hash::{BuildHasher, Hasher};
        if max.is_zero() {
            return Duration::ZERO;
        }
        let h = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        Duration::from_millis(h % max.as_millis().max(1) as u64)
    }

    /// Run every job that is due as of `now`, serially, and return when the
    /// next one is due. Extracted from the loop so tests drive time instead
    /// of sleeping through it.
    pub fn tick(&mut self, now: Instant) -> Option<Instant> {
        for slot in &mut self.slots {
            if slot.next > now {
                continue;
            }
            let outcome = (slot.job.run)();
            // Completion-based rescheduling: measure from AFTER the run, so
            // a long run delays its successor instead of stacking on it.
            let done = Instant::now();
            slot.stats.runs += 1;
            match &outcome {
                Ok(_) => {
                    slot.stats.consecutive_failures = 0;
                    slot.stats.last_ok_wall_ms = Some(wall_ms());
                    slot.next = done + slot.job.every;
                }
                Err(_) => {
                    slot.stats.failures += 1;
                    slot.stats.consecutive_failures =
                        slot.stats.consecutive_failures.saturating_add(1);
                    let backoff = slot
                        .job
                        .retry_after
                        .saturating_mul(1u32 << (slot.stats.consecutive_failures - 1).min(16))
                        .min(slot.job.every);
                    slot.next = done + backoff;
                }
            }
            slot.stats.last = Some(outcome);
        }
        self.slots.iter().map(|s| s.next).min()
    }

    /// The blocking loop. `stop` makes it return at the next wakeup; the
    /// sleep is capped so a stop is honored within a second even when the
    /// next job is hours out.
    pub fn run(&mut self, stop: &std::sync::atomic::AtomicBool) {
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            let next = self.tick(Instant::now());
            let sleep = next
                .map(|n| n.saturating_duration_since(Instant::now()))
                .unwrap_or(Duration::from_secs(1))
                .min(Duration::from_secs(1));
            std::thread::sleep(sleep);
        }
    }

    pub fn stats(&self) -> Vec<(String, JobStats)> {
        self.slots
            .iter()
            .map(|s| (s.job.name.clone(), s.stats.clone()))
            .collect()
    }
}

fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

    /// ADR-0011 verification item 8: drive a job whose run outlasts its own
    /// interval and assert exactly one is ever in flight — and that it
    /// KEEPS running (the overlap prevention must not degrade into running
    /// once and stopping).
    #[test]
    fn a_run_that_outlasts_its_interval_never_overlaps_itself() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let runs = Arc::new(AtomicUsize::new(0));
        let (f, m, r) = (in_flight.clone(), max_seen.clone(), runs.clone());
        let mut sched = Scheduler::new();
        sched.add(
            Job::new(
                "slow",
                Duration::from_millis(20), // interval far shorter than the run
                Duration::from_millis(20),
                move || {
                    let now = f.fetch_add(1, Ordering::SeqCst) + 1;
                    m.fetch_max(now, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(80));
                    f.fetch_sub(1, Ordering::SeqCst);
                    r.fetch_add(1, Ordering::SeqCst);
                    Ok("done".into())
                },
            ),
            Duration::ZERO,
        );
        let stop = Arc::new(AtomicBool::new(false));
        let s2 = stop.clone();
        let h = std::thread::spawn(move || {
            sched.run(&s2);
            sched.stats()
        });
        std::thread::sleep(Duration::from_millis(400));
        stop.store(true, Ordering::Relaxed);
        let stats = h.join().expect("scheduler thread");
        assert_eq!(
            max_seen.load(Ordering::SeqCst),
            1,
            "a job overlapped itself — two live backups pin two checkpoints of SSTs"
        );
        let total = runs.load(Ordering::SeqCst);
        assert!(
            total >= 2,
            "only {total} run(s): overlap prevention degraded into not rescheduling"
        );
        assert_eq!(stats[0].1.runs as usize, total);
    }

    #[test]
    fn a_long_run_reschedules_from_completion_not_in_a_burst() {
        // One slow run misses several nominal slots; what follows must be
        // ONE next run after `every`, not a catch-up burst — each catch-up
        // backup would checkpoint data its predecessor just shipped.
        let stamps: Arc<std::sync::Mutex<Vec<Instant>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let st = stamps.clone();
        let slow_first = Arc::new(AtomicBool::new(true));
        let sf = slow_first.clone();
        let mut sched = Scheduler::new();
        sched.add(
            Job::new(
                "j",
                Duration::from_millis(50),
                Duration::from_millis(50),
                move || {
                    st.lock().expect("lock").push(Instant::now());
                    if sf.swap(false, Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(200)); // 4 slots
                    }
                    Ok(String::new())
                },
            ),
            Duration::ZERO,
        );
        let stop = Arc::new(AtomicBool::new(false));
        let s2 = stop.clone();
        let h = std::thread::spawn(move || sched.run(&s2));
        std::thread::sleep(Duration::from_millis(350));
        stop.store(true, Ordering::Relaxed);
        h.join().expect("scheduler thread");
        let stamps = stamps.lock().expect("lock");
        // First run ends ~200ms in; a burst would fire runs 2..5 immediately.
        // Completion-based scheduling puts run 2 a full interval later.
        assert!(
            stamps.len() >= 2,
            "needed at least two runs to observe the gap"
        );
        let gap = stamps[1] - stamps[0];
        assert!(
            gap >= Duration::from_millis(240),
            "run 2 fired {gap:?} after run 1 started — a catch-up burst"
        );
    }

    #[test]
    fn failures_back_off_exponentially_and_cap_at_the_interval() {
        // Observe the gaps between failing runs directly: ~retry, ~2x retry,
        // then capped at `every` — never sooner, never tomorrow.
        let stamps: Arc<std::sync::Mutex<Vec<Instant>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let st = stamps.clone();
        let mut sched = Scheduler::new();
        sched.add(
            Job::new(
                "failing",
                Duration::from_millis(120), // the cap
                Duration::from_millis(30),  // first retry
                move || {
                    st.lock().expect("lock").push(Instant::now());
                    Err("still broken".into())
                },
            ),
            Duration::ZERO,
        );
        let stop = Arc::new(AtomicBool::new(false));
        let s2 = stop.clone();
        let h = std::thread::spawn(move || {
            sched.run(&s2);
            sched.stats()
        });
        std::thread::sleep(Duration::from_millis(600));
        stop.store(true, Ordering::Relaxed);
        let stats = h.join().expect("scheduler thread");
        let stamps = stamps.lock().expect("lock");
        assert!(
            stamps.len() >= 4,
            "wanted >=4 attempts, got {}",
            stamps.len()
        );
        let gap1 = stamps[1] - stamps[0];
        let gap2 = stamps[2] - stamps[1];
        let gap3 = stamps[3] - stamps[2];
        assert!(
            gap1 >= Duration::from_millis(25),
            "first retry too eager: {gap1:?}"
        );
        assert!(gap2 > gap1, "no backoff growth: {gap1:?} then {gap2:?}");
        assert!(
            gap3 <= Duration::from_millis(200),
            "backoff blew past the interval cap: {gap3:?}"
        );
        let (_, s) = &stats[0];
        assert_eq!(s.failures, s.runs);
        assert!(s.consecutive_failures >= 4);
        assert!(
            s.last_ok_wall_ms.is_none(),
            "a failing job must not report an OK time"
        );
    }

    #[test]
    fn success_resets_the_backoff_and_stamps_the_ok_time() {
        let n = Arc::new(AtomicU32::new(0));
        let n2 = n.clone();
        let mut sched = Scheduler::new();
        sched.add(
            Job::new(
                "flappy",
                Duration::from_millis(500),
                Duration::from_millis(5),
                move || {
                    if n2.fetch_add(1, Ordering::SeqCst) < 2 {
                        Err("warming up".into())
                    } else {
                        Ok("fine".into())
                    }
                },
            ),
            Duration::ZERO,
        );
        let mut now = Instant::now();
        for _ in 0..10 {
            sched.tick(now);
            now += Duration::from_millis(50);
        }
        let (_, s) = &sched.stats()[0];
        assert_eq!(s.consecutive_failures, 0, "success must clear the streak");
        assert_eq!(s.failures, 2);
        assert!(s.last_ok_wall_ms.is_some());
        assert!(matches!(&s.last, Some(Ok(m)) if m == "fine"));
    }

    #[test]
    fn jitter_stays_inside_its_bound_and_zero_is_zero() {
        for _ in 0..64 {
            let j = Scheduler::startup_jitter(Duration::from_millis(250));
            assert!(j < Duration::from_millis(250));
        }
        assert_eq!(Scheduler::startup_jitter(Duration::ZERO), Duration::ZERO);
    }
}
