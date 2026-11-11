// SPDX-License-Identifier: Elastic-2.0
//! What the backup schedule says about itself, and the numbers an alert reads
//! (ADR-0011's D8 amendment, accepted 2026-11-11, option C).
//!
//! D8 names the alertable metric: the age of the newest set proven
//! restorable. Before this, the schedule wrote it to a status file nobody
//! read. This module is the pure half of carrying it: which set each check
//! last passed, how old the newest proven set may get before something is
//! wrong, the `FLINTINFO` the seat's read-only listener answers, and the
//! failure edges it journals. No clock, no socket, no store: every input is
//! an argument, so each rule has a test.
use flint_sched::JobStats;
use std::collections::HashMap;

/// A check a set can pass, weakest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Check {
    /// The set completed: its manifest landed after every object.
    Backup,
    /// Every object re-hashed against the manifest.
    Verify,
    /// The set was restored into scratch and opened.
    Rehearse,
}

impl Check {
    pub fn name(self) -> &'static str {
        match self {
            Check::Backup => "backup",
            Check::Verify => "verify",
            Check::Rehearse => "rehearse",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "backup" => Some(Check::Backup),
            "verify" => Some(Check::Verify),
            "rehearse" => Some(Check::Rehearse),
            _ => None,
        }
    }
}

/// The newest set each check has passed: its id and the set's own time.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Proofs {
    backup: Option<(String, u64)>,
    verify: Option<(String, u64)>,
    rehearse: Option<(String, u64)>,
}

impl Proofs {
    fn slot(&mut self, check: Check) -> &mut Option<(String, u64)> {
        match check {
            Check::Backup => &mut self.backup,
            Check::Verify => &mut self.verify,
            Check::Rehearse => &mut self.rehearse,
        }
    }

    pub fn get(&self, check: Check) -> Option<&(String, u64)> {
        match check {
            Check::Backup => self.backup.as_ref(),
            Check::Verify => self.verify.as_ref(),
            Check::Rehearse => self.rehearse.as_ref(),
        }
    }

    /// `set`, taken at `set_ms`, passed `check`. An older set passing does
    /// not replace a newer one's proof: the age is of the NEWEST proven set.
    pub fn record(&mut self, check: Check, set: &str, set_ms: u64) {
        let slot = self.slot(check);
        if slot.as_ref().is_none_or(|(_, ms)| set_ms >= *ms) {
            *slot = Some((set.to_string(), set_ms));
        }
    }

    /// Status-file lines, `proof <check> <set> <set_ms>`, read back by the
    /// next start so a restart does not reset the age to unknown.
    pub fn lines(&self) -> String {
        let mut s = String::new();
        for check in [Check::Backup, Check::Verify, Check::Rehearse] {
            if let Some((set, ms)) = self.get(check) {
                s.push_str(&format!("proof {} {set} {ms}\n", check.name()));
            }
        }
        s
    }

    /// The proofs a previous life's status file recorded. A line that does
    /// not parse is skipped: the cost is one unknown age until the next check
    /// passes, never a wrong one.
    pub fn parse(status: &str) -> Self {
        let mut p = Proofs::default();
        for line in status.lines() {
            let mut f = line.split_whitespace();
            if f.next() != Some("proof") {
                continue;
            }
            let (Some(check), Some(set), Some(ms)) = (
                f.next().and_then(Check::parse),
                f.next(),
                f.next().and_then(|v| v.parse().ok()),
            ) else {
                continue;
            };
            p.record(check, set, ms);
        }
        p
    }
}

/// The schedule's cadence in seconds, as its flags set it.
#[derive(Clone, Copy, Debug)]
pub struct Cadence {
    pub every_s: u64,
    pub verify_s: Option<u64>,
    pub rehearse_s: Option<u64>,
}

impl Cadence {
    /// The strongest check this schedule runs, and how often. Without a
    /// rehearsal, "restorable" degrades to "verified", and says so.
    pub fn strongest(&self) -> (Check, u64) {
        match (self.rehearse_s, self.verify_s) {
            (Some(r), _) => (Check::Rehearse, r),
            (None, Some(v)) => (Check::Verify, v),
            (None, None) => (Check::Backup, self.every_s),
        }
    }

    /// How old the newest proven set may be: 1.25 x (every + R). A set is at
    /// most `every` old when a check proves it, and the next proof comes
    /// within R; each job jitters by up to an eighth of its interval, and the
    /// rest covers the runs themselves.
    pub fn bound_s(&self) -> u64 {
        let (_, r) = self.strongest();
        (self.every_s + r).saturating_mul(5) / 4
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Restorable {
    pub by: Check,
    pub set: Option<String>,
    pub age_s: Option<u64>,
    pub bound_s: u64,
    /// The newest proven set is older than the bound, or nothing has been
    /// proven since a start longer ago than the bound.
    pub overdue: bool,
}

/// The alertable metric, from the proofs, at `now_ms`, for a seat started at
/// `started_ms`.
pub fn restorable(p: &Proofs, c: &Cadence, now_ms: u64, started_ms: u64) -> Restorable {
    let (by, _) = c.strongest();
    let bound_s = c.bound_s();
    match p.get(by) {
        Some((set, set_ms)) => {
            let age_s = now_ms.saturating_sub(*set_ms) / 1000;
            Restorable {
                by,
                set: Some(set.clone()),
                age_s: Some(age_s),
                bound_s,
                overdue: age_s > bound_s,
            }
        }
        // Nothing proven yet. Not overdue while the seat is younger than the
        // bound: its first checks are still due. Overdue after, because "it
        // has never managed one" is the failure this exists to report.
        None => Restorable {
            by,
            set: None,
            age_s: None,
            bound_s,
            overdue: now_ms.saturating_sub(started_ms) / 1000 > bound_s,
        },
    }
}

/// The body of the seat's `FLINTINFO`: `field:value` lines, numbers where a
/// gauge should be, so `flint-exporter` and the ops agent read it as they
/// read any seat. A field that has no value yet is left out, never zeroed:
/// zero would read as fresh.
pub fn info(
    stats: &[(String, JobStats)],
    r: &Restorable,
    now_ms: u64,
    started_ms: u64,
    build: &str,
) -> String {
    let mut s = format!(
        "role:backup\r\nbuild:{build}\r\nuptime_s:{}\r\n",
        now_ms.saturating_sub(started_ms) / 1000
    );
    for (name, st) in stats {
        s.push_str(&format!(
            "job_{name}_runs:{}\r\njob_{name}_failures:{}\r\njob_{name}_consecutive_failures:{}\r\n",
            st.runs, st.failures, st.consecutive_failures
        ));
        if let Some(ms) = st.last_ok_wall_ms {
            s.push_str(&format!(
                "job_{name}_last_ok_age_s:{}\r\n",
                now_ms.saturating_sub(ms) / 1000
            ));
        }
        if let Some(Err(e)) = &st.last {
            s.push_str(&format!("job_{name}_last_error:{}\r\n", one_line(e)));
        }
    }
    s.push_str(&format!("restorable_by:{}\r\n", r.by.name()));
    s.push_str(&format!(
        "restorable_set:{}\r\n",
        r.set.as_deref().unwrap_or("none")
    ));
    if let Some(age) = r.age_s {
        s.push_str(&format!("restorable_age_s:{age}\r\n"));
    }
    s.push_str(&format!(
        "restorable_bound_s:{}\r\nrestorable_overdue:{}\r\n",
        r.bound_s,
        u8::from(r.overdue)
    ));
    s
}

/// An error on one line: a field value that broke the `field:value` framing
/// would truncate every field after it.
fn one_line(e: &str) -> String {
    e.replace(['\r', '\n'], " ")
}

/// A failure streak starting or ending: what the seat journals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Edge {
    Failed { job: String, error: String },
    Recovered { job: String },
}

/// Each job's failure streak against the last look at it, which `prev`
/// carries and this updates. A streak that started in a previous life is
/// seeded into `prev` from its status file ([`streaks`]), so a restart
/// mid-streak neither journals a second start nor loses the recovery.
pub fn edges(prev: &mut HashMap<String, u32>, stats: &[(String, JobStats)]) -> Vec<Edge> {
    let mut out = Vec::new();
    for (name, st) in stats {
        let before = prev.get(name).copied().unwrap_or(0);
        let now = st.consecutive_failures;
        // A job that has not run since this start has said nothing yet.
        if st.runs == 0 {
            continue;
        }
        if before == 0 && now > 0 {
            let error = match &st.last {
                Some(Err(e)) => one_line(e),
                _ => String::new(),
            };
            out.push(Edge::Failed {
                job: name.clone(),
                error,
            });
        } else if before > 0 && now == 0 {
            out.push(Edge::Recovered { job: name.clone() });
        }
        prev.insert(name.clone(), now);
    }
    out
}

/// Each job's failure streak as a previous life's status file recorded it:
/// `job <name> runs N failures N consecutive N ...`.
pub fn streaks(status: &str) -> HashMap<String, u32> {
    let mut m = HashMap::new();
    for line in status.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() >= 8
            && f[0] == "job"
            && f[6] == "consecutive"
            && let Ok(n) = f[7].parse::<u32>()
        {
            m.insert(f[1].to_string(), n);
        }
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(
        name: &str,
        runs: u64,
        consecutive: u32,
        last: Result<&str, &str>,
    ) -> (String, JobStats) {
        (
            name.to_string(),
            JobStats {
                runs,
                failures: u64::from(consecutive),
                consecutive_failures: consecutive,
                last: Some(last.map(str::to_string).map_err(str::to_string)),
                last_ok_wall_ms: None,
            },
        )
    }

    #[test]
    fn the_strongest_configured_check_sets_the_evidence_and_the_bound() {
        let daily = 86_400;
        let weekly = 7 * daily;
        let c = Cadence {
            every_s: daily,
            verify_s: Some(daily),
            rehearse_s: Some(weekly),
        };
        assert_eq!(c.strongest(), (Check::Rehearse, weekly));
        assert_eq!(c.bound_s(), (daily + weekly) * 5 / 4, "10 days");
        let c = Cadence {
            every_s: daily,
            verify_s: Some(daily),
            rehearse_s: None,
        };
        assert_eq!(c.strongest(), (Check::Verify, daily));
        let c = Cadence {
            every_s: daily,
            verify_s: None,
            rehearse_s: None,
        };
        assert_eq!(c.strongest(), (Check::Backup, daily));
        assert_eq!(c.bound_s(), daily * 5 / 2);
    }

    #[test]
    fn the_age_is_of_the_newest_set_the_strongest_check_proved() {
        let c = Cadence {
            every_s: 100,
            verify_s: None,
            rehearse_s: Some(100),
        };
        let mut p = Proofs::default();
        // A set backed up and verified, but never rehearsed, is not proven.
        p.record(Check::Backup, "backup-9000", 9_000);
        let r = restorable(&p, &c, 10_000, 9_500);
        assert_eq!(
            (r.by, r.set.clone(), r.age_s),
            (Check::Rehearse, None, None)
        );
        p.record(Check::Rehearse, "backup-5000", 5_000);
        // An older set rehearsed later does not move the proof backward.
        p.record(Check::Rehearse, "backup-1000", 1_000);
        let r = restorable(&p, &c, 305_000, 0);
        assert_eq!(r.set.as_deref(), Some("backup-5000"));
        assert_eq!(r.age_s, Some(300));
        assert_eq!(r.bound_s, 250);
        assert!(r.overdue, "300 s against a 250 s bound");
        let r = restorable(&p, &c, 200_000, 0);
        assert!(!r.overdue, "195 s against a 250 s bound");
    }

    #[test]
    fn nothing_proven_is_overdue_only_once_the_seat_is_older_than_the_bound() {
        let c = Cadence {
            every_s: 2,
            verify_s: None,
            rehearse_s: None,
        };
        let p = Proofs::default();
        assert!(!restorable(&p, &c, 4_000, 0).overdue, "4 s up, bound 5 s");
        assert!(
            restorable(&p, &c, 6_000, 0).overdue,
            "6 s up, nothing proven"
        );
    }

    #[test]
    fn proofs_survive_a_restart_through_the_status_file() {
        let mut p = Proofs::default();
        p.record(Check::Verify, "backup-1700000000000", 1_700_000_000_000);
        p.record(Check::Rehearse, "backup-1690000000000", 1_690_000_000_000);
        let file = format!(
            "job backup runs 3 failures 0 consecutive 0 last_ok_ms 1 ok\n{}rubbish\nproof rehearse x\n",
            p.lines()
        );
        assert_eq!(Proofs::parse(&file), p);
    }

    #[test]
    fn a_streak_journals_its_start_and_its_end_once_each() {
        let mut prev = HashMap::new();
        // A healthy run says nothing.
        assert!(edges(&mut prev, &[stats("backup", 1, 0, Ok("set a"))]).is_empty());
        let e = edges(
            &mut prev,
            &[stats("backup", 2, 1, Err("GET: HTTP 403\nAccessDenied"))],
        );
        assert_eq!(
            e,
            vec![Edge::Failed {
                job: "backup".into(),
                error: "GET: HTTP 403 AccessDenied".into()
            }]
        );
        // Its retries are the same streak.
        assert!(edges(&mut prev, &[stats("backup", 3, 2, Err("again"))]).is_empty());
        assert_eq!(
            edges(&mut prev, &[stats("backup", 4, 0, Ok("set b"))]),
            vec![Edge::Recovered {
                job: "backup".into()
            }]
        );
        assert!(edges(&mut prev, &[stats("backup", 5, 0, Ok("set c"))]).is_empty());
    }

    #[test]
    fn a_restart_mid_streak_neither_repeats_the_start_nor_loses_the_end() {
        let file = "job backup runs 9 failures 4 consecutive 4 last_ok_ms 0 err HTTP 403\njob verify runs 2 failures 0 consecutive 0 last_ok_ms 5 ok\n";
        let mut prev = streaks(file);
        assert_eq!(prev.get("backup"), Some(&4));
        // Not run since the start: silent.
        assert!(edges(&mut prev, &[stats("backup", 0, 0, Ok(""))]).is_empty());
        assert!(edges(&mut prev, &[stats("backup", 1, 1, Err("HTTP 403"))]).is_empty());
        assert_eq!(
            edges(&mut prev, &[stats("backup", 2, 0, Ok("set"))]),
            vec![Edge::Recovered {
                job: "backup".into()
            }]
        );
    }

    #[test]
    fn flintinfo_leaves_out_what_it_does_not_know() {
        let r = Restorable {
            by: Check::Rehearse,
            set: None,
            age_s: None,
            bound_s: 250,
            overdue: false,
        };
        let body = info(
            &[stats("rehearse", 1, 1, Err("restore: x\r\ny"))],
            &r,
            10_000,
            0,
            "v0",
        );
        assert!(body.contains("restorable_set:none\r\n"), "{body}");
        assert!(
            !body.contains("restorable_age_s"),
            "an unknown age must not read as 0: {body}"
        );
        assert!(
            body.contains("job_rehearse_last_error:restore: x  y\r\n"),
            "{body}"
        );
        assert!(body.contains("restorable_overdue:0\r\n"), "{body}");
        for line in body.split("\r\n").filter(|l| !l.is_empty()) {
            assert!(line.contains(':'), "a line with no field name: {line:?}");
        }
    }
}
