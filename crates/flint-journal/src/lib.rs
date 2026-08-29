// SPDX-License-Identifier: Elastic-2.0
//! Fleet journal (design.md §2.10, interface 3): a typed event log of every
//! state transition with **actor**, **cause**, and **epoch**. Gauges say what
//! is; the journal says what happened and why. Catalog recommendations emit
//! into the same journal, closing the loop: the shadow journal records what
//! the agent WOULD have done, the fleet journal records what the system DID —
//! joining the two (RECOMMENDED vs PROMOTED, epoch for epoch) is the
//! false-action-rate evaluation that gates execute authority. The journal is
//! also the incident corpus that trains the agent.
//!
//! Storage lives in the control plane (`CPJOURNAL` append / `CPJOURNALREAD`
//! tail) as a local append-only JSONL file — observability, NOT Raft intent:
//! nothing re-derives cluster state from it, so it is deliberately outside
//! the consensus path and loss-tolerant across CP failover.
//!
//! Emission is **best-effort and bounded**: an unreachable journal must never
//! affect the transition being reported — the transition already happened;
//! the report is history, not a precondition.

use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;

use flint_resp::{Decoded, Value, decode, encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    /// A controller confirmed a master unreachable (post-confirm streak).
    Detected,
    /// A controller issued an epoch-fenced promotion command.
    PromoteIssued,
    /// A node durably applied a promotion (it IS the new master).
    Promoted,
    /// A node durably applied a demotion (fenced to replica).
    Demoted,
    /// A node fenced itself read-only (lease expiry: partitioned from
    /// every controller).
    SelfFenced,
    /// The shadow agent recommended a catalog action (the agent's side of
    /// the recommended-vs-actual evaluation join).
    Recommended,
    /// A controller triggered a durable off-node snapshot on a master.
    SnapshotTaken,
    /// A fresh node seeded itself from the latest snapshot after whole-pair
    /// loss and asserted mastership in a bumped generation.
    SpareRestored,
    /// A master began REFUSING writes because live replicas fell below
    /// `--min-replicas-to-write`. This is the client-visible write outage
    /// STARTING, and it is not the same instant as `Promoted`: a freshly
    /// promoted master has no replicas yet, so the gate shuts as a
    /// consequence of the promotion rather than at it.
    WriteQuorumLost,
    /// The same master admitted a write again — the outage ENDING. Paired
    /// with `WriteQuorumLost` these bracket the whole refusal window on the
    /// node that did the refusing, which is the only place it is exactly
    /// observable; everything else measures it through a client.
    WriteQuorumRestored,
    /// A replica began rejoining (process up, catch-up not yet chosen).
    /// Bounds restart latency, which nothing else in the journal did — the
    /// window between `Promoted` and `Supervised` was previously silent, so
    /// a 9.9 s recovery could not be split into boot, decision and catch-up.
    RejoinStarted,
    /// A replica chose HOW to catch up. `cause` carries which path and from
    /// where: rewound-and-tail, full re-seed, or warm rejoin. The expensive
    /// branch is a full re-seed, and knowing which one ran is the difference
    /// between tuning the probe and tuning the transfer.
    RejoinDecided,
    /// A controller observed a pair converged for the first time in its
    /// process lifetime: the degraded-window gate is open and auto-failover
    /// is ARMED for that pair. Tooling that (re)starts controllers waits
    /// for this before declaring an operation complete.
    Supervised,
    /// The metering loop flipped a tenant's storage-quota verdict (M5).
    /// detail carries "on <used>/<cap>" or "off <used>/<cap>".
    QuotaVerdict,
    /// Tier 2: the agent EXECUTED an allowlisted catalog action (M5).
    /// detail carries the exact command run.
    ActionExecuted,
    /// Tier 2: the executed action's declared success signals verified
    /// healthy — the incident is closed by the agent.
    ActionVerified,
    /// Tier 2 escalation: the agent needed to act but could not (budget
    /// exhausted, dead-man tripped, verify timeout) — a human is paged.
    PageHuman,
    /// The agent's consolidation cron swept the CP slot-ownership table
    /// (CPCONSOLIDATE) and the row count DROPPED — adjacent runs merged or
    /// rows made redundant by topology change retired. detail carries
    /// "before -> after". No-op sweeps are not journaled.
    SlotsConsolidated,
    /// The disk headroom guard began shedding writes on this node
    /// (ADR-0013 D3). detail carries "free <bytes> of <bytes>". This is the
    /// edge an external GC policy daemon triggers on — Flint never evicts,
    /// so reclaiming space from here is the operator's (or their tooling's)
    /// move.
    DiskShed,
    /// The guard cleared (with hysteresis) and writes resumed.
    /// detail carries "free <bytes> of <bytes>".
    DiskResumed,
    /// Tier-1 (OPS-ADR-0029): a tier READ something from the catalog of
    /// evidence it is armed for. `subject` is the evidence key
    /// (`<verb>:<what>`); `detail` says what came back.
    ///
    /// One row per lookup, and that is the point rather than an accident.
    /// OPS-0057 established that the constraint on debugging was never
    /// capability but the RECORD: nine pages reached the pager and could not
    /// be attributed afterwards, from a position with more access than any
    /// tier will ever have, because the send path wrote nothing down. An
    /// investigating tier that leaves no trail rebuilds exactly that, one
    /// level up — a conclusion nobody can check, drawn from reads nobody can
    /// see.
    ///
    /// It will be among the noisiest kinds here. That is affordable for
    /// VOLUME: `CPJOURNALREAD` filters by kind SERVER-side (ADR-0018 item 1),
    /// so a reader asking for decisions never pays for these, and the journal
    /// rotates, so quantity costs disk rather than horizon.
    ///
    /// **Volume is not the only property a new kind can break, and the other
    /// one has already bitten.** `tools/fleet_journal_drill.sh` asserts that a
    /// HEALTHY pair's journal holds the supervision arming and NOTHING ELSE.
    /// Against that invariant one unexpected row is exactly as fatal as ten
    /// thousand, and server-side filtering does not help because the drill
    /// reads unfiltered. A `RejoinStarted` kind added the same day fired on
    /// every replica start and put ops main red, appearing at an unrelated
    /// ops commit because ops CI builds against core main — so the failure
    /// surfaces one repo away from its cause.
    ///
    /// The rule that follows, and it is stronger than "do not emit during
    /// drills": **gate an event on the condition it NAMES.** A row that can
    /// fire when nothing of that name happened is a misnomer before it is an
    /// invariant violation. `EvidenceGathered` satisfies this — it can only
    /// be emitted by an actual Tier-1 lookup, which requires both a caller
    /// and `--evidence` arming, and a healthy fleet has neither.
    EvidenceGathered,
    /// The rotation loop retired a drained previous token (ADR-0006 D3):
    /// its auth count stayed flat across the tenant's subset for a full
    /// drain window. detail carries the drained digest (non-secret).
    TokenRetired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub at_ms: u64,
    /// Who reports: "controller:<id>", "node:<addr>", "agent:shadow".
    pub actor: String,
    pub kind: EventKind,
    /// What the event is about (a node address, a pair label, an action key).
    pub subject: String,
    /// The fencing epoch involved, when the transition has one — the join
    /// key of the recommended-vs-actual evaluation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Report `event` to the control plane's journal. Best-effort with bounded
/// timeouts: errors are swallowed by design (see module docs). Reads the
/// reply so the append is processed before the connection drops.
pub fn emit(target: &str, tls: &Option<Arc<flint_tls::ClientConfig>>, event: &Event) {
    let Ok(json) = serde_json::to_string(event) else {
        return;
    };
    let _ = (|| -> std::io::Result<()> {
        let mut s = flint_tls::connect(target, tls)?;
        s.set_read_timeout(Some(Duration::from_millis(400)))?;
        s.set_write_timeout(Some(Duration::from_millis(400)))?;
        let frame = Value::Array(Some(vec![
            Value::Bulk(Some(b"CPJOURNAL".to_vec())),
            Value::Bulk(Some(json.into_bytes())),
        ]));
        let mut out = Vec::new();
        encode(&frame, &mut out);
        s.write_all(&out)?;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            match decode(&buf) {
                Ok(Decoded::Complete(_, _)) => return Ok(()),
                Ok(Decoded::NeedMore) => {
                    let n = s.read(&mut chunk)?;
                    if n == 0 {
                        return Ok(());
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                Err(_) => return Ok(()),
            }
        }
    })();
}

/// Fire-and-forget variant for hot-ish paths: the bounded network wait moves
/// to a short-lived thread so the caller never blocks. Events are rare
/// (state transitions), so a thread per event is fine.
pub fn emit_detached(target: String, tls: Option<Arc<flint_tls::ClientConfig>>, event: Event) {
    std::thread::spawn(move || emit(&target, &tls, &event));
}

/// Control-plane side: append one pre-serialized event line to the journal
/// file. Plain append, no fsync — loss-tolerant observability (module docs).
///
/// ONE write, and that is the whole point. This was `writeln!(f, "{}", line)`,
/// which on an unbuffered `File` is `write_fmt` and issues a SEPARATE write()
/// syscall per format piece: one for the body, one for the newline. Any other
/// appender landing between those two produces `{..}{..}` on a single line,
/// and a reader parsing json-per-line gets `Extra data: line 1 column 166` —
/// which is how this was found, as an intermittent drill failure.
///
/// Not hypothetical: the control plane appends from a thread per connection,
/// and the scheduled-verify timer added a SECOND PROCESS appending to the same
/// file on a live fleet every five minutes. The journal is what the ops portal
/// shows and what `upgrade` reads to judge a soak clean, so a line that cannot
/// be parsed is a line that does not exist.
///
/// Building the newline into the buffer leaves exactly one `write` for an
/// O_APPEND handle, which is what makes the append indivisible. `write_all`
/// would still loop on a short write, but a short write on a few hundred bytes
/// to a regular file does not happen in practice, and the alternative — two
/// writes every time — is broken by construction rather than by bad luck.
/// A BACKSTOP, not the retention policy. Deliberately twice the policy window.
///
/// The policy is ninety days, chosen by the operator on 2026-08-27 — long
/// enough that a quarter's incidents stay reconstructable on the box. (At the
/// playground's measured 6,610 rows/day = 1.2 MB/day, that is ~110 MB.) But
/// the pruner that ENFORCES ninety days is `tools/agent/flint-archive.sh` in
/// the ops repo, because it is the only one that can keep the promise that
/// matters: it uploads in the same pass, so nothing is deleted locally that
/// was not shipped first.
///
/// This pruner cannot make that promise — it runs inside the control plane
/// and knows nothing about S3 — so at an equal window it would race the
/// archive and could delete a segment that was never shipped. Sitting at
/// twice the window means the archive always gets there first on any box
/// running it, while a fleet with no archiver is still bounded instead of
/// growing forever.
///
/// If you shorten either window, check this one still sits outside the
/// archive's `FLINT_ARCHIVE_RETAIN_DAYS`.
pub const RETENTION_MS: u64 = 180 * 24 * 60 * 60 * 1000;

/// Rotate the live file once it passes this. Bounds the READ cost as much as
/// the disk: `tail` and `tail_kinds` both `read_to_string` the whole file to
/// return the last n lines, so before rotation existed, asking for 30 rows
/// allocated the entire 41 MB journal. Rotation caps that at this number.
pub const ROTATE_BYTES: u64 = 32 * 1024 * 1024;

/// Serialises rotation between the control plane's two writer paths
/// (`main.rs` and `ha.rs`). Appends themselves need no lock — they are
/// O_APPEND — but two threads deciding to rotate at once would have the
/// second rename the FRESH file away.
static ROTATE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Rotated segments, oldest first, as (rotation_ms, path).
///
/// The suffix is the rotation TIME, so a segment holds rows strictly older
/// than its own name. Pruning on that name therefore keeps at least
/// `RETENTION_MS`, never less — the conservative direction, since the cost of
/// keeping too much is disk and the cost of keeping too little is an incident
/// nobody can reconstruct.
fn segments(path: &str) -> Vec<(u64, std::path::PathBuf)> {
    let p = std::path::Path::new(path);
    let (Some(dir), Some(base)) = (p.parent(), p.file_name().and_then(|s| s.to_str())) else {
        return Vec::new();
    };
    let prefix = format!("{base}.");
    let mut out: Vec<(u64, std::path::PathBuf)> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_str()?.to_string();
            let ms: u64 = name.strip_prefix(&prefix)?.parse().ok()?;
            Some((ms, e.path()))
        })
        .collect();
    out.sort_by_key(|(ms, _)| *ms);
    out
}

/// Rotate the live file aside and drop segments past the retention window.
///
/// Renaming is safe against a concurrent append: `append_line` opens the path
/// on every call, so a writer that already has the fd finishes into the
/// renamed inode — its row lands in the rotated segment rather than being
/// lost — and the next writer creates a fresh live file.
fn rotate_and_prune(path: &str) {
    let _g = match ROTATE_LOCK.lock() {
        Ok(g) => g,
        Err(e) => e.into_inner(),
    };
    let now = now_ms();
    // Re-check under the lock: the thread that lost the race must not rotate
    // the empty file its winner just created.
    if std::fs::metadata(path).map(|m| m.len()).unwrap_or(0) >= ROTATE_BYTES {
        let _ = std::fs::rename(path, format!("{path}.{now}"));
    }
    for (ms, seg) in segments(path) {
        if now.saturating_sub(ms) > RETENTION_MS {
            let _ = std::fs::remove_file(seg);
        }
    }
}

pub fn append_line(path: &str, json_line: &str) -> std::io::Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let line = json_line.trim_end();
    let mut buf = Vec::with_capacity(line.len() + 1);
    buf.extend_from_slice(line.as_bytes());
    buf.push(b'\n');
    let r = f.write_all(&buf);
    // Rotation is checked AFTER the write and never fails the append: a
    // journal that refuses to record because housekeeping failed is worse
    // than a large one. `metadata` on the open handle costs a stat, not a
    // read.
    if r.is_ok() && f.metadata().map(|m| m.len()).unwrap_or(0) >= ROTATE_BYTES {
        rotate_and_prune(path);
    }
    r
}

/// Control-plane side: the last `n` journal lines, oldest first.
pub fn tail(path: &str, n: usize) -> Vec<String> {
    tail_across(path, n, &|_| true)
}

/// The last `n` lines matching `keep`, oldest first, ACROSS the live file and
/// its rotated segments.
///
/// Spanning segments is a correctness requirement, not a convenience. A read
/// that stopped at the live file would report a short history the moment a
/// rotation happened, and `tail_kinds` is what tier2 counts `ActionExecuted`
/// against inside its budget window. A budget counter that silently cannot
/// reach back over its window is the 2026-08-17 failure documented on
/// `tail_kinds` — the agent had the fix and the authority and paged instead.
/// Rotation without this would have rebuilt that bug on a timer.
///
/// Newest segment first, stopping as soon as `n` are in hand, so the common
/// case reads only the live file and rotation costs nothing on a normal read.
fn tail_across(path: &str, n: usize, keep: &dyn Fn(&str) -> bool) -> Vec<String> {
    if n == 0 {
        return Vec::new();
    }
    let mut files: Vec<std::path::PathBuf> = segments(path).into_iter().map(|(_, p)| p).collect();
    files.push(std::path::PathBuf::from(path));

    let mut out: Vec<String> = Vec::new();
    let live = std::path::Path::new(path);
    for f in files.iter().rev() {
        let raw = match std::fs::read_to_string(f) {
            Ok(r) => r,
            Err(e) => {
                // A SEGMENT that fails to read is not the same as one that is
                // not there. `segments()` just enumerated it, so the file
                // existed a moment ago; skipping it silently hands a caller
                // FEWER rows and no indication, and the caller most likely to
                // be hurt is tier2's budget counter, for which "cannot count"
                // and "counted fewer" are the difference between refusing to
                // act and acting freely. That collapse is OPS-0037, and a
                // rotation-shortened horizon is the exact failure `tail_kinds`
                // documents from 2026-08-17.
                //
                // Still `continue` rather than returning an error: this function has
                // no error channel and losing the readable segments too would
                // be worse. But it must not be quiet about it.
                if f.as_path() != live {
                    eprintln!(
                        "journal: segment {} unreadable ({e}) — this read is SHORT by \
                         whatever it held; a horizon computed from it is not trustworthy",
                        f.display()
                    );
                }
                // The live path legitimately does not exist between a
                // rotation and the next append; that case stays silent.
                continue;
            }
        };
        let mut kept: Vec<String> = raw
            .lines()
            .filter(|l| keep(l))
            .map(|s| s.to_string())
            .collect();
        let need = n - out.len();
        if kept.len() > need {
            kept = kept.split_off(kept.len() - need);
        }
        kept.extend(out);
        out = kept;
        if out.len() >= n {
            break;
        }
    }
    out
}

/// Parse a wire kind name into its variant.
///
/// serde is the parser rather than a hand-written match, deliberately: the
/// enum is the single source of truth for what a kind is called, so this
/// cannot drift when a variant is added or renamed. A hand-written table
/// would compile fine while silently rejecting a kind the system emits.
pub fn parse_kind(name: &str) -> Option<EventKind> {
    serde_json::from_str::<EventKind>(&format!("\"{name}\"")).ok()
}

/// Parse the optional `KINDS <kind,kind,...>` suffix of `CPJOURNALREAD`.
///
/// ONE implementation because there are TWO control-plane command paths —
/// the single-node one in `main.rs` and the Raft one in `ha.rs`. They have
/// already diverged once in production: ADR-0014 D1's `build:` field landed
/// on only one of them, nothing noticed, and every Raft fleet became
/// unrollable because `flintctl upgrade` died on the missing field. A filter
/// that guards an action budget is a worse thing to get half-deployed, so
/// the logic lives here and both arms call it.
///
/// `Ok(vec![])` means no filter was requested. Every other outcome that is
/// not a clean parse is `Err`, never an empty filter — see `tail_kinds` for
/// why an empty result is the dangerous answer for a budget counter.
pub fn parse_kinds_arg(
    keyword: Option<&str>,
    list: Option<&str>,
) -> Result<Vec<EventKind>, String> {
    let Some(kw) = keyword else {
        return Ok(Vec::new());
    };
    if !kw.eq_ignore_ascii_case("KINDS") {
        return Err(format!("CPJOURNALREAD: expected KINDS, got {kw}"));
    }
    let Some(list) = list else {
        return Err("CPJOURNALREAD <n> KINDS <kind,kind,...>".into());
    };
    let mut ks = Vec::new();
    for name in list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        match parse_kind(name) {
            Some(k) if ks.contains(&k) => {}
            Some(k) => ks.push(k),
            None => return Err(format!("unknown journal kind {name}")),
        }
    }
    // `KINDS` with nothing usable after it would fall through to an
    // unfiltered tail: the caller asked to NARROW and would silently get
    // everything. Widening on a malformed narrow request is how a consumer
    // ends up reasoning about a window it did not ask for.
    if ks.is_empty() {
        return Err("CPJOURNALREAD KINDS needs at least one kind".into());
    }
    Ok(ks)
}

/// The last `n` journal lines **whose kind is in `kinds`**, oldest first.
///
/// ADR-0018 item 1. `tail` takes the last n lines of any kind, so a consumer
/// that reasons about two kinds must read a window sized for every kind the
/// fleet emits. On 2026-08-17 that broke a safety guard: tier2 counts
/// `ActionExecuted` inside its budget window from a 500-line tail, a
/// controller emitted `Detected` at roughly 2725/hour during the outage, and
/// those 500 lines stopped reaching back over the window. Tier 2 correctly
/// refused to act on a count it could not complete — so the agent had the
/// fix, had the authority, and paged instead. **The noise did not merely
/// bury the signal; it disarmed the responder.**
///
/// Filtering happens BEFORE the count, which is the whole point: "the last n
/// matching lines", never "the matching lines among the last n". The latter
/// is what a client-side filter gives you, and it is exactly the behaviour
/// that failed — tier2 already filtered by kind after reading, and that did
/// not help at all.
///
/// An EMPTY `kinds` means no filter, matching `tail`. Callers must not use
/// empty to mean "nothing": a filter that matches nothing is indistinguish-
/// able from a journal with nothing in it, and for a budget counter those
/// two answers are "act freely" and "cannot count". The command layer
/// rejects an unparseable kind rather than passing an empty set down here.
pub fn tail_kinds(path: &str, n: usize, kinds: &[EventKind]) -> Vec<String> {
    if kinds.is_empty() {
        return tail(path, n);
    }
    /// Only the discriminant is needed to decide whether to keep a line, and
    /// the rest of the event can be malformed without making the kind
    /// unreadable. serde ignores the other fields.
    #[derive(Deserialize)]
    struct KindOnly {
        kind: EventKind,
    }
    // Across segments, for the reason on `tail_across`: this function is what
    // tier2's budget window is counted from, and a rotation must not shorten
    // the reach of that count.
    tail_across(path, n, &|line: &str| {
        serde_json::from_str::<KindOnly>(line)
            .map(|k| kinds.contains(&k.kind))
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod append_tests {
    use super::*;

    /// A test directory that lives exactly as long as the binding holds it.
    ///
    /// Removing the journal FILE at the end of a test is not enough — the
    /// directory outlives the process and accumulates one per `cargo test`
    /// run, forever. Drop is also the only cleanup that survives a failing
    /// assertion: a tail `remove_dir_all` never runs on the panicking path,
    /// which is precisely when you re-run the test most.
    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let p = std::env::temp_dir().join(format!(
                "flint-journal-race-{tag}-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).expect("temp dir");
            Self(p)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Concurrent appenders must not interleave WITHIN a line.
    ///
    /// This failed before `append_line` built the line and its newline into one
    /// buffer: `writeln!` on an unbuffered File is `write_fmt`, which issues a
    /// separate write() per format piece — one for the body, one for the "\n".
    /// Another appender landing between those two syscalls produces `{..}{..}`
    /// on a single line, and a reader doing json-per-line gets
    /// `Extra data: line 1 column 166`, which is how this was noticed.
    ///
    /// It is not a hypothetical race. The control plane appends from a thread
    /// per connection, and as of the scheduled-verify timer a SECOND PROCESS
    /// appends to the same file on a live fleet every five minutes. The journal
    /// is what the ops portal shows and what `upgrade` reads to decide a soak
    /// was clean: a line that cannot be parsed is a line that does not exist.
    #[test]
    fn concurrent_appends_never_interleave_within_a_line() {
        let dir = TempDir::new("interleave");
        let path = dir.0.join("j.jsonl");
        let p = path.to_string_lossy().to_string();

        const WRITERS: usize = 8;
        const EACH: usize = 250;
        // Long enough that a torn write has a wide window to be caught in; a
        // 40-byte line would make this test pass by luck rather than by fix.
        let filler = "x".repeat(300);

        std::thread::scope(|s| {
            for w in 0..WRITERS {
                let p = p.clone();
                let filler = filler.clone();
                s.spawn(move || {
                    for i in 0..EACH {
                        let line = serde_json::json!({
                            "at_ms": 1_u64, "actor": format!("w{w}"), "kind": "Detected",
                            "subject": format!("seat-{i}"), "detail": filler,
                        })
                        .to_string();
                        append_line(&p, &line).expect("append");
                    }
                });
            }
        });

        let raw = std::fs::read_to_string(&path).expect("read journal");
        let lines: Vec<&str> = raw.lines().collect();
        assert_eq!(
            lines.len(),
            WRITERS * EACH,
            "expected one line per append; got {} — lines were merged or split",
            lines.len()
        );
        for (n, l) in lines.iter().enumerate() {
            serde_json::from_str::<serde_json::Value>(l).unwrap_or_else(|e| {
                panic!(
                    "line {n} is not one JSON object ({e}); first 120 bytes: {:?}",
                    &l[..l.len().min(120)]
                )
            });
        }
    }
}

#[cfg(test)]
mod tail_kinds_tests {
    use super::*;

    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new(tag: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let p = std::env::temp_dir().join(format!(
                "flint-journal-kinds-{tag}-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).expect("temp dir");
            Self(p)
        }
        fn path(&self) -> String {
            self.0.join("j.jsonl").to_string_lossy().into_owned()
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn write(path: &str, events: &[(u64, EventKind)]) {
        let body: String = events
            .iter()
            .map(|(at, k)| {
                let e = Event {
                    at_ms: *at,
                    actor: "test".into(),
                    kind: *k,
                    subject: format!("s{at}"),
                    epoch: None,
                    cause: None,
                    detail: None,
                };
                format!("{}\n", serde_json::to_string(&e).expect("event serializes"))
            })
            .collect();
        std::fs::write(path, body).expect("write the fixture journal");
    }

    /// THE REGRESSION FOR THE 2026-08-17 OUTAGE, in its actual shape: a
    /// `Detected` flood between the actions a guard must count. Unfiltered,
    /// a 500-line tail reaches back only as far as the flood allows.
    #[test]
    fn a_flood_of_one_kind_cannot_push_another_out_of_the_window() {
        let d = TempDir::new("flood");
        let p = d.path();
        let mut ev = vec![(1_000u64, EventKind::ActionExecuted)];
        // The controller emitted Detected at ~2725/hour during the outage.
        for i in 0..2000u64 {
            ev.push((2_000 + i, EventKind::Detected));
        }
        ev.push((900_000, EventKind::ActionVerified));
        write(&p, &ev);

        let unfiltered = tail(&p, 500);
        let has_action = unfiltered.iter().any(|l| l.contains("ActionExecuted"));
        assert!(
            !has_action,
            "precondition: the flood must actually bury the action, or this \
             test proves nothing about the fix"
        );

        let filtered = tail_kinds(
            &p,
            500,
            &[EventKind::ActionExecuted, EventKind::ActionVerified],
        );
        assert_eq!(filtered.len(), 2, "both actions survive the flood");
        assert!(filtered[0].contains("ActionExecuted"));
        assert!(filtered[1].contains("ActionVerified"));
    }

    /// Oldest-first, same as `tail` — a consumer that swapped one for the
    /// other must not silently get reversed history.
    #[test]
    fn results_are_oldest_first_like_tail() {
        let d = TempDir::new("order");
        let p = d.path();
        write(
            &p,
            &[
                (10, EventKind::ActionExecuted),
                (20, EventKind::Detected),
                (30, EventKind::ActionExecuted),
            ],
        );
        let got = tail_kinds(&p, 10, &[EventKind::ActionExecuted]);
        assert_eq!(got.len(), 2);
        assert!(got[0].contains("\"at_ms\":10"), "oldest first");
        assert!(got[1].contains("\"at_ms\":30"));
    }

    /// n bounds the MATCHING lines, and keeps the newest of them.
    #[test]
    fn n_counts_matches_not_scanned_lines() {
        let d = TempDir::new("bound");
        let p = d.path();
        let mut ev = Vec::new();
        for i in 0..10u64 {
            ev.push((i * 10, EventKind::ActionExecuted));
            ev.push((i * 10 + 1, EventKind::Detected));
        }
        write(&p, &ev);
        let got = tail_kinds(&p, 3, &[EventKind::ActionExecuted]);
        assert_eq!(got.len(), 3, "three matches, not three lines scanned");
        assert!(got[2].contains("\"at_ms\":90"), "keeps the newest matches");
    }

    /// Empty means no filter — `tail` parity, so the unfiltered command path
    /// keeps its exact behaviour.
    #[test]
    fn an_empty_filter_is_the_unfiltered_tail() {
        let d = TempDir::new("empty");
        let p = d.path();
        write(
            &p,
            &[(1, EventKind::Detected), (2, EventKind::ActionExecuted)],
        );
        assert_eq!(tail_kinds(&p, 50, &[]), tail(&p, 50));
    }

    /// A torn line must not truncate the history a guard reads. The journal
    /// has multiple writers; #116 showed interleaving is real.
    #[test]
    fn a_torn_line_does_not_stop_the_scan() {
        let d = TempDir::new("torn");
        let p = d.path();
        let good = |at: u64| {
            serde_json::to_string(&Event {
                at_ms: at,
                actor: "t".into(),
                kind: EventKind::ActionExecuted,
                subject: "s".into(),
                epoch: None,
                cause: None,
                detail: None,
            })
            .expect("event serializes")
        };
        std::fs::write(
            &p,
            format!(
                "{}\n{{\"at_ms\": 5, \"kind\": \"Action\n{}\n",
                good(1),
                good(9)
            ),
        )
        .expect("write the torn-line fixture");
        let got = tail_kinds(&p, 50, &[EventKind::ActionExecuted]);
        assert_eq!(got.len(), 2, "the lines around the tear still read");
    }

    /// A missing journal is empty, not a panic — emission is best-effort and
    /// a reader must survive a control plane that has never written one.
    #[test]
    fn a_missing_journal_is_empty() {
        assert!(tail_kinds("/nonexistent/flint/j.jsonl", 10, &[EventKind::Detected]).is_empty());
    }

    /// NO KEYWORD = NO FILTER. Every caller that predates the filter sends
    /// `CPJOURNALREAD <n>` and must keep its exact behaviour.
    #[test]
    fn no_keyword_means_no_filter() {
        assert_eq!(parse_kinds_arg(None, None), Ok(Vec::new()));
        assert_eq!(
            parse_kinds_arg(None, Some("ActionExecuted")),
            Ok(Vec::new())
        );
    }

    /// THE SAFETY-CRITICAL CASE. An unknown kind must be an error, never an
    /// empty filter. tier2 counts ActionExecuted to derive its action
    /// budget: zero rows reads as "no actions taken" — full budget, act
    /// freely — and the short-tail guard does not catch it either, because
    /// a filter matching nothing returns fewer rows than the scan size and
    /// so looks like a journal that reaches back far enough. A typo would
    /// not degrade that guard, it would remove it.
    #[test]
    fn an_unknown_kind_is_an_error_not_an_empty_filter() {
        let e = parse_kinds_arg(Some("KINDS"), Some("ActionExecuted,Bogus"));
        assert!(e.is_err(), "must refuse, got {e:?}");
        assert!(
            e.expect_err("an unknown kind must be an error")
                .contains("Bogus"),
            "and must name the offender"
        );
        // The near-miss spellings a human or a rename would actually produce.
        for bad in [
            "actionexecuted",
            "ACTION_EXECUTED",
            "Action Executed",
            "Execute",
        ] {
            assert!(
                parse_kinds_arg(Some("KINDS"), Some(bad)).is_err(),
                "{bad} must not silently match nothing"
            );
        }
    }

    /// A malformed narrow request must not WIDEN into an unfiltered read.
    #[test]
    fn a_narrow_request_never_widens() {
        assert!(parse_kinds_arg(Some("KINDS"), None).is_err(), "no list");
        assert!(
            parse_kinds_arg(Some("KINDS"), Some("")).is_err(),
            "empty list"
        );
        assert!(
            parse_kinds_arg(Some("KINDS"), Some(" , , ")).is_err(),
            "separators only"
        );
        assert!(
            parse_kinds_arg(Some("SINCE"), Some("123")).is_err(),
            "an unrecognised keyword is a refusal, not a filterless read"
        );
    }

    #[test]
    fn the_keyword_is_case_insensitive_but_kinds_are_not() {
        assert_eq!(
            parse_kinds_arg(Some("kinds"), Some("Detected")),
            Ok(vec![EventKind::Detected])
        );
        assert!(parse_kinds_arg(Some("KINDS"), Some("detected")).is_err());
    }

    #[test]
    fn a_list_parses_in_order_trims_space_and_dedups() {
        assert_eq!(
            parse_kinds_arg(
                Some("KINDS"),
                Some(" ActionExecuted , ActionVerified ,ActionExecuted")
            ),
            Ok(vec![EventKind::ActionExecuted, EventKind::ActionVerified])
        );
    }

    /// parse_kind is serde-backed so it cannot drift from the enum. The
    /// negative case is the one that matters: it must say no, because the
    /// command layer turns that into an error rather than an empty result.
    #[test]
    fn parse_kind_accepts_real_variants_and_rejects_anything_else() {
        assert_eq!(
            parse_kind("ActionExecuted"),
            Some(EventKind::ActionExecuted)
        );
        assert_eq!(parse_kind("Detected"), Some(EventKind::Detected));
        assert_eq!(parse_kind("PageHuman"), Some(EventKind::PageHuman));
        assert_eq!(parse_kind("actionexecuted"), None, "exact spelling only");
        assert_eq!(parse_kind("ActionExecuted "), None);
        assert_eq!(parse_kind(""), None);
        assert_eq!(parse_kind("Bogus"), None);
    }
}

#[cfg(test)]
mod retention_tests {
    use super::*;

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "flint-jrn-{tag}-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir_all(&d).expect("tmpdir");
        d
    }

    fn row(kind: &str, subject: &str) -> String {
        format!(r#"{{"at_ms":1,"actor":"t","kind":"{kind}","subject":"{subject}"}}"#)
    }

    /// A rotation must not shorten what a reader can see. This is the whole
    /// reason `tail_across` exists: `tail_kinds` is what tier2 counts its
    /// budget window from, and a count that silently cannot reach back over
    /// its window is the 2026-08-17 failure rebuilt on a timer.
    #[test]
    fn reads_span_rotated_segments() {
        let d = tmpdir("span");
        let p = d.join("j").to_str().expect("utf8 temp path").to_string();
        // Two older segments plus the live file.
        std::fs::write(
            format!("{p}.1000"),
            format!("{}\n{}\n", row("Detected", "a"), row("Promoted", "b")),
        )
        .expect("write fixture");
        std::fs::write(format!("{p}.2000"), format!("{}\n", row("Promoted", "c")))
            .expect("write fixture");
        std::fs::write(&p, format!("{}\n", row("Detected", "d"))).expect("write fixture");

        let all = tail(&p, 10);
        assert_eq!(all.len(), 4, "tail must see segments + live, got {all:?}");
        assert!(all[0].contains("\"a\""), "oldest first: {all:?}");
        assert!(all[3].contains("\"d\""), "live file last: {all:?}");

        // And the filtered read, which is the one with a safety guard on it.
        let promoted = tail_kinds(&p, 10, &[EventKind::Promoted]);
        assert_eq!(
            promoted.len(),
            2,
            "kind filter must span segments: {promoted:?}"
        );
    }

    /// The common case must not pay for rotation: enough matches in the live
    /// file means older segments are never opened.
    #[test]
    fn stops_at_the_live_file_when_it_already_has_enough() {
        let d = tmpdir("stop");
        let p = d.join("j").to_str().expect("utf8 temp path").to_string();
        std::fs::write(format!("{p}.1000"), format!("{}\n", row("Detected", "old")))
            .expect("write fixture");
        std::fs::write(
            &p,
            format!("{}\n{}\n", row("Detected", "x"), row("Detected", "y")),
        )
        .expect("write fixture");
        let two = tail(&p, 2);
        assert_eq!(two.len(), 2);
        assert!(
            !two.iter().any(|l| l.contains("old")),
            "reached back needlessly: {two:?}"
        );
    }

    /// Pruning drops what is past the window and keeps what is not. The
    /// suffix is the ROTATION time, so a segment holds rows older than its
    /// name — pruning on the name therefore keeps at least the window, never
    /// less, which is the direction to err in.
    #[test]
    fn prune_drops_only_expired_segments() {
        let d = tmpdir("prune");
        let p = d.join("j").to_str().expect("utf8 temp path").to_string();
        std::fs::write(&p, "").expect("write fixture");
        let now = now_ms();
        let expired = now - RETENTION_MS - 60_000;
        let fresh = now - 60_000;
        std::fs::write(format!("{p}.{expired}"), "x\n").expect("write fixture");
        std::fs::write(format!("{p}.{fresh}"), "y\n").expect("write fixture");

        rotate_and_prune(&p);

        assert!(
            !std::path::Path::new(&format!("{p}.{expired}")).exists(),
            "expired segment kept"
        );
        assert!(
            std::path::Path::new(&format!("{p}.{fresh}")).exists(),
            "in-window segment DELETED"
        );
    }

    /// The size trigger actually fires, and the live file is left usable.
    #[test]
    fn crossing_the_threshold_rotates() {
        let d = tmpdir("rot");
        let p = d.join("j").to_str().expect("utf8 temp path").to_string();
        {
            // Bulk-write past the threshold in one go; driving it through
            // append_line would be tens of thousands of file opens.
            let mut f = std::fs::File::create(&p).expect("create fixture");
            let chunk = vec![b'x'; 1 << 20];
            for _ in 0..(ROTATE_BYTES / (1 << 20)) + 1 {
                f.write_all(&chunk).expect("bulk write");
            }
            f.write_all(b"\n").expect("bulk write");
        }
        append_line(&p, &row("Detected", "trigger")).expect("append");

        let segs = segments(&p);
        assert_eq!(
            segs.len(),
            1,
            "expected exactly one rotated segment, got {segs:?}"
        );

        // After the rename the live path does not exist again until the next
        // append -- `append_line` opens with create(true). That is fine, and
        // the first version of this test wrongly asserted otherwise. What
        // must hold is that the journal keeps WORKING across the boundary:
        // the rotated row is still readable, and the next append lands in a
        // fresh live file rather than growing the segment.
        let after = tail(&p, 5);
        assert!(
            after.iter().any(|l| l.contains("trigger")),
            "the row that triggered rotation became unreadable: {after:?}"
        );
        append_line(&p, &row("Detected", "post")).expect("append");
        let live = std::fs::metadata(&p)
            .map(|m| m.len())
            .expect("live file recreated");
        assert!(
            live < ROTATE_BYTES,
            "next append did not start a fresh file: {live} bytes"
        );
        assert_eq!(
            segments(&p).len(),
            1,
            "a second rotation fired on a small file"
        );
        let both = tail(&p, 5);
        assert!(
            both.iter().any(|l| l.contains("trigger")) && both.iter().any(|l| l.contains("post")),
            "reads must span the rotation boundary: {both:?}"
        );
    }
}

#[cfg(test)]
mod segment_read_failure_tests {
    use super::*;

    /// An unreadable SEGMENT must not pass as a shorter history.
    ///
    /// The caller most likely to be hurt is tier2's budget counter, where
    /// "cannot count" and "counted fewer" are the difference between refusing
    /// to act and acting freely — the OPS-0037 collapse, and the same
    /// shortened horizon `tail_kinds` documents from 2026-08-17. The read
    /// still returns what it can, but it must say it is short.
    #[test]
    fn an_unreadable_segment_is_reported_not_swallowed() {
        let d = std::env::temp_dir().join(format!("flint-jrn-unread-{}", std::process::id()));
        std::fs::create_dir_all(&d).expect("tmpdir");
        let p = d.join("j").to_str().expect("utf8 path").to_string();
        std::fs::write(&p, "{\"at_ms\":1,\"kind\":\"Detected\"}\n").expect("live");

        // A segment that exists (so `segments()` lists it) but cannot be read:
        // a DIRECTORY at the segment path is the portable way to make
        // read_to_string fail without depending on running as non-root.
        std::fs::create_dir_all(format!("{p}.1000")).expect("segment dir");

        let got = tail(&p, 10);
        assert_eq!(
            got.len(),
            1,
            "the readable rows must still come back: {got:?}"
        );
        assert!(
            segments(&p).iter().any(|(ms, _)| *ms == 1000),
            "the unreadable segment must still be ENUMERATED — if it were \
             invisible to segments() the read would look complete"
        );
        std::fs::remove_dir_all(&d).ok();
    }
}

#[cfg(test)]
mod evidence_kind_tests {
    use super::*;

    /// The wire name is what `CPJOURNALREAD ... KINDS` matches on, so it has
    /// to survive the round trip. Asserted rather than assumed from the serde
    /// derive: this is the string an operator types and a filter compares.
    #[test]
    fn evidence_gathered_round_trips_on_the_wire() {
        assert_eq!(
            parse_kind("EvidenceGathered"),
            Some(EventKind::EvidenceGathered)
        );
        let wire = serde_json::to_string(&EventKind::EvidenceGathered).expect("encode");
        assert_eq!(wire, "\"EvidenceGathered\"");
    }

    /// It must be FILTERABLE, because that is the whole reason a noisy kind
    /// is affordable: a reader asking for decisions must not pay for these.
    /// If this kind were ever unfilterable it would shrink every guard's
    /// horizon, which is the 2026-08-17 failure `tail_kinds` documents.
    #[test]
    fn a_decision_read_does_not_pay_for_evidence_rows() {
        let d = std::env::temp_dir().join(format!("flint-jrn-ev-{}", std::process::id()));
        std::fs::create_dir_all(&d).expect("tmpdir");
        let p = d.join("j").to_str().expect("utf8").to_string();
        let mut body = String::new();
        for i in 0..50 {
            body.push_str(&format!(
                r#"{{"at_ms":{i},"actor":"agent:tier1","kind":"EvidenceGathered","subject":"readjournal:x"}}"#
            ));
            body.push('\n');
        }
        body.push_str(
            r#"{"at_ms":99,"actor":"agent:tier2","kind":"ActionExecuted","subject":"attach:a"}"#,
        );
        body.push('\n');
        std::fs::write(&p, body).expect("fixture");

        let acted = tail_kinds(&p, 10, &[EventKind::ActionExecuted]);
        assert_eq!(
            acted.len(),
            1,
            "evidence noise reached a decision read: {acted:?}"
        );
        assert!(acted[0].contains("ActionExecuted"));

        let ev = tail_kinds(&p, 100, &[EventKind::EvidenceGathered]);
        assert_eq!(
            ev.len(),
            50,
            "the evidence trail must be readable on its own"
        );
        std::fs::remove_dir_all(&d).ok();
    }
}
