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
    /// A controller observed a pair converged for the first time in its
    /// process lifetime: the degraded-window gate is open and auto-failover
    /// is ARMED for that pair. Tooling that (re)starts controllers waits
    /// for this before declaring an operation complete.
    Supervised,
    /// The metering loop flipped a tenant's storage-quota verdict (M5).
    /// detail carries "on <used>/<cap>" or "off <used>/<cap>".
    QuotaVerdict,
    /// Tier-1: the agent EXECUTED an allowlisted catalog action (M5).
    /// detail carries the exact command run.
    ActionExecuted,
    /// Tier-1: the executed action's declared success signals verified
    /// healthy — the incident is closed by the agent.
    ActionVerified,
    /// Tier-1 escalation: the agent needed to act but could not (budget
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
pub fn append_line(path: &str, json_line: &str) -> std::io::Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let line = json_line.trim_end();
    let mut buf = Vec::with_capacity(line.len() + 1);
    buf.extend_from_slice(line.as_bytes());
    buf.push(b'\n');
    f.write_all(&buf)
}

/// Control-plane side: the last `n` journal lines, oldest first.
pub fn tail(path: &str, n: usize) -> Vec<String> {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let lines: Vec<&str> = raw.lines().collect();
    lines
        .iter()
        .skip(lines.len().saturating_sub(n))
        .map(|s| s.to_string())
        .collect()
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
