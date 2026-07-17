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
pub fn append_line(path: &str, json_line: &str) -> std::io::Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(f, "{}", json_line.trim_end())
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
