// SPDX-License-Identifier: Elastic-2.0
//! The KV ledger oracle — the ONE definition shared by every KV chaos
//! workload (`flint-chaos` direct-to-node, `proxy_chaos` through the proxy),
//! so the corruption/loss checks can never drift between what the data plane
//! is proven to guarantee and what the full client→proxy→node path is.
//!
//! Each value embeds its OWNING KEY literally, plus the write sequence and a
//! crc, so a value that surfaces under the wrong key — or is torn — is
//! detectable with certainty rather than by heuristic.

use flint_slot::crc16;

/// "flint|<key>|<seq>|<crc>" — self-identifying so misrouting and tearing
/// are provable, not guessed.
pub fn value_for(key: &str, seq: u64) -> String {
    let crc = crc16(format!("{key}|{seq}").as_bytes());
    format!("flint|{key}|{seq}|{crc:04x}")
}

/// Parse a stored value back to (owning-key, seq), rejecting anything whose
/// crc does not match — a torn or corrupted value returns None.
pub fn parse_value(raw: &[u8]) -> Option<(String, u64)> {
    let s = std::str::from_utf8(raw).ok()?;
    let mut parts = s.split('|');
    if parts.next()? != "flint" {
        return None;
    }
    let key = parts.next()?.to_string();
    let seq: u64 = parts.next()?.parse().ok()?;
    let crc: u16 = u16::from_str_radix(parts.next()?, 16).ok()?;
    if crc != crc16(format!("{key}|{seq}").as_bytes()) {
        return None;
    }
    Some((key, seq))
}

/// Per-key write history: every seq attempted, the last one ACKed (the
/// durability floor a kill must never regress below on a replica kill), and
/// the last one written (the ceiling — a read above it is time travel).
///
/// `acked_at` is what makes the RPO claim testable rather than assumed. The
/// harness used to quiesce the writer and wait for seq_lag==0 before killing
/// a master, which made the master's unreplicated suffix empty BY
/// CONSTRUCTION — so "acked keys regressed: 0" was a property of the test
/// shape, not evidence about the engine. Writing right up to the kill means
/// some acked writes legitimately do not survive: async replication with a
/// lag cap promises only that the loss window is BOUNDED. Bounded by what is
/// checkable only if each ack carries the wall-clock time it happened.
#[derive(Default)]
pub struct KeyLedger {
    pub written: Vec<u64>,
    pub last_acked: u64,
    pub last_written: u64,
    /// (seq, ack wall-clock ms), ascending by seq.
    pub acked_at: Vec<(u64, u64)>,
}

impl KeyLedger {
    pub fn record_ack(&mut self, seq: u64, at_ms: u64) {
        self.last_acked = seq;
        self.acked_at.push((seq, at_ms));
    }

    /// Acked writes that did NOT survive a promotion and were acked at or
    /// before `must_have_replicated_by_ms` — old enough that the lag cap
    /// guarantees replication carried them. Every entry is an RPO breach.
    ///
    /// `got` is the seq the survivor actually holds. Anything acked above it
    /// is lost; whether that is ALLOWED depends only on when it was acked.
    /// Writes acked inside the cap's window before the kill may vanish — that
    /// is the published contract, not a defect — so they are excluded here.
    pub fn breaches(&self, got: u64, must_have_replicated_by_ms: u64) -> Vec<(u64, u64)> {
        self.acked_at
            .iter()
            .filter(|&&(seq, at)| seq > got && at <= must_have_replicated_by_ms)
            .copied()
            .collect()
    }

    /// The newest ack that survived scrutiny, for reporting how deep the
    /// observed loss actually went versus the cap that permits it.
    pub fn newest_lost_ack_ms(&self, got: u64) -> Option<u64> {
        self.acked_at
            .iter()
            .filter(|&&(seq, _)| seq > got)
            .map(|&(_, at)| at)
            .max()
    }
}
