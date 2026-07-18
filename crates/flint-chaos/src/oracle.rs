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
#[derive(Default)]
pub struct KeyLedger {
    pub written: Vec<u64>,
    pub last_acked: u64,
    pub last_written: u64,
}
