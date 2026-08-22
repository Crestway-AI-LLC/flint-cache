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
/// Which master could have served an ack, judged by its SEND stamp against the
/// instant the dead master stopped being able to serve anything. Both stamps
/// are wall-clock MICROSECONDS.
///
/// This exists as a named three-way answer because the two-way version was
/// wrong in a way that could not be seen: the old code wrote `sent >= dead`
/// and folded the boundary case into "the new master's", so a tie was reported
/// with the same confidence as a decided case. BUG-0014 then rested an entire
/// durability verdict on one entry sitting exactly there.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Served {
    /// Sent strictly before the death stamp — the dying master may have served
    /// it, so its loss is the async-replication window, not a regression.
    MaybeOldMaster,
    /// Sent strictly after — provably the new master's, and not this
    /// failover's business.
    NewMaster,
    /// Sent at the death stamp exactly. The death instant lies at or before
    /// that stamp, so both readings remain possible and NOTHING recorded
    /// separates them. Callers must exclude these from measurements rather
    /// than assign them; an unattributable write is not a conservative
    /// estimate, it is a number with no referent.
    Ambiguous,
}

/// The single implementation of that judgement. Two call sites used to spell
/// it out independently — the loss loop and the failure dump — and they
/// disagreed at the boundary, which is how the dump could print `false` for a
/// tie while the loop silently treated it as the new master's.
pub fn classify_send(sent_us: u64, dead_us: u64) -> Served {
    match sent_us.cmp(&dead_us) {
        std::cmp::Ordering::Less => Served::MaybeOldMaster,
        std::cmp::Ordering::Equal => Served::Ambiguous,
        std::cmp::Ordering::Greater => Served::NewMaster,
    }
}

#[derive(Default)]
pub struct KeyLedger {
    pub written: Vec<u64>,
    pub last_acked: u64,
    pub last_written: u64,
    /// `(seq, SENT wall-clock MICROSECONDS, ACKED wall-clock ms)`, ascending
    /// by seq. The two stamps are deliberately in different units — see
    /// `writer::now_us`. The send stamp answers an ORDERING question against
    /// the death instant, which milliseconds cannot resolve; the ack stamp
    /// feeds the RPO bound, which is a claim about milliseconds.
    ///
    /// Both times are kept because they answer different questions and
    /// conflating them caused a false data-loss verdict (#130):
    ///
    /// * **Which master could have served this?** — the SEND time. A request
    ///   already in flight when a master is killed may be acked by that dying
    ///   master, and the reply is read *after* the kill instant. Judging by
    ///   ack time therefore files it under "acked after the kill, so it
    ///   belongs to the new master", the ledger keeps claiming a value the
    ///   survivor never had, and the next REPLICA kill — which is allowed no
    ///   loss at all — reports data loss for a write the MASTER kill lost
    ///   legitimately, inside the async window.
    /// * **Was the durability promise honoured?** — the ACK time. That is
    ///   when the client was told the write was safe, which is what the RPO
    ///   bound is a claim about.
    pub acked_at: Vec<(u64, u64, u64)>,
}

impl KeyLedger {
    pub fn record_ack(&mut self, seq: u64, sent_us: u64, acked_ms: u64) {
        self.last_acked = seq;
        self.acked_at.push((seq, sent_us, acked_ms));
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
            .filter(|&&(seq, _sent_us, acked)| seq > got && acked <= must_have_replicated_by_ms)
            .map(|&(seq, _sent_us, acked)| (seq, acked))
            .collect()
    }

    /// The newest ack that survived scrutiny, for reporting how deep the
    /// observed loss actually went versus the cap that permits it.
    pub fn newest_lost_ack_ms(&self, got: u64) -> Option<u64> {
        self.acked_at
            .iter()
            .filter(|&&(seq, _sent_us, _acked)| seq > got)
            .map(|&(_, _sent_us, acked)| acked)
            .max()
    }
}

#[cfg(test)]
mod boundary_tests {
    use super::{Served, classify_send};

    #[test]
    fn a_send_strictly_before_the_death_stamp_may_be_the_old_masters() {
        assert_eq!(classify_send(1_000_000, 1_000_001), Served::MaybeOldMaster);
    }

    #[test]
    fn a_send_strictly_after_the_death_stamp_is_provably_the_new_masters() {
        assert_eq!(classify_send(1_000_001, 1_000_000), Served::NewMaster);
    }

    /// The regression test for BUG-0014. The old code was `sent >= dead`, so a
    /// tie took the same branch as a send provably after the kill and was
    /// silently attributed to the new master — which is what let one entry at
    /// the clock's resolution limit carry a durability verdict.
    ///
    /// Asserting `Ambiguous` here fails against that old behaviour, which is
    /// the only reason this test is worth having: it distinguishes the fix
    /// from the bug rather than confirming the code does what it says.
    #[test]
    fn a_tie_is_neither_master_and_must_not_be_silently_assigned() {
        let verdict = classify_send(1_000_000, 1_000_000);
        assert_eq!(verdict, Served::Ambiguous);
        assert_ne!(verdict, Served::NewMaster, "the pre-fix `>=` reading");
        assert_ne!(verdict, Served::MaybeOldMaster, "the opposite bias");
    }

    /// Ordering must come from the stamps alone. A classifier that read the
    /// magnitudes rather than their relation would pass the three cases above
    /// while breaking on any other epoch.
    #[test]
    fn the_judgement_is_relational_not_absolute() {
        for &(sent, dead) in &[(0u64, 0u64), (u64::MAX, u64::MAX), (7, 7)] {
            assert_eq!(classify_send(sent, dead), Served::Ambiguous);
        }
        assert_eq!(classify_send(0, u64::MAX), Served::MaybeOldMaster);
        assert_eq!(classify_send(u64::MAX, 0), Served::NewMaster);
    }
}
