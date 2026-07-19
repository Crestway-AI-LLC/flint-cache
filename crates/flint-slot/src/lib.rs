// SPDX-License-Identifier: Elastic-2.0
//! Redis Cluster-compatible key→slot mapping.
//!
//! The key→slot mapping is fixed forever; only slot *ownership* moves.
//! Compatibility contract: identical results to `CLUSTER KEYSLOT` in
//! Redis/Valkey, including hash-tag semantics.

/// Number of hash slots per namespace. Fixed for the lifetime of the system.
pub const SLOT_COUNT: u16 = 16384;

/// CRC16 (CCITT/XMODEM variant used by Redis Cluster): poly 0x1021, init 0,
/// no reflection, no final XOR.
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// Extract the effective hashable portion of a key per Redis hash-tag rules:
/// if the key contains `{` and a subsequent `}` with at least one byte
/// between them, only the bytes between the *first* `{` and the *first*
/// following `}` are hashed. Otherwise the whole key is hashed.
pub fn hash_tag(key: &[u8]) -> &[u8] {
    if let Some(open) = key.iter().position(|&b| b == b'{')
        && let Some(close_rel) = key[open + 1..].iter().position(|&b| b == b'}')
        && close_rel > 0
    {
        return &key[open + 1..open + 1 + close_rel];
    }
    key
}

/// Map a key to its slot.
pub fn slot_for_key(key: &[u8]) -> u16 {
    crc16(hash_tag(key)) % SLOT_COUNT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc16_reference_vector() {
        // Canonical XMODEM check value, also cited in the Redis Cluster spec.
        assert_eq!(crc16(b"123456789"), 0x31C3);
    }

    #[test]
    fn known_redis_keyslots() {
        // Values verified against `CLUSTER KEYSLOT` on Redis 7.
        assert_eq!(slot_for_key(b"foo"), 12182);
        assert_eq!(slot_for_key(b"bar"), 5061);
        assert_eq!(slot_for_key(b"123456789"), 0x31C3 % SLOT_COUNT);
    }

    #[test]
    fn hash_tags_group_keys() {
        assert_eq!(
            slot_for_key(b"{user1000}.following"),
            slot_for_key(b"{user1000}.followers")
        );
        assert_eq!(hash_tag(b"{user1000}.following"), b"user1000");
    }

    #[test]
    fn hash_tag_edge_cases() {
        // Empty tag `{}` → whole key is hashed.
        assert_eq!(hash_tag(b"foo{}bar"), b"foo{}bar");
        // No closing brace → whole key.
        assert_eq!(hash_tag(b"foo{bar"), b"foo{bar");
        // First { pairs with first } after it.
        assert_eq!(hash_tag(b"foo{{bar}}baz"), b"{bar");
        // Only the first tag counts.
        assert_eq!(hash_tag(b"{a}{b}"), b"a");
        // Brace after close is irrelevant.
        assert_eq!(hash_tag(b"{a}b{c}"), b"a");
    }

    #[test]
    fn all_slots_in_range() {
        for i in 0u32..100_000 {
            let key = format!("key:{i}");
            assert!(slot_for_key(key.as_bytes()) < SLOT_COUNT);
        }
    }
}

/// An ordered, non-overlapping set of slot intervals, each mapping a
/// contiguous `[lo, hi]` range to a small index — the interval model of
/// routing state at EVERY level (ADR-0007): level-1 uses it as
/// `range → pair index` inside a cluster; level-0 (federation) uses it as
/// `range → cluster index` for a federated tenant. Single-slot intervals
/// (`lo == hi`) are legal — the mid-migration shape. Compact at rest and
/// on the wire; expand to a dense array only where O(1) lookup matters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotIntervals {
    /// Sorted by `lo`, non-overlapping: `(lo, hi, index)`, `lo <= hi`.
    entries: Vec<(u16, u16, u16)>,
}

impl SlotIntervals {
    /// The whole slot space owned by one index — the non-federated default
    /// (every slot → cluster 0) and the fresh-pair default.
    pub fn single(index: u16) -> Self {
        SlotIntervals {
            entries: vec![(0, SLOT_COUNT - 1, index)],
        }
    }

    /// Build from arbitrary entries; sorts and validates. `None` when an
    /// interval is inverted (`lo > hi`), out of the slot space, or two
    /// intervals overlap — a malformed map must fail loudly at parse time,
    /// never route wrongly at lookup time.
    pub fn from_entries(mut entries: Vec<(u16, u16, u16)>) -> Option<Self> {
        entries.sort_unstable();
        for (i, &(lo, hi, _)) in entries.iter().enumerate() {
            if lo > hi || hi >= SLOT_COUNT {
                return None;
            }
            if let Some(&(next_lo, _, _)) = entries.get(i + 1)
                && next_lo <= hi
            {
                return None;
            }
        }
        Some(SlotIntervals { entries })
    }

    /// The index owning `slot`, or `None` for an uncovered slot (callers
    /// choose the fallback — the proxy falls back to its default map).
    pub fn lookup(&self, slot: u16) -> Option<u16> {
        let i = self.entries.partition_point(|&(lo, _, _)| lo <= slot);
        let &(lo, hi, idx) = self.entries.get(i.checked_sub(1)?)?;
        (slot >= lo && slot <= hi).then_some(idx)
    }

    /// The raw sorted entries (wire encoding, consolidation planning).
    pub fn entries(&self) -> &[(u16, u16, u16)] {
        &self.entries
    }

    /// Dense O(1) form: one index per slot (`u16::MAX` = uncovered). The
    /// in-RAM shape for hot-path routing; ~32 KB per map.
    pub fn dense(&self) -> Vec<u16> {
        let mut d = vec![u16::MAX; SLOT_COUNT as usize];
        for &(lo, hi, idx) in &self.entries {
            for s in lo..=hi {
                d[s as usize] = idx;
            }
        }
        d
    }
}

#[cfg(test)]
mod interval_tests {
    use super::*;

    #[test]
    fn single_covers_everything() {
        let m = SlotIntervals::single(3);
        assert_eq!(m.lookup(0), Some(3));
        assert_eq!(m.lookup(16383), Some(3));
        assert_eq!(m.lookup(9999), Some(3));
    }

    #[test]
    fn from_entries_sorts_validates_and_looks_up() {
        // Out of order in, sorted out; single-slot interval legal.
        let m = SlotIntervals::from_entries(vec![(100, 100, 7), (0, 99, 1), (101, 16383, 2)])
            .expect("valid");
        assert_eq!(m.lookup(0), Some(1));
        assert_eq!(m.lookup(99), Some(1));
        assert_eq!(m.lookup(100), Some(7));
        assert_eq!(m.lookup(101), Some(2));
        assert_eq!(m.lookup(16383), Some(2));
    }

    #[test]
    fn gaps_are_uncovered_not_wrong() {
        let m = SlotIntervals::from_entries(vec![(10, 20, 1)]).expect("valid");
        assert_eq!(m.lookup(9), None);
        assert_eq!(m.lookup(21), None);
        assert_eq!(m.lookup(15), Some(1));
    }

    #[test]
    fn malformed_maps_fail_at_parse() {
        assert!(SlotIntervals::from_entries(vec![(20, 10, 1)]).is_none()); // inverted
        assert!(SlotIntervals::from_entries(vec![(0, 16384, 1)]).is_none()); // out of space
        assert!(SlotIntervals::from_entries(vec![(0, 10, 1), (10, 20, 2)]).is_none()); // overlap
        assert!(SlotIntervals::from_entries(vec![(0, 10, 1), (5, 8, 2)]).is_none()); // nested
    }

    #[test]
    fn dense_matches_lookup() {
        let m = SlotIntervals::from_entries(vec![(0, 8191, 0), (8192, 16383, 1)]).expect("valid");
        let d = m.dense();
        for s in [0u16, 8191, 8192, 16383, 4000, 12000] {
            assert_eq!(d[s as usize], m.lookup(s).unwrap_or(u16::MAX));
        }
    }
}

/// The DEFAULT owner of `slot` given per-pair contiguous ranges — the one
/// definition shared by the proxy's routing fallback and the control
/// plane's exception-redundancy check (an exception row that agrees with
/// this is not an exception and retires itself). Semantics: a range-owned
/// slot belongs to that pair; otherwise the count-derived split applies —
/// across the RANGED prefix when any ranges exist (an unranged expansion
/// pair never absorbs slots by mere existence), across all pairs when none
/// do. `None` when there are no pairs.
pub fn default_pair(slot: u16, ranges: &[Option<(u16, u16)>], pair_count: usize) -> Option<usize> {
    if pair_count == 0 {
        return None;
    }
    if let Some(i) = ranges
        .iter()
        .position(|r| matches!(r, Some((a, b)) if (*a..=*b).contains(&slot)))
    {
        return Some(i);
    }
    let ranged = ranges.iter().filter(|r| r.is_some()).count();
    let n = if ranged > 0 { ranged } else { pair_count };
    Some((slot as usize * n) / 16384)
}

#[cfg(test)]
mod default_pair_tests {
    use super::*;

    #[test]
    fn range_owned_wins_then_count_derived_over_ranged_prefix() {
        let ranges = vec![Some((0u16, 99u16)), Some((100, 199)), None];
        assert_eq!(default_pair(50, &ranges, 3), Some(0));
        assert_eq!(default_pair(150, &ranges, 3), Some(1));
        // Uncovered slot: count-derived across the 2 RANGED pairs only —
        // the unranged expansion pair (idx 2) absorbs nothing.
        assert_eq!(default_pair(16000, &ranges, 3), Some(1));
        // No ranges at all: split across every pair.
        assert_eq!(default_pair(0, &[None, None], 2), Some(0));
        assert_eq!(default_pair(16383, &[None, None], 2), Some(1));
        assert_eq!(default_pair(0, &[], 0), None);
    }
}
