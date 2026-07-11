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
