//! Intra-group slot migration — the mechanism behind capacity expansion and
//! data balance within a shard group (docs/design.md §2.3).
//!
//! When a pair fills or runs hot, the group controller moves whole slots to
//! another pair. This module is the storage-layer core of that move: it
//! relies on the envelope invariant (`cf | ns | slot(BE) | user_key`) that
//! makes every row of one slot a contiguous key range, so a slot is copied
//! by a prefix scan and dropped by deleting the scanned keys — no per-key
//! bookkeeping, no dual-write phase.
//!
//! A slot's rows span EVERY column family: a hash or zset keeps its metadata
//! in `M` but its fields/scores in `S`/`Z`. Moving only `M` would strand a
//! complex value's contents, so migration walks all CFs per slot.
//!
//! Ordering is copy-before-delete per (slot, cf): a crash leaves the rows on
//! BOTH sides — readable, never lost — and the higher-layer cutover
//! (IMPORTING/MIGRATING manifest states, ADR-0004) resolves the duplication
//! by flipping ownership atomically. The primitive here is therefore
//! at-least-once and idempotent: re-running drains an already-empty source
//! and moves nothing.

use crate::Kv;
use crate::encoding::{Cf, slot_prefix};

/// Every column family a slot's rows can live in. Migration must cover all of
/// them or complex values arrive gutted.
pub const ALL_CFS: [Cf; 3] = [Cf::Metadata, Cf::Subkey, Cf::ZScore];

/// Outcome of a migration, for accounting and the fleet journal.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MigrationReport {
    /// Rows copied to the destination and removed from the source.
    pub rows_moved: usize,
    /// Distinct slots that held at least one row.
    pub slots_nonempty: usize,
}

/// Move every row of `slots` (across all CFs) from `src` to `dst` within one
/// namespace. Copy-before-delete for crash safety; idempotent. Returns what
/// moved.
pub fn migrate_slots(src: &dyn Kv, dst: &dyn Kv, ns: &[u8], slots: &[u16]) -> MigrationReport {
    let mut report = MigrationReport::default();
    for &slot in slots {
        let mut slot_had_rows = false;
        for cf in ALL_CFS {
            let prefix = slot_prefix(cf, ns, slot);
            let rows = src.scan_prefix(&prefix);
            if rows.is_empty() {
                continue;
            }
            slot_had_rows = true;
            // Copy first: the destination owns the data before the source
            // gives it up, so an interruption can duplicate but never drop.
            for (k, v) in &rows {
                dst.put(k, v);
            }
            for (k, _) in &rows {
                src.delete(k);
            }
            report.rows_moved += rows.len();
        }
        if slot_had_rows {
            report.slots_nonempty += 1;
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;
    use crate::strings::{SetOptions, StringStore, system_clock};
    use flint_slot::slot_for_key;

    const NS: &[u8] = b"0";

    fn count_rows(kv: &dyn Kv) -> usize {
        ALL_CFS
            .iter()
            .map(|&cf| kv.scan_prefix(&[cf as u8]).len())
            .sum()
    }

    /// The load-bearing test: moving a subset of slots to a new store must
    /// carry every migrated key (readable on the destination, gone from the
    /// source), leave every other key exactly where it was, and conserve the
    /// total row count — no loss, no duplication, no cross-slot bleed.
    #[test]
    fn migrating_a_slot_subset_balances_data_losslessly() {
        let src = MemKv::default();
        let dst = MemKv::default();
        let write = StringStore::new(&src, NS, system_clock);

        // Populate keys spread across many slots; remember each key's slot.
        let mut keys_by_slot: std::collections::BTreeMap<u16, Vec<(String, String)>> =
            Default::default();
        for i in 0..2_000u32 {
            let key = format!("user:{i}:profile");
            let val = format!("value-{i}");
            let slot = slot_for_key(key.as_bytes());
            write
                .set(slot, key.as_bytes(), val.as_bytes(), SetOptions::default())
                .expect("seed set");
            keys_by_slot.entry(slot).or_default().push((key, val));
        }

        let total_rows_before = count_rows(&src);
        assert_eq!(total_rows_before, 2_000, "one meta row per string key");

        // Migrate the lower half of the occupied slots to the destination.
        let occupied: Vec<u16> = keys_by_slot.keys().copied().collect();
        let split = occupied.len() / 2;
        let moving: Vec<u16> = occupied[..split].to_vec();
        let staying: Vec<u16> = occupied[split..].to_vec();
        let expected_moved: usize = moving.iter().map(|s| keys_by_slot[s].len()).sum();

        let report = migrate_slots(&src, &dst, NS, &moving);
        assert_eq!(report.rows_moved, expected_moved);
        assert_eq!(report.slots_nonempty, moving.len());

        // Conservation: nothing created or destroyed overall.
        assert_eq!(
            count_rows(&src) + count_rows(&dst),
            total_rows_before,
            "rows conserved across the move (no loss, no duplication)"
        );

        let on_dst = StringStore::new(&dst, NS, system_clock);
        let on_src = StringStore::new(&src, NS, system_clock);

        // Every moved key: present on dst with its value, absent from src.
        for slot in &moving {
            for (key, val) in &keys_by_slot[slot] {
                assert_eq!(
                    on_dst
                        .get(*slot, key.as_bytes())
                        .expect("store read")
                        .as_deref(),
                    Some(val.as_bytes()),
                    "moved key {key} not readable on destination"
                );
                assert!(
                    on_src
                        .get(*slot, key.as_bytes())
                        .expect("store read")
                        .is_none(),
                    "moved key {key} still on source (cross-slot leak or missed delete)"
                );
            }
        }
        // Every stayed key: untouched on src, never on dst.
        for slot in &staying {
            for (key, val) in &keys_by_slot[slot] {
                assert_eq!(
                    on_src
                        .get(*slot, key.as_bytes())
                        .expect("store read")
                        .as_deref(),
                    Some(val.as_bytes()),
                    "stayed key {key} disturbed on source"
                );
                assert!(
                    on_dst
                        .get(*slot, key.as_bytes())
                        .expect("store read")
                        .is_none(),
                    "stayed key {key} leaked to destination"
                );
            }
        }
    }

    /// A complex value (hash) keeps its fields in the Subkey CF; migrating its
    /// slot must carry the fields, not just the metadata row. A move that
    /// covered only `M` would leave an empty hash on the destination.
    #[test]
    fn migrating_a_slot_carries_complex_value_subrows() {
        use crate::hashes::HashStore;

        let src = MemKv::default();
        let dst = MemKv::default();

        let key = b"session:42";
        let slot = slot_for_key(key);
        let fields: Vec<(Vec<u8>, Vec<u8>)> = (0..50)
            .map(|i| (format!("f{i}").into_bytes(), format!("v{i}").into_bytes()))
            .collect();
        {
            let h = HashStore::new(&src, NS, system_clock);
            h.hset(slot, key, &fields).expect("hset");
        }
        // Meta row (M) + 50 field rows (S) = 51 rows for this one slot.
        assert_eq!(count_rows(&src), 51);

        let report = migrate_slots(&src, &dst, NS, &[slot]);
        assert_eq!(report.rows_moved, 51, "meta + all fields move together");
        assert_eq!(count_rows(&src), 0, "source slot fully drained");

        let on_dst = HashStore::new(&dst, NS, system_clock);
        for (f, v) in &fields {
            assert_eq!(
                on_dst.hget(slot, key, f).expect("store read").as_deref(),
                Some(v.as_slice()),
                "hash field lost in migration"
            );
        }
    }

    /// Re-running a completed migration is a no-op: the source is already
    /// drained, so nothing moves and the destination is unchanged. This is
    /// the property that makes a controller safe to retry a move after a
    /// crash without double-applying.
    #[test]
    fn migration_is_idempotent() {
        let src = MemKv::default();
        let dst = MemKv::default();
        let write = StringStore::new(&src, NS, system_clock);
        let key = b"k";
        let slot = slot_for_key(key);
        write
            .set(slot, key, b"v", SetOptions::default())
            .expect("set");

        let first = migrate_slots(&src, &dst, NS, &[slot]);
        assert_eq!(first.rows_moved, 1);
        let second = migrate_slots(&src, &dst, NS, &[slot]);
        assert_eq!(
            second.rows_moved, 0,
            "already migrated: nothing left to move"
        );
        assert_eq!(count_rows(&dst), 1, "destination unchanged on re-run");
    }

    /// Adjacent slots must not bleed: keys of slot N stay put when only slot
    /// M is moved, even where user keys share a textual prefix. This is the
    /// envelope's slot(BE) segment doing its job — the guard the whole
    /// rebalancing design leans on.
    #[test]
    fn migration_respects_slot_boundaries() {
        let src = MemKv::default();
        let dst = MemKv::default();
        let write = StringStore::new(&src, NS, system_clock);

        // Find two distinct keys that fall in different slots.
        let a = b"balance:aaaa";
        let b = b"balance:zzzz";
        let sa = slot_for_key(a);
        let sb = slot_for_key(b);
        assert_ne!(sa, sb, "test needs two keys in different slots");
        write
            .set(sa, a, b"A", SetOptions::default())
            .expect("set a");
        write
            .set(sb, b, b"B", SetOptions::default())
            .expect("set b");

        migrate_slots(&src, &dst, NS, &[sa]);

        let on_src = StringStore::new(&src, NS, system_clock);
        let on_dst = StringStore::new(&dst, NS, system_clock);
        // Only slot sa moved.
        assert_eq!(
            on_dst.get(sa, a).expect("store read").as_deref(),
            Some(b"A".as_ref())
        );
        assert!(on_src.get(sa, a).expect("store read").is_none());
        // Slot sb untouched.
        assert_eq!(
            on_src.get(sb, b).expect("store read").as_deref(),
            Some(b"B".as_ref())
        );
        assert!(on_dst.get(sb, b).expect("store read").is_none());
    }
}
