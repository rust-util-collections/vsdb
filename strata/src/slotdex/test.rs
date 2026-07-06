use super::*;
use rand::random;

#[test]
fn workflow_normal() {
    [32, 16, 8].into_iter().for_each(|i| {
        slot_db(i, false);
    });
}

#[test]
fn workflow_swap_order() {
    [32, 16, 8].into_iter().for_each(|i| {
        slot_db(i, true);
    });
}

fn slot_db(mn: u64, swap_order: bool) {
    let mut db = SlotDex::new(mn, swap_order);
    let mut test_db = testdb::TestDB::default();

    let mut slot_min = u64::MAX;
    let mut slot_max = u64::MIN;

    (0..siz()).for_each(|i| {
        slot_min = i.min(slot_min);
        slot_max = i.max(slot_max);

        db.insert(i, i).unwrap();
        test_db.insert(i, i);
    });

    // Duplicate-slot entries, inserted in descending key order, so the
    // reference comparison also covers within-slot ordering (ascending
    // regardless of insertion order or paging direction).
    (0..64u64).for_each(|i| {
        let slot = i % 8;
        let key = siz() + 1000 - i;
        db.insert(slot, key).unwrap();
        test_db.insert(slot, key);
    });

    assert_eq!(siz() + 64, db.total());

    assert_queryable(&db, &test_db, slot_min, slot_max);

    db.clear();
    assert_eq!(0, db.total());
    assert!(db.get_entries_by_page(10, 0, true).is_empty());
    assert!(db.get_entries_by_page(10, 0, false).is_empty());
}

fn assert_queryable(
    db: &SlotDex<u64, u64>,
    test_db: &testdb::TestDB<u64>,
    slot_min: u64,
    slot_max: u64,
) {
    for _ in 0..16 {
        let page_size = 1 + (random::<u16>() as u32) % 128;
        let max_page = 100 + (db.total() / (page_size as u64)) as u32;

        // Ensure the first page case is covered
        let page_number = random::<u32>() % max_page;

        let page_size = page_size as u64;
        let page_number = page_number as u64;
        let _ = (page_number, page_size);

        let a = test_db.get_entries_by_page_slot(
            None,
            None,
            page_size as u16,
            page_number as u32,
            true,
        );
        let b = db.get_entries_by_page(page_size as u16, page_number as u32, true);
        assert_eq!(a, b);

        let a = test_db.get_entries_by_page_slot(
            None,
            None,
            page_size as u16,
            page_number as u32,
            false,
        );
        let b = db.get_entries_by_page(page_size as u16, page_number as u32, false);
        assert_eq!(a, b);

        //////////////////////////////////
        // Cases with custom slot range //
        //////////////////////////////////

        let smin = random::<u64>() % (slot_min.saturating_add(100));
        let smax = smin + random::<u64>() % ((slot_max - slot_min).saturating_add(100));

        ////////////////////////////////////////
        ////////////////////////////////////////

        let a = test_db.get_entries_by_page_slot(
            Some(smin),
            Some(smax),
            page_size as u16,
            page_number as u32,
            true,
        );

        let b = db.get_entries_by_page_slot(
            Some(smin),
            Some(smax),
            page_size as u16,
            page_number as u32,
            true,
        );
        assert_eq!(a, b);

        let a = test_db.get_entries_by_page_slot(
            Some(smin),
            Some(smax),
            page_size as u16,
            page_number as u32,
            false,
        );

        let b = db.get_entries_by_page_slot(
            Some(smin),
            Some(smax),
            page_size as u16,
            page_number as u32,
            false,
        );
        assert_eq!(a, b);
    }
}

const fn siz() -> u64 {
    if cfg!(debug_assertions) {
        1_000
    } else {
        100_000
    }
}

#[test]
fn single_handle_rows() {
    let mut db: SlotDex<u64, u32> = SlotDex::new(16, false);

    (0..100u32).for_each(|i| {
        db.insert(0, i).unwrap();
    });

    assert_eq!(db.total(), 100);
    // One slot holding 100 entries: within-slot order is ascending key order.
    let all = db.get_entries_by_page(200, 0, false);
    assert_eq!(all.len(), 100);
    assert_eq!(all.first().copied().unwrap(), 0);
    assert_eq!(all.last().copied().unwrap(), 99);

    // Everything lives in a single handle: entry rows + per-slot count
    // row + tier level rows + the total row, nothing else.
    let rows = db.store.iter().count();
    let level_rows: usize = db.levels.iter().map(|l| l.buckets.len()).sum();
    assert_eq!(rows, 100 + 1 + level_rows + 1);

    db.clear();
    assert_eq!(db.total(), 0);
    assert!(db.store.iter().next().is_none());
}

#[test]
fn reverse_paging_reverses_slots_only_not_within_slot() {
    // A slot is a set of keys (always ascending). Reverse paging must reverse
    // only the slot order, never the within-slot order, and must return
    // identical results under both storage layouts (`swap_order`).
    for swap_order in [false, true] {
        let mut db: SlotDex<u64, u64> = SlotDex::new(16, swap_order);
        // slot 5: {10,20,30}; slot 9: {40,50}
        for k in [10u64, 20, 30] {
            db.insert(5, k).unwrap();
        }
        for k in [40u64, 50] {
            db.insert(9, k).unwrap();
        }

        // Ascending: (slot asc, key asc).
        assert_eq!(
            db.get_entries_by_page(10, 0, false),
            vec![10, 20, 30, 40, 50]
        );

        // Reverse: (slot desc, key asc within slot).
        assert_eq!(
            db.get_entries_by_page(10, 0, true),
            vec![40, 50, 10, 20, 30]
        );

        // A single full slot in reverse keeps its within-slot ascending order.
        assert_eq!(
            db.get_entries_by_page_slot(Some(5), Some(5), 10, 0, true),
            vec![10, 20, 30]
        );

        // Reverse pagination membership across the slot boundary.
        assert_eq!(db.get_entries_by_page(2, 0, true), vec![40, 50]);
        assert_eq!(db.get_entries_by_page(2, 1, true), vec![10, 20]);
        assert_eq!(db.get_entries_by_page(2, 2, true), vec![30]);
    }
}

#[test]
fn hdr_meta_is_create_time_constant() {
    // The serialized handle metadata must never change after creation:
    // tier growth, removals, and clears only write ordinary data rows.
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    let at_creation = postcard::to_allocvec(&db).unwrap();

    for i in 0..500u64 {
        db.insert(i, i).unwrap();
    }
    assert!(!db.levels.is_empty(), "growth must have happened");
    assert_eq!(at_creation, postcard::to_allocvec(&db).unwrap());

    for i in 0..400u64 {
        db.remove(i, &i);
    }
    assert_eq!(at_creation, postcard::to_allocvec(&db).unwrap());

    db.clear();
    assert_eq!(at_creation, postcard::to_allocvec(&db).unwrap());

    // save_meta is idempotent pure persistence on a shared reference.
    let id1 = db.save_meta().unwrap();
    let id2 = db.save_meta().unwrap();
    assert_eq!(id1, id2);
}

// ---- Deterministic edge-case tests for in-memory tier cache ----

#[test]
fn empty_db_queries() {
    let db: SlotDex<u64, u64> = SlotDex::new(8, false);
    assert_eq!(db.total(), 0);
    assert!(db.get_entries_by_page(10, 0, false).is_empty());
    assert!(db.get_entries_by_page(10, 0, true).is_empty());
    assert!(db.get_entries_by_page(0, 0, false).is_empty());
    assert_eq!(db.entry_cnt_within_two_slots(0, 100), 0);
    assert_eq!(db.total_by_slot(Some(0), Some(100)), 0);
}

#[test]
fn single_entry() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    db.insert(42, 100).unwrap();

    assert_eq!(db.total(), 1);
    assert_eq!(db.get_entries_by_page(10, 0, false), vec![100]);
    assert_eq!(db.get_entries_by_page(10, 0, true), vec![100]);
    // Page 1 should be empty
    assert!(db.get_entries_by_page(10, 1, false).is_empty());
    // page_size=1
    assert_eq!(db.get_entries_by_page(1, 0, false), vec![100]);
    assert!(db.get_entries_by_page(1, 1, false).is_empty());
}

#[test]
fn insert_remove_reinsert() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    db.insert(0, 10).unwrap();
    db.insert(0, 20).unwrap();
    assert_eq!(db.total(), 2);

    db.remove(0, &10);
    assert_eq!(db.total(), 1);
    assert_eq!(db.get_entries_by_page(10, 0, false), vec![20]);

    db.remove(0, &20);
    assert_eq!(db.total(), 0);
    assert!(db.get_entries_by_page(10, 0, false).is_empty());

    // Reinsert
    db.insert(0, 30).unwrap();
    assert_eq!(db.total(), 1);
    assert_eq!(db.get_entries_by_page(10, 0, false), vec![30]);
}

#[test]
fn remove_nonexistent_is_noop() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    db.insert(0, 1).unwrap();
    db.remove(0, &999); // does not exist
    assert_eq!(db.total(), 1);
    db.remove(1, &1); // wrong slot
    assert_eq!(db.total(), 1);
}

#[test]
fn duplicate_insert_is_noop() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    db.insert(0, 1).unwrap();
    db.insert(0, 1).unwrap(); // duplicate
    assert_eq!(db.total(), 1);
}

#[test]
fn page_boundaries_exact() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(64, false);
    // Insert exactly 20 entries: slots 0..20, key = slot
    for i in 0u64..20 {
        db.insert(i, i).unwrap();
    }
    assert_eq!(db.total(), 20);

    // page_size=5: should have 4 pages
    assert_eq!(db.get_entries_by_page(5, 0, false), vec![0, 1, 2, 3, 4]);
    assert_eq!(db.get_entries_by_page(5, 1, false), vec![5, 6, 7, 8, 9]);
    assert_eq!(
        db.get_entries_by_page(5, 2, false),
        vec![10, 11, 12, 13, 14]
    );
    assert_eq!(
        db.get_entries_by_page(5, 3, false),
        vec![15, 16, 17, 18, 19]
    );
    assert!(db.get_entries_by_page(5, 4, false).is_empty());

    // Reverse
    assert_eq!(db.get_entries_by_page(5, 0, true), vec![19, 18, 17, 16, 15]);
    assert_eq!(db.get_entries_by_page(5, 3, true), vec![4, 3, 2, 1, 0]);
}

#[test]
fn page_size_larger_than_total() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    for i in 0u64..3 {
        db.insert(i, i).unwrap();
    }
    let result = db.get_entries_by_page(100, 0, false);
    assert_eq!(result, vec![0, 1, 2]);

    let result = db.get_entries_by_page(100, 0, true);
    assert_eq!(result, vec![2, 1, 0]);
}

#[test]
fn slot_range_query() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    for i in 0u64..100 {
        db.insert(i, i).unwrap();
    }

    // Query only slots [10, 20)
    let result = db.get_entries_by_page_slot(Some(10), Some(19), 100, 0, false);
    assert_eq!(result.len(), 10);
    assert_eq!(result[0], 10);
    assert_eq!(result[9], 19);

    let result = db.get_entries_by_page_slot(Some(10), Some(19), 100, 0, true);
    assert_eq!(result.len(), 10);
    assert_eq!(result[0], 19);
    assert_eq!(result[9], 10);
}

#[test]
fn entry_cnt_within_range() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    for i in 0u64..100 {
        db.insert(i, i).unwrap();
    }

    assert_eq!(db.entry_cnt_within_two_slots(0, 99), 100);
    assert_eq!(db.entry_cnt_within_two_slots(10, 19), 10);
    assert_eq!(db.entry_cnt_within_two_slots(50, 50), 1);
    // Out of range
    assert_eq!(db.entry_cnt_within_two_slots(200, 300), 0);
}

#[test]
fn multiple_entries_per_slot() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    // Slot 0 gets 5 entries, slot 1 gets 3 entries
    for i in 0u64..5 {
        db.insert(0, i).unwrap();
    }
    for i in 10u64..13 {
        db.insert(1, i).unwrap();
    }
    assert_eq!(db.total(), 8);

    // Forward page: first 5 from slot 0, then 3 from slot 1
    let result = db.get_entries_by_page(10, 0, false);
    assert_eq!(result.len(), 8);
    assert_eq!(&result[..5], &[0, 1, 2, 3, 4]);
    assert_eq!(&result[5..], &[10, 11, 12]);

    // Page that spans two slots
    let result = db.get_entries_by_page(4, 1, false);
    assert_eq!(result, vec![4, 10, 11, 12]);
}

#[test]
fn tier_growth_and_shrink() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    // Insert enough to trigger tier creation (> 8 distinct slot floors)
    for i in 0u64..100 {
        db.insert(i, i).unwrap();
    }
    let tier_count_after_insert = db.levels.len();
    assert!(tier_count_after_insert >= 1, "should have at least 1 tier");

    // Verify queries still work
    assert_eq!(
        db.get_entries_by_page(10, 0, false),
        (0u64..10).collect::<Vec<_>>()
    );

    // Remove most entries
    for i in 0u64..95 {
        db.remove(i, &i);
    }
    assert_eq!(db.total(), 5);
    assert_eq!(
        db.get_entries_by_page(10, 0, false),
        (95u64..100).collect::<Vec<_>>()
    );
}

#[test]
fn swap_order_basic() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, true);
    for i in 0u64..10 {
        db.insert(i, i).unwrap();
    }
    assert_eq!(db.total(), 10);

    // With swap_order, forward should still return logical order
    let fwd = db.get_entries_by_page(10, 0, false);
    assert_eq!(fwd, (0u64..10).collect::<Vec<_>>());

    let rev = db.get_entries_by_page(10, 0, true);
    assert_eq!(rev, (0u64..10).rev().collect::<Vec<_>>());
}

#[test]
fn clear_and_reuse() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    for i in 0u64..50 {
        db.insert(i, i).unwrap();
    }
    assert_eq!(db.total(), 50);

    db.clear();
    assert_eq!(db.total(), 0);
    assert!(db.get_entries_by_page(10, 0, false).is_empty());

    // Reuse after clear
    for i in 100u64..105 {
        db.insert(i, i).unwrap();
    }
    assert_eq!(db.total(), 5);
    assert_eq!(
        db.get_entries_by_page(10, 0, false),
        (100u64..105).collect::<Vec<_>>()
    );
}

#[test]
fn large_tier_capacity() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(64, false);
    for i in 0u64..200 {
        db.insert(i, i).unwrap();
    }
    // With capacity 64, 200 entries should create at least 1 tier
    assert!(!db.levels.is_empty());

    let page = db.get_entries_by_page(20, 5, false);
    assert_eq!(page, (100u64..120).collect::<Vec<_>>());

    let page = db.get_entries_by_page(20, 5, true);
    assert_eq!(page, (80u64..100).rev().collect::<Vec<_>>());
}

#[test]
fn sparse_slots() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    // Widely spaced slots
    let slots = [0, 100, 10000, 1000000, u64::MAX / 2];
    for (i, &s) in slots.iter().enumerate() {
        db.insert(s, i as u64).unwrap();
    }
    assert_eq!(db.total(), 5);

    let result = db.get_entries_by_page(10, 0, false);
    assert_eq!(result, vec![0, 1, 2, 3, 4]);

    let result = db.get_entries_by_page(2, 1, false);
    assert_eq!(result, vec![2, 3]);
}

/// `insert_batch` must be observationally identical to per-key `insert`,
/// across container promotion, tier growth, duplicates, and both slot
/// orders. Compare a batch-built dex against a serially built one on
/// every read surface.
#[test]
fn insert_batch_equivalence_with_serial() {
    for swap_order in [false, true] {
        let mut serial: SlotDex<u64, u64> = SlotDex::new(8, swap_order);
        let mut batched: SlotDex<u64, u64> = SlotDex::new(8, swap_order);

        // Mixed workload: hot slot 3 crosses the inline-container
        // threshold (promotion), 300 distinct slots force tier growth,
        // duplicates appear both within one batch and across batches.
        let mut ops: Vec<(u64, u64)> = Vec::new();
        for i in 0u64..300 {
            ops.push((i, i));
        }
        for k in 0u64..40 {
            ops.push((3, 10_000 + k));
        }
        ops.push((3, 10_000)); // duplicate within the same batch
        ops.push((7, 7)); // duplicate of the i-loop entry

        for &(s, k) in &ops {
            serial.insert(s, k).unwrap();
        }
        // Split into uneven chunks so batch boundaries land mid-slot.
        for chunk in ops.chunks(17) {
            batched.insert_batch(chunk.iter().copied()).unwrap();
        }

        assert_eq!(serial.total(), batched.total());
        assert_eq!(serial.levels.len(), batched.levels.len());
        let total = serial.total();
        for reverse in [false, true] {
            let mut off = 0u32;
            while u64::from(off) * 50 < total {
                assert_eq!(
                    serial.get_entries_by_page(50, off, reverse),
                    batched.get_entries_by_page(50, off, reverse),
                    "page {off} reverse={reverse} swap_order={swap_order}"
                );
                off += 1;
            }
        }
        assert_eq!(
            serial.total_by_slot(Some(2), Some(5)),
            batched.total_by_slot(Some(2), Some(5))
        );
        assert_eq!(
            serial.entry_cnt_within_two_slots(0, 150),
            batched.entry_cnt_within_two_slots(0, 150)
        );
    }
}

#[test]
fn insert_batch_empty_and_all_duplicates() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    db.insert_batch(std::iter::empty()).unwrap();
    assert_eq!(db.total(), 0);

    db.insert_batch([(0u64, 1u64), (0, 2)]).unwrap();
    assert_eq!(db.total(), 2);

    // Re-inserting the same pairs must be a no-op for the total.
    db.insert_batch([(0u64, 1u64), (0, 2)]).unwrap();
    assert_eq!(db.total(), 2);
    assert_eq!(db.get_entries_by_page(10, 0, false), vec![1, 2]);
}

#[test]
fn insert_batch_interleaved_with_serial_ops() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    let mut reference = testdb::TestDB::default();

    db.insert_batch((0u64..50).map(|i| (i / 4, i))).unwrap();
    (0u64..50).for_each(|i| reference.insert(i / 4, i));

    db.insert(100, 999).unwrap();
    reference.insert(100, 999);
    db.remove(0, &1);
    reference.remove(0, &1);

    db.insert_batch([(0u64, 1u64), (200, 2000)]).unwrap();
    reference.insert(0, 1);
    reference.insert(200, 2000);

    assert_eq!(db.total(), reference.total());
    let mut page = 0u32;
    loop {
        let got = db.get_entries_by_page(16, page, false);
        let want = reference.get_entries_by_page_slot(None, None, 16, page, false);
        assert_eq!(got, want, "page {page}");
        if got.is_empty() {
            break;
        }
        page += 1;
    }
}

mod testdb {
    use std::collections::{BTreeMap, BTreeSet};

    type PageSize = u16;
    type PageIndex = u32;

    /// Reference model mirroring SlotDex's documented ordering contract:
    /// entries within a slot are an ordered set (always ascending);
    /// `reverse` reverses the slot order only, never the within-slot
    /// order.
    #[derive(Default)]
    pub struct TestDB<T: Clone + Ord> {
        data: BTreeMap<u64, BTreeSet<T>>,
    }

    impl<T: Clone + Ord> TestDB<T> {
        pub fn insert(&mut self, slot: u64, v: T) {
            self.data.entry(slot).or_default().insert(v);
        }

        pub fn remove(&mut self, slot: u64, v: &T) {
            if let Some(set) = self.data.get_mut(&slot) {
                set.remove(v);
                if set.is_empty() {
                    self.data.remove(&slot);
                }
            }
        }

        pub fn total(&self) -> u64 {
            self.data.values().map(|s| s.len() as u64).sum()
        }

        pub fn get_entries_by_page_slot(
            &self,
            slot_start: Option<u64>,
            slot_end: Option<u64>,
            page_size: PageSize,
            page_index: PageIndex,
            reverse: bool,
        ) -> Vec<T> {
            let page_size = page_size as usize;
            let page_index = page_index as usize;

            let slot_start = slot_start.unwrap_or(u64::MIN);
            let slot_end = slot_end.unwrap_or(u64::MAX);

            if reverse {
                self.data
                    .range(slot_start..=slot_end)
                    .rev()
                    .flat_map(|(_, keys)| keys.iter())
                    .skip(page_size * page_index)
                    .take(page_size)
                    .cloned()
                    .collect()
            } else {
                self.data
                    .range(slot_start..=slot_end)
                    .flat_map(|(_, keys)| keys.iter())
                    .skip(page_size * page_index)
                    .take(page_size)
                    .cloned()
                    .collect()
            }
        }
    }
}

#[test]
fn test_save_and_from_meta() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(16, false);
    db.insert(10, 100).unwrap();
    db.insert(20, 200).unwrap();

    let id = db.save_meta().unwrap();
    assert_eq!(id, db.instance_id());

    let restored: SlotDex<u64, u64> = SlotDex::from_meta(id).unwrap();
    assert_eq!(restored.total(), 2);
}

/// The typed-handle envelope must reject restoring under different
/// slot/key type parameters.
#[test]
fn test_from_meta_rejects_wrong_type_params() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(16, false);
    db.insert(10, 100).unwrap();

    let id = db.save_meta().unwrap();
    assert!(SlotDex::<u32, u64>::from_meta(id).is_err());
    assert!(SlotDex::<u64, u32>::from_meta(id).is_err());
}

/// Postcard serde roundtrip for SlotDex (derived serde, but inner types are hand-written).
#[test]
fn test_serde_roundtrip() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(16, false);
    for i in 1..=50 {
        db.insert(i, i * 10).unwrap();
    }

    let bytes = postcard::to_allocvec(&db).unwrap();
    let restored: SlotDex<u64, u64> = postcard::from_bytes(&bytes).unwrap();

    assert_eq!(restored.total(), 50);

    // Verify paging still works
    let entries = restored.get_entries_by_page(10, 0, false);
    assert_eq!(entries.len(), 10);
}

/// from_meta nonexistent.
#[test]
fn test_from_meta_nonexistent() {
    assert!(SlotDex::<u64, u64>::from_meta(u64::MAX).is_err());
}

/// Restore from meta with substantial data, verify queries work.
#[test]
fn test_meta_restore_with_data() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(16, false);
    for i in 0..100 {
        db.insert(i, i * 3).unwrap();
    }

    let id = db.save_meta().unwrap();
    let restored: SlotDex<u64, u64> = SlotDex::from_meta(id).unwrap();

    assert_eq!(restored.total(), 100);

    // Verify page queries produce correct results
    let entries = restored.get_entries_by_page(20, 0, false);
    assert_eq!(entries.len(), 20);

    let entries_p4 = restored.get_entries_by_page(20, 4, false);
    assert_eq!(entries_p4.len(), 20);
}

// ---- Restore consistency tests (atomic single-handle model) ----

#[test]
fn restore_without_save_meta_preserves_total() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(10, false);
    for i in 0..15u64 {
        db.insert(i, i * 10).unwrap();
    }

    // No clean-shutdown protocol exists: persisting the (constant)
    // metadata at any point yields a fully consistent restore, because
    // every mutation was committed atomically.
    let id = db.instance_id();
    crate::common::save_instance_meta(id, &db).unwrap();

    let restored: SlotDex<u64, u64> = SlotDex::from_meta(id).unwrap();
    assert_eq!(restored.total(), 15);
}

#[test]
fn serde_roundtrip_rehydrates_caches() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(10, false);
    for i in 0..8u64 {
        db.insert(i, i).unwrap();
    }

    let bytes = postcard::to_allocvec(&db).unwrap();
    let restored: SlotDex<u64, u64> = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(restored.total(), 8);
    assert_eq!(restored.levels.len(), db.levels.len());
    for (a, b) in restored.levels.iter().zip(db.levels.iter()) {
        assert_eq!(a.floor_base, b.floor_base);
        assert_eq!(a.buckets, b.buckets);
    }
}

#[test]
fn restore_keeps_tier_acceleration() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false);
    for i in 0..500u64 {
        db.insert(i, i).unwrap();
    }
    assert!(!db.levels.is_empty());

    let bytes = postcard::to_allocvec(&db).unwrap();
    let restored: SlotDex<u64, u64> = postcard::from_bytes(&bytes).unwrap();

    assert_eq!(restored.total(), 500);
    // The tier stack is hydrated from its persisted rows, so deep pages
    // are tier-accelerated immediately after restore.
    assert!(!restored.levels.is_empty());
    assert_eq!(
        restored.get_entries_by_page_slot(Some(90), Some(99), 20, 0, false),
        (90u64..100).collect::<Vec<_>>()
    );
    assert_eq!(
        restored.get_entries_by_page_slot(Some(0), Some(499), 20, 0, true),
        (480u64..500).rev().collect::<Vec<_>>()
    );
}

#[test]
fn restore_after_interleaved_mutations_matches_reference() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(4, false);
    let mut reference: Vec<(u64, u64)> = vec![];
    for i in 0..200u64 {
        db.insert(i % 37, i).unwrap();
        reference.push((i % 37, i));
        if i % 3 == 0 {
            let (s, k) = reference.remove((i as usize * 7) % reference.len());
            db.remove(s, &k);
        }
    }
    reference.sort();

    let bytes = postcard::to_allocvec(&db).unwrap();
    let restored: SlotDex<u64, u64> = postcard::from_bytes(&bytes).unwrap();

    assert_eq!(restored.total(), reference.len() as u64);
    let all = restored.get_entries_by_page(u16::MAX, 0, false);
    assert_eq!(all, reference.iter().map(|(_, k)| *k).collect::<Vec<_>>());
}
