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

    let mut slot_min = Slot::MAX;
    let mut slot_max = Slot::MIN;

    (0..siz()).for_each(|i| {
        slot_min = i.min(slot_min);
        slot_max = i.max(slot_max);

        db.insert(i, i).unwrap();
        test_db.insert(i, i);
    });

    assert_eq!(siz(), db.total());

    assert_queryable(&db, &test_db, slot_min, slot_max);

    db.clear();
    assert_eq!(0, db.total());
    assert!(db.get_entries_by_page(10, 0, true).is_empty());
    assert!(db.get_entries_by_page(10, 0, false).is_empty());
}

fn assert_queryable(
    db: &SlotDex<u64>,
    test_db: &testdb::TestDB<u64>,
    slot_min: Slot,
    slot_max: Slot,
) {
    for _ in 0..16 {
        let page_size = 1 + (random::<u16>() as u32) % 128;
        let max_page = 100 + (db.total() / (page_size as u64)) as u32;

        // Ensure the first page case is covered
        let page_number = random::<u32>() % max_page;

        let page_size = page_size as u64;
        let page_number = page_number as u64;
        dbg!(page_number, page_size);

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
            Some(dbg!(smin)),
            Some(dbg!(smax)),
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
fn data_container() {
    let mut db = SlotDex::new(16, false);

    db.insert(0, 0).unwrap();

    assert!(matches!(
        db.data.iter().next().unwrap().1,
        DataCtner::Small(_)
    ));

    (0..100u32).for_each(|i| {
        db.insert(0, i).unwrap();
    });

    assert!(matches!(
        db.data.iter().next().unwrap().1,
        DataCtner::Large { .. }
    ));
    assert_eq!(db.data.iter().count(), 1);
    assert_eq!(db.data.first().unwrap().1.len(), 100);
    assert_eq!(db.data.first().unwrap().1.iter().next().unwrap(), 0);
    assert_eq!(db.data.first().unwrap().1.iter().last().unwrap(), 99);

    db.clear();
}

// ---- Deterministic edge-case tests for in-memory tier cache ----

#[test]
fn empty_db_queries() {
    let db: SlotDex<u64> = SlotDex::new(8, false);
    assert_eq!(db.total(), 0);
    assert!(db.get_entries_by_page(10, 0, false).is_empty());
    assert!(db.get_entries_by_page(10, 0, true).is_empty());
    assert!(db.get_entries_by_page(0, 0, false).is_empty());
    assert_eq!(db.entry_cnt_within_two_slots(0, 100), 0);
    assert_eq!(db.total_by_slot(Some(0), Some(100)), 0);
}

#[test]
fn single_entry() {
    let mut db: SlotDex<u64> = SlotDex::new(8, false);
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
    let mut db: SlotDex<u64> = SlotDex::new(8, false);
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
    let mut db: SlotDex<u64> = SlotDex::new(8, false);
    db.insert(0, 1).unwrap();
    db.remove(0, &999); // does not exist
    assert_eq!(db.total(), 1);
    db.remove(1, &1); // wrong slot
    assert_eq!(db.total(), 1);
}

#[test]
fn duplicate_insert_is_noop() {
    let mut db: SlotDex<u64> = SlotDex::new(8, false);
    db.insert(0, 1).unwrap();
    db.insert(0, 1).unwrap(); // duplicate
    assert_eq!(db.total(), 1);
}

#[test]
fn page_boundaries_exact() {
    let mut db: SlotDex<u64> = SlotDex::new(64, false);
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
    let mut db: SlotDex<u64> = SlotDex::new(8, false);
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
    let mut db: SlotDex<u64> = SlotDex::new(8, false);
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
    let mut db: SlotDex<u64> = SlotDex::new(8, false);
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
    let mut db: SlotDex<u64> = SlotDex::new(8, false);
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
    let mut db: SlotDex<u64> = SlotDex::new(8, false);
    // Insert enough to trigger tier creation (> 8 distinct slot floors)
    for i in 0u64..100 {
        db.insert(i, i).unwrap();
    }
    let tier_count_after_insert = db.tiers.len();
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
    let mut db: SlotDex<u64> = SlotDex::new(8, true);
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
    let mut db: SlotDex<u64> = SlotDex::new(8, false);
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
    let mut db: SlotDex<u64> = SlotDex::new(64, false);
    for i in 0u64..200 {
        db.insert(i, i).unwrap();
    }
    // With capacity 64, 200 entries should create at least 1 tier
    assert!(db.tiers.len() >= 1);

    let page = db.get_entries_by_page(20, 5, false);
    assert_eq!(page, (100u64..120).collect::<Vec<_>>());

    let page = db.get_entries_by_page(20, 5, true);
    assert_eq!(page, (80u64..100).rev().collect::<Vec<_>>());
}

#[test]
fn sparse_slots() {
    let mut db: SlotDex<u64> = SlotDex::new(8, false);
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

mod testdb {
    use super::*;
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicU64, Ordering},
    };

    static INNER_ID: AtomicU64 = AtomicU64::new(0);

    #[derive(Default)]
    pub struct TestDB<T: Clone + Eq> {
        data: BTreeMap<[Slot; 2], T>,
    }

    impl<T: Clone + Eq> TestDB<T> {
        pub fn insert(&mut self, slot: Slot, v: T) {
            let inner_id = INNER_ID.fetch_add(1, Ordering::Relaxed);
            self.data.insert([slot, inner_id], v);
        }

        pub fn get_entries_by_page_slot(
            &self,
            slot_start: Option<Slot>,
            slot_end: Option<Slot>,
            page_size: PageSize,
            page_index: PageIndex,
            reverse: bool,
        ) -> Vec<T> {
            let page_size = page_size as usize;
            let page_index = page_index as usize;

            let slot_start = slot_start.unwrap_or(Slot::MIN);
            let slot_end = slot_end.unwrap_or(Slot::MAX);

            if reverse {
                self.data
                    .range([slot_start, 0]..=[slot_end, Slot::MAX])
                    .map(|(_, v)| v)
                    .rev()
                    .skip(page_size * page_index)
                    .take(page_size)
                    .cloned()
                    .collect()
            } else {
                self.data
                    .range([slot_start, 0]..=[slot_end, Slot::MAX])
                    .map(|(_, v)| v)
                    .skip(page_size * page_index)
                    .take(page_size)
                    .cloned()
                    .collect()
            }
        }
    }
}
