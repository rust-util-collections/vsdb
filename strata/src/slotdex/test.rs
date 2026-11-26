use super::*;
use rand::random;

thread_local! {
    pub(super) static REVERSE_SPLIT_FLOORS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[test]
fn first_reverse_page_visits_bounded_tier_floors() {
    for swap_order in [false, true] {
        let mut db = SlotDex::<u64, u64>::new(16, swap_order).unwrap();
        db.insert_batch((0..4096).map(|slot| (slot, slot))).unwrap();
        let (order, expected) = if swap_order {
            (Order::Asc, 0)
        } else {
            (Order::Desc, 4095)
        };
        REVERSE_SPLIT_FLOORS.with(|count| count.set(0));
        assert_eq!(db.page(.., 1, 0, order), vec![expected]);
        let visited = REVERSE_SPLIT_FLOORS.with(std::cell::Cell::get);
        assert!(
            visited <= 64,
            "one-key page visited {visited} tier floors (swap_order={swap_order})"
        );
    }
}

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

fn slot_db(mn: u32, swap_order: bool) {
    let mut db = SlotDex::new(mn, swap_order).unwrap();
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

    db.clear().unwrap();
    assert_eq!(0, db.total());
    assert!(db.page(.., 10, 0, Order::Desc).is_empty());
    assert!(db.page(.., 10, 0, Order::Asc).is_empty());
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
        let b = db.page(.., page_size as u16, page_number as u32, Order::Desc);
        assert_eq!(a, b);

        let a = test_db.get_entries_by_page_slot(
            None,
            None,
            page_size as u16,
            page_number as u32,
            false,
        );
        let b = db.page(.., page_size as u16, page_number as u32, Order::Asc);
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

        let b = db.page(
            smin..=smax,
            page_size as u16,
            page_number as u32,
            Order::Desc,
        );
        assert_eq!(a, b);

        let a = test_db.get_entries_by_page_slot(
            Some(smin),
            Some(smax),
            page_size as u16,
            page_number as u32,
            false,
        );

        let b = db.page(
            smin..=smax,
            page_size as u16,
            page_number as u32,
            Order::Asc,
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
    let mut db: SlotDex<u64, u32> = SlotDex::new(16, false).unwrap();

    (0..100u32).for_each(|i| {
        db.insert(0, i).unwrap();
    });

    assert_eq!(db.total(), 100);
    // One slot holding 100 entries: within-slot order is ascending key order.
    let all = db.page(.., 200, 0, Order::Asc);
    assert_eq!(all.len(), 100);
    assert_eq!(all.first().copied().unwrap(), 0);
    assert_eq!(all.last().copied().unwrap(), 99);

    // Everything lives in a single handle: entry rows + per-slot count
    // row + tier level rows + the total row, nothing else.
    let rows = db.store.iter().count();
    let level_rows: usize = db.levels.iter().map(|l| l.buckets.len()).sum();
    assert_eq!(rows, 100 + 1 + level_rows + 1);

    db.clear().unwrap();
    assert_eq!(db.total(), 0);
    assert_eq!(
        db.store.iter().collect::<Vec<_>>(),
        vec![(TOTAL_KEY.to_vec(), total_record(0, 101).to_vec())]
    );
}

#[test]
fn reverse_paging_reverses_slots_only_not_within_slot() {
    // A slot is a set of keys (always ascending). Reverse paging must reverse
    // only the slot order, never the within-slot order, and must return
    // identical results under both storage layouts (`swap_order`).
    for swap_order in [false, true] {
        let mut db: SlotDex<u64, u64> = SlotDex::new(16, swap_order).unwrap();
        // slot 5: {10,20,30}; slot 9: {40,50}
        for k in [10u64, 20, 30] {
            db.insert(5, k).unwrap();
        }
        for k in [40u64, 50] {
            db.insert(9, k).unwrap();
        }

        // Ascending: (slot asc, key asc).
        assert_eq!(db.page(.., 10, 0, Order::Asc), vec![10, 20, 30, 40, 50]);

        // Reverse: (slot desc, key asc within slot).
        assert_eq!(db.page(.., 10, 0, Order::Desc), vec![40, 50, 10, 20, 30]);

        // A single full slot in reverse keeps its within-slot ascending order.
        assert_eq!(db.page(5..=5, 10, 0, Order::Desc), vec![10, 20, 30]);

        // Reverse pagination membership across the slot boundary.
        assert_eq!(db.page(.., 2, 0, Order::Desc), vec![40, 50]);
        assert_eq!(db.page(.., 2, 1, Order::Desc), vec![10, 20]);
        assert_eq!(db.page(.., 2, 2, Order::Desc), vec![30]);
    }
}

#[test]
fn hdr_meta_is_create_time_constant() {
    // The serialized handle metadata must never change after creation:
    // tier growth, removals, and clears only write ordinary data rows.
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    let at_creation = postcard::to_allocvec(&db).unwrap();

    for i in 0..500u64 {
        db.insert(i, i).unwrap();
    }
    assert!(!db.levels.is_empty(), "growth must have happened");
    assert_eq!(at_creation, postcard::to_allocvec(&db).unwrap());

    for i in 0..400u64 {
        db.remove(i, &i).unwrap();
    }
    assert_eq!(at_creation, postcard::to_allocvec(&db).unwrap());

    db.clear().unwrap();
    assert_eq!(at_creation, postcard::to_allocvec(&db).unwrap());

    // save_meta is idempotent pure persistence on a shared reference.
    let id1 = db.save_meta().unwrap();
    let id2 = db.save_meta().unwrap();
    assert_eq!(id1, id2);
}

// ---- Deterministic edge-case tests for in-memory tier cache ----

#[test]
fn empty_db_queries() {
    let db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    assert_eq!(db.total(), 0);
    assert!(db.page(.., 10, 0, Order::Asc).is_empty());
    assert!(db.page(.., 10, 0, Order::Desc).is_empty());
    assert!(db.page(.., 0, 0, Order::Asc).is_empty());
    assert_eq!(db.count(0..=100), 0);
    assert_eq!(db.count(0..=100), 0);
}

#[test]
fn single_entry() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    db.insert(42, 100).unwrap();

    assert_eq!(db.total(), 1);
    assert_eq!(db.page(.., 10, 0, Order::Asc), vec![100]);
    assert_eq!(db.page(.., 10, 0, Order::Desc), vec![100]);
    // Page 1 should be empty
    assert!(db.page(.., 10, 1, Order::Asc).is_empty());
    // page_size=1
    assert_eq!(db.page(.., 1, 0, Order::Asc), vec![100]);
    assert!(db.page(.., 1, 1, Order::Asc).is_empty());
}

#[test]
fn insert_remove_reinsert() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    db.insert(0, 10).unwrap();
    db.insert(0, 20).unwrap();
    assert_eq!(db.total(), 2);

    db.remove(0, &10).unwrap();
    assert_eq!(db.total(), 1);
    assert_eq!(db.page(.., 10, 0, Order::Asc), vec![20]);

    db.remove(0, &20).unwrap();
    assert_eq!(db.total(), 0);
    assert!(db.page(.., 10, 0, Order::Asc).is_empty());

    // Reinsert
    db.insert(0, 30).unwrap();
    assert_eq!(db.total(), 1);
    assert_eq!(db.page(.., 10, 0, Order::Asc), vec![30]);
}

#[test]
fn remove_nonexistent_is_noop() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    db.insert(0, 1).unwrap();
    db.remove(0, &999).unwrap(); // does not exist
    assert_eq!(db.total(), 1);
    db.remove(1, &1).unwrap(); // wrong slot
    assert_eq!(db.total(), 1);
}

#[test]
fn duplicate_insert_is_noop() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    db.insert(0, 1).unwrap();
    db.insert(0, 1).unwrap(); // duplicate
    assert_eq!(db.total(), 1);
}

#[test]
fn page_boundaries_exact() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(64, false).unwrap();
    // Insert exactly 20 entries: slots 0..20, key = slot
    for i in 0u64..20 {
        db.insert(i, i).unwrap();
    }
    assert_eq!(db.total(), 20);

    // page_size=5: should have 4 pages
    assert_eq!(db.page(.., 5, 0, Order::Asc), vec![0, 1, 2, 3, 4]);
    assert_eq!(db.page(.., 5, 1, Order::Asc), vec![5, 6, 7, 8, 9]);
    assert_eq!(db.page(.., 5, 2, Order::Asc), vec![10, 11, 12, 13, 14]);
    assert_eq!(db.page(.., 5, 3, Order::Asc), vec![15, 16, 17, 18, 19]);
    assert!(db.page(.., 5, 4, Order::Asc).is_empty());

    // Reverse
    assert_eq!(db.page(.., 5, 0, Order::Desc), vec![19, 18, 17, 16, 15]);
    assert_eq!(db.page(.., 5, 3, Order::Desc), vec![4, 3, 2, 1, 0]);
}

#[test]
fn page_size_larger_than_total() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    for i in 0u64..3 {
        db.insert(i, i).unwrap();
    }
    let result = db.page(.., 100, 0, Order::Asc);
    assert_eq!(result, vec![0, 1, 2]);

    let result = db.page(.., 100, 0, Order::Desc);
    assert_eq!(result, vec![2, 1, 0]);
}

#[test]
fn slot_range_query() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    for i in 0u64..100 {
        db.insert(i, i).unwrap();
    }

    // Query only slots [10, 20)
    let result = db.page(10..=19, 100, 0, Order::Asc);
    assert_eq!(result.len(), 10);
    assert_eq!(result[0], 10);
    assert_eq!(result[9], 19);

    let result = db.page(10..=19, 100, 0, Order::Desc);
    assert_eq!(result.len(), 10);
    assert_eq!(result[0], 19);
    assert_eq!(result[9], 10);
}

#[test]
fn entry_cnt_within_range() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    for i in 0u64..100 {
        db.insert(i, i).unwrap();
    }

    assert_eq!(db.count(0..=99), 100);
    assert_eq!(db.count(10..=19), 10);
    assert_eq!(db.count(50..=50), 1);
    // Out of range
    assert_eq!(db.count(200..=300), 0);
}

#[test]
fn multiple_entries_per_slot() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    // Slot 0 gets 5 entries, slot 1 gets 3 entries
    for i in 0u64..5 {
        db.insert(0, i).unwrap();
    }
    for i in 10u64..13 {
        db.insert(1, i).unwrap();
    }
    assert_eq!(db.total(), 8);

    // Forward page: first 5 from slot 0, then 3 from slot 1
    let result = db.page(.., 10, 0, Order::Asc);
    assert_eq!(result.len(), 8);
    assert_eq!(&result[..5], &[0, 1, 2, 3, 4]);
    assert_eq!(&result[5..], &[10, 11, 12]);

    // Page that spans two slots
    let result = db.page(.., 4, 1, Order::Asc);
    assert_eq!(result, vec![4, 10, 11, 12]);
}

#[test]
fn tier_growth_and_shrink() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    // Insert enough to trigger tier creation (> 8 distinct slot floors)
    for i in 0u64..100 {
        db.insert(i, i).unwrap();
    }
    let tier_count_after_insert = db.levels.len();
    assert!(tier_count_after_insert >= 1, "should have at least 1 tier");

    // Verify queries still work
    assert_eq!(
        db.page(.., 10, 0, Order::Asc),
        (0u64..10).collect::<Vec<_>>()
    );

    // Remove most entries
    for i in 0u64..95 {
        db.remove(i, &i).unwrap();
    }
    assert_eq!(db.total(), 5);
    assert_eq!(
        db.page(.., 10, 0, Order::Asc),
        (95u64..100).collect::<Vec<_>>()
    );
}

#[test]
fn swap_order_basic() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, true).unwrap();
    for i in 0u64..10 {
        db.insert(i, i).unwrap();
    }
    assert_eq!(db.total(), 10);

    // With swap_order, forward should still return logical order
    let fwd = db.page(.., 10, 0, Order::Asc);
    assert_eq!(fwd, (0u64..10).collect::<Vec<_>>());

    let rev = db.page(.., 10, 0, Order::Desc);
    assert_eq!(rev, (0u64..10).rev().collect::<Vec<_>>());
}

#[test]
fn clear_and_reuse() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    for i in 0u64..50 {
        db.insert(i, i).unwrap();
    }
    assert_eq!(db.total(), 50);

    db.clear().unwrap();
    assert_eq!(db.total(), 0);
    assert!(db.page(.., 10, 0, Order::Asc).is_empty());

    // Reuse after clear
    for i in 100u64..105 {
        db.insert(i, i).unwrap();
    }
    assert_eq!(db.total(), 5);
    assert_eq!(
        db.page(.., 10, 0, Order::Asc),
        (100u64..105).collect::<Vec<_>>()
    );
}

#[test]
fn large_tier_capacity() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(64, false).unwrap();
    for i in 0u64..200 {
        db.insert(i, i).unwrap();
    }
    // With capacity 64, 200 entries should create at least 1 tier
    assert!(!db.levels.is_empty());

    let page = db.page(.., 20, 5, Order::Asc);
    assert_eq!(page, (100u64..120).collect::<Vec<_>>());

    let page = db.page(.., 20, 5, Order::Desc);
    assert_eq!(page, (80u64..100).rev().collect::<Vec<_>>());
}

#[test]
fn sparse_slots() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    // Widely spaced slots
    let slots = [0, 100, 10000, 1000000, u64::MAX / 2];
    for (i, &s) in slots.iter().enumerate() {
        db.insert(s, i as u64).unwrap();
    }
    assert_eq!(db.total(), 5);
    // Regression: 5 distinct slots is within `tier_capacity` (8), so no
    // tier should have been promoted yet — `stage_level_growth_over`'s
    // "no tiers yet" branch used to promote unconditionally on the very
    // first insert, bypassing this capacity gate entirely.
    assert!(
        db.levels.is_empty(),
        "should not promote a tier below capacity"
    );

    let result = db.page(.., 10, 0, Order::Asc);
    assert_eq!(result, vec![0, 1, 2, 3, 4]);

    let result = db.page(.., 2, 1, Order::Asc);
    assert_eq!(result, vec![2, 3]);
}

/// Regression: a fresh `SlotDex` must NOT create a premature tier on its
/// very first insert(s) when the entry count stays within
/// `tier_capacity` — `stage_level_growth_over`'s "no tiers yet" branch
/// used to promote unconditionally, bypassing the same capacity gate the
/// "tiers already exist" branch already enforced (defeating
/// `tier_capacity` for every fresh instance's first insert).
#[test]
fn first_inserts_within_capacity_create_no_premature_tier() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(1_000_000, false).unwrap();
    assert!(db.levels.is_empty());

    db.insert(1, 1).unwrap();
    assert!(
        db.levels.is_empty(),
        "a single insert must not create a tier when tier_capacity is huge"
    );

    // insert_batch shares the same growth-check code path.
    let mut db2: SlotDex<u64, u64> = SlotDex::new(1_000_000, false).unwrap();
    db2.insert_batch(vec![(1, 1)]).unwrap();
    assert!(
        db2.levels.is_empty(),
        "insert_batch must not create a tier when tier_capacity is huge"
    );
}

/// `insert_batch` must be observationally identical to per-key `insert`,
/// across container promotion, tier growth, duplicates, and both slot
/// orders. Compare a batch-built dex against a serially built one on
/// every read surface — both chunked (batch boundaries mid-slot) and as
/// one single bulk load (regression: the no-tiers growth gate used to
/// see only committed rows, so a single batch into a fresh index never
/// promoted any tier, leaving every query on the level-0 slow path).
#[test]
fn insert_batch_equivalence_with_serial() {
    for swap_order in [false, true] {
        let mut serial: SlotDex<u64, u64> = SlotDex::new(8, swap_order).unwrap();
        let mut batched: SlotDex<u64, u64> = SlotDex::new(8, swap_order).unwrap();
        let mut bulk: SlotDex<u64, u64> = SlotDex::new(8, swap_order).unwrap();

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
        // The whole workload as ONE batch: tier growth must happen
        // mid-batch, exactly as if the pairs were inserted serially.
        bulk.insert_batch(ops.iter().copied()).unwrap();

        assert_eq!(serial.total(), batched.total());
        assert_eq!(serial.levels.len(), batched.levels.len());
        assert_eq!(serial.total(), bulk.total());
        assert_eq!(serial.levels.len(), bulk.levels.len());
        assert!(!bulk.levels.is_empty(), "bulk load must build tiers");
        let total = serial.total();
        for reverse in [false, true] {
            let mut off = 0u32;
            while u64::from(off) * 50 < total {
                assert_eq!(
                    serial.page(
                        ..,
                        50,
                        off,
                        if reverse { Order::Desc } else { Order::Asc }
                    ),
                    batched.page(
                        ..,
                        50,
                        off,
                        if reverse { Order::Desc } else { Order::Asc }
                    ),
                    "page {off} reverse={reverse} swap_order={swap_order}"
                );
                assert_eq!(
                    serial.page(
                        ..,
                        50,
                        off,
                        if reverse { Order::Desc } else { Order::Asc }
                    ),
                    bulk.page(
                        ..,
                        50,
                        off,
                        if reverse { Order::Desc } else { Order::Asc }
                    ),
                    "bulk page {off} reverse={reverse} swap_order={swap_order}"
                );
                off += 1;
            }
        }
        assert_eq!(serial.count(2..=5), batched.count(2..=5));
        assert_eq!(serial.count(0..=150), batched.count(0..=150));
        assert_eq!(serial.count(2..=5), bulk.count(2..=5));
        assert_eq!(serial.count(0..=150), bulk.count(0..=150));
    }
}

#[test]
fn insert_batch_checks_growth_before_each_unique_key() {
    for swap_order in [false, true] {
        let mut serial: SlotDex<u64, u64> = SlotDex::new(2, swap_order).unwrap();
        let mut bulk: SlotDex<u64, u64> = SlotDex::new(2, swap_order).unwrap();
        let mut chunked: SlotDex<u64, u64> = SlotDex::new(2, swap_order).unwrap();

        for slot in 0..4 {
            serial.insert(slot, slot).unwrap();
            bulk.insert(slot, slot).unwrap();
            chunked.insert(slot, slot).unwrap();
        }

        serial.insert(4, 40).unwrap();
        serial.insert(4, 41).unwrap();
        bulk.insert_batch([(4, 40), (4, 41)]).unwrap();
        chunked.insert_batch([(4, 40)]).unwrap();
        chunked.insert_batch([(4, 41)]).unwrap();

        assert_eq!(serial.levels.len(), 2);
        assert_eq!(bulk.levels.len(), serial.levels.len());
        assert_eq!(chunked.levels.len(), serial.levels.len());
        assert_eq!(
            bulk.page(.., 16, 0, Order::Asc),
            serial.page(.., 16, 0, Order::Asc)
        );

        let bytes = postcard::to_allocvec(&bulk).unwrap();
        let reopened: SlotDex<u64, u64> = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(reopened.levels.len(), serial.levels.len());
        assert_eq!(
            reopened.page(.., 16, 0, Order::Asc),
            serial.page(.., 16, 0, Order::Asc)
        );
    }
}

#[test]
fn batch_growth_tracks_new_floors_across_promotions_and_reopen() {
    for capacity in [2u32, 16] {
        for swap_order in [false, true] {
            let mut serial = SlotDex::<u64, u64>::new(capacity, swap_order).unwrap();
            let mut bulk = SlotDex::<u64, u64>::new(capacity, swap_order).unwrap();
            let stride = u64::from(capacity).pow(3);
            // Sparse floors force several promotions. Hot-slot updates and
            // duplicates must not be counted as additional floors.
            let items: Vec<_> = (0..512)
                .flat_map(|n| [(n * stride, n), (0, 10_000 + n), (n * stride, n)])
                .collect();
            for chunk in items.chunks(193) {
                for &(slot, key) in chunk {
                    serial.insert(slot, key).unwrap();
                }
                bulk.insert_batch(chunk.iter().copied()).unwrap();
                assert_eq!(
                    bulk.store
                        .iter()
                        .filter(|(k, _)| k.as_slice() != TOTAL_KEY)
                        .collect::<Vec<_>>(),
                    serial
                        .store
                        .iter()
                        .filter(|(k, _)| k.as_slice() != TOTAL_KEY)
                        .collect::<Vec<_>>(),
                    "capacity={capacity}, swap_order={swap_order}"
                );
            }
            assert!(bulk.levels.len() >= 3);

            let bytes = postcard::to_allocvec(&bulk).unwrap();
            drop(bulk);
            let mut restored: SlotDex<u64, u64> = postcard::from_bytes(&bytes).unwrap();
            let next: Vec<_> = (512..768)
                .flat_map(|n| [(n * stride, n), (0, 10_000 + n), (n * stride, n)])
                .collect();
            for &(slot, key) in &next {
                serial.insert(slot, key).unwrap();
            }
            restored.insert_batch(next).unwrap();
            assert_eq!(
                restored
                    .store
                    .iter()
                    .filter(|(k, _)| k.as_slice() != TOTAL_KEY)
                    .collect::<Vec<_>>(),
                serial
                    .store
                    .iter()
                    .filter(|(k, _)| k.as_slice() != TOTAL_KEY)
                    .collect::<Vec<_>>()
            );
            assert_eq!(restored.total(), serial.total());
            for order in [Order::Asc, Order::Desc] {
                for page in [0, 3, 40] {
                    assert_eq!(
                        restored.page(.., 17, page, order),
                        serial.page(.., 17, page, order)
                    );
                }
            }
        }
    }
}

/// Regression: tiers built inside one `insert_batch` must survive a
/// `save_meta`/`from_meta` round trip — `hydrate` has no rebuild path,
/// so a bulk-loaded index that persisted without tier rows would be
/// stuck on the level-0 slow path forever.
#[test]
fn bulk_load_tiers_persist_across_reopen() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    db.insert_batch((0u64..100).map(|i| (i, i))).unwrap();
    assert!(!db.levels.is_empty());
    let levels = db.levels.len();

    let id = db.save_meta().unwrap();
    let db2: SlotDex<u64, u64> = SlotDex::from_meta(id).unwrap();
    assert_eq!(db2.levels.len(), levels);
    assert_eq!(db2.total(), 100);
    assert_eq!(
        db2.page(.., 10, 3, Order::Asc),
        (30u64..40).collect::<Vec<_>>()
    );
}

/// Regression: after `remove` truncates every tier away, the growth
/// gate's committed-row mirror must be re-seeded so later inserts still
/// promote at the same cadence as on a fresh instance.
#[test]
fn growth_gate_survives_tier_truncation() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    for i in 0u64..20 {
        db.insert(i, i).unwrap();
    }
    assert!(!db.levels.is_empty());

    // Remove down to a single slot, then remove that one too: the final
    // remove sees a degenerate (single-bucket) top level, and the
    // truncation cascade drops every tier.
    for i in 1u64..20 {
        db.remove(i, &i).unwrap();
    }
    db.remove(0, &0).unwrap();
    assert!(db.levels.is_empty(), "tiers should be truncated away");
    assert_eq!(db.total(), 0);

    // Growing past capacity again must re-promote a tier.
    for i in 100u64..120 {
        db.insert(i, i).unwrap();
    }
    assert!(!db.levels.is_empty(), "growth gate must re-arm");
    assert_eq!(
        db.page(.., 30, 0, Order::Asc),
        (100u64..120).collect::<Vec<_>>()
    );
}

#[test]
fn insert_batch_empty_and_all_duplicates() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    db.insert_batch(std::iter::empty()).unwrap();
    assert_eq!(db.total(), 0);

    db.insert_batch([(0u64, 1u64), (0, 2)]).unwrap();
    assert_eq!(db.total(), 2);

    // Re-inserting the same pairs must be a no-op for the total.
    db.insert_batch([(0u64, 1u64), (0, 2)]).unwrap();
    assert_eq!(db.total(), 2);
    assert_eq!(db.page(.., 10, 0, Order::Asc), vec![1, 2]);
}

#[test]
fn insert_batch_interleaved_with_serial_ops() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
    let mut reference = testdb::TestDB::default();

    db.insert_batch((0u64..50).map(|i| (i / 4, i))).unwrap();
    (0u64..50).for_each(|i| reference.insert(i / 4, i));

    db.insert(100, 999).unwrap();
    reference.insert(100, 999);
    db.remove(0, &1).unwrap();
    reference.remove(0, &1);

    db.insert_batch([(0u64, 1u64), (200, 2000)]).unwrap();
    reference.insert(0, 1);
    reference.insert(200, 2000);

    assert_eq!(db.total(), reference.total());
    let mut page = 0u32;
    loop {
        let got = db.page(.., 16, page, Order::Asc);
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
    let mut db: SlotDex<u64, u64> = SlotDex::new(16, false).unwrap();
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
    let mut db: SlotDex<u64, u64> = SlotDex::new(16, false).unwrap();
    db.insert(10, 100).unwrap();

    let id = db.save_meta().unwrap();
    assert!(SlotDex::<u32, u64>::from_meta(id).is_err());
    assert!(SlotDex::<u64, u32>::from_meta(id).is_err());
}

/// Postcard serde roundtrip for SlotDex (derived serde, but inner types are hand-written).
#[test]
fn test_serde_roundtrip() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(16, false).unwrap();
    for i in 1..=50 {
        db.insert(i, i * 10).unwrap();
    }

    let bytes = postcard::to_allocvec(&db).unwrap();
    let restored: SlotDex<u64, u64> = postcard::from_bytes(&bytes).unwrap();

    assert_eq!(restored.total(), 50);

    // Verify paging still works
    let entries = restored.page(.., 10, 0, Order::Asc);
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
    let mut db: SlotDex<u64, u64> = SlotDex::new(16, false).unwrap();
    for i in 0..100 {
        db.insert(i, i * 3).unwrap();
    }

    let id = db.save_meta().unwrap();
    let restored: SlotDex<u64, u64> = SlotDex::from_meta(id).unwrap();

    assert_eq!(restored.total(), 100);

    // Verify page queries produce correct results
    let entries = restored.page(.., 20, 0, Order::Asc);
    assert_eq!(entries.len(), 20);

    let entries_p4 = restored.page(.., 20, 4, Order::Asc);
    assert_eq!(entries_p4.len(), 20);
}

// ---- Restore consistency tests (atomic single-handle model) ----

#[test]
fn restore_without_save_meta_preserves_total() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(10, false).unwrap();
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
    let mut db: SlotDex<u64, u64> = SlotDex::new(10, false).unwrap();
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
    let mut db: SlotDex<u64, u64> = SlotDex::new(8, false).unwrap();
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
        restored.page(90..=99, 20, 0, Order::Asc),
        (90u64..100).collect::<Vec<_>>()
    );
    assert_eq!(
        restored.page(0..=499, 20, 0, Order::Desc),
        (480u64..500).rev().collect::<Vec<_>>()
    );
}

#[test]
fn restore_after_interleaved_mutations_matches_reference() {
    let mut db: SlotDex<u64, u64> = SlotDex::new(4, false).unwrap();
    let mut reference: Vec<(u64, u64)> = vec![];
    for i in 0..200u64 {
        db.insert(i % 37, i).unwrap();
        reference.push((i % 37, i));
        if i % 3 == 0 {
            let (s, k) = reference.remove((i as usize * 7) % reference.len());
            db.remove(s, &k).unwrap();
        }
    }
    reference.sort();

    let bytes = postcard::to_allocvec(&db).unwrap();
    let restored: SlotDex<u64, u64> = postcard::from_bytes(&bytes).unwrap();

    assert_eq!(restored.total(), reference.len() as u64);
    let all = restored.page(.., u16::MAX, 0, Order::Asc);
    assert_eq!(all, reference.iter().map(|(_, k)| *k).collect::<Vec<_>>());
}

#[test]
fn tier_capacity_below_two_is_rejected() {
    assert!(matches!(
        SlotDex64::<u64>::new(0, false),
        Err(crate::common::error::VsdbError::InvalidConfig { .. })
    ));
    assert!(matches!(
        SlotDex64::<u64>::new(1, false),
        Err(crate::common::error::VsdbError::InvalidConfig { .. })
    ));

    // Malformed or corrupt metadata carrying tier_capacity < 2 must also be
    // rejected at deserialization rather than panicking on reverse page scans
    // (division by zero) or overflowing u8 tier levels.
    struct BadMeta<'a> {
        store: &'a MapxRaw,
        cap: u64,
    }
    impl Serialize for BadMeta<'_> {
        fn serialize<Ser: serde::Serializer>(
            &self,
            serializer: Ser,
        ) -> StdResult<Ser::Ok, Ser::Error> {
            crate::common::serialize_prefix_handle::<SlotDex64<u64>, Ser>(
                self.store,
                &postcard::to_allocvec(&(LAYOUT_VERSION, self.cap, false)).unwrap(),
                serializer,
            )
        }
    }
    let store = MapxRaw::new();
    for bad_cap in [0u64, 1] {
        let bytes = postcard::to_allocvec(&BadMeta {
            store: &store,
            cap: bad_cap,
        })
        .unwrap();
        assert!(
            postcard::from_bytes::<SlotDex64<u64>>(&bytes).is_err(),
            "SlotDex deserialize must reject tier_capacity={bad_cap}"
        );
    }

    let mut sd = SlotDex64::<u64>::new(2, false).unwrap();
    sd.insert(1, 10).unwrap();
    sd.remove(1, &10).unwrap();
    // Absent keys are a no-op, not an error.
    sd.remove(1, &10).unwrap();
    sd.insert(2, 20).unwrap();
    sd.clear().unwrap();
    assert_eq!(sd.total(), 0);
}

#[test]
fn range_queries_match_the_inclusive_api() {
    for swap in [false, true] {
        let mut sd = SlotDex64::<u64>::new(4, swap).unwrap();
        for i in 0..300u64 {
            sd.insert(i / 3, i).unwrap();
        }
        for order in [Order::Asc, Order::Desc] {
            for idx in 0..4 {
                let slots: Vec<_> = match order {
                    Order::Asc => (10..40).collect(),
                    Order::Desc => (10..40).rev().collect(),
                };
                let expected = slots
                    .into_iter()
                    .flat_map(|s| s * 3..s * 3 + 3)
                    .skip(idx as usize * 7)
                    .take(7)
                    .collect::<Vec<_>>();
                assert_eq!(sd.page(10..40, 7, idx, order), expected);
            }
        }
        assert_eq!(sd.count(..), 300);
        assert_eq!(sd.count(10..=19), 30);
        assert_eq!(sd.count(10..20), 30);
        assert_eq!(sd.count(..10), 30);
        // Empty and inverted ranges.
        assert_eq!(sd.count(20..20), 0);
        // An inverted range (start > end) is empty, not a panic.
        let inverted = (std::ops::Bound::Included(20), std::ops::Bound::Included(10));
        assert!(sd.page(inverted, 5, 0, Order::Asc).is_empty());
        assert_eq!(sd.count(u64::MAX..), 0);
        assert!(
            sd.page(
                (
                    std::ops::Bound::Excluded(u64::MAX),
                    std::ops::Bound::Unbounded
                ),
                5,
                0,
                Order::Asc
            )
            .is_empty()
        );
    }
}

#[test]
fn reverse_paging_bounds_hot_slot_decoding() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static DECODES: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct CountingKey(u64);

    impl KeyEnDeOrdered for CountingKey {
        fn with_ordered_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
            f(&self.0.to_be_bytes())
        }

        fn from_slice(bytes: &[u8]) -> Result<Self> {
            DECODES.fetch_add(1, Ordering::Relaxed);
            u64::from_slice(bytes).map(Self)
        }
    }

    for swap in [false, true] {
        let mut sd = SlotDex64::<CountingKey>::new(16, swap).unwrap();
        sd.insert_batch((0..10_000).map(|key| (0, CountingKey(key))))
            .unwrap();

        // A page entirely inside a hot slot must stop at its quota, including
        // when tier counts locate its start in the middle of that slot.
        for order in [Order::Asc, Order::Desc] {
            DECODES.store(0, Ordering::Relaxed);
            let stream = sd.iter(0..=0, order);
            assert_eq!(DECODES.load(Ordering::Relaxed), 0);
            assert_eq!(
                stream.take(10).collect::<Vec<_>>(),
                (0..10).map(CountingKey).collect::<Vec<_>>()
            );
            assert_eq!(DECODES.load(Ordering::Relaxed), 10);
            for page_index in [0, 100] {
                DECODES.store(0, Ordering::Relaxed);
                let got = sd.page(0..=0, 10, page_index, order);
                let start = u64::from(page_index) * 10;
                let expected = (start..start + 10).map(CountingKey).collect::<Vec<_>>();
                assert_eq!(got, expected, "swap={swap}, order={order:?}");
                assert_eq!(DECODES.load(Ordering::Relaxed), got.len());
            }
        }

        // Two small slots between hot slots force a three-slot page whose
        // lowest storage boundary has a large unused tail. Seeking past that
        // tail must preserve ascending key order inside every slot.
        for (slot, keys) in [
            (1, 10_000..10_003),
            (2, 20_000..20_003),
            (3, 30_000..40_000),
        ] {
            sd.insert_batch(keys.map(|key| (slot, CountingKey(key))))
                .unwrap();
        }
        for order in [Order::Asc, Order::Desc] {
            let mut slots =
                vec![0..10_000, 10_000..10_003, 20_000..20_003, 30_000..40_000];
            if order == Order::Desc {
                slots.reverse();
            }
            let reference = slots
                .into_iter()
                .flatten()
                .map(CountingKey)
                .collect::<Vec<_>>();
            for page_index in [0, 999, 1000, 1001, 2000, 2001] {
                DECODES.store(0, Ordering::Relaxed);
                let got = sd.page(0..=3, 10, page_index, order);
                let start = page_index as usize * 10;
                let expected = reference
                    .iter()
                    .skip(start)
                    .take(10)
                    .cloned()
                    .collect::<Vec<_>>();
                assert_eq!(
                    got, expected,
                    "swap={swap}, order={order:?}, page={page_index}"
                );
                assert_eq!(DECODES.load(Ordering::Relaxed), got.len());
            }
        }
    }
}

#[test]
fn independent_aliases_read_consistent_snapshots_and_reject_obsolete_writes() {
    for swap in [false, true] {
        let mut writer = SlotDex64::<u64>::new(3, swap).unwrap();
        writer.insert_batch((0..70).map(|i| (i, i))).unwrap();
        let bytes = postcard::to_allocvec(&writer).unwrap();
        let mut other: SlotDex64<u64> = postcard::from_bytes(&bytes).unwrap();
        let outstanding = other.iter(.., Order::Desc);
        writer.clear().unwrap();
        writer.insert_batch([(8, 10), (8, 11), (12, 15)]).unwrap();
        assert_eq!(other.total(), 3);
        assert_eq!(other.count(8..=8), 2);
        assert_eq!(other.page(.., 2, 0, Order::Desc), [15, 10]);
        assert_eq!(
            outstanding.collect::<Vec<_>>(),
            (0..70).rev().collect::<Vec<_>>()
        );
        assert!(other.insert(7, 5).is_err());
        assert!(other.remove(8, &10).is_err());
        assert!(other.insert_batch([(2, 3)]).is_err());
        assert!(other.clear().is_err());
        let mut independent = other.clone();
        assert_ne!(other.instance_id(), independent.instance_id());
        assert_eq!(independent.generation, 0);
        independent.clear().unwrap();
        assert_eq!(other.total(), 3);
        assert_eq!(writer.generation, 3);
    }
}

#[test]
fn generation_overflow_preserves_rows() {
    let mut db = SlotDex64::<u64>::new(3, false).unwrap();
    db.insert(1, 2).unwrap();
    db.store.insert(TOTAL_KEY, total_record(1, u64::MAX));
    let mut db: SlotDex64<u64> =
        postcard::from_bytes(&postcard::to_allocvec(&db).unwrap()).unwrap();
    assert!(db.clear().is_err());
    assert!(db.insert(2, 3).is_err());
    assert!(db.remove(1, &2).is_err());
    assert!(db.insert_batch([(3, 3)]).is_err());
    assert_eq!(db.iter(.., Order::Asc).collect::<Vec<_>>(), [2]);
}

#[test]
fn odd_signed_strides_and_extremes() {
    assert_eq!(i64::MIN.floor_align(3), i64::MIN);
    assert_eq!((i64::MIN + 2).floor_align(3), i64::MIN);
    assert_eq!(i64::tier_stride(2, 64), None);
    for a in i8::MIN..i8::MAX {
        let b = a + 1;
        assert!(SlotType::to_bytes(a) < SlotType::to_bytes(b));
        assert!(!b < !a);
        assert_eq!(
            <i8 as SlotType>::from_bytes(SlotType::to_bytes(a)).unwrap(),
            a
        );
    }
    for swap in [false, true] {
        let mut db = SlotDex::<i8, i16>::new(3, swap).unwrap();
        db.insert_batch((i8::MIN..=i8::MAX).map(|slot| (slot, i16::from(slot))))
            .unwrap();
        for lo in [-128i8, -93, -1, 0, 1, 47, 126, 127] {
            for hi in [lo, lo.saturating_add(3), i8::MAX] {
                let range = i16::from(lo)..=i16::from(hi);
                assert_eq!(db.count(lo..=hi), range.clone().count() as u64);
                assert!(db.iter(lo..=hi, Order::Desc).eq(range.clone().rev()));
                assert!(db.iter(lo..=hi, Order::Asc).eq(range.clone()));
                assert_eq!(
                    db.page(lo..=hi, 3, 2, Order::Desc),
                    range.rev().skip(6).take(3).collect::<Vec<_>>()
                );
            }
        }
    }
}

#[test]
fn large_keys_and_tierless_reverse_chunks() {
    let mut db = SlotDex::<u64, Vec<u8>>::new(1_000_000, false).unwrap();
    db.insert_batch((0..1024u64).map(|slot| (slot, slot.to_be_bytes().to_vec())))
        .unwrap();
    db.insert(900, vec![0xff; 64 * 1024]).unwrap();
    let expected = (0..1024u64).rev().flat_map(|slot| {
        let small = slot.to_be_bytes().to_vec();
        std::iter::once(small).chain((slot == 900).then(|| vec![0xff; 64 * 1024]))
    });
    assert!(db.iter(.., Order::Desc).eq(expected));
}

#[test]
fn insert_batch_coalescing_matches_serial_rows_across_multi_tier_promotions() {
    for swap in [false, true] {
        let mut batch_db = SlotDex::<u64, u64>::new(4, swap).unwrap();
        let mut serial_db = SlotDex::<u64, u64>::new(4, swap).unwrap();

        // Pre-seed a few slots before the batch so both existing-slot reads
        // and fresh-slot promotions are exercised.
        for (slot, key) in [(0u64, 1000u64), (3, 1003), (12, 1012)] {
            batch_db.insert(slot, key).unwrap();
            serial_db.insert(slot, key).unwrap();
        }

        // 0 -> 1 -> 2 -> 3 mid-batch promotions, hot-slot repeats, and duplicates.
        let mut items = Vec::new();
        for i in 0..160u64 {
            items.push((i / 2, i));
            if i % 5 == 0 {
                // Repeat hot slots (both pre-existing and newly created) + duplicate pair.
                items.push((0, 2000 + i));
                items.push((i / 2, i));
            }
        }

        batch_db.insert_batch(items.iter().copied()).unwrap();
        for (slot, key) in items {
            serial_db.insert(slot, key).unwrap();
        }

        assert!(batch_db.levels.len() >= 3);
        assert_eq!(batch_db.levels.len(), serial_db.levels.len());
        for (a, b) in batch_db.levels.iter().zip(serial_db.levels.iter()) {
            assert_eq!(a.floor_base, b.floor_base);
            assert_eq!(a.buckets, b.buckets);
        }
        assert_eq!(batch_db.slot_rows, serial_db.slot_rows);
        assert_eq!(batch_db.total(), serial_db.total());

        // Compare every persisted [0x00] entry and [0x01] level-0 / tier row byte-for-bit
        // (ignoring only [0x02] TOTAL_KEY's generation counter, which advances once per batch
        // vs once per serial insert).
        let batch_rows: Vec<_> = batch_db
            .store
            .iter()
            .filter(|(k, _)| k.as_slice() != TOTAL_KEY)
            .collect();
        let serial_rows: Vec<_> = serial_db
            .store
            .iter()
            .filter(|(k, _)| k.as_slice() != TOTAL_KEY)
            .collect();
        assert_eq!(batch_rows, serial_rows, "swap={swap}");
    }
}

#[test]
fn restore_rejects_invalid_tier_rows_without_mutating_storage() {
    for tier in [0u8, 1] {
        let mut index = SlotDex64::<u64>::new(2, false).unwrap();
        let n = if tier == 0 { 1 } else { 10 };
        index.insert_batch((0..n).map(|k| (k, k))).unwrap();
        let id = index.save_meta().unwrap();
        let key = level_key(tier, &0u64);
        let original = index.store.get(&key).unwrap();
        for (k, v) in [
            (key.clone(), vec![1]),
            (key.clone(), vec![0; 9]),
            (key[..3].to_vec(), original.clone()),
        ] {
            index.store.insert(&k, &v);
            assert!(matches!(
                SlotDex64::<u64>::from_meta(id),
                Err(VsdbError::Corrupt { .. })
            ));
            assert_eq!(index.store.get(&k), Some(v));
            index.store.remove(&k);
            index.store.insert(&key, &original);
        }
        assert_eq!(SlotDex64::<u64>::from_meta(id).unwrap().total(), n);
    }
}

#[test]
fn restore_propagates_a_custom_slot_decode_error() {
    #[derive(
        Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
    )]
    struct Slot(u8);
    impl std::ops::Not for Slot {
        type Output = Self;
        fn not(self) -> Self {
            Self(!self.0)
        }
    }
    impl SlotType for Slot {
        type Bytes = [u8; 2];
        const MIN: Self = Self(0);
        const MAX: Self = Self(u8::MAX);
        fn to_bytes(self) -> Self::Bytes {
            [0, self.0]
        }
        fn from_bytes(bytes: Self::Bytes) -> Result<Self> {
            match bytes {
                [0, value] => Ok(Self(value)),
                _ => Err(VsdbError::Decode {
                    detail: "invalid slot tag".into(),
                }),
            }
        }
        fn tier_stride(capacity: u32, level: u8) -> Option<Self> {
            u8::tier_stride(capacity, level).map(Self)
        }
        fn floor_align(self, stride: Self) -> Self {
            Self(self.0.floor_align(stride.0))
        }
        fn checked_succ(self) -> Option<Self> {
            self.0.checked_succ().map(Self)
        }
        fn checked_pred(self) -> Option<Self> {
            self.0.checked_pred().map(Self)
        }
    }
    let mut index = SlotDex::<Slot, u64>::new(2, false).unwrap();
    index
        .insert_batch((0..10).map(|i| (Slot(i), u64::from(i))))
        .unwrap();
    let id = index.save_meta().unwrap();
    index.store.insert([TAG_LEVEL, 1, 3, 0], 1u64.to_le_bytes());
    assert!(
        matches!(SlotDex::<Slot, u64>::from_meta(id), Err(VsdbError::Decode { detail }) if detail == "invalid slot tag")
    );
    let handle = postcard::to_allocvec(&index).unwrap();
    assert!(postcard::from_bytes::<SlotDex<Slot, u64>>(&handle).is_err());
}

#[test]
fn large_tier_restore_checks_the_streamed_tail() {
    let mut index = SlotDex64::<u64>::new(2, false).unwrap();
    index
        .insert_batch((0..5000).map(|key| (2 * key, key)))
        .unwrap();
    let id = index.save_meta().unwrap();
    let reopened = SlotDex64::<u64>::from_meta(id).unwrap();
    assert_eq!(reopened.levels[0].buckets.len(), 5000);
    assert_eq!(reopened.page(9998.., 1, 0, Order::Asc), [4999]);

    let last = level_key(1, &9998u64);
    index.store.insert(&last, [1]);
    assert!(matches!(
        SlotDex64::<u64>::from_meta(id),
        Err(VsdbError::Corrupt { .. })
    ));
    assert_eq!(index.store.get(last), Some(vec![1]));
}

#[test]
fn failed_clone_in_wipes_copied_destination_prefix() {
    use serde::{
        Deserialize,
        de::value::{BytesDeserializer, Error as ValueError},
    };

    let dst = crate::Namespace::create().unwrap();
    let dst_id = dst.id();
    let open_prefix = |map_id: u64| -> MapxRaw {
        let frame = [
            b"VSMAPX02".as_slice(),
            &map_id.to_le_bytes(),
            &dst_id.to_le_bytes(),
        ]
        .concat();
        MapxRaw::deserialize(BytesDeserializer::<ValueError>::new(&frame)).unwrap()
    };

    // Case 1: corrupt TOTAL_KEY fails before hydration.
    let mut bad_total = SlotDex64::<u64>::new(4, false).unwrap();
    bad_total.insert_batch((0..20).map(|k| (k, k))).unwrap();
    bad_total.store.insert(TOTAL_KEY, [0xaa; 3]);
    let base = MapxRaw::new_in(&dst).instance_id().map_id;
    assert!(matches!(
        bad_total.clone_in(&dst),
        Err(VsdbError::Corrupt { .. })
    ));
    assert_eq!(MapxRaw::new_in(&dst).instance_id().map_id, base + 2);
    assert_eq!(open_prefix(base + 1).iter().count(), 0);

    // Case 2: corrupt tier row fails during hydration after TOTAL_KEY rewrite.
    let mut bad_tier = SlotDex64::<u64>::new(2, false).unwrap();
    bad_tier.insert_batch((0..16).map(|k| (k, k))).unwrap();
    bad_tier.store.insert(level_key(1, &0u64), [1]);
    let base = MapxRaw::new_in(&dst).instance_id().map_id;
    assert!(matches!(
        bad_tier.clone_in(&dst),
        Err(VsdbError::Corrupt { .. })
    ));
    assert_eq!(MapxRaw::new_in(&dst).instance_id().map_id, base + 2);
    assert_eq!(open_prefix(base + 1).iter().count(), 0);

    drop(dst);
    crate::Namespace::close_by_id(dst_id).unwrap();
    crate::Namespace::destroy(dst_id).unwrap();
}

#[test]
fn fallible_slotdex_operations_propagate_corrupt_rows_and_sst_errors() {
    use std::fs;

    // 1. Malformed level-0 count row returns VsdbError::Corrupt instead of panicking.
    let mut sd = SlotDex64::<u64>::new(2, false).unwrap();
    sd.insert(5, 100).unwrap();
    sd.store.insert(level_key(0, &5u64), [0x01; 3]);
    assert!(matches!(sd.insert(5, 200), Err(VsdbError::Corrupt { .. })));
    assert!(matches!(
        sd.insert_batch([(5, 200)]),
        Err(VsdbError::Corrupt { .. })
    ));
    assert!(matches!(sd.remove(5, &100), Err(VsdbError::Corrupt { .. })));

    // 2. SST block checksum corruption surfaces as VsdbError::Storage.
    fn pseudo_key(seed: u64, words: u64) -> Vec<u8> {
        let mut s = seed.wrapping_add(0x9e3779b97f4a7c15);
        (0..words)
            .flat_map(|_| {
                s = s.wrapping_add(0x9e3779b97f4a7c15);
                let mut z = s;
                z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
                (z ^ (z >> 31)).to_le_bytes()
            })
            .collect()
    }

    let ns = crate::Namespace::create_with(crate::NamespaceOpts {
        shards: 1,
        mem_budget_mb: Some(64),
        ..Default::default()
    })
    .unwrap();
    let ns_id = ns.id();
    let ns_path = ns.path().to_path_buf();

    let mut sd_meta = SlotDex64::<Vec<u8>>::new_in(&ns, 4, false).unwrap();
    let meta_key = pseudo_key(10, 256);
    sd_meta.insert(0, meta_key.clone()).unwrap();
    let meta_id = sd_meta.save_meta().unwrap();

    let mut sd_sst = SlotDex64::<Vec<u8>>::new_in(&ns, 4, false).unwrap();
    let target_key = pseudo_key(1, 2_560);
    let tail_key = pseudo_key(2, 2_560);
    sd_sst.insert(0, target_key.clone()).unwrap();
    sd_sst.insert(u64::MAX, tail_key).unwrap();
    let id = sd_sst.save_meta().unwrap();
    ns.flush();
    drop(sd_meta);
    drop(sd_sst);
    drop(ns);
    crate::Namespace::close_by_id(ns_id).unwrap();

    let sst = fs::read_dir(ns_path.join("mmdb/shard_00"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|ext| ext == "sst"))
        .unwrap();
    let mut bytes = fs::read(&sst).unwrap();
    for needle in [&meta_key[..32], &target_key[..32]] {
        let offset = bytes.windows(32).position(|w| w == needle).unwrap();
        bytes[offset + 8] ^= 0x01;
    }
    fs::write(&sst, bytes).unwrap();

    assert!(matches!(
        SlotDex64::<Vec<u8>>::from_meta(meta_id),
        Err(VsdbError::Storage {
            kind: crate::StorageErrorKind::Corruption,
            ..
        })
    ));

    let mut sd_sst = SlotDex64::<Vec<u8>>::from_meta(id).unwrap();
    for (idx, res) in [
        sd_sst.insert(0, target_key.clone()),
        sd_sst.insert_batch([(0, target_key.clone())]),
        sd_sst.remove(0, &target_key),
    ]
    .into_iter()
    .enumerate()
    {
        assert!(
            matches!(
                res,
                Err(VsdbError::Storage {
                    kind: crate::StorageErrorKind::Corruption,
                    ..
                })
            ),
            "case {idx}: got {res:?}"
        );
    }
    drop(sd_sst);

    crate::Namespace::close_by_id(ns_id).unwrap();
    crate::Namespace::destroy(ns_id).unwrap();
}
