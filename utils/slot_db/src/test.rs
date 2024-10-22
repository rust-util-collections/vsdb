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
    let mut db = SlotDB::new(mn, swap_order);
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
    db: &SlotDB<u64>,
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
        let b =
            db.get_entries_by_page(page_size as u16, page_number as u32, true);
        assert_eq!(a, b);

        let a = test_db.get_entries_by_page_slot(
            None,
            None,
            page_size as u16,
            page_number as u32,
            false,
        );
        let b = db.get_entries_by_page(
            page_size as u16,
            page_number as u32,
            false,
        );
        assert_eq!(a, b);

        //////////////////////////////////
        // Cases with custom slot range //
        //////////////////////////////////

        let smin = random::<u64>() % (slot_min.saturating_add(100));
        let smax = smin
            + random::<u64>() % ((slot_max - slot_min).saturating_add(100));

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
        1_0000
    } else {
        100_0000
    }
}

#[test]
fn data_container() {
    let mut db = SlotDB::new(16, false);

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
        DataCtner::Large(_)
    ));
    assert_eq!(db.data.len(), 1);
    assert_eq!(db.data.first().unwrap().1.len(), 100);
    assert_eq!(db.data.first().unwrap().1.iter().next().unwrap(), 0);
    assert_eq!(db.data.first().unwrap().1.iter().last().unwrap(), 99);

    db.clear();
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
