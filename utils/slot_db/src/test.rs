use super::*;
use rand::random;

#[test]
fn data_container() {
    let mut db = SlotDB::new(4, false);

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
}

#[test]
fn workflow() {
    [16, 8, 4].into_iter().for_each(|i| {
        slot_db_original_order(i);
        slot_db_swap_order(i);
    });
}

fn slot_db_original_order(mn: u64) {
    let mut db = SlotDB::new(mn, false);

    let max = ts!();
    let min = max - 20000;

    (min..max).for_each(|i| {
        db.insert(i, i).unwrap();
    });

    dbg!(db.total);
    assert_eq!(max - min, db.total);

    assert_queryable(&db, false, 1, 256, min, max - 1);

    for _ in 0..3 {
        (min..max).for_each(|i| {
            db.insert(i, i).unwrap();
        });
    }

    assert_queryable(&db, false, 4, 256, min, max - 1);

    // Cover the remove scene
    (min..max).for_each(|i| {
        db.remove(i, &i);
    });
    assert_eq!(0, db.total);
    assert!(db.get_entries_by_page(10, 0, true).is_empty());
    assert!(db.get_entries_by_page(10, 0, false).is_empty());
}

fn slot_db_swap_order(mn: u64) {
    let mut db = SlotDB::new(mn, true);

    let max = ts!();
    let min = max - max / 100_000;

    (min..max).for_each(|i| {
        db.insert(i, i).unwrap();
    });

    dbg!(db.total);
    assert_eq!(max - min, db.total);

    assert_queryable(&db, true, 1, 256, min, max - 1);

    for _ in 0..3 {
        (min..max).for_each(|i| {
            db.insert(i, i).unwrap();
        });
    }

    assert_queryable(&db, true, 4, 256, min, max - 1);

    // Cover the remove scene
    (min..max).for_each(|i| {
        db.remove(i, &i);
    });
    assert_eq!(0, db.total);
    assert!(db.get_entries_by_page(0, 10, true).is_empty());
    assert!(db.get_entries_by_page(0, 10, true).is_empty());
}

fn assert_queryable(
    db: &SlotDB<u64>,
    swap_order: bool,
    step: u32,
    times: u64,
    slot_min: u64,
    slot_max: u64,
) {
    dbg!(step, times, slot_min, slot_max);
    assert!(0 < step);
    assert!(0 < times);
    assert!(slot_min <= slot_max);

    for i in 0..times {
        let page_size = step + (random::<u16>() as u32) % 128 / step * step;
        let max_page = min!(u32::MAX, (db.total / (page_size as u64) - 1) as u32);

        dbg!("||<-----===========----->||", max_page);

        // Ensure the first page case is covered
        let page_number = alt!(0 == i, 0, random::<u32>() % max_page);

        let page_size = page_size as u64;
        let page_number = page_number as u64;

        dbg!(page_number, page_size);
        let end = slot_max - page_number * page_size;
        let mut a = (0..=dbg!(end))
            .rev()
            .take((page_size) as usize)
            .collect::<Vec<_>>();
        a.sort_unstable_by(|a, b| b.cmp(&a));
        let c = db.get_entries_by_page(page_size as u16, page_number as u32, true);
        assert_eq!(a, c);

        if !swap_order {
            let b = db
                .data
                .range(..=end)
                .rev()
                .take((page_size) as usize)
                .map(|(_, v)| v.iter().collect::<Vec<_>>())
                .flatten()
                .collect::<Vec<_>>();
            assert_eq!(b, c);
        }

        let start = slot_min + page_number * page_size;
        let mut a = (dbg!(start)..)
            .take((page_size) as usize)
            .collect::<Vec<_>>();
        a.sort_unstable();
        let c = db.get_entries_by_page(page_size as u16, page_number as u32, false);
        assert_eq!(a, c);

        if !swap_order {
            let b = db
                .data
                .range(start..)
                .take((page_size) as usize)
                .map(|(_, v)| v.iter().collect::<Vec<_>>())
                .flatten()
                .collect::<Vec<_>>();
            assert_eq!(b, c);
        }

        //////////////////////////////////
        // Cases with custom slot range //
        //////////////////////////////////

        let smin = random::<u64>() % slot_min;
        let smax = slot_min.saturating_add(random::<u64>() % 99999);

        let smin_actual = max!(smin, slot_min); // should always be slot_min
        let smax_actual = min!(smax, slot_max);

        let actual_gap = 1 + smax_actual - smin_actual;

        ////////////////////////////////////////
        ////////////////////////////////////////

        println!("Step {} Round {} ==>", step, i);
        dbg!(
            slot_min,
            slot_max,
            smin,
            smax,
            smin_actual,
            smax_actual,
            actual_gap,
            page_number,
            page_size,
        );
        println!("Step {} Round {} <==\n", step, i);

        let end = smax_actual - page_number * page_size;
        let take_n = min!(
            alt!(end < smin_actual, 0, 1 + end.saturating_sub(smin_actual)),
            page_size
        );
        let mut a = (smin_actual..=dbg!(end))
            .rev()
            .take(dbg!(take_n) as usize)
            .collect::<Vec<_>>();
        a.sort_unstable_by(|a, b| b.cmp(&a));
        let c = db.get_entries_by_page_slot(
            Some([smin, smax]),
            page_size as u16,
            page_number as u32,
            true,
        );
        assert_eq!(a, c);

        if !swap_order {
            let b = db
                .data
                .range(smin_actual..=end)
                .rev()
                .take(take_n as usize)
                .map(|(_, v)| v.iter().collect::<Vec<_>>())
                .flatten()
                .collect::<Vec<_>>();
            assert_eq!(b, c);
        }

        ////////////////////////////////////////
        ////////////////////////////////////////

        println!("Step {} Round {} ==>", step, i);
        dbg!(
            slot_min,
            slot_max,
            smin,
            smax,
            smin_actual,
            smax_actual,
            actual_gap,
            page_number,
            page_size,
        );
        println!("Step {} Round {} <==\n", step, i);

        let start = smin_actual + page_number * page_size;
        let take_n = min!(
            alt!(
                start > smax_actual,
                0,
                1 + smax_actual.saturating_sub(start)
            ),
            page_size
        );
        let mut a = (dbg!(start)..=smax_actual)
            .take(dbg!(take_n) as usize)
            .collect::<Vec<_>>();
        a.sort_unstable();
        let c = db.get_entries_by_page_slot(
            Some([smin, smax]),
            page_size as u16,
            page_number as u32,
            false,
        );
        assert_eq!(a, c);

        if !swap_order {
            let b = db
                .data
                .range(start..=smax_actual)
                .take(take_n as usize)
                .map(|(_, v)| v.iter().collect::<Vec<_>>())
                .flatten()
                .collect::<Vec<_>>();
            assert_eq!(b, c);
        }
    }
}
