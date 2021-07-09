//!
//! # Test Cases
//!

use super::*;
use ruc::*;
use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SampleBlock {
    idx: usize,
    data: Vec<usize>,
}

fn gen_sample(idx: usize) -> SampleBlock {
    SampleBlock {
        idx,
        data: vec![idx],
    }
}

#[test]
fn t_vecx() {
    crate::clear();

    let cnt = 200;

    let db = {
        let mut db = crate::Vecx::new();

        assert_eq!(0, db.len());
        (0..cnt).for_each(|i| {
            assert!(db.get(i).is_none());
        });

        (0..cnt).map(|i| (i, gen_sample(i))).for_each(|(i, b)| {
            db.push(b.clone());
            assert_eq!(1 + i as usize, db.len());
            assert_eq!(pnk!(db.get(i as usize)), b);
            assert_eq!(pnk!(db.last()), b);
        });

        assert_eq!(cnt, db.len());

        pnk!(bincode::serialize(&db))
    };

    let mut db_restore = pnk!(bincode::deserialize::<Vecx<SampleBlock>>(&db));

    (0..cnt).for_each(|i| {
        assert_eq!(i, db_restore.get(i).unwrap().idx);
    });

    assert_eq!(cnt, db_restore.len());

    db_restore.update_value(0, gen_sample(100 * cnt)).unwrap();
    assert_eq!(cnt, db_restore.len());
    *db_restore.get_mut(0).unwrap() = gen_sample(999 * cnt);
    assert_eq!(db_restore.get(0).unwrap(), gen_sample(999 * cnt));

    // out of index
    assert!(db_restore
        .update_value(2 * cnt, gen_sample(1000 * cnt))
        .is_err());

    db_restore.pop();
    assert_eq!(cnt - 1, db_restore.len());

    crate::clear();
    unsafe { db_restore.set_len(0) };
    assert!(db_restore.is_empty());
}
