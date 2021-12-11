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
    crate::reset_db();

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

    db_restore.set_value(0, gen_sample(100 * cnt));
    assert_eq!(cnt, db_restore.len());

    db_restore.set_value(2 * cnt, gen_sample(1000 * cnt));
    assert_eq!(1 + cnt, db_restore.len());

    assert_eq!(db_restore.get(2 * cnt).unwrap(), gen_sample(1000 * cnt));
    *db_restore.get_mut(2 * cnt).unwrap() = gen_sample(999 * cnt);
    assert_eq!(db_restore.get(2 * cnt).unwrap(), gen_sample(999 * cnt));

    crate::reset_db();
    assert!(db_restore.is_empty());
}
