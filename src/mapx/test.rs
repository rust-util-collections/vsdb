//!
//! # Test Cases
//!

use super::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
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
fn t_mapx() {
    let cnt = 200;

    let db = {
        let mut db = crate::new_mapx!();

        assert_eq!(0, db.len());
        (0..cnt).for_each(|i| {
            assert!(db.get(&i).is_none());
        });

        (0..cnt).map(|i| (i, gen_sample(i))).for_each(|(i, b)| {
            db.entry(i).or_insert(b.clone());
            assert_eq!(1 + i as usize, db.len());
            assert_eq!(pnk!(db.get(&i)).idx, i);
            assert_eq!(db.remove(&i), Some(b.clone()));
            assert_eq!(i as usize, db.len());
            assert!(db.get(&i).is_none());
            assert!(db.insert(i, b.clone()).is_none());
            assert!(db.insert(i, b).is_some());
        });

        assert_eq!(cnt, db.len());

        pnk!(serde_json::to_vec(&db))
    };

    let mut db_restore = pnk!(serde_json::from_slice::<Mapx<usize, SampleBlock>>(&db));

    assert_eq!(cnt, db_restore.len());

    (0..cnt).for_each(|i| {
        assert_eq!(i, db_restore.get(&i).unwrap().idx);
    });

    (0..cnt).for_each(|i| {
        pnk!(db_restore.get_mut(&i)).idx = 1 + i;
        assert_eq!(pnk!(db_restore.get(&i)).idx, 1 + i);
        assert!(db_restore.contains_key(&i));
        assert!(db_restore.remove(&i).is_some());
        assert!(!db_restore.contains_key(&i));
    });
}
