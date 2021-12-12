//!
//! # Test Cases
//!

use super::*;

#[test]
fn t_mapx_raw() {
    crate::clear();

    let cnt = 200;

    let db = {
        let mut dbi = crate::MapxRaw::new();

        assert_eq!(0, dbi.len());
        (0..cnt).for_each(|i: usize| {
            assert!(dbi.get(&i.to_be_bytes()).is_none());
        });

        (0..cnt)
            .map(|i: usize| (i.to_be_bytes(), i.to_be_bytes()))
            .for_each(|(i, b)| {
                dbi.entry(&i).or_insert(&b);
                assert_eq!(pnk!(dbi.get(&i)).as_ref(), &i);
                assert_eq!(dbi.remove(&i).unwrap().as_ref(), &b);
                assert!(dbi.get(&i).is_none());
                assert!(dbi.insert(&i, &b).is_none());
                assert!(dbi.insert(&i, &b).is_some());
            });

        assert_eq!(cnt, dbi.len());

        pnk!(bincode::serialize(&dbi))
    };

    let mut db_restore = pnk!(bincode::deserialize::<MapxRaw>(&db));

    assert_eq!(cnt, db_restore.len());

    (0..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        assert_eq!(&i, db_restore.get(&i).unwrap().as_ref());
    });

    (1..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        *pnk!(db_restore.get_mut(&i)) = IVec::from(&i);
        assert_eq!(pnk!(db_restore.get(&i)).as_ref(), &i);
        assert!(db_restore.contains_key(&i));
        assert!(db_restore.remove(&i).is_some());
        assert!(!db_restore.contains_key(&i));
    });

    assert_eq!(1, db_restore.len());
    crate::clear();
    unsafe { db_restore.set_len(0) };
    assert!(db_restore.is_empty());

    db_restore.insert(&[1], &[1]);
    db_restore.insert(&[4], &[4]);
    db_restore.insert(&[6], &[6]);
    db_restore.insert(&[80], &[80]);

    assert!(db_restore.range(&[][..]..&[1][..]).next().is_none());
    assert_eq!(
        &[4],
        db_restore
            .range(&[2][..]..&[10][..])
            .next()
            .unwrap()
            .1
            .as_ref()
    );

    assert_eq!(&[80], db_restore.get_ge(&[79]).unwrap().1.as_ref());
    assert_eq!(&[80], db_restore.get_ge(&[80]).unwrap().1.as_ref());
    assert_eq!(&[80], db_restore.get_le(&[80]).unwrap().1.as_ref());
    assert_eq!(&[80], db_restore.get_le(&[100]).unwrap().1.as_ref());
}
