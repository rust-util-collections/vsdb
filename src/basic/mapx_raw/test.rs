//!
//! # Test Cases
//!

use super::*;

#[test]
fn t_mapx_raw() {
    crate::vsdb_clear();

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

    let mut reloaded = pnk!(bincode::deserialize::<MapxRaw>(&db));

    assert_eq!(cnt, reloaded.len());

    (0..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        assert_eq!(&i, reloaded.get(&i).unwrap().as_ref());
    });

    (1..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        *pnk!(reloaded.get_mut(&i)) = IVec::from(&i);
        assert_eq!(pnk!(reloaded.get(&i)).as_ref(), &i);
        assert!(reloaded.contains_key(&i));
        assert!(reloaded.remove(&i).is_some());
        assert!(!reloaded.contains_key(&i));
    });

    assert_eq!(1, reloaded.len());
    crate::vsdb_clear();
    unsafe { reloaded.set_len(0) };
    assert!(reloaded.is_empty());

    reloaded.insert(&[1], &[1]);
    reloaded.insert(&[4], &[4]);
    reloaded.insert(&[6], &[6]);
    reloaded.insert(&[80], &[80]);

    assert!(reloaded.range(&[][..]..&[1][..]).next().is_none());
    assert_eq!(
        &[4],
        reloaded
            .range(&[2][..]..&[10][..])
            .next()
            .unwrap()
            .1
            .as_ref()
    );

    assert_eq!(&[80], reloaded.get_ge(&[79]).unwrap().1.as_ref());
    assert_eq!(&[80], reloaded.get_ge(&[80]).unwrap().1.as_ref());
    assert_eq!(&[80], reloaded.get_le(&[80]).unwrap().1.as_ref());
    assert_eq!(&[80], reloaded.get_le(&[100]).unwrap().1.as_ref());
}
