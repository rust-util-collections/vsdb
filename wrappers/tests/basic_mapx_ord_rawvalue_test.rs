use ruc::*;
use std::ops::Bound;
use vsdb::{basic::mapx_ord_rawvalue::MapxOrdRawValue, vsdb_set_base_dir, ValueEnDe};

#[test]
fn basic_cases() {
    let cnt = 200;
    vsdb_set_base_dir("/tmp/.vsdb/basic_mapx_ord_rawvalue_test").unwrap();
    let hdr = {
        let mut hdr_i = MapxOrdRawValue::new();

        assert_eq!(0, hdr_i.len());
        (0usize..cnt).for_each(|i| {
            assert!(hdr_i.get(&i).is_none());
        });

        (0usize..cnt)
            .map(|i| (i, i.to_be_bytes()))
            .for_each(|(i, b)| {
                hdr_i.entry(&i).or_insert(&b);
                assert_eq!(1 + i as usize, hdr_i.len());
                assert_eq!(&hdr_i.get(&i).unwrap()[..], &b[..]);
                assert_eq!(&hdr_i.remove(&i).unwrap()[..], &b);
                assert_eq!(i as usize, hdr_i.len());
                assert!(hdr_i.get(&i).is_none());
                assert!(hdr_i.insert(&i, &b).is_none());
                assert!(hdr_i.insert(&i, &b).is_some());
            });

        assert_eq!(cnt, hdr_i.len());

        <MapxOrdRawValue<usize> as ValueEnDe>::encode(&hdr_i)
    };

    let mut reloaded = pnk!(<MapxOrdRawValue<usize> as ValueEnDe>::decode(&hdr));

    assert_eq!(cnt, reloaded.len());

    (0usize..cnt).for_each(|i| {
        assert_eq!(&i.to_be_bytes(), &reloaded.get(&i).unwrap()[..]);
    });

    (1usize..cnt).for_each(|i| {
        *reloaded.get_mut(&i).unwrap() =
            (1 + i).to_be_bytes().to_vec().into_boxed_slice();
        assert_eq!(&reloaded.get(&i).unwrap()[..], &(1 + i).to_be_bytes());
        assert!(reloaded.contains_key(&i));
        assert!(reloaded.remove(&i).is_some());
        assert!(!reloaded.contains_key(&i));
    });

    assert_eq!(1, reloaded.len());
    reloaded.clear();
    assert!(reloaded.is_empty());

    reloaded.insert(&1, &1usize.to_be_bytes());
    reloaded.insert(&10, &10usize.to_be_bytes());
    reloaded.insert(&100, &100usize.to_be_bytes());
    reloaded.insert(&1000, &1000usize.to_be_bytes());

    assert!(reloaded.range(&0..&1).next().is_none());

    assert_eq!(
        &100usize.to_be_bytes()[..],
        &reloaded.range(&12..&999).next().unwrap().1[..]
    );
    assert_eq!(
        &100usize.to_be_bytes()[..],
        &reloaded.range(&12..=&999).next().unwrap().1[..]
    );

    assert_eq!(
        &100usize.to_be_bytes()[..],
        &reloaded.range(&100..=&999).next().unwrap().1[..]
    );
    assert!(
        reloaded
            .range((Bound::Excluded(&100), Bound::Included(&999)))
            .next()
            .is_none()
    );

    assert_eq!(
        &100usize.to_be_bytes()[..],
        &reloaded.get_ge(&99).unwrap().1[..]
    );
    assert_eq!(
        &100usize.to_be_bytes()[..],
        &reloaded.get_ge(&100).unwrap().1[..]
    );
    assert_eq!(
        &100usize.to_be_bytes()[..],
        &reloaded.get_le(&100).unwrap().1[..]
    );
    assert_eq!(
        &100usize.to_be_bytes()[..],
        &reloaded.get_le(&101).unwrap().1[..]
    );
}
