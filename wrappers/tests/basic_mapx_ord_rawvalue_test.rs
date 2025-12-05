use ruc::*;
use std::ops::Bound;
use vsdb::{ValueEnDe, basic::mapx_ord_rawvalue::MapxOrdRawValue, vsdb_set_base_dir};

#[test]
fn basic_cases() {
    let cnt = 200;
    info_omit!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    let hdr = {
        let mut hdr_i = MapxOrdRawValue::new();

        (0usize..cnt).for_each(|i| {
            assert!(hdr_i.get(&i).is_none());
        });

        (0usize..cnt)
            .map(|i| (i, i.to_be_bytes()))
            .for_each(|(i, b)| {
                hdr_i.entry(i).or_insert(b.clone());
                assert_eq!(&hdr_i.get(&i).unwrap()[..], &b[..]);
                hdr_i.remove(&i);
                assert!(hdr_i.get(&i).is_none());
                hdr_i.insert(&i, &b);
                hdr_i.insert(&i, &b);
            });

        <MapxOrdRawValue<usize> as ValueEnDe>::encode(&hdr_i)
    };

    let mut reloaded = pnk!(<MapxOrdRawValue<usize> as ValueEnDe>::decode(&hdr));

    (0usize..cnt).for_each(|i| {
        assert_eq!(&i.to_be_bytes(), &reloaded.get(&i).unwrap()[..]);
    });

    (1usize..cnt).for_each(|i| {
        *reloaded.get_mut(&i).unwrap() = (1 + i).to_be_bytes().to_vec();
        assert_eq!(&reloaded.get(&i).unwrap()[..], &(1 + i).to_be_bytes());
        assert!(reloaded.contains_key(&i));
        reloaded.remove(&i);
        assert!(!reloaded.contains_key(&i));
    });

    reloaded.clear();

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
