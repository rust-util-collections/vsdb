mod common;

use serde::{Deserialize, Serialize};
use std::ops::Bound;
use vsdb::{MapxOrd, ValueEnDe};

#[derive(Serialize, Deserialize, Default, Debug, Eq, PartialEq, Clone)]
struct SampleBlock {
    idx: u64,
    data: Vec<u64>,
}

fn gen_sample(idx: u64) -> SampleBlock {
    SampleBlock {
        idx,
        data: vec![idx],
    }
}

#[test]
fn basic_cases() {
    common::setup();
    let cnt = 200;

    let hdr = {
        let mut hdr_i = MapxOrd::new();

        (0..cnt).for_each(|i| {
            assert!(hdr_i.get(&i).is_none());
        });

        (0..cnt).map(|i| (i, gen_sample(i))).for_each(|(i, b)| {
            hdr_i.entry(&i).or_insert(b.clone());
            assert_eq!(hdr_i.get(&i).unwrap().idx, i);
            hdr_i.remove(&i);
            assert!(hdr_i.get(&i).is_none());
            hdr_i.insert(&i, &b);
            hdr_i.insert(&i, &b);
        });

        <MapxOrd<u64, SampleBlock> as ValueEnDe>::encode_value(&hdr_i)
    };

    let mut reloaded =
        <MapxOrd<u64, SampleBlock> as ValueEnDe>::decode_value(&hdr).unwrap();

    (0..cnt).for_each(|i| {
        assert_eq!(i, reloaded.get(&i).unwrap().idx);
    });

    (1..cnt).for_each(|i| {
        reloaded.get_mut(&i).unwrap().idx = 1 + i;
        assert_eq!(reloaded.get(&i).unwrap().idx, 1 + i);
        assert!(reloaded.contains_key(&i));
        reloaded.remove(&i);
        assert!(!reloaded.contains_key(&i));
    });

    reloaded.clear();

    reloaded.insert(&1, &gen_sample(1));
    reloaded.insert(&1, &gen_sample(1));
    reloaded.insert(&10, &gen_sample(10));
    reloaded.insert(&100, &gen_sample(100));
    reloaded.insert(&1000, &gen_sample(1000));

    assert!(reloaded.range(0..1).next().is_none());

    assert_eq!(100, reloaded.range(12..999).next().unwrap().1.idx);
    assert_eq!(100, reloaded.range(12..=999).next().unwrap().1.idx);

    assert_eq!(100, reloaded.range(100..=999).next().unwrap().1.idx);
    assert!(
        reloaded
            .range((Bound::Excluded(100), Bound::Included(999)))
            .next()
            .is_none()
    );

    assert_eq!(100, reloaded.get_ge(&99).unwrap().1.idx);
    assert_eq!(100, reloaded.get_ge(&100).unwrap().1.idx);
    assert_eq!(100, reloaded.get_le(&100).unwrap().1.idx);
    assert_eq!(100, reloaded.get_le(&101).unwrap().1.idx);
}

#[test]
fn negative_int_range() {
    common::setup();
    macro_rules! run {
        ($int: ty) => {
            let mut hdr = MapxOrd::<$int, $int>::new();
            (-50..50).for_each(|i| {
                hdr.insert(&i, &i);
            });

            hdr.range(..)
                .map(|(i, _)| i)
                .enumerate()
                .for_each(|(idx, i)| {
                    assert_eq!((idx as $int) - 50, i);
                });
        };
    }

    run!(i8);
    run!(i16);
    run!(i32);
    run!(i64);
    run!(i128);
}
