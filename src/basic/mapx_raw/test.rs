use super::*;

#[test]
fn basic_cases() {
    let cnt = 200;

    let hdr = {
        let mut hdr_i = crate::MapxRaw::new();

        assert_eq!(0, hdr_i.len());
        (0..cnt).for_each(|i: usize| {
            assert!(hdr_i.get(&i.to_be_bytes()).is_none());
        });

        (0..cnt)
            .map(|i: usize| (i.to_be_bytes(), i.to_be_bytes()))
            .for_each(|(i, b)| {
                hdr_i.entry(&i).or_insert(&b);
                assert_eq!(&hdr_i.get(&i).unwrap(), &i);
                assert_eq!(&hdr_i.remove(&i).unwrap(), &b);
                assert!(hdr_i.get(&i).is_none());
                assert!(hdr_i.insert(&i, &b).is_none());
                assert!(hdr_i.insert(&i, &b).is_some());
            });

        assert_eq!(cnt, hdr_i.len());

        pnk!(bcs::to_bytes(&hdr_i))
    };

    let mut reloaded = pnk!(bcs::from_bytes::<MapxRaw>(&hdr));

    assert_eq!(cnt, reloaded.len());

    (0..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        assert_eq!(i.to_vec(), reloaded.get(&i).unwrap());
    });

    (1..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        *pnk!(reloaded.get_mut(&i)) = i.to_vec();
        assert_eq!(&reloaded.get(&i).unwrap(), &i);
        assert!(reloaded.contains_key(&i));
        assert!(reloaded.remove(&i).is_some());
        assert!(!reloaded.contains_key(&i));
    });

    assert_eq!(1, reloaded.len());
    reloaded.clear();
    assert!(reloaded.is_empty());

    reloaded.insert(&[1], &[1]);
    reloaded.insert(&[4], &[4]);
    reloaded.insert(&[6], &[6]);
    reloaded.insert(&[80], &[80]);

    assert!(reloaded.range(&[][..]..&[1][..]).next().is_none());
    assert_eq!(
        vec![4],
        reloaded.range(&[2][..]..&[10][..]).next().unwrap().1
    );

    assert_eq!(vec![80], reloaded.get_ge(&[79]).unwrap().1);
    assert_eq!(vec![80], reloaded.get_ge(&[80]).unwrap().1);
    assert_eq!(vec![80], reloaded.get_le(&[80]).unwrap().1);
    assert_eq!(vec![80], reloaded.get_le(&[100]).unwrap().1);
}
