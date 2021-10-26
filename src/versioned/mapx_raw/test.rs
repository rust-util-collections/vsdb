use super::*;

#[test]
fn basic_cases() {
    let cnt = 200;

    let hdr = {
        let mut hdr_i = crate::MapxRawVersioned::new();
        hdr_i.version_create(b"test1").unwrap();

        assert_eq!(0, hdr_i.len());
        (0..cnt).for_each(|i: usize| {
            assert!(hdr_i.get(&i.to_be_bytes()).is_none());
        });

        (0..cnt)
            .map(|i: usize| (i.to_be_bytes(), i.to_be_bytes()))
            .for_each(|(i, b)| {
                hdr_i.insert(i.to_vec(), b.to_vec()).unwrap();
                assert_eq!(&hdr_i.get(&i).unwrap(), &i);
                assert_eq!(&hdr_i.remove(&i).unwrap().unwrap(), &b);
                assert!(hdr_i.get(&i).is_none());
                assert!(hdr_i.insert(i.to_vec(), b.to_vec()).unwrap().is_none());
                assert!(hdr_i.insert(i.to_vec(), b.to_vec()).unwrap().is_some());
            });

        assert_eq!(cnt, hdr_i.len());

        pnk!(bcs::to_bytes(&hdr_i))
    };

    let mut reloaded = pnk!(bcs::from_bytes::<MapxRawVersioned>(&hdr));

    assert_eq!(cnt, reloaded.len());

    (0..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        assert_eq!(i.to_vec(), reloaded.get(&i).unwrap());
    });

    (1..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        *pnk!(reloaded.get_mut(&i)) = i.to_vec();
        assert_eq!(&reloaded.get(&i).unwrap(), &i);
        assert!(reloaded.contains_key(&i));
        assert!(reloaded.remove(&i).unwrap().is_some());
        assert!(!reloaded.contains_key(&i));
    });

    assert_eq!(1, reloaded.len());
    reloaded.clear();
    assert!(reloaded.is_empty());

    reloaded.version_create(b"test2").unwrap();

    reloaded.insert(vec![1], vec![1]).unwrap();
    reloaded.insert(vec![4], vec![4]).unwrap();
    reloaded.insert(vec![6], vec![6]).unwrap();
    reloaded.insert(vec![80], vec![80]).unwrap();

    assert!(reloaded.range(vec![]..vec![1]).next().is_none());
    assert_eq!(vec![4], reloaded.range(vec![2]..vec![10]).next().unwrap().1);

    assert_eq!(vec![80], reloaded.get_ge(&[79]).unwrap().1);
    assert_eq!(vec![80], reloaded.get_ge(&[80]).unwrap().1);
    assert_eq!(vec![80], reloaded.get_le(&[80]).unwrap().1);
    assert_eq!(vec![80], reloaded.get_le(&[100]).unwrap().1);
}

// create branch
//
#[test]
fn branch_verison_mgmt() {
    //
}

#[test]
fn prune_outdated_data() {
    //
}
