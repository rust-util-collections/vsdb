use super::*;

#[test]
fn generic_mk_ops() {
    let map = MapxRawMk::new(4);

    // key size mismatch
    assert!(map.insert(&[&[1]], &[]).is_err());

    assert!(map.insert(&[&[1], &[2], &[3], &[4]], &[9]).is_ok());
    assert!(map.insert(&[&[1], &[2], &[3], &[40]], &[8]).is_ok());
    assert!(map.insert(&[&[1], &[2], &[30], &[40]], &[7]).is_ok());
    assert!(map.insert(&[&[1], &[2], &[30], &[41]], &[6]).is_ok());

    assert_eq!(map.get(&[&[1], &[2], &[3], &[4]]).unwrap().as_ref(), &[9]);
    assert_eq!(map.get(&[&[1], &[2], &[3], &[40]]).unwrap().as_ref(), &[8]);
    assert_eq!(map.get(&[&[1], &[2], &[30], &[40]]).unwrap().as_ref(), &[7]);
    assert_eq!(map.get(&[&[1], &[2], &[30], &[41]]).unwrap().as_ref(), &[6]);

    // key size mismatch
    assert!(map.get(&[&[1], &[2], &[3]]).is_none());
    assert!(map.get(&[&[1], &[2]]).is_none());
    assert!(map.get(&[&[1]]).is_none());
    assert!(map.get(&[]).is_none());

    // does not exist
    assert!(map.remove(&[&[1], &[2], &[3], &[200]]).unwrap().is_none());

    assert!(map.remove(&[&[1], &[2], &[3], &[40]]).unwrap().is_some());
    assert!(map.get(&[&[1], &[2], &[3], &[40]]).is_none());

    // partial-path remove
    assert!(map.remove(&[&[1], &[2], &[30]]).unwrap().is_none()); // yes, is none
    assert!(map.get(&[&[1], &[2], &[30], &[40]]).is_none());
    assert!(map.get(&[&[1], &[2], &[30], &[41]]).is_none());

    // nothing will be removed by an empty key
    assert!(map.remove(&[]).unwrap().is_none());

    assert!(map.get(&[&[1], &[2], &[3], &[4]]).is_some());
    assert!(map.remove(&[&[1]]).unwrap().is_none()); // yes, is none
    assert!(map.get(&[&[1], &[2], &[3], &[4]]).is_none());

    assert!(map.entry_ref(&[]).or_insert_ref(&[]).is_err());
    assert!(
        map.entry_ref(&[&[11], &[12], &[13], &[14]])
            .or_insert_ref(&[])
            .is_ok()
    );
    assert_eq!(
        map.get(&[&[11], &[12], &[13], &[14]]).unwrap().as_ref(),
        &[]
    );
}
