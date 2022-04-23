use super::*;

#[test]
fn basic_cases() {
    let map = MapxDkVs::new();
    pnk!(map.version_create(VersionName(b"")));

    assert!(pnk!(map.insert((1u8, 1u8), 9u8)).is_none());
    assert!(pnk!(map.insert((1, 2), 8)).is_none());
    assert!(pnk!(map.insert((1, 3), 7)).is_none());

    assert_eq!(map.get(&(&1, &1)).unwrap(), 9);
    assert_eq!(map.get(&(&1, &2)).unwrap(), 8);
    assert_eq!(map.get(&(&1, &3)).unwrap(), 7);

    // does not exist
    assert!(pnk!(map.remove(&(&1, Some(&4)))).is_none());

    assert!(pnk!(map.remove(&(&1, Some(&1)))).is_some());
    assert!(map.get(&(&1, &1)).is_none());

    // partial-path remove
    assert!(pnk!(map.remove(&(&1, None))).is_none()); // yes, is none
    assert!(map.get(&(&1, &2)).is_none());
    assert!(map.get(&(&1, &3)).is_none());

    map.entry_ref(&(&1, &99)).or_insert_ref(&100);
    assert_eq!(map.get(&(&1, &99)).unwrap(), 100);

    let mut cb = |k: (u8, u8), v: u8| -> Result<()> {
        assert_eq!(v, map.remove(&(&k.0, Some(&k.1))).unwrap().unwrap());
        Ok(())
    };

    pnk!(map.iter_op_with_key_prefix(&mut cb, &0));
    assert_eq!(map.get(&(&1, &99)).unwrap(), 100);

    assert!(map.iter_op_by_branch(BranchName(b"aaa"), &mut cb).is_err());
    assert_eq!(map.get(&(&1, &99)).unwrap(), 100);

    pnk!(map.iter_op(&mut cb));
    assert!(map.get(&(&1, &99)).is_none());
}
