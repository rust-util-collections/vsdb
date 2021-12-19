use super::*;
use crate::ValueEnDe;

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
                hdr_i.insert(&i, &b).unwrap();
                assert_eq!(&hdr_i.get(&i).unwrap()[..], &i);
                assert_eq!(&hdr_i.remove(&i).unwrap().unwrap()[..], &b);
                assert!(hdr_i.get(&i).is_none());
                assert!(hdr_i.insert(&i, &b).unwrap().is_none());
                assert!(hdr_i.insert(&i, &b).unwrap().is_some());
            });

        assert_eq!(cnt, hdr_i.len());

        <MapxRawVersioned as ValueEnDe>::encode(&hdr_i)
    };

    let mut reloaded = pnk!(<MapxRawVersioned as ValueEnDe>::decode(&hdr));

    assert_eq!(cnt, reloaded.len());

    (0..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        assert_eq!(i.to_vec().into_boxed_slice(), reloaded.get(&i).unwrap());
    });

    (1..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        *pnk!(reloaded.get_mut(&i)) = i.to_vec().into_boxed_slice();
        assert_eq!(&reloaded.get(&i).unwrap()[..], &i);
        assert!(reloaded.contains_key(&i));
        assert!(reloaded.remove(&i).unwrap().is_some());
        assert!(!reloaded.contains_key(&i));
    });

    assert_eq!(1, reloaded.len());
    reloaded.clear();
    assert!(reloaded.is_empty());

    reloaded.version_create(b"test2").unwrap();

    reloaded.insert(&[1], &[1]).unwrap();
    reloaded.insert(&[4], &[4]).unwrap();
    reloaded.insert(&[6], &[6]).unwrap();
    reloaded.insert(&[80], &[80]).unwrap();

    assert!(reloaded.range(&[][..]..&[1][..]).next().is_none());
    assert_eq!(
        &[4],
        &reloaded.range(&[2][..]..&[10][..]).next().unwrap().1[..]
    );

    assert_eq!(&[80], &reloaded.get_ge(&[79]).unwrap().1[..]);
    assert_eq!(&[80], &reloaded.get_ge(&[80]).unwrap().1[..]);
    assert_eq!(&[80], &reloaded.get_le(&[80]).unwrap().1[..]);
    assert_eq!(&[80], &reloaded.get_le(&[100]).unwrap().1[..]);
}

// # VCS(version control system) scene
//
// version:
// - can not write data before creating a version for the branch
//     - use existing version name will fail
// - newer version can read data created by older version
//     - assume they have not been re-writed by the newer branch
// - can not read data created by newer versions from an older version
// - can not remove a version except it is the HEAD version
// - data created by a version will disappear after the version has been removed
// - insert different values for a same key within one version will change the version sig
// - insert same values for a same key within one version will not change the version sig
//
// create branch:
// - use existing branch name will fail
// - new created branch has not any version
// - new created branch have the view of all its parent branches
// - new branch can only be created based on the HEAD of its parent
// - total branch number can not exceed 1024(maybe change in the future)
// - new created data by this branch can not be seen on other branches
//
// remove branch:
// - remove non-existing branch will fail
// - remove branch with children will fail
// - remove the initial branch will fail(branch name: "main")
// - versions and data directly created by this branch will not be deleted
//
// truncate branch:
// - a `pop` operation will always delete the HEAD version if it exists
// - versions after the guard version will be deleted in a stack-pop style
// - truncate will not affect any verion belong to other branches
//
// merge branch:
// - a branch can only be merged to its parent branch
// - every branch with a same parent can be merged to their parent branch
//     - all verisons will be ordered by the inner-defined version id
// - version sigs except the latest version will not be changed
//
// prune version:
// - versions(with all changes created directly by them) older than the guard version will deleted
// - non-changed value creatd by an old version will prevent that old version from being deleted
#[test]
#[allow(non_snake_case)]
fn VCS_operations() {
    let mut hdr = crate::MapxRawVersioned::new();

    // haven't create any version yet
    assert!(hdr.insert(b"key", b"value").is_err());

    hdr.version_create(b"001").unwrap();

    // use existing version name
    assert!(hdr.version_create(b"001").is_err());

    assert!(hdr.is_empty());

    hdr.version_create(b"002").unwrap();
    assert!(hdr.is_empty());
    assert!(hdr.is_empty_by_branch(b"main"));
    assert!(hdr.is_empty_by_branch_version(b"main", b"001"));
    assert!(hdr.is_empty_by_branch_version(b"main", b"002"));

    assert!(
        hdr.insert(b"version-002/key-01", b"version-002/value-01")
            .is_ok()
    );
    assert!(
        hdr.insert(b"version-002/key-02", b"version-002/value-02")
            .is_ok()
    );

    assert_eq!(
        hdr.get(b"version-002/key-01"),
        Some(b"version-002/value-01".to_vec().into_boxed_slice())
    );
    assert_eq!(
        hdr.get(b"version-002/key-02"),
        Some(b"version-002/value-02".to_vec().into_boxed_slice())
    );

    assert!(!hdr.is_empty());
    assert!(!hdr.is_empty_by_branch(b"main"));
    assert!(hdr.is_empty_by_branch_version(b"main", b"001"));
    assert!(!hdr.is_empty_by_branch_version(b"main", b"002"));
}
