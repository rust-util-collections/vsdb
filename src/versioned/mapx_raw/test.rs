use super::*;
use crate::{
    common::{BranchName, ParentBranchName, VersionName, BRANCH_CNT_LIMIT},
    ValueEnDe,
};
use std::{sync::mpsc::channel, thread};

#[test]
fn basic_cases() {
    let cnt = 200;

    let hdr = {
        let mut hdr_i = MapxRawVs::new();
        hdr_i.version_create(VersionName(b"test")).unwrap();

        assert_eq!(0, hdr_i.len());
        (0..cnt).for_each(|i: usize| {
            assert!(hdr_i.get(&i.to_be_bytes()).is_none());
        });

        (0..cnt)
            .map(|i: usize| (i.to_be_bytes(), i.to_be_bytes()))
            .for_each(|(i, b)| {
                hdr_i.insert(&i, &b).unwrap();
                assert_eq!(&hdr_i.get(&i).unwrap()[..], &b);
                assert_eq!(&hdr_i.remove(&i).unwrap().unwrap()[..], &b);
                assert!(hdr_i.get(&i).is_none());
                assert!(hdr_i.insert(&i, &b).unwrap().is_none());
                assert!(hdr_i.insert(&i, &b).unwrap().is_some());
            });

        assert_eq!(cnt, hdr_i.len());

        <MapxRawVs as ValueEnDe>::encode(&hdr_i)
    };

    let mut reloaded = pnk!(<MapxRawVs as ValueEnDe>::decode(&hdr));

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

    reloaded.version_create(VersionName(b"test2")).unwrap();

    reloaded.insert(&[1], &[1]).unwrap();
    reloaded.insert(&[4], &[4]).unwrap();
    reloaded.insert(&[6], &[6]).unwrap();
    reloaded.insert(&[80], &[80]).unwrap();

    assert!(reloaded.range_ref(&[][..]..&[1][..]).next().is_none());
    assert_eq!(
        &[4],
        &reloaded.range_ref(&[2][..]..&[10][..]).next().unwrap().1[..]
    );

    assert_eq!(&[80], &reloaded.get_ge(&[79]).unwrap().1[..]);
    assert_eq!(&[80], &reloaded.get_ge(&[80]).unwrap().1[..]);
    assert_eq!(&[80], &reloaded.get_le(&[80]).unwrap().1[..]);
    assert_eq!(&[80], &reloaded.get_le(&[100]).unwrap().1[..]);
}

// # VCS(version control system) scene
#[test]
#[allow(non_snake_case)]
fn VCS_mgmt() {
    let mut hdr = MapxRawVs::new();
    version_operations(&mut hdr);
    branch_operations(&mut hdr);
    prune_operations(&mut hdr);
    default_branch(&mut hdr);
}

// version:
//
// - can not write data before creating a version for the branch
//     - use existing version name will fail
// - newer version can read data created by older version
//     - assume they have not been re-writed by the newer branch
// - can not read data created by newer versions from an older version
// - remove a non-existing key will sucess
// - data created by a version will disappear after the version has been removed
// - insert same values for a same key within one version will not change the version checksum
// - insert different values for a same key within one version will change the version checksum
// - can not remove a version except it is the HEAD version(no public functions privided)
// - can not remove a KV except its version is the HEAD version(no public functions privided)
// - can not write data to a version except it is the HEAD version(no public functions privided)
fn version_operations(hdr: &mut MapxRawVs) {
    // haven't create any version yet
    assert!(hdr.insert(b"key", b"value").is_err());

    hdr.version_create(VersionName(b"v-001")).unwrap();
    let checksum_v_001_initial = hdr.checksum_get().unwrap();

    // use existing version name
    assert!(hdr.version_create(VersionName(b"v-001")).is_err());
    assert_eq!(checksum_v_001_initial, hdr.checksum_get().unwrap());

    assert!(hdr.is_empty());

    hdr.version_create(VersionName(b"v-002")).unwrap();

    let checksum_v_002 = hdr.checksum_get().unwrap();
    assert!(checksum_v_001_initial != checksum_v_002);
    assert_eq!(
        checksum_v_001_initial,
        hdr.checksum_get_by_branch_version(BranchName(b"main"), VersionName(b"v-001"))
            .unwrap()
    );

    assert!(hdr.is_empty());
    assert!(hdr.is_empty_by_branch(BranchName(b"main")));
    assert!(hdr.is_empty_by_branch_version(BranchName(b"main"), VersionName(b"v-001")));
    assert!(hdr.is_empty_by_branch_version(BranchName(b"main"), VersionName(b"v-002")));

    assert!(hdr.insert(b"v-002/key-01", b"v-002/value-01").is_ok());
    let checksum_v_002_01 = hdr.checksum_get().unwrap();
    assert!(checksum_v_002 != checksum_v_002_01);

    assert!(hdr.insert(b"v-002/key-02", b"v-002/value-02").is_ok());
    let checksum_v_002_02 = hdr.checksum_get().unwrap();
    assert!(checksum_v_002_01 != checksum_v_002_02);

    // insert a same key-value will not change the checksum value
    assert!(hdr.insert(b"v-002/key-02", b"v-002/value-02").is_ok());
    assert_eq!(checksum_v_002_02, hdr.checksum_get().unwrap());

    assert_eq!(
        hdr.get(b"v-002/key-01"),
        Some(b"v-002/value-01".to_vec().into_boxed_slice())
    );
    assert!(
        hdr.get_by_branch(b"v-002/key-01", BranchName(b"fake branch"))
            .is_none()
    );
    assert!(
        hdr.get_by_branch_version(
            b"v-002/key-01",
            BranchName(b"main"),
            VersionName(b"v-001")
        )
        .is_none()
    );
    assert_eq!(
        hdr.get(b"v-002/key-02"),
        Some(b"v-002/value-02".to_vec().into_boxed_slice())
    );
    assert!(
        hdr.get_by_branch(b"v-002/key-02", BranchName(b"fake branch"))
            .is_none()
    );
    assert!(
        hdr.get_by_branch_version(
            b"v-002/key-02",
            BranchName(b"main"),
            VersionName(b"v-001")
        )
        .is_none()
    );

    assert!(!hdr.is_empty());
    assert!(!hdr.is_empty_by_branch(BranchName(b"main")));
    assert!(hdr.is_empty_by_branch_version(BranchName(b"main"), VersionName(b"v-001")));
    assert!(!hdr.is_empty_by_branch_version(BranchName(b"main"), VersionName(b"v-002")));

    hdr.version_create(VersionName(b"v-003")).unwrap();
    assert!(hdr.insert(b"v-003/key-01", b"v-003/value-01").is_ok());
    assert_eq!(&hdr.get(b"v-003/key-01").unwrap()[..], b"v-003/value-01");

    assert_eq!(hdr.remove(b"fake key").unwrap(), None);

    assert_eq!(
        &hdr.remove(b"v-003/key-01").unwrap().unwrap()[..],
        b"v-003/value-01"
    );
    assert!(hdr.get(b"v-003/key-01").is_none());
    assert!(
        hdr.get_by_branch_version(
            b"v-003/key-01",
            BranchName(b"main"),
            VersionName(b"v-003")
        )
        .is_none()
    );

    assert_eq!(
        &pnk!(hdr.remove(b"v-002/key-01")).unwrap()[..],
        b"v-002/value-01"
    );
    assert!(hdr.get(b"v-002/key-01").is_none());
    // still available in a old version
    assert_eq!(
        &hdr.get_by_branch_version(
            b"v-002/key-01",
            BranchName(b"main"),
            VersionName(b"v-002")
        )
        .unwrap()[..],
        b"v-002/value-01"
    );

    assert!(!hdr.is_empty());
    assert!(!hdr.is_empty_by_branch(BranchName(b"main")));
    assert!(hdr.is_empty_by_branch_version(BranchName(b"main"), VersionName(b"v-001")));
    assert!(!hdr.is_empty_by_branch_version(BranchName(b"main"), VersionName(b"v-002")));
    assert!(!hdr.is_empty_by_branch_version(BranchName(b"main"), VersionName(b"v-003")));

    assert!(hdr.insert(b"v-003/key-02", b"v-003/value-02").is_ok());
    assert!(
        hdr.get_by_branch_version(
            b"v-003/key-02",
            BranchName(b"main"),
            VersionName(b"v-001")
        )
        .is_none()
    );
    assert!(
        hdr.get_by_branch_version(
            b"v-003/key-02",
            BranchName(b"main"),
            VersionName(b"v-002")
        )
        .is_none()
    );
    assert_eq!(
        &hdr.get_by_branch_version(
            b"v-003/key-02",
            BranchName(b"main"),
            VersionName(b"v-003")
        )
        .unwrap()[..],
        b"v-003/value-02"
    );
    assert_eq!(&hdr.get(b"v-003/key-02").unwrap()[..], b"v-003/value-02");

    assert_eq!(
        &hdr.get_by_branch_version(
            b"v-002/key-02",
            BranchName(b"main"),
            VersionName(b"v-003")
        )
        .unwrap()[..],
        b"v-002/value-02"
    );

    // update a existing key
    assert!(hdr.insert(b"v-002/key-02", b"v-002/value-03").is_ok());
    // get the new view of this value
    assert_eq!(
        &hdr.get_by_branch_version(
            b"v-002/key-02",
            BranchName(b"main"),
            VersionName(b"v-003")
        )
        .unwrap()[..],
        b"v-002/value-03"
    );

    assert!(hdr.version_pop().is_ok());
    assert!(
        hdr.get_by_branch_version(
            b"v-003/key-02",
            BranchName(b"main"),
            VersionName(b"v-003")
        )
        .is_none()
    );

    // the head version is v-002 after the `pop`, so will get the v2 value
    assert_eq!(&hdr.get(b"v-002/key-02").unwrap()[..], b"v-002/value-02");
    assert_eq!(
        &hdr.get_by_branch_version(
            b"v-002/key-02",
            BranchName(b"main"),
            VersionName(b"v-002")
        )
        .unwrap()[..],
        b"v-002/value-02"
    );
}

// create branch:
//
// - use existing branch name will fail
// - new created branch have the view of all its parent branches
// - new created branch has not any version, so insert will fail before create a new one
// - new branch can only be created based on the HEAD of its parent(no public functions privided)
// - write data to non-existing branch will fail
// - new created data by this branch can not be seen on other branches
// - total branch number can not exceed 1024(maybe change in the future)
//
// remove branch:
//
// - remove non-existing branch will fail
// - remove branch with children will fail
// - remove the initial branch will fail(branch name: "main")
// - versions and data directly created by this branch will not be deleted
//
// truncate branch:
//
// - versions after the guard version will be deleted in a stack-pop style
// - truncate will not affect any verion belong to other branches
//
// merge branch:
//
// - a branch can only be merged to its parent branch
// - every branch with a same parent can be merged to their parent branch
//     - all verisons will be ordered by the inner-defined version id
// - version checksums except the latest version will not be changed
fn branch_operations(hdr: &mut MapxRawVs) {
    hdr.branch_create(BranchName(b"b-1")).unwrap();
    hdr.branch_create(BranchName(b"b-2")).unwrap();

    assert!(hdr.branch_create(BranchName(b"main")).is_err());
    assert!(hdr.branch_create(BranchName(b"b-1")).is_err());
    assert!(hdr.branch_create(BranchName(b"b-2")).is_err());

    assert_eq!(
        &hdr.get_by_branch(b"v-002/key-02", BranchName(b"b-1"))
            .unwrap()[..],
        b"v-002/value-02"
    );
    assert_eq!(
        &hdr.get_by_branch(b"v-002/key-02", BranchName(b"b-2"))
            .unwrap()[..],
        b"v-002/value-02"
    );

    // no version on the new branch
    assert!(
        hdr.insert_by_branch(b"v-001/key-01", b"v-001/value-01", BranchName(b"b-1"))
            .is_err()
    );

    // Version ID can not be repeated within the branch view, but can be repeated globally.
    //
    // Although "v-001" is not unique globally("v-001" exists on "main" branch),
    // but it is unique in the "b-1" branch, so we can use it.
    assert!(
        hdr.version_create_by_branch(VersionName(b"v-001"), BranchName(b"b-1"))
            .is_ok()
    );

    hdr.version_create_by_branch(VersionName(b"v-004"), BranchName(b"b-1"))
        .unwrap();
    assert!(
        hdr.insert_by_branch(b"v-001/key-01", b"v-001/value-01", BranchName(b"b-1"))
            .is_ok()
    );

    // multi-layers view
    hdr.branch_create_by_base_branch(BranchName(b"b-1-child"), ParentBranchName(b"b-1"))
        .unwrap();
    assert_eq!(
        &hdr.get_by_branch(b"v-002/key-02", BranchName(b"b-1-child"))
            .unwrap()[..],
        b"v-002/value-02"
    );
    assert_eq!(
        &hdr.get_by_branch(b"v-001/key-01", BranchName(b"b-1"))
            .unwrap()[..],
        b"v-001/value-01"
    );

    // insert to a non-existing branch
    assert!(
        hdr.insert_by_branch(b"k", b"v", BranchName(b"fake branch"))
            .is_err()
    );

    // try go get versions either on self nor on parents
    assert!(
        hdr.get_by_branch_version(
            b"v-001/key-01",
            BranchName(b"b-2"),
            VersionName(b"v-004")
        )
        .is_none()
    );

    // branch number limits
    (4..BRANCH_CNT_LIMIT).for_each(|i| {
        assert!(hdr.branch_create(BranchName(&i.to_be_bytes())).is_ok());
    });
    assert!(
        hdr.branch_create(BranchName(&1usize.to_be_bytes()))
            .is_err()
    );
    (4..BRANCH_CNT_LIMIT).for_each(|i| {
        assert!(hdr.branch_remove(BranchName(&i.to_be_bytes())).is_ok());
    });
    assert!(hdr.branch_create(BranchName(&1usize.to_be_bytes())).is_ok());
    assert!(hdr.branch_remove(BranchName(&1usize.to_be_bytes())).is_ok());

    // not exist
    assert!(hdr.branch_remove(BranchName(b"fake branch")).is_err());
    // has children
    assert!(hdr.branch_remove(BranchName(b"b-1")).is_err());
    // initial branch is not allowed to be removed
    assert!(hdr.branch_remove(BranchName(b"main")).is_err());
    assert!(hdr.branch_remove(BranchName(b"main")).is_err());

    // remove its children
    assert!(hdr.branch_remove(BranchName(b"b-1-child")).is_ok());
    // not it can be removed
    assert!(hdr.branch_remove(BranchName(b"b-1")).is_ok());
    assert!(
        hdr.get_by_branch(b"v-001/key-01", BranchName(b"b-1"))
            .is_none()
    );

    // create some versions to be truncated
    (0..100u64).for_each(|i| {
        hdr.version_create_by_branch(VersionName(&i.to_be_bytes()), BranchName(b"b-2"))
            .unwrap();
        assert!(
            hdr.insert_by_branch(&i.to_be_bytes(), &i.to_be_bytes(), BranchName(b"b-2"))
                .is_ok()
        );
    });

    // verion names already exist
    (0..100u64).for_each(|i| {
        assert!(
            hdr.version_create_by_branch(
                VersionName(&i.to_be_bytes()),
                BranchName(b"b-2")
            )
            .is_err()
        );
    });

    // all values are correct
    (0..100u64).for_each(|i| {
        assert_eq!(
            &hdr.get_by_branch_version(
                &i.to_be_bytes(),
                BranchName(b"b-2"),
                VersionName(&i.to_be_bytes())
            )
            .unwrap()[..],
            &i.to_be_bytes()
        );
    });

    // clear all version on "b-2" branch
    hdr.branch_truncate(BranchName(b"b-2")).unwrap();

    // now we can use these version names again
    (11..=100u64).for_each(|i| {
        assert!(
            hdr.version_create_by_branch(
                VersionName(&i.to_be_bytes()),
                BranchName(b"b-2")
            )
            .is_ok()
        );
    });

    // get very old value after passing through many verions
    assert_eq!(
        &hdr.get_by_branch_version(
            b"v-002/key-02",
            BranchName(b"b-2"),
            VersionName(&100u64.to_be_bytes())
        )
        .unwrap()[..],
        b"v-002/value-02"
    );

    // ensure the view of main branch is not affected
    assert_eq!(&hdr.get(b"v-002/key-02").unwrap()[..], b"v-002/value-02");
    assert_eq!(
        &hdr.get_by_branch(b"v-002/key-02", BranchName(b"main"))
            .unwrap()[..],
        b"v-002/value-02"
    );

    // keep one version to support other branches forked from it
    hdr.branch_truncate_to(BranchName(b"b-2"), VersionName(&11u64.to_be_bytes()))
        .unwrap();

    // created data to be merged
    let mut checksums = vec![];
    (0..10u64).for_each(|i| {
        hdr.branch_create_by_base_branch(
            BranchName(&i.to_be_bytes()),
            ParentBranchName(b"b-2"),
        )
        .unwrap();
        (1000..1010u64).for_each(|j| {
            hdr.version_create_by_branch(
                VersionName(&((1 + i) * j).to_be_bytes()),
                BranchName(&i.to_be_bytes()),
            )
            .unwrap();
            assert!(
                hdr.insert_by_branch(
                    &((1 + i) * j).to_be_bytes(),
                    &((1 + i) * j).to_be_bytes(),
                    BranchName(&i.to_be_bytes())
                )
                .is_ok()
            );
            checksums.push(
                hdr.checksum_get_by_branch(BranchName(&i.to_be_bytes()))
                    .unwrap(),
            );
        });
    });

    (0..10u64).for_each(|i| {
        assert!(
            hdr.branch_merge_to_parent(BranchName(&i.to_be_bytes()))
                .is_ok()
        );
    });

    // All versions and their chanegs are belong to the base branch now
    let mut checksums_after_merge = vec![];
    (0..10u64).for_each(|i| {
        (1000..1010u64).for_each(|j| {
            // children branches have been removed
            assert!(
                hdr.get_by_branch_version(
                    &((1 + i) * j).to_be_bytes(),
                    BranchName(&i.to_be_bytes()),
                    VersionName(&((1 + i) * j).to_be_bytes())
                )
                .is_none()
            );
            // all changes have been move to the parent branch
            assert_eq!(
                &hdr.get_by_branch_version(
                    &((1 + i) * j).to_be_bytes(),
                    BranchName(b"b-2"),
                    VersionName(&((1 + i) * j).to_be_bytes())
                )
                .unwrap()[..],
                &((1 + i) * j).to_be_bytes()
            );
            checksums_after_merge.push(
                hdr.checksum_get_by_branch_version(
                    BranchName(b"b-2"),
                    VersionName(&((1 + i) * j).to_be_bytes()),
                )
                .unwrap(),
            );
        });
    });

    // the checksum of the latest version will be changed,
    // all other versions will keep their original checksum
    let latest_checksum = *checksums_after_merge.last().unwrap();
    assert_eq!(checksums.len(), checksums_after_merge.len());
    // merged 10 times
    assert_eq!(
        10,
        checksums
            .iter()
            .zip(checksums_after_merge.iter())
            .filter(|(a, b)| a != b)
            .count()
    );
    assert!(checksums.binary_search(&latest_checksum).is_err());
}

// prune version:
//
// - versions(with all changes created directly by them) older than the guard version will deleted
// - non-changed value creatd by an old version will prevent that old version from being deleted
fn prune_operations(hdr: &mut MapxRawVs) {
    // prune "nain",
    // have not enought versions, nothing to be pruned
    hdr.prune(None).unwrap();

    assert_eq!(&hdr.get(b"v-002/key-02").unwrap()[..], b"v-002/value-02");
    assert_eq!(
        &hdr.get_by_branch(b"v-002/key-02", BranchName(b"main"))
            .unwrap()[..],
        b"v-002/value-02"
    );

    // add a version for all data
    (0..10u64).for_each(|i| {
        (1000..1010u64).for_each(|j| {
            hdr.version_create_by_branch(
                VersionName(&((1 + i) * j * 1000).to_be_bytes()),
                BranchName(b"b-2"),
            )
            .unwrap();
            assert!(
                hdr.insert_by_branch(
                    &((1 + i) * j).to_be_bytes(),
                    &[0],
                    BranchName(b"b-2")
                )
                .is_ok()
            );
        });
    });

    // only keep one version, so older data should be clear
    hdr.prune_by_branch(BranchName(b"b-2"), Some(1)).unwrap();

    // this key has only one version of value, so it will not be removed
    assert_eq!(&hdr.get(b"v-002/key-02").unwrap()[..], b"v-002/value-02");
    assert_eq!(
        &hdr.get_by_branch(b"v-002/key-02", BranchName(b"b-2"))
            .unwrap()[..],
        b"v-002/value-02"
    );

    (0..10u64).for_each(|i| {
        (1000..1010u64).for_each(|j| {
            // old version view does not exist any more
            assert!(
                hdr.get_by_branch_version(
                    &((1 + i) * j).to_be_bytes(),
                    BranchName(b"b-2"),
                    VersionName(&((1 + i) * j).to_be_bytes())
                )
                .is_none()
            );
            // the latest value exist
            assert_eq!(
                &hdr.get_by_branch(&((1 + i) * j).to_be_bytes(), BranchName(b"b-2"),)
                    .unwrap()[..],
                &[0]
            );
        });
    });
}

fn default_branch(hdr: &mut MapxRawVs) {
    hdr.branch_create(BranchName(b"fork")).unwrap();

    hdr.branch_set_default(BranchName(b"fork")).unwrap();
    hdr.version_create(VersionName(b"ver-on-fork")).unwrap();
    hdr.insert(b"key", b"value").unwrap();
    assert_eq!(&hdr.get(b"key").unwrap()[..], b"value");
    assert!(hdr.get_by_branch(b"key", BranchName(b"main")).is_none());

    hdr.branch_set_default(BranchName(b"main")).unwrap();
    assert!(hdr.get(b"key").is_none());
    assert_eq!(
        &hdr.get_by_branch(b"key", BranchName(b"fork")).unwrap()[..],
        b"value"
    );

    let (s, r) = channel();
    for i in (u64::MAX - 10)..u64::MAX {
        let ss = s.clone();
        let mut h = hdr.clone();
        thread::spawn(move || {
            pnk!(h.branch_create(BranchName(&i.to_be_bytes())));
            pnk!(h.branch_set_default(BranchName(&i.to_be_bytes())));
            pnk!(h.version_create(VersionName(b"ver-on-fork")));
            pnk!(h.insert(b"key", &i.to_be_bytes()));
            ss.send("done").unwrap();
        });
    }

    for _ in 0..10 {
        r.recv().unwrap();
    }

    for i in (u64::MAX - 10)..u64::MAX {
        assert_eq!(
            &hdr.get_by_branch_version(
                b"key",
                BranchName(&i.to_be_bytes()),
                VersionName(b"ver-on-fork")
            )
            .unwrap()[..],
            &i.to_be_bytes()
        );
    }
}
