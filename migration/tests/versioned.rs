//! Fixture produced by the v17.0.21 engine at da9a325. This binary has
//! its own universe, including the original allocator ceiling and WAL.
use std::{fs, path::Path};
use vsdb::{BranchId, CommitId, VerMap, VsdbOptions, vsdb_configure};
use vsdb_migrate::{migrate_universe, versioned::migrate_v17};

#[test]
fn converts_retained_versions_and_working_root_at_original_id() {
    let scratch =
        std::env::temp_dir().join(format!("vsdb-version-migration-{}", rand::random::<u64>()));
    fs::create_dir(&scratch).unwrap();
    let files: Vec<(&str, &[u8])> =
        postcard::from_bytes(include_bytes!("fixtures/vermap-v17.postcard")).unwrap();
    for (name, contents) in files {
        let path = scratch.join(Path::new(name));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, contents).unwrap();
    }
    migrate_universe(&scratch).unwrap();
    vsdb_configure(VsdbOptions::new(&scratch)).unwrap();
    const ID: u64 = 40_960_000;
    let meta_path = vsdb::Namespace::default_ns().meta_path(ID);
    let old_metadata = fs::read(&meta_path).unwrap();
    let components: Vec<_> = old_metadata
        .windows(16)
        .filter(|b| &b[..8] == b"VSMAPX01")
        .map(|b| u64::from_le_bytes(b[8..].try_into().unwrap()))
        .collect();
    assert_eq!(components.len(), 8);
    assert!(VerMap::<u64, Vec<u8>>::from_meta(ID).is_err());
    assert!(migrate_v17::<u64, String>(ID).is_err());
    // Seed a stale pre-publication 9-byte blob above the target ceiling and a
    // damaged next_commit == 0 on a temporary commit-less copy test first:
    // verify stale 9-byte objects are cleared and damaged headers fail before
    // deleting 8-byte source pages.
    let mut store = vsdb_migrate::raw_map(ID.into()).unwrap();
    let mut stale_blob = [0x12u8; 9];
    stale_blob[1..].copy_from_slice(&((3u64 << 62) | 5000).to_be_bytes());
    store.insert(stale_blob, vec![0xee; 1500]);

    let mut next_commit_raw = vsdb_migrate::raw_map(components[4].into()).unwrap();
    let saved_next_commit = next_commit_raw.get([]).unwrap();
    next_commit_raw.insert([], postcard::to_allocvec(&0u64).unwrap());
    assert!(migrate_v17::<u64, Vec<u8>>(ID).is_err());
    assert!(store.get([0]).is_none());
    assert!(store.get(1u64.to_le_bytes()).is_some());
    next_commit_raw.insert([], saved_next_commit);

    let map = migrate_v17::<u64, Vec<u8>>(ID).unwrap();
    for &id in &components[1..] {
        assert!(
            vsdb_migrate::raw_map(id.into())
                .unwrap()
                .iter()
                .next()
                .is_none()
        );
    }
    // Resume with old metadata still present after the durable switch and
    // retirement of all component prefixes.
    drop(map);
    fs::write(&meta_path, &old_metadata).unwrap();
    let mut map = migrate_v17::<u64, Vec<u8>>(ID).unwrap();
    let main = map.main_branch();
    let side = map.branch_id("side").unwrap();
    let base = CommitId::from_raw(1);
    let tip = CommitId::from_raw(2);
    assert_eq!(map.instance_id().map_id, ID);
    assert_eq!(main, BranchId::from_raw(1));
    assert_eq!(side, BranchId::from_raw(2));
    assert_eq!(map.get_commit(tip).unwrap().parents(), [base]);
    assert_eq!(map.get(main, &0).unwrap(), Some(vec![0, 1]));
    assert_eq!(map.at(base).unwrap().get(&0), Some(vec![0; 3]));
    assert_eq!(map.at(base).unwrap().get(&17), Some(vec![17; 2048]));
    assert_eq!(map.get(side, &42).unwrap(), Some(vec![42]));
    assert_eq!(map.get(side, &1).unwrap(), None);
    map.discard(main).unwrap();
    map.merge(side, main).unwrap();
    map.delete_branch(side).unwrap();
    map.flush().unwrap();
    drop(map);
    let again = migrate_v17::<u64, Vec<u8>>(ID).unwrap();
    assert_eq!(again.get(main, &42).unwrap(), Some(vec![42]));
    fs::remove_dir_all(&scratch).unwrap();
}
