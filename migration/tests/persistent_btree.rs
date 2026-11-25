//! Standalone v17 tree with two application-owned roots (1 and 2).
use std::fs;
use vsdb::{PersistentBTree, VsdbOptions, vsdb_configure};
use vsdb_migrate::{
    migrate_universe,
    versioned::{migrate_tree_handle_v17, migrate_tree_v17},
};

#[test]
fn preserves_the_primary_prefix_and_maps_external_roots() {
    let scratch =
        std::env::temp_dir().join(format!("vsdb-tree-migration-{}", rand::random::<u64>()));
    fs::create_dir(&scratch).unwrap();
    let files: Vec<(&str, &[u8])> =
        postcard::from_bytes(include_bytes!("fixtures/tree-v17.postcard")).unwrap();
    for (name, contents) in files {
        let path = scratch.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, contents).unwrap();
    }
    migrate_universe(&scratch).unwrap();
    vsdb_configure(VsdbOptions::new(&scratch)).unwrap();
    const ID: u64 = 40_960_000;
    let path = vsdb::Namespace::default_ns().meta_path(ID);
    let old = fs::read(&path).unwrap();
    let mut raw = vsdb_migrate::raw_map(ID.into()).unwrap();
    // Seed a v17 8-byte leaf at ID 16 ([0x10, 0..0]) plus stale 9-byte
    // pre-publication objects above the final ceiling, and a stray [0] row.
    let mut leaf16 = vec![0u8];
    leaf16.extend_from_slice(&1u32.to_le_bytes());
    leaf16.extend_from_slice(&1u32.to_le_bytes());
    leaf16.push(b'z');
    leaf16.extend_from_slice(&2u32.to_le_bytes());
    leaf16.extend_from_slice(b"16");
    raw.insert(16u64.to_le_bytes(), &leaf16);
    let mut stale_key = [0x12u8; 9];
    stale_key[1..].copy_from_slice(&((3u64 << 62) | 999).to_be_bytes());
    raw.insert(stale_key, vec![0xab; 2048]);
    raw.insert([0], [18u8; 41]);
    assert!(migrate_tree_v17(ID, &[1, 2, 16]).is_err());
    // Source 8-byte pages remain intact after pre-publication rejection.
    assert!(raw.get(1u64.to_le_bytes()).is_some());
    assert!(raw.get(16u64.to_le_bytes()).is_some());
    raw.remove([0]);

    assert!(PersistentBTree::from_meta(ID).is_err());
    assert!(migrate_tree_v17(ID, &[3]).is_err());
    // Re-seed stale 9-byte object at serial 999 to simulate an aborted >16 MiB
    // run before .v17-roots was written, then retry with the smaller root set.
    raw.insert(stale_key, vec![0xab; 2048]);
    let (tree, roots) = migrate_tree_v17(ID, &[1, 2, 16]).unwrap();
    assert_eq!(tree.instance_id().map_id, ID);
    assert_eq!(tree.get(roots[0], b"a"), Some(b"first".to_vec()));
    assert_eq!(tree.get(roots[0], b"b"), None);
    assert_eq!(tree.get(roots[1], b"b"), Some(vec![2; 1536]));
    assert_eq!(tree.get(roots[2], b"z"), Some(b"16".to_vec()));
    drop(tree);
    fs::write(path, &old).unwrap();
    // The resume journal retains all owners when a caller retries with a subset.
    let (resumed, r) = migrate_tree_v17(ID, &[2]).unwrap();
    assert_eq!(r, vec![roots[1]]);
    assert_eq!(resumed.get(roots[0], b"a"), Some(b"first".to_vec()));
    drop(resumed);
    let (reopened, remapped) = migrate_tree_v17(ID, &[2, 1, 16]).unwrap();
    assert_eq!(remapped, vec![roots[1], roots[0], roots[2]]);
    assert_eq!(reopened.get(remapped[0], b"a"), Some(b"first".to_vec()));
    assert_eq!(reopened.get(remapped[2], b"z"), Some(b"16".to_vec()));

    // Byte-slice tree handle conversion in a fresh namespace without __instance_meta__.
    let ns = vsdb::Namespace::create().unwrap();
    let mut pool = vsdb::MapxRaw::new_in(&ns);
    pool.insert(1u64.to_le_bytes(), &leaf16);
    let mut raw_v17 = b"VSMAPX01".to_vec();
    raw_v17.extend_from_slice(&pool.instance_id().map_id.to_le_bytes());
    raw_v17.extend_from_slice(&ns.id().to_le_bytes());
    let handle_v17 = postcard::to_allocvec(&(raw_v17, 2u64)).unwrap();
    let (handle_v18, mapped_roots) = migrate_tree_handle_v17(&handle_v17, &[1]).unwrap();
    assert!(!ns.meta_path(pool.instance_id().map_id).exists());
    assert_eq!(
        migrate_tree_handle_v17(&handle_v18, &[1]).unwrap(),
        (handle_v18.clone(), mapped_roots.clone())
    );
    let decoded_tree: PersistentBTree = postcard::from_bytes(&handle_v18).unwrap();
    assert_eq!(
        decoded_tree.get(mapped_roots[0], b"z"),
        Some(b"16".to_vec())
    );
    drop(reopened);
    drop(decoded_tree);
    fs::remove_dir_all(scratch).unwrap();
}
