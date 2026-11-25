//! Standalone v17 tree with two application-owned roots (1 and 2).
use std::fs;
use vsdb::{PersistentBTree, VsdbOptions, vsdb_configure};
use vsdb_migrate::{migrate_universe, versioned::migrate_tree_v17};

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
    assert!(PersistentBTree::from_meta(ID).is_err());
    assert!(migrate_tree_v17(ID, &[3]).is_err());
    let (tree, roots) = migrate_tree_v17(ID, &[1, 2]).unwrap();
    assert_eq!(tree.instance_id().map_id, ID);
    assert_eq!(tree.get(roots[0], b"a"), Some(b"first".to_vec()));
    assert_eq!(tree.get(roots[0], b"b"), None);
    assert_eq!(tree.get(roots[1], b"b"), Some(vec![2; 1536]));
    drop(tree);
    fs::write(path, &old).unwrap();
    // The resume journal retains both owners when a caller retries with a subset.
    let (resumed, r) = migrate_tree_v17(ID, &[2]).unwrap();
    assert_eq!(r, vec![roots[1]]);
    assert_eq!(resumed.get(roots[0], b"a"), Some(b"first".to_vec()));
    drop(resumed);
    let (reopened, remapped) = migrate_tree_v17(ID, &[2, 1]).unwrap();
    assert_eq!(remapped, vec![roots[1], roots[0]]);
    assert_eq!(reopened.get(remapped[0], b"a"), Some(b"first".to_vec()));
    fs::remove_dir_all(scratch).unwrap();
}
