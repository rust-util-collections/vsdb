//! The fixture contains the three v17 prefixes of four DAG nodes and the
//! unmodified root metadata. One child masks an inherited value.
use vsdb::{DagMapRaw, Namespace, VsdbOptions, vsdb_configure};
use vsdb_core::MapxRaw;
use vsdb_migrate::{LegacyRaw, dagmap::migrate_v17};

type Component = (Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>);
#[derive(serde::Deserialize)]
struct Fixture(
    Vec<u8>,
    Vec<Vec<u8>>,
    Vec<Vec<u8>>,
    [u64; 4],
    Vec<Component>,
);

#[test]
fn preserves_root_and_edges_and_reports_relocated_children() {
    let dir = std::env::temp_dir().join(format!("vsdb-dag-migration-{}", rand::random::<u128>()));
    vsdb_configure(VsdbOptions::new(dir)).unwrap();
    let Fixture(meta, root_tokens, a_tokens, ids, components) =
        postcard::from_bytes(include_bytes!("fixtures/dagmap-v17.postcard")).unwrap();
    let _issued = MapxRaw::new().instance_id();
    for (bytes, rows) in components {
        let LegacyRaw(mut store) = postcard::from_bytes(&bytes).unwrap();
        let mut batch = store.batch();
        for (k, v) in rows {
            batch.insert(k, v);
        }
        batch.commit().unwrap();
    }
    let ns = Namespace::default_ns();
    let root_id = vsdb::InstanceId::new(ids[0], ns.id());
    let path = ns.meta_path(root_id.map_id);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, meta).unwrap();
    assert!(DagMapRaw::from_meta(root_id).is_err());
    let (root, map) = migrate_v17(root_id).unwrap();
    assert_eq!(root.instance_id(), root_id);
    assert_eq!(root.child_ids(), root_tokens);
    let child = |idx: usize| {
        let id = vsdb::InstanceId::new(ids[idx], ns.id());
        let new = map.get(&id).copied().unwrap_or(id);
        DagMapRaw::from_meta(new).unwrap()
    };
    let a = child(1);
    let b = child(2);
    let c = child(3);
    assert_eq!(a.child_ids(), a_tokens);
    assert_eq!(root.child_id(&a), Some(root_tokens[0].clone()));
    assert_eq!(c.get("root"), None);
    assert_eq!(c.get("local"), Some(b"a".to_vec()));
    assert_eq!(b.get("root"), Some(b"v".to_vec()));
    assert_eq!(b.get("local"), Some(b"b".to_vec()));
    assert_eq!(migrate_v17(root_id).unwrap().1, map);
    let merged = c.prune().unwrap();
    assert_eq!(merged.instance_id(), root_id);
    assert_eq!(merged.get("local"), Some(b"a".to_vec()));
    assert!(b.is_dead());
}
