//! A v17 typed root could contain saved children with a different value codec.
use vsdb::{DagMapRawKey, InstanceId, MapxRaw, Namespace, VsdbOptions, vsdb_configure};
use vsdb_migrate::{LegacyRaw, dagmap::migrate_rawkey_v17};
type Rows = Vec<(Vec<u8>, Vec<u8>)>;
#[derive(serde::Deserialize)]
struct Fixture(Vec<u8>, Vec<u8>, [u64; 2], Vec<(Vec<u8>, Rows)>);

#[test]
fn migrates_mixed_saved_types() {
    vsdb_configure(VsdbOptions::new(
        std::env::temp_dir().join(format!("vsdb-dag-typed-{}", rand::random::<u128>())),
    ))
    .unwrap();
    let Fixture(root_meta, child_meta, [r, c], parts) =
        postcard::from_bytes(include_bytes!("fixtures/dag-mixed-v17.postcard")).unwrap();
    let _issued = MapxRaw::new().instance_id();
    let ns = Namespace::default_ns();
    for (meta, rows) in parts {
        let LegacyRaw(mut map) = postcard::from_bytes(&meta).unwrap();
        for (k, v) in rows {
            map.insert(k, v);
        }
    }
    for (id, data) in [(r, root_meta), (c, child_meta)] {
        let path = ns.meta_path(id);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, data).unwrap();
    }
    let (root, remap) = migrate_rawkey_v17::<String>(r).unwrap();
    assert_eq!(root.get("shared").as_deref(), Some("base"));
    let id = InstanceId::new(c, ns.id());
    let id = *remap.get(&id).unwrap_or(&id);
    let child = DagMapRawKey::<Vec<u8>>::from_meta(id).unwrap();
    assert_eq!(child.get("shared").unwrap(), b"base");
    assert_eq!(child.get("key").unwrap(), [5, 7, 8]);
    assert!(DagMapRawKey::<String>::from_meta(id).is_err());
}
