//! The fixture contains the three v17 prefixes of four DAG nodes and the
//! unmodified root metadata. One child masks an inherited value.
#[path = "common/scratch.rs"]
mod scratch;

use vsdb::{DagMapRaw, Namespace, VsdbOptions, vsdb_configure};
use vsdb_core::MapxRaw;
use vsdb_migrate::{
    LegacyRaw,
    dagmap::{migrate_handle_v17, migrate_v17},
};

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
    let dir = scratch::database_dir();
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
    // The seeding thread owns the first allocation window. Planning on a new
    // thread needs a durable refill for its first private prefix.
    let prefix_ceiling = ns.system_dir().join("__prefix_ceiling__");
    let saved_ceiling = prefix_ceiling.with_extension("saved");
    std::fs::rename(&prefix_ceiling, &saved_ceiling).unwrap();
    std::fs::create_dir(&prefix_ceiling).unwrap();
    let outcome = std::thread::spawn(move || migrate_v17(root_id).map(|_| ())).join();
    std::fs::remove_dir(&prefix_ceiling).unwrap();
    std::fs::rename(saved_ceiling, prefix_ceiling).unwrap();
    assert!(matches!(outcome.unwrap(), Err(vsdb::VsdbError::Io(_))));
    assert!(
        !ns.meta_path(root_id.map_id)
            .with_extension("dag-v17")
            .exists()
    );
    let ceiling_path = vsdb_core::vsdb_get_system_dir().join("dag_id_ceiling");
    assert!(migrate_v17(root_id).is_err());
    std::fs::write(&ceiling_path, u128::MAX.to_le_bytes()).unwrap();
    assert!(migrate_v17(root_id).is_err());
    std::fs::write(&ceiling_path, 128u128.to_le_bytes()).unwrap();
    let mut older_counter = MapxRaw::new();
    older_counter.insert([], postcard::to_allocvec(&256u128).unwrap());
    std::fs::write(
        vsdb_core::vsdb_get_system_dir().join("id_num"),
        postcard::to_allocvec(&older_counter.instance_id().map_id.to_le_bytes().as_slice())
            .unwrap(),
    )
    .unwrap();
    let (mut root, map) = migrate_v17(root_id).unwrap();
    assert_eq!(root.instance_id(), root_id);
    assert_eq!(root.child_ids(), root_tokens);
    let retired = root_tokens
        .iter()
        .map(|token| u128::from_le_bytes(token.as_slice().try_into().unwrap()))
        .max()
        .unwrap()
        + 1;
    let fresh = DagMapRaw::new(Some(&mut root));
    let fresh_token = root.child_id(&fresh).unwrap();
    assert!(u128::from_le_bytes(fresh_token.as_slice().try_into().unwrap()) > 256);
    root.prune_children_include(&[retired.to_le_bytes()]);
    assert_eq!(root.child_id(&fresh), Some(fresh_token.clone()));
    root.prune_children_include(&[fresh_token]);
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

    // Byte-slice root handle conversion in a fresh namespace without prior save_meta().
    let ns2 = Namespace::create().unwrap();
    let d = MapxRaw::new_in(&ns2);
    let mut p = d.new_colocated();
    let mut ch = d.new_colocated();
    p.insert([], [0]);
    let raw_b = |id: u64| {
        let mut v = b"VSMAPX01".to_vec();
        v.extend_from_slice(&id.to_le_bytes());
        v.extend_from_slice(&ns2.id().to_le_bytes());
        v
    };
    let fnv1a = |s: &str| {
        s.bytes().fold(0xcbf29ce484222325u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x100000001b3)
        })
    };
    let mut p_in = b"VSTYPE03".to_vec();
    p_in.extend_from_slice(
        &fnv1a("vsdb::basic::mapx_ord_rawkey::MapxOrdRawKey<core::option::Option<vsdb::dagmap::raw::DagMapRaw>>")
            .to_le_bytes(),
    );
    p_in.extend_from_slice(&postcard::to_allocvec(&raw_b(p.instance_id().map_id)).unwrap());
    let mut p_out = b"VSTYPE03".to_vec();
    p_out.extend_from_slice(
        &fnv1a("vsdb::basic::orphan::Orphan<core::option::Option<vsdb::dagmap::raw::DagMapRaw>>")
            .to_le_bytes(),
    );
    p_out.extend_from_slice(&postcard::to_allocvec(&p_in).unwrap());
    let mut ch_out = b"VSTYPE03".to_vec();
    ch_out.extend_from_slice(
        &fnv1a("vsdb::basic::mapx_ord_rawkey::MapxOrdRawKey<vsdb::dagmap::raw::DagMapRaw>")
            .to_le_bytes(),
    );
    ch_out.extend_from_slice(&postcard::to_allocvec(&raw_b(ch.instance_id().map_id)).unwrap());
    ch.clear();
    let dag_bytes = postcard::to_allocvec(&(raw_b(d.instance_id().map_id), p_out, ch_out)).unwrap();
    let (v18_bytes, remap_empty) = migrate_handle_v17(&dag_bytes).unwrap();
    assert!(remap_empty.is_empty());
    assert_eq!(migrate_handle_v17(&v18_bytes).unwrap().0, v18_bytes);
    let dag_restored: DagMapRaw = postcard::from_bytes(&v18_bytes).unwrap();
    assert_eq!(dag_restored.instance_id(), d.instance_id());
}
