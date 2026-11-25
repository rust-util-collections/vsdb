//! A v17 typed root could contain saved children with a different value codec.
#[path = "common/scratch.rs"]
mod scratch;

use vsdb::{
    CommitId, DagMapRaw, DagMapRawKey, InstanceId, MapxRaw, Namespace, VsdbOptions, vsdb_configure,
};
use vsdb_migrate::{
    LegacyRaw,
    dagmap::{
        DagHandleConverter, migrate_child_v17, migrate_rawkey_handle_v17, migrate_rawkey_v17,
        migrate_v17,
    },
};
type Rows = Vec<(Vec<u8>, Vec<u8>)>;
#[derive(serde::Deserialize)]
struct Fixture(Vec<u8>, Vec<u8>, [u64; 2], Vec<(Vec<u8>, Rows)>);

#[test]
fn migrates_mixed_saved_types() {
    vsdb_configure(VsdbOptions::new(scratch::database_dir())).unwrap();
    // The fixture captures map rows and metadata; v17 stored its shared edge
    // allocator separately at the universe root.
    std::fs::write(
        vsdb_core::vsdb_get_system_dir().join("dag_id_ceiling"),
        128u128.to_le_bytes(),
    )
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

    // Build a synthetic v17 3-node DAG: DagMapRaw root with a direct
    // DagMapRawKey<CommitId> child and a composite DagMapRawKey<Vec<CommitId>>
    // child saved under VSTYPE03.
    let fnv1a = |s: &str| {
        s.bytes().fold(0xcbf29ce484222325u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x100000001b3)
        })
    };
    let v17_raw = |id: u64| {
        let mut b = b"VSMAPX01".to_vec();
        b.extend_from_slice(&id.to_le_bytes());
        b
    };
    let v17_typed = |outer: &str, inner: &str, id: u64| {
        let mut i = b"VSTYPE03".to_vec();
        i.extend_from_slice(&fnv1a(inner).to_le_bytes());
        i.extend_from_slice(&postcard::to_allocvec(&v17_raw(id)).unwrap());
        let mut o = b"VSTYPE03".to_vec();
        o.extend_from_slice(&fnv1a(outer).to_le_bytes());
        o.extend_from_slice(&postcard::to_allocvec(&i).unwrap());
        o
    };
    let v17_single = |ty: &str, id: u64| {
        let mut o = b"VSTYPE03".to_vec();
        o.extend_from_slice(&fnv1a(ty).to_le_bytes());
        o.extend_from_slice(&postcard::to_allocvec(&v17_raw(id)).unwrap());
        o
    };
    let mk_node = || {
        let d = MapxRaw::new();
        let p = d.new_colocated();
        let ch = d.new_colocated();
        let ids = [
            d.instance_id().map_id,
            p.instance_id().map_id,
            ch.instance_id().map_id,
        ];
        let raw_bytes = postcard::to_allocvec(&(
            v17_raw(ids[0]),
            v17_typed(
                "vsdb::basic::orphan::Orphan<core::option::Option<vsdb::dagmap::raw::DagMapRaw>>",
                "vsdb::basic::mapx_ord_rawkey::MapxOrdRawKey<core::option::Option<vsdb::dagmap::raw::DagMapRaw>>",
                ids[1],
            ),
            v17_single(
                "vsdb::basic::mapx_ord_rawkey::MapxOrdRawKey<vsdb::dagmap::raw::DagMapRaw>",
                ids[2],
            ),
        ))
        .unwrap();
        ((d, p, ch), ids, raw_bytes)
    };
    let ((_, mut rp, mut rch), r_ids, r_bytes) = mk_node();
    let ((mut c1d, mut c1p, _), c1_ids, c1_raw) = mk_node();
    let ((mut c2d, mut c2p, _), c2_ids, c2_raw) = mk_node();
    let ((mut c3d, mut c3p, _), c3_ids, c3_raw) = mk_node();
    c3p.insert([], [&[1][..], &r_bytes].concat());
    rch.insert(3u128.to_le_bytes(), &c3_raw);
    c3d.insert(b"number", postcard::to_allocvec(&55u16).unwrap());
    rp.insert([], [0]); // None
    c1p.insert([], [&[1][..], &r_bytes].concat()); // Some(root)
    c2p.insert([], [&[1][..], &r_bytes].concat()); // Some(root)
    rch.insert(1u128.to_le_bytes(), &c1_raw);
    rch.insert(2u128.to_le_bytes(), &c2_raw);
    c1d.insert(
        b"cid",
        postcard::to_allocvec(&CommitId::from_raw(9)).unwrap(),
    );
    c2d.insert(
        b"cids",
        postcard::to_allocvec(&vec![CommitId::from_raw(3), CommitId::from_raw(4)]).unwrap(),
    );

    let wrap_child = |v17_type: &str, node_raw: &[u8]| {
        let mut f = b"VSTYPE03".to_vec();
        f.extend_from_slice(&fnv1a(v17_type).to_le_bytes());
        f.extend_from_slice(node_raw);
        postcard::to_allocvec(&f).unwrap()
    };
    std::fs::write(ns.meta_path(r_ids[0]), &r_bytes).unwrap();
    std::fs::write(
        ns.meta_path(c1_ids[0]),
        wrap_child(
            "vsdb::dagmap::rawkey::DagMapRawKey<vsdb::versioned::CommitId>",
            &c1_raw,
        ),
    )
    .unwrap();
    std::fs::write(
        ns.meta_path(c2_ids[0]),
        wrap_child(
            "vsdb::dagmap::rawkey::DagMapRawKey<alloc::vec::Vec<vsdb::versioned::CommitId>>",
            &c2_raw,
        ),
    )
    .unwrap();

    // The application stored an independently typed view of the raw root and a
    // typed child without a saved sidecar. Preserve all three handle flavors.
    let root_handle = wrap_child("vsdb::dagmap::rawkey::DagMapRawKey<u64>", &r_bytes);
    let embedded = wrap_child("vsdb::dagmap::rawkey::DagMapRawKey<u16>", &c3_raw);
    let (new_root, moved) = migrate_rawkey_handle_v17::<u64>(&root_handle).unwrap();
    let _: DagMapRawKey<u64> = postcard::from_bytes(&new_root).unwrap();
    assert!(DagMapRaw::from_meta(r_ids[0]).is_ok());
    assert_eq!(
        migrate_rawkey_handle_v17::<u64>(&root_handle).unwrap().0,
        new_root
    );
    let (_migrated_root, remap2) = migrate_v17(r_ids[0]).unwrap();
    assert_eq!(moved, remap2);
    let converter = DagHandleConverter::open(r_ids[0]).unwrap();
    let (data, _, _): (Vec<u8>, Vec<u8>, Vec<u8>) = postcard::from_bytes(&c3_raw).unwrap();
    let (_, other_parent, other_edges): (Vec<u8>, Vec<u8>, Vec<u8>) =
        postcard::from_bytes(&c1_raw).unwrap();
    let mixed = postcard::to_allocvec(&(data, other_parent, other_edges)).unwrap();
    assert!(converter.raw_handle(&mixed).is_err());
    let translated = converter.rawkey_handle::<u16>(&embedded).unwrap();
    let node: DagMapRawKey<u16> = postcard::from_bytes(&translated).unwrap();
    let old = InstanceId::from(c3_ids[0]);
    assert_eq!(node.instance_id(), moved.get(&old).copied().unwrap_or(old));
    assert_eq!(node.get(b"number"), Some(55));
    assert_eq!(
        converter.rawkey_handle::<u16>(&translated).unwrap(),
        translated
    );
    assert!(converter.rawkey_handle::<u32>(&embedded).is_err());
    assert!(converter.raw_handle(&c3_raw).is_ok());
    let wrong_graph = DagHandleConverter::open(r).unwrap();
    assert!(wrong_graph.rawkey_handle::<u16>(&embedded).is_err());
    let c1_id = InstanceId::new(c1_ids[0], ns.id());
    let c1_id = *remap2.get(&c1_id).unwrap_or(&c1_id);
    let c2_id = InstanceId::new(c2_ids[0], ns.id());
    let c2_id = *remap2.get(&c2_id).unwrap_or(&c2_id);
    // Direct DagMapRawKey<CommitId> child opens immediately after root migration.
    let opened_c1 = DagMapRawKey::<CommitId>::from_meta(c1_id).unwrap();
    assert_eq!(opened_c1.get("cid"), Some(CommitId::from_raw(9)));
    // Composite DagMapRawKey<Vec<CommitId>> child re-tags cleanly via migrate_child_v17.
    assert!(migrate_child_v17::<Vec<u8>>(c2_id).is_err());
    let opened_c2 = migrate_child_v17::<Vec<CommitId>>(c2_id).unwrap();
    assert_eq!(
        opened_c2.get("cids"),
        Some(vec![CommitId::from_raw(3), CommitId::from_raw(4)])
    );
    assert_eq!(
        DagMapRawKey::<Vec<CommitId>>::from_meta(c2_id)
            .unwrap()
            .get("cids"),
        Some(vec![CommitId::from_raw(3), CommitId::from_raw(4)])
    );
}
