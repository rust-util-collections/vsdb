//! A process that only uses non-default namespaces must not open the
//! default namespace's engine just to allocate DagMap ids (the id ceiling
//! lives in the default system dir, which is path-only state).

use vsdb::{DagMapRaw, Namespace, VsdbOptions, vsdb_configure, vsdb_get_base_dir};

#[test]
fn dagmap_in_a_namespace_leaves_the_default_engine_closed() {
    let dir = format!("/tmp/vsdb_testing/dagmap_ns_only_{}", rand::random::<u64>());
    vsdb_configure(VsdbOptions::new(&dir)).unwrap();
    let ns = Namespace::create().unwrap();
    let mut root = DagMapRaw::new_in(&ns, None);
    root.insert("k", "v");
    let child = DagMapRaw::new_in(&ns, Some(&mut root));
    assert_eq!(child.get("k").unwrap().as_slice(), b"v");
    // The default engine keeps its shards under `{base}/mmdb`.
    assert!(
        !vsdb_get_base_dir().join("mmdb").exists(),
        "creating a namespaced DagMap opened the default engine"
    );
}
