//! A DAG in a secondary namespace only opens its own engine.

mod common;

use vsdb::{DagMapRaw, Namespace, vsdb_get_base_dir};

#[test]
fn dagmap_in_a_namespace_leaves_the_default_engine_closed() {
    common::setup();
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
