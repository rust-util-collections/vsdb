//! A DAG in a secondary namespace only opens its own engine.

mod common;

use vsdb::{DagMap, DagMapRaw, DagMapRawKey, Namespace, vsdb_get_base_dir};

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

#[test]
fn child_uses_its_parents_namespace_even_with_a_different_root_default() {
    common::setup();
    let owner = Namespace::create().unwrap();
    let other = Namespace::create().unwrap();
    let mut parent = DagMapRaw::new_in(&owner, None);
    let child = DagMapRaw::new_in(&other, Some(&mut parent));
    assert_eq!(child.namespace().id(), owner.id());
    assert!(parent.child_id(&child).is_some());
    let mut raw_keys = DagMapRawKey::<u64>::new_in(&owner, None);
    let child = DagMapRawKey::new_in(&other, Some(&mut raw_keys));
    assert_eq!(child.namespace().id(), owner.id());
    let mut typed = DagMap::<u64, u64>::new_in(&owner, None);
    let child = DagMap::new_in(&other, Some(&mut typed));
    assert_eq!(child.namespace().id(), owner.id());

    assert_eq!(DagMapRaw::new_in(&other, None).namespace().id(), other.id());
}
