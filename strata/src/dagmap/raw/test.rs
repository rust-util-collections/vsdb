use super::*;
use ruc::*;

#[test]
fn dagmapraw_functions() {
    let mut i0 = DagMapRaw::new(&mut Orphan::new(None)).unwrap();
    i0.insert("k0", "v0");
    assert_eq!(i0.get("k0").unwrap().as_slice(), "v0".as_bytes());
    assert!(i0.get("k1").is_none());
    let mut i0 = Orphan::new(Some(i0));

    let mut i1 = DagMapRaw::new(&mut i0).unwrap();
    i1.insert("k1", "v1");
    assert_eq!(i1.get("k1").unwrap().as_slice(), "v1".as_bytes());
    assert_eq!(i1.get("k0").unwrap().as_slice(), "v0".as_bytes());
    let mut i1 = Orphan::new(Some(i1));

    let mut i2 = DagMapRaw::new(&mut i1).unwrap();
    i2.insert("k2", "v2");
    assert_eq!(i2.get("k2").unwrap().as_slice(), "v2".as_bytes());
    assert_eq!(i2.get("k1").unwrap().as_slice(), "v1".as_bytes());
    assert_eq!(i2.get("k0").unwrap().as_slice(), "v0".as_bytes());
    i2.insert("k2", "v2x");
    assert_eq!(i2.get("k2").unwrap().as_slice(), "v2x".as_bytes());
    assert_eq!(i2.get("k1").unwrap().as_slice(), "v1".as_bytes());
    assert_eq!(i2.get("k0").unwrap().as_slice(), "v0".as_bytes());
    i2.insert("k1", "v1x");
    assert_eq!(i2.get("k2").unwrap().as_slice(), "v2x".as_bytes());
    assert_eq!(i2.get("k1").unwrap().as_slice(), "v1x".as_bytes());
    assert_eq!(i2.get("k0").unwrap().as_slice(), "v0".as_bytes());
    i2.insert("k0", "v0x");
    assert_eq!(i2.get("k2").unwrap().as_slice(), "v2x".as_bytes());
    assert_eq!(i2.get("k1").unwrap().as_slice(), "v1x".as_bytes());
    assert_eq!(i2.get("k0").unwrap().as_slice(), "v0x".as_bytes());

    assert!(i1.get_value().unwrap().get("k2").is_none());
    assert_eq!(
        i1.get_value().unwrap().get("k1").unwrap().as_slice(),
        "v1".as_bytes()
    );
    assert_eq!(
        i1.get_value().unwrap().get("k0").unwrap().as_slice(),
        "v0".as_bytes()
    );

    assert!(i0.get_value().unwrap().get("k2").is_none());
    assert!(i0.get_value().unwrap().get("k1").is_none());
    assert_eq!(
        i0.get_value().unwrap().get("k0").unwrap().as_slice(),
        "v0".as_bytes()
    );

    let mut head = i2.prune().unwrap();
    sleep_ms!(1000);

    assert_eq!(head.get("k2").unwrap().as_slice(), "v2x".as_bytes());
    assert_eq!(head.get("k1").unwrap().as_slice(), "v1x".as_bytes());
    assert_eq!(head.get("k0").unwrap().as_slice(), "v0x".as_bytes());

    assert!(i1.get_value().is_none());
    assert!(i1.get_value().is_none());
    assert!(i1.get_value().is_none());

    assert!(i0.get_value().is_none());
    assert!(i0.get_value().is_none());
    assert!(i0.get_value().is_none());

    // prune with deep stack
    for i in 10u8..=255 {
        head.insert(i.to_be_bytes(), i.to_be_bytes());
        head = DagMapRaw::new(&mut Orphan::new(Some(head))).unwrap();
    }

    let mut head = head.prune().unwrap();
    sleep_ms!(1000);
    assert!(head.parent.get_value().is_none());
    assert!(head.children.iter().next().is_none());

    for i in 10u8..=255 {
        assert_eq!(
            head.get(i.to_be_bytes()).unwrap().as_slice(),
            i.to_be_bytes()
        );
    }

    for i in 0u8..=254 {
        head.remove(i.to_be_bytes());
        assert!(head.get(i.to_be_bytes()).is_none());
    }

    *(head.get_mut(255u8.to_be_bytes()).unwrap()) = 0u8.to_be_bytes().to_vec();
    assert_eq!(
        head.get(255u8.to_be_bytes()).unwrap().as_slice(),
        0u8.to_be_bytes()
    );
}

#[test]
fn test_save_and_from_meta() {
    let mut dag = DagMapRaw::new(&mut Orphan::new(None)).unwrap();
    dag.insert("k1", "v1");
    dag.insert("k2", "v2");

    let id = dag.save_meta().unwrap();
    assert_eq!(id, dag.instance_id());

    let restored = DagMapRaw::from_meta(id).unwrap();
    assert_eq!(restored.get("k1").unwrap().as_slice(), "v1".as_bytes());
    assert_eq!(restored.get("k2").unwrap().as_slice(), "v2".as_bytes());
}

/// Postcard serde roundtrip for DagMapRaw (hand-written tuple serde, 3 fields).
#[test]
fn test_serde_roundtrip() {
    let mut dag = DagMapRaw::new(&mut Orphan::new(None)).unwrap();
    dag.insert("alpha", "A");
    dag.insert("beta", "B");

    let bytes = postcard::to_allocvec(&dag).unwrap();
    let restored: DagMapRaw = postcard::from_bytes(&bytes).unwrap();

    assert_eq!(restored.get("alpha").unwrap().as_slice(), b"A");
    assert_eq!(restored.get("beta").unwrap().as_slice(), b"B");
}

/// from_meta nonexistent.
#[test]
fn test_from_meta_nonexistent() {
    assert!(DagMapRaw::from_meta(u64::MAX).is_err());
}

/// Restore from meta, mutate, verify shared storage.
#[test]
fn test_meta_restore_then_mutate() {
    let mut dag = DagMapRaw::new(&mut Orphan::new(None)).unwrap();
    dag.insert("k1", "v1");

    let id = dag.save_meta().unwrap();
    let mut restored = DagMapRaw::from_meta(id).unwrap();

    restored.insert("k2", "v2");
    assert_eq!(dag.get("k2").unwrap().as_slice(), b"v2");
}

#[test]
#[should_panic(expected = "empty value is a tombstone")]
fn insert_empty_value_panics() {
    let mut dag = DagMapRaw::new(&mut Orphan::new(None)).unwrap();
    dag.insert("empty", []);
}

#[test]
fn destroy_unlinks_from_parent() {
    let parent = DagMapRaw::new(&mut Orphan::new(None)).unwrap();
    let mut parent_slot = Orphan::new(Some(parent));
    let mut child = DagMapRaw::new(&mut parent_slot).unwrap();

    let parent_handle = parent_slot.get_value().unwrap();
    assert!(!parent_handle.no_children());
    child.destroy();
    assert!(parent_handle.no_children());
}

#[test]
fn deep_acyclic_chain_remains_readable_and_prunable() {
    let mut head = DagMapRaw::new(&mut Orphan::new(None)).unwrap();
    head.insert("root", "value");

    for _ in 0..1030 {
        head = DagMapRaw::new(&mut Orphan::new(Some(head))).unwrap();
    }

    assert_eq!(head.get("root").unwrap().as_slice(), b"value");
    let pruned = head.prune().unwrap();
    assert_eq!(pruned.get("root").unwrap().as_slice(), b"value");
}

/// Save meta of a DagMapRaw with parent-child relationship,
/// restore, and verify the lineage is intact.
#[test]
fn test_meta_with_parent_child() {
    let mut i0 = DagMapRaw::new(&mut Orphan::new(None)).unwrap();
    i0.insert("base", "v0");

    let mut i1 = DagMapRaw::new(&mut Orphan::new(Some(i0))).unwrap();
    i1.insert("child", "v1");

    let id = i1.save_meta().unwrap();
    let restored = DagMapRaw::from_meta(id).unwrap();

    // Child data
    assert_eq!(restored.get("child").unwrap().as_slice(), b"v1");
    // Inherited from parent
    assert_eq!(restored.get("base").unwrap().as_slice(), b"v0");
}
