use super::*;
use std::{thread, time::Duration};

#[test]
fn dagmapraw_functions() {
    let mut i0 = DagMapRaw::new(None);
    i0.insert("k0", "v0");
    assert_eq!(i0.get("k0").unwrap().as_slice(), "v0".as_bytes());
    assert!(i0.get("k1").is_none());

    let mut i1 = DagMapRaw::new(Some(&mut i0));
    i1.insert("k1", "v1");
    assert_eq!(i1.get("k1").unwrap().as_slice(), "v1".as_bytes());
    assert_eq!(i1.get("k0").unwrap().as_slice(), "v0".as_bytes());

    let mut i2 = DagMapRaw::new(Some(&mut i1));
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

    // Overlay isolation: parents never see descendant writes.
    assert!(i1.get("k2").is_none());
    assert_eq!(i1.get("k1").unwrap().as_slice(), "v1".as_bytes());
    assert_eq!(i1.get("k0").unwrap().as_slice(), "v0".as_bytes());

    assert!(i0.get("k2").is_none());
    assert!(i0.get("k1").is_none());
    assert_eq!(i0.get("k0").unwrap().as_slice(), "v0".as_bytes());

    // The original owned handles alias the same storage and observe the
    // post-prune state (`Clone` would deep-copy instead).
    let mut head = i2.prune().unwrap();
    thread::sleep(Duration::from_millis(1000));

    assert_eq!(head.get("k2").unwrap().as_slice(), "v2x".as_bytes());
    assert_eq!(head.get("k1").unwrap().as_slice(), "v1x".as_bytes());
    assert_eq!(head.get("k0").unwrap().as_slice(), "v0x".as_bytes());

    // The intermediate mainline node was merged into genesis and cleared.
    assert!(i1.is_dead());
    // The genesis handle sees the merged result (same storage as `head`).
    assert_eq!(i0.get("k2").unwrap().as_slice(), "v2x".as_bytes());

    // prune with deep stack
    for i in 10u8..=255 {
        head.insert(i.to_be_bytes(), i.to_be_bytes());
        head = DagMapRaw::new(Some(&mut head));
    }

    let mut head = head.prune().unwrap();
    thread::sleep(Duration::from_millis(1000));
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
    let mut dag = DagMapRaw::new(None);
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
    let mut dag = DagMapRaw::new(None);
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
    let mut dag = DagMapRaw::new(None);
    dag.insert("k1", "v1");

    let id = dag.save_meta().unwrap();
    let mut restored = DagMapRaw::from_meta(id).unwrap();

    restored.insert("k2", "v2");
    assert_eq!(dag.get("k2").unwrap().as_slice(), b"v2");
}

#[test]
#[should_panic(expected = "empty value is a tombstone")]
fn insert_empty_value_panics() {
    let mut dag = DagMapRaw::new(None);
    dag.insert("empty", []);
}

#[test]
fn destroy_unlinks_from_parent() {
    let mut parent = DagMapRaw::new(None);
    let mut child = DagMapRaw::new(Some(&mut parent));

    assert!(!parent.no_children());
    child.destroy();
    assert!(parent.no_children());
}

#[test]
fn deep_acyclic_chain_remains_readable_and_prunable() {
    let mut head = DagMapRaw::new(None);
    head.insert("root", "value");

    for _ in 0..1030 {
        head = DagMapRaw::new(Some(&mut head));
    }

    assert_eq!(head.get("root").unwrap().as_slice(), b"value");
    let pruned = head.prune().unwrap();
    assert_eq!(pruned.get("root").unwrap().as_slice(), b"value");
}

/// Save meta of a DagMapRaw with parent-child relationship,
/// restore, and verify the lineage is intact.
#[test]
fn test_meta_with_parent_child() {
    let mut i0 = DagMapRaw::new(None);
    i0.insert("base", "v0");

    let mut i1 = DagMapRaw::new(Some(&mut i0));
    i1.insert("child", "v1");

    let id = i1.save_meta().unwrap();
    let restored = DagMapRaw::from_meta(id).unwrap();

    // Child data
    assert_eq!(restored.get("child").unwrap().as_slice(), b"v1");
    // Inherited from parent
    assert_eq!(restored.get("base").unwrap().as_slice(), b"v0");
}

#[test]
fn test_prune_with_side_branches() {
    let mut i0 = DagMapRaw::new(None);
    i0.insert("k0", "v0");

    let mut mid = DagMapRaw::new(Some(&mut i0));

    // Side-branch child of mid
    let mut side = DagMapRaw::new(Some(&mut mid));
    side.insert("k_side", "v_side");

    // Another side-branch child of mid
    let mut side2 = DagMapRaw::new(Some(&mut mid));
    side2.insert("k_side2", "v_side2");

    // Head node on mainline
    let mut head = DagMapRaw::new(Some(&mut mid));
    head.insert("k_head", "v_head");

    // Prune
    let pruned = head.prune().unwrap();

    assert_eq!(pruned.get("k0").unwrap().as_slice(), b"v0");
    assert_eq!(pruned.get("k_head").unwrap().as_slice(), b"v_head");

    // Side branches of intermediate nodes should be destroyed
    assert!(side.get("k_side").is_none());
    assert!(side2.get("k_side2").is_none());
}

#[test]
fn destroy_sibling_preserves_other_siblings_and_parent() {
    // Destroying one child must leave the parent and the other children
    // intact — each node owns its parent slot, so the unlink is local.
    let mut p = DagMapRaw::new(None);
    p.insert("shared", "pval");

    let mut c1 = DagMapRaw::new(Some(&mut p));
    c1.insert("c1", "v1");
    let c2 = DagMapRaw::new(Some(&mut p));

    assert_eq!(c1.get("shared").unwrap().as_slice(), b"pval");
    assert_eq!(c2.get("shared").unwrap().as_slice(), b"pval");

    c1.destroy();
    assert!(c1.get("c1").is_none());

    // Parent must be untouched.
    assert_eq!(p.get("shared").unwrap().as_slice(), b"pval");
    // Surviving sibling must still reach the inherited parent data.
    assert_eq!(c2.get("shared").unwrap().as_slice(), b"pval");
}

#[test]
fn destroyed_node_does_not_serve_inherited_reads() {
    // After destroy(), no handle of the node may fall through to the
    // parent and return inherited data — the unlink is persisted in the
    // node's own parent slot, so clones and restored handles see it too.
    let mut parent = DagMapRaw::new(None);
    parent.insert("k", "v");

    let mut child = DagMapRaw::new(Some(&mut parent));
    // Before destroy the child inherits the parent's value.
    assert_eq!(child.get("k").unwrap().as_slice(), b"v");

    // A shadow (aliasing handle) taken BEFORE the destroy call.
    // SAFETY: used strictly after the destroy completes; accesses are
    // sequential on one thread, satisfying SWMR.
    let stale_alias = unsafe { child.shadow() };

    child.destroy();
    // Neither the destroying handle nor the stale alias serves
    // inherited reads afterwards — the parent unlink is persisted in
    // the node's own parent slot.
    assert!(child.get("k").is_none());
    assert!(stale_alias.get("k").is_none());

    // The parent itself is untouched.
    assert_eq!(parent.get("k").unwrap().as_slice(), b"v");
}

#[test]
fn destroy_is_visible_to_meta_restored_handles() {
    // A handle restored via from_meta AFTER the node was destroyed must
    // observe the destroyed state (cleared data + nulled parent link).
    let mut parent = DagMapRaw::new(None);
    parent.insert("k", "v");

    let mut child = DagMapRaw::new(Some(&mut parent));
    let id = child.save_meta().unwrap();

    child.destroy();

    let restored = DagMapRaw::from_meta(id).unwrap();
    assert!(restored.get("k").is_none());
    assert!(restored.is_dead());
}

#[test]
fn destroy_deep_child_chain_does_not_overflow_stack() {
    // Regression: destroy() recursed once per descendant generation and
    // overflowed the stack on deep DAGs. Build a deep child chain rooted
    // at `genesis` and destroy it iteratively.
    let mut genesis = DagMapRaw::new(None);
    genesis.insert("root", "value");

    // SAFETY: the shadow is only used as an attachment cursor while
    // building the chain; all accesses are sequential on one thread,
    // satisfying SWMR.
    let mut cur = unsafe { genesis.shadow() };
    for _ in 0..5000 {
        cur = DagMapRaw::new(Some(&mut cur));
    }

    // `genesis` owns the full descendant chain through its children map.
    // Destroying it must not overflow the stack.
    genesis.destroy();
    assert!(genesis.get("root").is_none());
}
