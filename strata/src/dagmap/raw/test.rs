use super::*;
use crate::{Namespace, NamespaceOpts, VsdbOptions, vsdb_configure};
use std::{
    env, fs,
    path::Path,
    process::{self, Command},
};

#[test]
fn child_registration_and_parent_slot_share_a_wal() {
    let mut root = DagMapRaw::new(None);
    root.insert(b"ancestor", b"value");
    let mut parent = DagMapRaw::new(Some(&mut root));
    let child = DagMapRaw::new(Some(&mut parent));
    for (owner, descendant) in [(&root, &parent), (&parent, &child)] {
        assert!(owner.store.is_colocated_with(&descendant.store));
        let (e, id) = owner.links().next().unwrap();
        let registered = owner.owned(e, id).unwrap();
        assert_eq!(registered.instance_id(), descendant.instance_id());
        assert_eq!(
            registered.get(b"ancestor").as_deref(),
            Some(b"value".as_slice())
        );
    }
}

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
    assert!(head.meta().unwrap().state == State::Root);
    assert!(head.no_children());

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
fn serde_rejects_mixed_namespace_components() {
    let ns = crate::Namespace::create().unwrap();
    let data = MapxRaw::new();
    use crate::{MapxOrdRawKey, Orphan};
    let parent: Orphan<Option<DagMapRaw>> = Orphan::new_in(&ns, None);
    let children: MapxOrdRawKey<DagMapRaw> = MapxOrdRawKey::new();
    let bytes = postcard::to_allocvec(&(data, parent, children)).unwrap();

    assert!(postcard::from_bytes::<DagMapRaw>(&bytes).is_err());
}

#[test]
fn empty_value_roundtrip() {
    let mut dag = DagMapRaw::new(None);
    dag.insert("empty", []);
    assert_eq!(dag.get("empty"), Some(Vec::new()));
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
fn destroy_retry_unlinks_and_reclaims_descendants() {
    for mode in ["self", "prune_children", "prune_head"] {
        let mut root = DagMapRaw::new(None);
        let mut parent = if mode == "prune_head" {
            DagMapRaw::new(Some(&mut root))
        } else {
            DagMapRaw::new(None)
        };
        let mut child = DagMapRaw::new(Some(&mut parent));
        let mut grandchild = DagMapRaw::new(Some(&mut child));
        grandchild.insert("key", "value");
        let mut meta = child.meta().unwrap();
        meta.state = State::Destroying(meta.state.owner());
        child.set_meta(meta).unwrap();
        match mode {
            "self" => {
                child.destroy();
                assert!(parent.no_children());
            }
            "prune_children" => {
                parent.prune_children_exclude(&[] as &[Vec<u8>]);
                assert!(parent.no_children());
            }
            "prune_head" => {
                let pruned = parent.prune().unwrap();
                assert!(pruned.no_children());
            }
            _ => unreachable!(),
        }
        assert!(grandchild.is_dead());
        assert!(child.is_dead());
        assert_eq!(grandchild.store.range(DATA..END).count(), 0);
    }
}

/// Regression: `prune()`'s crash-safety WAL barriers must be scoped to
/// the DAG's own namespace, not the whole
/// process (the previous `vsdb_flush()` calls flushed the default
/// namespace plus every other open namespace, so a flush failure in any
/// unrelated namespace could panic a `prune()` on a perfectly healthy
/// one). Exercising `prune()` end-to-end on a DAG placed in an explicit
/// non-default namespace only compiles and behaves correctly if the
/// barriers are genuinely namespace-scoped.
#[test]
fn prune_works_in_a_non_default_namespace() {
    let ns = crate::common::Namespace::create().unwrap();
    let mut head = DagMapRaw::new_in(&ns, None);
    head.insert("root", "value");

    let mut child = DagMapRaw::new_in(&ns, Some(&mut head));
    child.insert("leaf", "cv");

    let pruned = head.prune().unwrap();
    assert_eq!(pruned.get("root").unwrap().as_slice(), b"value");
    assert_eq!(child.get("root").unwrap().as_slice(), b"value");
    assert_eq!(child.get("leaf").unwrap().as_slice(), b"cv");
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
fn selective_prune_ids_are_publicly_discoverable() {
    let mut parent = DagMapRaw::new(None);
    let child1 = DagMapRaw::new(Some(&mut parent));
    let child2 = DagMapRaw::new(Some(&mut parent));

    let id1 = parent.child_id(&child1).unwrap();
    let id2 = parent.child_id(&child2).unwrap();
    assert_ne!(id1, id2);
    assert_eq!(parent.child_ids().len(), 2);

    parent.prune_children_include(&[id1]);
    assert!(child1.is_dead());
    assert!(!child2.is_dead());
    assert_eq!(parent.child_ids(), vec![id2]);
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
    // used strictly after the destroy completes; accesses are
    // sequential on one thread, satisfying SWMR.
    let stale_alias = child.open(child.id()).unwrap();

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

    // the shadow is only used as an attachment cursor while
    // building the chain; all accesses are sequential on one thread,
    // satisfying SWMR.
    let mut cur = genesis.open(genesis.id()).unwrap();
    for _ in 0..5000 {
        cur = DagMapRaw::new(Some(&mut cur));
    }

    // `genesis` owns the full descendant chain through its children map.
    // Destroying it must not overflow the stack.
    genesis.destroy();
    assert!(genesis.get("root").is_none());
}

// ---------------------------------------------------------------------------
// Crash-safety tests for prune_mainline.
//
// The prune is phased (collect → destroy branches → merge → re-parent →
// clear); a crash can stop it between any two writes.  These tests drive
// the private phase methods directly to freeze the on-disk state at each
// phase boundary and assert the canonical views (head / genesis meta /
// surviving children) stay value-exact — the contract documented on
// `prune`.
// ---------------------------------------------------------------------------

/// Chain: genesis(k0,kg) → i1(k1, kill kg) → head(k2, k1 override).
/// Returns (genesis, i1, head).
fn build_prune_fixture() -> (DagMapRaw, DagMapRaw, DagMapRaw) {
    let mut genesis = DagMapRaw::new(None);
    genesis.insert("k0", "v0");
    genesis.insert("kg", "doomed");

    let mut i1 = DagMapRaw::new(Some(&mut genesis));
    i1.insert("k1", "v1");
    i1.remove("kg"); // tombstone over a genesis key

    let mut head = DagMapRaw::new(Some(&mut i1));
    head.insert("k2", "v2");
    head.insert("k1", "v1x"); // overrides i1

    (genesis, i1, head)
}

/// Asserts the full merged (post-prune) view on a handle.
fn assert_merged_view(h: &DagMapRaw) {
    assert_eq!(h.get("k0").unwrap().as_slice(), b"v0");
    assert_eq!(h.get("k1").unwrap().as_slice(), b"v1x");
    assert_eq!(h.get("k2").unwrap().as_slice(), b"v2");
    assert!(h.get("kg").is_none());
}

fn prepare_marked_clear(head: &mut DagMapRaw) {
    let (mut line, mut root) = head.open(head.id()).unwrap().mainline().unwrap();
    DagMapRaw::fold(&line, &mut root).unwrap();
    line[0].reparent(&mut root).unwrap();
    DagMapRaw::mark(&mut line, root.id()).unwrap();
}

#[test]
fn prune_partial_reparent_and_markers_converge() {
    for cut in 0..4 {
        let (root, mid, mut head) = build_prune_fixture();
        let mut children = [
            DagMapRaw::new(Some(&mut head)),
            DagMapRaw::new(Some(&mut head)),
        ];
        let (mut line, mut genesis) = head.open(head.id()).unwrap().mainline().unwrap();
        DagMapRaw::fold(&line, &mut genesis).unwrap();
        let id = children[0].id();
        let edge = genesis.register(genesis.meta().unwrap(), id).unwrap();
        if cut > 0 {
            let mut m = children[0].meta().unwrap();
            m.state = State::Child(Parent {
                map: genesis.id(),
                edge,
            });
            children[0].set_meta(m).unwrap();
        }
        if cut > 1 {
            head.reparent(&mut genesis).unwrap();
            DagMapRaw::mark(&mut line[..cut - 1], root.id()).unwrap();
        }
        for c in &children {
            assert_merged_view(c);
        }
        let recovered = head.prune().unwrap();
        assert_eq!(recovered.id(), root.id());
        assert!(mid.is_dead());
        assert_eq!(recovered.child_ids().len(), 2);
        for c in &children {
            assert_merged_view(c);
            assert!(recovered.child_id(c).is_some());
        }
    }
}

thread_local! {
    static CLEAR_STEPS_LEFT: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

pub(super) fn after_clear_step() {
    CLEAR_STEPS_LEFT.with(|steps| {
        if let Some(left) = steps.get() {
            steps.set(left.checked_sub(1));
            assert!(left > 0, "interrupted consumed-node cleanup");
        }
    });
}

#[test]
fn prune_cleanup_survives_repeated_interruptions() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    for already_marked in [false, true] {
        for cut in 0..8 {
            let mut ancestors = vec![DagMapRaw::new(None)];
            for i in 1..5u8 {
                let mut node = DagMapRaw::new(ancestors.last_mut());
                node.insert([i], [i]);
                ancestors.push(node);
            }
            let mut head = ancestors.pop().unwrap();
            let mut survivor = DagMapRaw::new(Some(&mut head));
            survivor.insert("survivor", "value");
            if already_marked {
                prepare_marked_clear(&mut head);
            }
            let head_id = head.save_meta().unwrap();
            head.namespace().try_sync_wal().unwrap();

            // Interrupt normal cleanup or its retry after each data or
            // registry clear, then restore the consumed head.
            CLEAR_STEPS_LEFT.with(|steps| steps.set(Some(cut)));
            let interrupted = catch_unwind(AssertUnwindSafe(|| head.prune()));
            CLEAR_STEPS_LEFT.with(|steps| steps.set(None));
            assert!(interrupted.is_err(), "cleanup cut {cut} was not reached");
            ancestors[0].namespace().try_sync_wal().unwrap();

            if cut < 4 {
                // No discovery edge may disappear during the data pass.
                for node in &ancestors {
                    assert!(!node.no_children());
                }
            } else {
                // Once registry removal starts, even a different shard's
                // early unlink cannot strand any user data.
                for node in &ancestors[1..] {
                    assert!(node.store.range(DATA..END).next().is_none());
                    assert!(matches!(
                        node.meta().unwrap().state,
                        State::Clearing { .. }
                    ));
                }
            }

            let recovered = DagMapRaw::from_meta(head_id).unwrap().prune().unwrap();
            assert_eq!(recovered.instance_id(), ancestors[0].instance_id());
            for i in 1..5u8 {
                assert_eq!(recovered.get([i]), Some(vec![i]));
                assert_eq!(survivor.get([i]), Some(vec![i]));
            }
            assert_eq!(
                survivor.get("survivor").as_deref(),
                Some(b"value".as_slice())
            );
            for node in &ancestors[1..] {
                assert!(node.is_dead(), "cut {cut} stranded an intermediate node");
                assert!(node.no_children());
            }
            let head = DagMapRaw::from_meta(head_id).unwrap();
            assert!(head.is_dead());
            assert!(head.no_children());
            assert_eq!(recovered.child_ids().len(), 1);
        }
    }
}

#[test]
fn registry_entries_cannot_destroy_foreign_nodes_or_live_roots() {
    let mut root = DagMapRaw::new(None);
    root.insert("root", "v");
    let mut a = DagMapRaw::new(Some(&mut root));
    let b = DagMapRaw::new(Some(&mut root));
    let mut foreign = DagMapRaw::new(None);
    foreign.insert("foreign", "v");
    for (id, e) in [(root.id(), 11), (b.id(), 12), (foreign.id(), 13)] {
        a.store.insert(edge_key(e), id.to_le_bytes());
    }
    a.destroy();
    assert_eq!(root.get("root"), Some(b"v".to_vec()));
    assert_eq!(foreign.get("foreign"), Some(b"v".to_vec()));
    assert_eq!(b.get("root"), Some(b"v".to_vec()));
}

#[test]
fn is_dead_recognizes_tombstoned_entries() {
    // `remove()` writes an empty-value tombstone rather than physically
    // deleting the entry (see `insert`'s doc comment on the tombstone
    // convention), so `is_dead()` must not be fooled by a backing store
    // that still has iterable (but empty) entries.
    let mut node = DagMapRaw::new(None);
    assert!(node.is_dead());

    node.insert("only", "v");
    assert!(!node.is_dead());
    assert_eq!(node.get("only").unwrap().as_slice(), b"v");

    node.remove("only");
    assert!(node.get("only").is_none());
    // The tombstone left `node.data` non-empty (one empty-value entry),
    // but the node has no live data, parent, or children.
    assert!(node.is_dead());

    // Multiple keys, only some removed: still not dead.
    node.insert("a", "va");
    node.insert("b", "vb");
    node.remove("a");
    assert!(!node.is_dead());
    node.remove("b");
    assert!(node.is_dead());
}

#[test]
fn get_mut_tombstone_edit_during_panic_does_not_abort() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let mut d = DagMapRaw::new(None);
    d.insert("k", "v");
    // Before the fix, the guard's tombstone assert fired while unwinding:
    // a second panic, i.e. a process abort.
    let r = catch_unwind(AssertUnwindSafe(|| {
        let mut g = d.get_mut("k").unwrap();
        g.clear();
        panic!("interrupted edit");
    }));
    assert!(r.is_err());
    assert_eq!(d.get("k").unwrap().as_slice(), b"v");
}
#[test]
fn clone_prune_preserves_original_graph() {
    let mut root = DagMapRaw::new(None);
    root.insert(b"inherited", b"root");
    let mut head = DagMapRaw::new(Some(&mut root));
    head.insert(b"head", b"value");
    let mut child = DagMapRaw::new(Some(&mut head));
    child.insert(b"child", b"value");
    let child_id = head.child_id(&child).unwrap();

    let copy = head.clone();
    assert_ne!(copy.instance_id(), head.instance_id());
    assert_eq!(copy.namespace().id(), head.namespace().id());
    assert_eq!(copy.get(b"inherited"), Some(b"root".to_vec()));
    assert_eq!(copy.child_ids(), vec![child_id.clone()]);
    let (e, id) = copy.links().next().unwrap();
    let copied_child = copy.owned(e, id).unwrap();
    assert_ne!(copied_child.instance_id(), child.instance_id());

    let mut copied_root = copy.prune().unwrap();
    assert_ne!(copied_root.instance_id(), root.instance_id());
    assert_eq!(copied_child.get(b"head"), Some(b"value".to_vec()));
    assert_eq!(copied_child.get(b"child"), Some(b"value".to_vec()));
    assert_eq!(head.get(b"head"), Some(b"value".to_vec()));
    assert_eq!(child.get(b"child"), Some(b"value".to_vec()));
    assert!(root.child_id(&head).is_some());

    copied_root.destroy();
    assert!(copied_child.is_dead());
    assert_eq!(child.get(b"child"), Some(b"value".to_vec()));
    assert_eq!(child.get(b"inherited"), Some(b"root".to_vec()));
}

#[test]
fn clone_remaps_prune_recovery_markers() {
    let mut root = DagMapRaw::new(None);
    let mut head = DagMapRaw::new(Some(&mut root));
    head.insert(b"key", b"value");
    let bytes = postcard::to_allocvec(&head).unwrap();
    let original_root = head.prune().unwrap();
    let consumed: DagMapRaw = postcard::from_bytes(&bytes).unwrap();

    let copied_root = consumed.clone().prune().unwrap();
    assert_ne!(copied_root.instance_id(), original_root.instance_id());
    assert_eq!(copied_root.get(b"key"), Some(b"value".to_vec()));
    assert_eq!(original_root.get(b"key"), Some(b"value".to_vec()));
}

fn dag_wal_phase(dir: &Path, stage: &str, action: &str) {
    vsdb_configure(VsdbOptions::new(dir)).unwrap();
    if stage == "register" {
        let handles = dir.join("registered-child");
        if action == "seed" {
            let mut parent = DagMapRaw::new(None);
            let sequence = parent.meta().unwrap().next_edge_id;
            let reader = parent.store.reader();
            let before = reader.read_view();
            let child = DagMapRaw::new(Some(&mut parent));
            let previous: NodeMeta =
                postcard::from_bytes(&before.get(META).unwrap()).unwrap();
            assert_eq!(previous.next_edge_id, sequence);
            assert!(!before.contains_key(edge_key(sequence)));
            assert_eq!(parent.meta().unwrap().next_edge_id, sequence + 1);
            assert_eq!(parent.edge_target(sequence), Some(child.id()));
            assert_eq!(
                child.meta().unwrap().state.owner(),
                Some(Parent {
                    map: parent.id(),
                    edge: sequence
                })
            );
            let saved = (
                parent.save_meta().unwrap(),
                child.save_meta().unwrap(),
                sequence,
            );
            fs::write(handles, postcard::to_allocvec(&saved).unwrap()).unwrap();
            parent.namespace().try_sync_wal().unwrap();
            process::exit(0);
        }
        let (parent, child, edge): (InstanceId, InstanceId, u64) =
            postcard::from_bytes(&fs::read(handles).unwrap()).unwrap();
        let mut parent = DagMapRaw::from_meta(parent).unwrap();
        let child = DagMapRaw::from_meta(child).unwrap();
        assert_eq!(parent.child_id(&child), Some(token(edge)));
        assert_eq!(parent.meta().unwrap().next_edge_id, edge + 1);
        let next = DagMapRaw::new(Some(&mut parent));
        assert_eq!(parent.child_id(&next), Some(token(edge + 1)));
        assert_eq!(parent.child_id(&child), Some(token(edge)));
        return;
    }
    let handles_path = dir.join("dag-wal-handles");
    if action == "seed" {
        let ns = Namespace::create_with(NamespaceOpts {
            path: None,
            shards: 4,
            mem_budget_mb: Some(64),
        })
        .unwrap();
        let (genesis, intermediate, mut head) = ns.scope(build_prune_fixture);
        let mut survivor = DagMapRaw::new(Some(&mut head));
        survivor.insert("survivor", "value");
        let mut unrelated = MapxRaw::new_in(&ns);
        unrelated.insert(b"unrelated", b"preserved");
        let ids = [
            genesis.save_meta().unwrap(),
            intermediate.save_meta().unwrap(),
            head.save_meta().unwrap(),
            survivor.save_meta().unwrap(),
            unrelated.save_meta().unwrap(),
        ];
        fs::write(&handles_path, postcard::to_allocvec(&ids).unwrap()).unwrap();
        ns.try_sync_wal().unwrap();
        let before = ns.shard_properties("stats.flushes_completed");
        assert!(before.iter().all(Option::is_some));

        if stage == "destroy" {
            // Exercise the production prune and destroy paths, including
            // durable range tombstones, without flushing unrelated data.
            let mut merged = head.prune().unwrap();
            merged.destroy();
            ns.try_sync_wal().unwrap();
        } else {
            let (mut line, mut root) = head.mainline().unwrap();
            DagMapRaw::fold(&line, &mut root).unwrap();
            if stage != "merge" {
                line[0].reparent(&mut root).unwrap();
            }
            if matches!(stage, "marker" | "clear") {
                DagMapRaw::mark(&mut line, root.id()).unwrap();
            }
            if stage == "clear" {
                DagMapRaw::clear_line(&mut line, &mut root).unwrap();
            }
            root.store.try_sync_wal().unwrap();
        }
        assert_eq!(ns.shard_properties("stats.flushes_completed"), before);
        // Deliberately skip destructors and their engine flushes. This
        // tests process-exit recovery after fences, not physical power loss.
        process::exit(0);
    }

    let ids: [InstanceId; 5] =
        postcard::from_bytes(&fs::read(handles_path).unwrap()).unwrap();
    let genesis = DagMapRaw::from_meta(ids[0]).unwrap();
    let intermediate = DagMapRaw::from_meta(ids[1]).unwrap();
    let head = DagMapRaw::from_meta(ids[2]).unwrap();
    let survivor = DagMapRaw::from_meta(ids[3]).unwrap();
    let unrelated = MapxRaw::from_meta(ids[4]).unwrap();
    let ns = genesis.namespace();
    let before = ns.shard_properties("stats.flushes_completed");
    assert_eq!(unrelated.get(b"unrelated").unwrap(), b"preserved");
    if stage == "destroy" {
        for node in [&genesis, &intermediate, &head, &survivor] {
            assert!(node.is_dead());
            assert!(node.get("k0").is_none());
            assert!(node.get("survivor").is_none());
        }
    } else {
        assert_merged_view(&genesis);
        assert_merged_view(&survivor);
        assert_eq!(survivor.get("survivor").unwrap(), b"value");
        let recovered = head.prune().unwrap();
        assert_eq!(recovered.instance_id(), genesis.instance_id());
        assert_merged_view(&recovered);
        assert_merged_view(&survivor);
        assert!(intermediate.is_dead());
        assert!(DagMapRaw::from_meta(ids[2]).unwrap().is_dead());
        assert_eq!(recovered.child_ids().len(), 1);
    }
    assert_eq!(ns.shard_properties("stats.flushes_completed"), before);
}

#[test]
fn dag_wal_barriers_survive_process_exit_without_table_flush() {
    if let Ok(action) = env::var("VSDB_DAG_WAL_PHASE") {
        dag_wal_phase(
            Path::new(&env::var_os("VSDB_DAG_WAL_DIR").unwrap()),
            &env::var("VSDB_DAG_WAL_STAGE").unwrap(),
            &action,
        );
        return;
    }
    for stage in [
        "register", "merge", "reparent", "marker", "clear", "destroy",
    ] {
        let dir =
            env::temp_dir().join(format!("vsdb-dag-wal-{}", rand::random::<u128>()));
        for action in ["seed", "recover"] {
            let result = Command::new(env::current_exe().unwrap())
                .args([
                    "--exact",
                    "dagmap::raw::test::dag_wal_barriers_survive_process_exit_without_table_flush",
                    "--nocapture",
                ])
                .env("VSDB_BASE_DIR", &dir)
                .env("VSDB_DAG_WAL_PHASE", action)
                .env("VSDB_DAG_WAL_STAGE", stage)
                .env("VSDB_DAG_WAL_DIR", &dir)
                .output()
                .unwrap();
            assert!(result.status.success(), "{stage}/{action}: {result:?}");
        }
        fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn typed_graph_copy_is_independent_across_namespaces() {
    let mut root = crate::DagMap::<String, ()>::new(None);
    let key = "key".to_string();
    root.insert(&key, &());
    let mut child = crate::DagMap::new(Some(&mut root));
    assert_eq!(child.get(&key), Some(()));
    child.remove(&key);
    let ns = crate::Namespace::create().unwrap();
    let mut copy = root.clone_in(&ns).unwrap();
    assert_eq!(copy.namespace().id(), ns.id());
    assert_eq!(copy.child_ids(), root.child_ids());
    copy.destroy();
    assert_eq!(root.get(&key), Some(()));
    assert_eq!(child.get(&key), None);
    let restored =
        crate::DagMap::<String, ()>::from_meta(root.save_meta().unwrap()).unwrap();
    assert!(restored.is_the_same_instance(&root));
    assert!(crate::DagMap::<u64, ()>::from_meta(root.instance_id()).is_err());
}

#[test]
fn clearing_retry_removes_duplicate_discovery_edges() {
    let (mut root, mid, mut head) = build_prune_fixture();
    let survivor = DagMapRaw::new(Some(&mut head));
    prepare_marked_clear(&mut head);
    root.store.insert(edge_key(987), mid.id().to_le_bytes());
    let recovered = head.prune().unwrap();
    assert!(mid.is_dead());
    assert_eq!(
        recovered.child_ids(),
        [recovered.child_id(&survivor).unwrap()]
    );
}

#[test]
fn intermediate_marker_reclaims_marked_descendants_and_preserves_retry_target() {
    let (root, mut mid, mut head) = build_prune_fixture();
    let survivor = DagMapRaw::new(Some(&mut head));
    prepare_marked_clear(&mut head);
    assert_eq!(mid.open(mid.id()).unwrap().prune().unwrap().id(), root.id());
    assert!(head.is_dead());
    assert_merged_view(&survivor);
    mid.destroy();
    assert_eq!(head.prune().unwrap().id(), root.id());
}

#[test]
fn get_mut_and_update_reject_consumed_clearing_nodes() {
    let (_root, _mid, mut head) = build_prune_fixture();
    prepare_marked_clear(&mut head);
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = head.get_mut("head_only");
    }));
    assert!(res.is_err());

    let mut typed_root = crate::DagMap::<String, u64>::new(None);
    let mut typed_head = crate::DagMap::new(Some(&mut typed_root));
    let key = "k".to_string();
    typed_head.insert(&key, &1);
    let frame: Vec<u8> =
        crate::common::load_instance_meta(typed_head.save_meta().unwrap()).unwrap();
    let mut raw_head = DagMapRaw::from_frame(
        crate::common::parse_prefix_handle::<crate::DagMap<String, u64>>(&frame)
            .unwrap(),
    )
    .unwrap();
    let mut meta = raw_head.meta().unwrap();
    meta.state = State::Clearing {
        owner: meta.state.owner(),
        genesis: typed_root.instance_id().map_id,
    };
    raw_head.set_meta(meta).unwrap();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = typed_head.update(&key, |v| *v = 2);
    }));
    assert!(res.is_err());
}

thread_local! {
    static FAIL_META_COMMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) fn before_meta_commit() -> Result<()> {
    if FAIL_META_COMMIT.with(|flag| flag.replace(false)) {
        return Err(std::io::Error::other("injected DAG metadata write failure").into());
    }
    Ok(())
}

#[test]
fn prune_returns_marker_write_error_and_can_resume() {
    let mut root = DagMapRaw::new(None);
    let mut head = DagMapRaw::new(Some(&mut root));
    head.insert(b"edited", b"value");
    let saved = head.save_meta().unwrap();

    FAIL_META_COMMIT.with(|flag| flag.set(true));
    assert!(matches!(head.prune(), Err(VsdbError::Io(_))));
    let head = DagMapRaw::from_meta(saved).unwrap();
    assert!(root.child_id(&head).is_some());
    let merged = head.prune().unwrap();
    assert_eq!(merged.get(b"edited").as_deref(), Some(b"value".as_slice()));
    assert!(merged.child_ids().is_empty());
}

#[test]
fn partial_markers_and_intermediate_destroy_preserve_merged_state_and_retry() {
    // Case 1: interrupted mark pass left `head` in State::Clearing while `mid`
    // remained State::Child. Calling `mid.prune()` must finish clearing `[mid, head]`
    // without destroying `survivor` or re-folding stale `mid` values over `root`.
    let (root, mid, mut head) = build_prune_fixture();
    let mut survivor = DagMapRaw::new(Some(&mut head));
    survivor.insert("survivor", "value");
    {
        let (mut line, mut genesis) = head.open(head.id()).unwrap().mainline().unwrap();
        DagMapRaw::fold(&line, &mut genesis).unwrap();
        line[0].reparent(&mut genesis).unwrap();
        DagMapRaw::mark(&mut line[..1], root.id()).unwrap();
    }
    let recovered = mid.open(mid.id()).unwrap().prune().unwrap();
    assert_eq!(recovered.id(), root.id());
    assert_merged_view(&recovered);
    assert_merged_view(&survivor);
    assert_eq!(
        survivor.get("survivor").as_deref(),
        Some(b"value".as_slice())
    );
    assert!(mid.is_dead());
    assert!(head.is_dead());
    assert_eq!(head.prune().unwrap().id(), root.id());

    // Case 2: `mid.destroy()` before `clear_line()` must wipe `mid` and `head`
    // data/edges without erasing `head`'s State::Clearing retry target.
    let (root2, mut mid2, mut head2) = build_prune_fixture();
    let survivor2 = DagMapRaw::new(Some(&mut head2));
    prepare_marked_clear(&mut head2);
    mid2.destroy();
    assert!(mid2.is_dead());
    assert!(head2.is_dead());
    let recovered2 = head2.prune().unwrap();
    assert_eq!(recovered2.id(), root2.id());
    assert_merged_view(&recovered2);
    assert_merged_view(&survivor2);
}

#[test]
fn fallible_dagmap_operations_propagate_corrupt_rows_and_sst_errors() {
    let mut root = DagMapRaw::new(None);
    let mut head = DagMapRaw::new(Some(&mut root));
    let head_id = head.save_meta().unwrap();

    // Malformed data row tag (neither [0] nor [1, ..]) returns Corrupt on prune.
    head.store.insert([2, b'k'], [9, b'v']);
    assert!(matches!(head.prune(), Err(VsdbError::Corrupt { .. })));

    // Malformed edge value width returns Corrupt on prune and clone_in.
    let mut head = DagMapRaw::from_meta(head_id).unwrap();
    head.store.remove([2, b'k']);
    head.store.insert(edge_key(77), [1, 2, 3]);
    let dst = Namespace::create().unwrap();
    let dst_id = dst.id();
    assert!(matches!(
        head.clone_in(&dst),
        Err(VsdbError::Corrupt { .. })
    ));
    assert!(matches!(head.prune(), Err(VsdbError::Corrupt { .. })));
    drop(dst);
    Namespace::close_by_id(dst_id).unwrap();
    Namespace::destroy(dst_id).unwrap();
}
