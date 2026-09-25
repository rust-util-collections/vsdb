//!
//! Three-way merge for persistent B+ trees.
//!
//! # Merge semantics: source wins on conflicts
//!
//! Given an **ancestor** (common base), a **source** branch, and a
//! **target** branch, the merge compares each key across all three
//! snapshots and applies these rules:
//!
//! 1. **Non-conflict (single-sided change):** if only one side differs
//!    from the ancestor, keep that side's change.
//! 2. **Conflict (both sides changed the same key differently):**
//!    **source wins**.
//!
//! A deletion is treated as "assigning the empty value ∅".  This means
//! delete-vs-modify is also a regular conflict, so source-side delete
//! wins over target-side modify.
//!
//! ## Complete decision matrix
//!
//! | ancestor | source | target | result          | rationale                     |
//! |----------|--------|--------|-----------------|-------------------------------|
//! | A        | A      | A      | A (keep)        | no change on either side      |
//! | A        | **S**  | A      | **S**           | only source changed           |
//! | A        | A      | **T**  | **T**           | only target changed           |
//! | A        | **S**  | **S**  | **S**           | both changed to same value    |
//! | A        | **S**  | **T**  | **S** ⚠         | conflict → source wins        |
//! | A        | ∅      | A      | ∅ (delete)      | source deleted, target unchanged → source wins |
//! | A        | A      | ∅      | ∅ (delete)      | target deleted, source unchanged → target wins (no conflict) |
//! | A        | ∅      | **T**  | ∅ (delete) ⚠    | conflict → source wins (delete beats modify)  |
//! | A        | **S**  | ∅      | **S** ⚠         | conflict → source wins (modify beats delete)  |
//! | A        | ∅      | ∅      | ∅ (delete)      | both deleted                  |
//! | ∅        | **S**  | ∅      | **S**           | only source added             |
//! | ∅        | ∅      | **T**  | **T**           | only target added             |
//! | ∅        | **S**  | **S**  | **S**           | both added same value         |
//! | ∅        | **S**  | **T**  | **S** ⚠         | conflict → source wins        |
//!
//! Rows marked ⚠ are conflict cases (source wins). Non-⚠ rows are
//! single-sided or non-conflicting updates determined by ancestor
//! comparison. The caller controls priority by choosing which branch
//! to pass as `source` vs `target` in
//! [`VerMap::merge(source, target)`](crate::versioned::map::VerMap::merge).
//!

//! ## Implementation: replay the source delta onto the target
//!
//! Every row of the matrix reduces to one rule: a key whose source state
//! differs from the ancestor takes the source state (added, modified, or
//! deleted); every other key keeps the target state. The merge therefore
//! computes `diff(ancestor → source)` — which skips shared subtrees — and
//! applies exactly those keys to the target tree by copy-on-write. Replay
//! work and buffering follow the changed paths. Diff traversal depends on
//! structural sharing and can scan an entire unshared tree; untouched
//! subtrees of the target remain shared where the COW paths permit it.
//!

use std::collections::BTreeMap;

use super::{
    EMPTY_ROOT, NodeId, PersistentBTree,
    diff::{TreeDiff, diff_walk},
};

/// Performs a three-way merge.
///
/// Deletion is treated as "assigning ∅", so all conflicts — including
/// delete-vs-modify — are resolved uniformly: **source wins**.
///
/// See the [module-level documentation](self) for the full decision
/// matrix.
///
/// Like [`PersistentBTree::bulk_load`], a newly built result root is
/// returned **unowned**: the caller adopts it with
/// [`PersistentBTree::acquire_node`].
fn three_way_merge(
    tree: &mut PersistentBTree,
    ancestor_root: NodeId,
    source_root: NodeId,
    target_root: NodeId,
) -> NodeId {
    // Fast paths.
    if ancestor_root == source_root {
        // Source made no changes — result is target.
        return target_root;
    }
    if ancestor_root == target_root {
        // Target made no changes — fast-forward to source.
        return source_root;
    }
    if source_root == target_root {
        // Both sides converged to the same state.
        return source_root;
    }

    let changes = source_changes(tree, &[ancestor_root], source_root);
    replay(tree, target_root, changes)
}

/// Performs a three-way merge against one or more merge bases.
///
/// When multiple lowest common ancestors exist, keys whose base values differ
/// are treated as criss-cross conflicts; if source and target differ, source
/// wins.  Keys whose base values agree use the normal decision matrix.
///
/// A key whose bases disagree necessarily differs from at least one base
/// on the source side, so replaying the union of the per-base source
/// deltas yields exactly that rule.
fn three_way_merge_many_bases(
    tree: &mut PersistentBTree,
    ancestor_roots: &[NodeId],
    source_root: NodeId,
    target_root: NodeId,
) -> NodeId {
    if ancestor_roots.len() <= 1 {
        return three_way_merge(
            tree,
            ancestor_roots.first().copied().unwrap_or(EMPTY_ROOT),
            source_root,
            target_root,
        );
    }

    if source_root == target_root {
        return source_root;
    }

    let changes = source_changes(tree, ancestor_roots, source_root);
    replay(tree, target_root, changes)
}

/// Every key where `source` differs from any of `bases`, mapped to the
/// source state (`None` = absent), in ascending key order.
fn source_changes(
    tree: &PersistentBTree,
    bases: &[NodeId],
    source: NodeId,
) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    let mut changes = BTreeMap::new();
    for &base in bases {
        diff_walk(tree, base, source, |entry| match entry {
            TreeDiff::Added { key, value }
            | TreeDiff::Modified {
                key,
                new_value: value,
                ..
            } => {
                changes.insert(key, Some(value));
            }
            TreeDiff::Removed { key, .. } => {
                changes.insert(key, None);
            }
        });
    }
    changes
}

/// Applies `changes` to `target_root` by copy-on-write and returns the
/// resulting root (unowned when newly built; `target_root` itself when
/// nothing changed).
///
/// All steps share one write buffer: each intermediate version is held
/// only until the next one exists, and its superseded nodes are dropped
/// from the buffer before they reach the engine — only nodes reachable
/// from the final root are written.
fn replay(
    tree: &mut PersistentBTree,
    target_root: NodeId,
    changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
) -> NodeId {
    let mut root = target_root;
    for (key, state) in changes {
        // Each key is applied once, so `root` still holds the target
        // state for it.
        if tree.get(root, &key) == state {
            continue;
        }
        let next = match &state {
            Some(value) => tree.insert_buffered(root, &key, value),
            None => tree.remove_buffered(root, &key),
        };
        if next == root {
            continue;
        }
        tree.acquire_node(next);
        if root != target_root {
            tree.release_buffered(root);
        }
        root = next;
        tree.flush_pending_if_large();
    }
    tree.flush_pending();
    if root != target_root {
        tree.disown_node(root);
    }
    root
}

impl PersistentBTree {
    /// Three-way merge of `source` into `target` against one or more merge
    /// bases, **source wins** on conflicts (a deletion counts as a change;
    /// see [`VerMap::merge`](crate::versioned::map::VerMap::merge) for the
    /// full decision table). With several bases, a key whose bases
    /// disagree takes the source state.
    ///
    /// The source-side delta is replayed onto `target` by copy-on-write:
    /// cost and memory follow the change set, and untouched subtrees stay
    /// shared with `target`. A newly built result root is returned
    /// **unowned** (like [`bulk_load`](Self::bulk_load)) — adopt it with
    /// [`acquire_node`](Self::acquire_node); when nothing changes, an
    /// existing root (`target` or `source`) is returned as is.
    pub fn merge(&mut self, bases: &[NodeId], source: NodeId, target: NodeId) -> NodeId {
        three_way_merge_many_bases(self, bases, source, target)
    }
}
