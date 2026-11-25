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

use super::{
    EMPTY_ROOT, InsertResult, MAX_KEYS, MIN_KEYS, Node, NodeId, PersistentBTree,
    RemoveResult, diff::TreeDiff,
};
use crate::Result;

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
) -> Result<NodeId> {
    // Fast paths.
    if ancestor_root == source_root {
        // Source made no changes — result is target.
        return Ok(target_root);
    }
    if ancestor_root == target_root {
        // Target made no changes — fast-forward to source.
        return Ok(source_root);
    }
    if source_root == target_root {
        // Both sides converged to the same state.
        return Ok(source_root);
    }

    let changes = source_changes(tree, &[ancestor_root], source_root);
    tree.apply_changes(target_root, changes, true)
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
) -> Result<NodeId> {
    if ancestor_roots.len() <= 1 {
        return three_way_merge(
            tree,
            ancestor_roots.first().copied().unwrap_or(EMPTY_ROOT),
            source_root,
            target_root,
        );
    }

    if source_root == target_root {
        return Ok(source_root);
    }

    let changes = source_changes(tree, ancestor_roots, source_root);
    tree.apply_changes(target_root, changes, true)
}

/// Every key where `source` differs from any of `bases`, mapped to the
/// source state (`None` = absent), in ascending key order.
fn source_changes(
    tree: &PersistentBTree,
    bases: &[NodeId],
    source: NodeId,
) -> impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)> + use<> {
    let mut streams: Vec<_> = bases
        .iter()
        .map(|&base| {
            tree.owned_diff(base, source)
                .map(|entry| match entry {
                    TreeDiff::Added { key, value }
                    | TreeDiff::Modified {
                        key,
                        new_value: value,
                        ..
                    } => (key, Some(value)),
                    TreeDiff::Removed { key, .. } => (key, None),
                })
                .peekable()
        })
        .collect();
    std::iter::from_fn(move || {
        let index = streams
            .iter_mut()
            .enumerate()
            .filter_map(|(i, s)| s.peek().map(|(k, _)| (i, k)))
            .min_by(|(_, a), (_, b)| a.cmp(b))
            .map(|(i, _)| i)?;
        let edit = streams[index].next().unwrap();
        for (i, s) in streams.iter_mut().enumerate() {
            if i != index {
                s.next_if(|(k, _)| k == &edit.0);
            }
        }
        Some(edit)
    })
}

/// Applies `changes` to `target_root` by copy-on-write and returns the
/// resulting root (unowned when newly built; `target_root` itself when
/// nothing changed).
///
/// All steps share one write buffer: each intermediate version is held
/// only until the next one exists, and its superseded nodes are dropped
/// from the buffer before they reach the engine — only nodes reachable
/// from the final root are written.
impl PersistentBTree {
    pub(crate) fn apply_changes(
        &mut self,
        target_root: NodeId,
        changes: impl IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
        chunked: bool,
    ) -> Result<NodeId> {
        let mut root = target_root;
        let mut changes = changes.into_iter().peekable();
        while changes.peek().is_some() {
            let next = self.apply_leaf_run(root, &mut changes);
            if next == root {
                continue;
            }
            self.acquire_node(next);
            if root != target_root {
                self.release_buffered(root);
                // Restored trees have no ref counts yet. Drop this intermediate
                // from the buffer anyway so the flush writes only `next`.
                self.drop_untracked_pending(root, next);
            }
            root = next;
            if chunked {
                self.flush_pending_if_large()?;
            }
        }
        self.flush_pending();
        if root != target_root {
            self.disown_node(root);
        }
        Ok(root)
    }

    /// Descend once for the sorted edits in a leaf's routing interval.
    /// Rebalance immediately when a removal crosses minimum occupancy.
    fn apply_leaf_run(
        &mut self,
        root: NodeId,
        changes: &mut std::iter::Peekable<
            impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
        >,
    ) -> NodeId {
        let first = &changes.peek().unwrap().0;
        let mut path = Vec::new();
        let mut upper = None;
        let mut id = root;
        let (mut keys, mut values) = loop {
            if id == EMPTY_ROOT {
                break (Vec::new(), Vec::new());
            }
            match self.node(id) {
                Node::Leaf { keys, values } => break (keys, values),
                Node::Internal { keys, children } => {
                    let ci = Self::child_index(&keys, first);
                    if let Some(end) = keys.get(ci) {
                        upper = Some(end.clone());
                    }
                    id = children[ci];
                    path.push((keys, children, ci));
                }
            }
        };
        let mut changed = false;
        for _ in 0..MAX_KEYS {
            let Some((key, value)) =
                changes.next_if(|(k, _)| upper.as_ref().is_none_or(|end| k < end))
            else {
                break;
            };
            let at = keys.binary_search(&key);
            match (at, value) {
                (Ok(i), Some(v)) if values[i] != v => {
                    values[i] = v;
                    changed = true;
                }
                (Err(i), Some(v)) => {
                    keys.insert(i, key);
                    values.insert(i, v);
                    changed = true;
                }
                (Ok(i), None) => {
                    keys.remove(i);
                    values.remove(i);
                    changed = true;
                    if !path.is_empty() && keys.len() < MIN_KEYS {
                        break;
                    }
                }
                _ => {}
            }
        }
        if !changed {
            return root;
        }
        if keys.is_empty() && path.is_empty() {
            return EMPTY_ROOT;
        }
        let mut underfull = keys.len() < MIN_KEYS;
        let mut change = if keys.len() > MAX_KEYS {
            self.split_leaf(keys, values)
        } else {
            InsertResult::Updated(self.alloc(&Node::Leaf { keys, values }))
        };
        while let Some((mut keys, mut children, ci)) = path.pop() {
            match change {
                InsertResult::Unchanged => unreachable!(),
                InsertResult::Updated(child) => {
                    children[ci] = child;
                    if underfull {
                        let (nid, short) = match self.fix_underflow(keys, children, ci) {
                            RemoveResult::Underflow(nid) => (nid, true),
                            RemoveResult::Done(nid) => (nid, false),
                            RemoveResult::NotFound => unreachable!(),
                        };
                        underfull = short;
                        change = InsertResult::Updated(nid);
                        continue;
                    }
                }
                InsertResult::Split { left, sep, right } => {
                    children[ci] = left;
                    keys.insert(ci, sep);
                    children.insert(ci + 1, right);
                }
            }
            underfull = false;
            change = if keys.len() > MAX_KEYS {
                self.split_internal(keys, children)
            } else {
                InsertResult::Updated(self.alloc(&Node::Internal { keys, children }))
            };
        }
        match change {
            InsertResult::Updated(id) if underfull => self.shrink_root(id),
            InsertResult::Updated(id) => id,
            InsertResult::Split { left, sep, right } => self.alloc(&Node::Internal {
                keys: vec![sep],
                children: vec![left, right],
            }),
            InsertResult::Unchanged => unreachable!(),
        }
    }

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
        self.try_merge(bases, source, target)
            .expect("B+ tree merge failed")
    }

    pub(crate) fn try_merge(
        &mut self,
        bases: &[NodeId],
        source: NodeId,
        target: NodeId,
    ) -> Result<NodeId> {
        three_way_merge_many_bases(self, bases, source, target)
    }
}
