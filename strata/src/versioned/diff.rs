//!
//! Two-way diff for persistent B+ tree snapshots.
//!
//! Given two B+ tree roots (`old` and `new`), produces a list of
//! [`DiffEntry`] describing every key that was added, removed, or
//! modified between the two snapshots.
//!
//! Snapshots share unchanged subtrees by `NodeId` (node ids are never
//! reused), so the walk skips every subtree the two sides have in
//! common: a diff costs O(changed keys × depth × fanout) node reads, not
//! O(size of the trees).
//!

use crate::basic::persistent_btree::{EMPTY_ROOT, Node, NodeId, PersistentBTree};
use std::cmp::Ordering;

/// A single difference between two snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiffEntry {
    /// Key was added (exists in `new` but not `old`).
    Added { key: Vec<u8>, value: Vec<u8> },
    /// Key was removed (exists in `old` but not `new`).
    Removed { key: Vec<u8>, value: Vec<u8> },
    /// Key exists in both but with different values.
    Modified {
        key: Vec<u8>,
        old_value: Vec<u8>,
        new_value: Vec<u8>,
    },
}

/// Computes the diff between two B+ tree snapshots.
///
/// Returns a list of [`DiffEntry`] in ascending key order, describing
/// every key that was added, removed, or modified between `old_root`
/// and `new_root`.
pub fn diff_roots(
    tree: &PersistentBTree,
    old_root: NodeId,
    new_root: NodeId,
) -> Vec<DiffEntry> {
    let mut result = Vec::new();
    diff_walk(tree, old_root, new_root, |e| result.push(e));
    result
}

/// One not-yet-compared element of a snapshot, in key order.
enum Item {
    /// An unexpanded subtree and its height (0 = leaf).
    Node(NodeId, u32),
    Entry(Vec<u8>, Vec<u8>),
}

/// A snapshot's pending items, smallest key last (a stack).
struct Cursor(Vec<Item>);

impl Cursor {
    fn new(tree: &PersistentBTree, root: NodeId) -> Self {
        if root == EMPTY_ROOT {
            Self(Vec::new())
        } else {
            Self(vec![Item::Node(root, tree.height(root))])
        }
    }

    /// Replaces the front node with its children (or a leaf's entries).
    fn expand(&mut self, tree: &PersistentBTree) {
        let Some(Item::Node(id, h)) = self.0.pop() else {
            unreachable!("expand called on a non-node front");
        };
        match tree.node(id) {
            Node::Leaf { keys, values } => {
                self.0.extend(
                    keys.into_iter()
                        .zip(values)
                        .rev()
                        .map(|(k, v)| Item::Entry(k, v)),
                );
            }
            Node::Internal { children, .. } => {
                let ch = h.saturating_sub(1);
                self.0
                    .extend(children.into_iter().rev().map(|c| Item::Node(c, ch)));
            }
        }
    }

    fn pop_entry(&mut self) -> (Vec<u8>, Vec<u8>) {
        match self.0.pop() {
            Some(Item::Entry(k, v)) => (k, v),
            _ => unreachable!("pop_entry called on a non-entry front"),
        }
    }
}

enum Step {
    Done,
    SkipShared,
    ExpandOld,
    ExpandNew,
    ExpandBoth,
    Removed,
    Added,
    Compare(Ordering),
}

/// Streams the diff between two snapshots to `emit`, in ascending key
/// order, skipping every subtree the snapshots share.
pub(crate) fn diff_walk(
    tree: &PersistentBTree,
    old_root: NodeId,
    new_root: NodeId,
    mut emit: impl FnMut(DiffEntry),
) {
    if old_root == new_root {
        return;
    }
    let mut old = Cursor::new(tree, old_root);
    let mut new = Cursor::new(tree, new_root);

    loop {
        // Identical fronts cover identical key sets (every earlier key is
        // already consumed on both sides), so they are skipped whole.
        // Otherwise the taller subtree is expanded first so both sides
        // descend in step and shared subtrees line up.
        let step = match (old.0.last(), new.0.last()) {
            (None, None) => Step::Done,
            (Some(Item::Node(a, _)), Some(Item::Node(b, _))) if a == b => {
                Step::SkipShared
            }
            (Some(Item::Node(_, ha)), Some(Item::Node(_, hb))) => match ha.cmp(hb) {
                Ordering::Greater => Step::ExpandOld,
                Ordering::Less => Step::ExpandNew,
                Ordering::Equal => Step::ExpandBoth,
            },
            (Some(Item::Node(..)), _) => Step::ExpandOld,
            (_, Some(Item::Node(..))) => Step::ExpandNew,
            (Some(Item::Entry(..)), None) => Step::Removed,
            (None, Some(Item::Entry(..))) => Step::Added,
            (Some(Item::Entry(ka, _)), Some(Item::Entry(kb, _))) => {
                Step::Compare(ka.cmp(kb))
            }
        };
        match step {
            Step::Done => break,
            Step::SkipShared => {
                old.0.pop();
                new.0.pop();
            }
            Step::ExpandOld => old.expand(tree),
            Step::ExpandNew => new.expand(tree),
            Step::ExpandBoth => {
                old.expand(tree);
                new.expand(tree);
            }
            Step::Removed | Step::Compare(Ordering::Less) => {
                let (key, value) = old.pop_entry();
                emit(DiffEntry::Removed { key, value });
            }
            Step::Added | Step::Compare(Ordering::Greater) => {
                let (key, value) = new.pop_entry();
                emit(DiffEntry::Added { key, value });
            }
            Step::Compare(Ordering::Equal) => {
                let (key, old_value) = old.pop_entry();
                let (_, new_value) = new.pop_entry();
                if old_value != new_value {
                    emit(DiffEntry::Modified {
                        key,
                        old_value,
                        new_value,
                    });
                }
            }
        }
    }
}
