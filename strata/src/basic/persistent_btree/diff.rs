//! Two-way diff between persistent B+ tree versions, skipping the
//! subtrees they share.

use super::{
    EMPTY_ROOT, LeafVal, MapxRawReader, Node, NodeId, PersistentBTree, read_object,
};
use std::cmp::Ordering;
use std::marker::PhantomData;

/// A single difference between two tree versions, as stored bytes
/// (see [`PersistentBTree::diff`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TreeDiff {
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

impl PersistentBTree {
    /// Every key added, removed, or modified between the versions rooted
    /// at `old_root` and `new_root`, in ascending key order.
    ///
    /// Versions share unchanged subtrees by `NodeId` (ids are never
    /// reused), and the walk skips every shared subtree. Work depends on
    /// unshared paths; independently built equal-content trees can still
    /// require a full walk. The output contains only changed keys.
    pub fn diff(&self, old_root: NodeId, new_root: NodeId) -> Vec<TreeDiff> {
        self.diff_iter(old_root, new_root).collect()
    }

    /// Streams the differences in ascending key order while pruning shared subtrees.
    /// Traversal retains at most a page of keys per level on each side.
    pub fn diff_iter(&self, old: NodeId, new: NodeId) -> BTreeDiffIter<'_> {
        self.owned_diff(old, new)
    }

    // Internal merge keeps all source roots live while it builds its result.
    pub(super) fn owned_diff(&self, old: NodeId, new: NodeId) -> BTreeDiffIter<'static> {
        let (old, new) = if old == new {
            (EMPTY_ROOT, EMPTY_ROOT)
        } else {
            (old, new)
        };
        BTreeDiffIter {
            nodes: self.nodes.reader(),
            borrow: PhantomData,
            old: Cursor::new(self, old),
            new: Cursor::new(self, new),
        }
    }

    /// Shared roots compare in constant time. Stop at the first changed entry.
    pub(crate) fn differs(&self, old: NodeId, new: NodeId) -> bool {
        if old == EMPTY_ROOT || new == EMPTY_ROOT {
            return old != new;
        }
        self.diff_iter(old, new).next().is_some()
    }
}

/// One not-yet-compared element of a snapshot, in key order.
enum Item {
    /// An unexpanded subtree and its height (0 = leaf).
    Node(NodeId, u32),
    Entry(Vec<u8>, LeafVal),
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
    fn expand(&mut self, nodes: &MapxRawReader) {
        let Some(Item::Node(id, h)) = self.0.pop() else {
            unreachable!("expand called on a non-node front");
        };
        match Node::decode(&read_object(nodes, id)) {
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

    fn pop_entry(&mut self) -> (Vec<u8>, LeafVal) {
        match self.0.pop() {
            Some(Item::Entry(k, v)) => (k, v),
            _ => unreachable!("pop_entry called on a non-entry front"),
        }
    }
}

fn materialize_val(val: LeafVal, nodes: &MapxRawReader) -> Vec<u8> {
    match val {
        LeafVal::Bytes(bytes) => bytes,
        LeafVal::Blob(id) => read_object(nodes, id),
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

/// Ordered streaming diff of two immutable B+ tree roots.
pub struct BTreeDiffIter<'a> {
    nodes: MapxRawReader,
    borrow: PhantomData<&'a PersistentBTree>,
    old: Cursor,
    new: Cursor,
}

impl Iterator for BTreeDiffIter<'_> {
    type Item = TreeDiff;
    fn next(&mut self) -> Option<TreeDiff> {
        let old = &mut self.old;
        let new = &mut self.new;
        let tree = &self.nodes;
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
                Step::Done => return None,
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
                    let (key, val) = old.pop_entry();
                    return Some(TreeDiff::Removed {
                        key,
                        value: materialize_val(val, tree),
                    });
                }
                Step::Added | Step::Compare(Ordering::Greater) => {
                    let (key, val) = new.pop_entry();
                    return Some(TreeDiff::Added {
                        key,
                        value: materialize_val(val, tree),
                    });
                }
                Step::Compare(Ordering::Equal) => {
                    let (key, old_val) = old.pop_entry();
                    let (_, new_val) = new.pop_entry();
                    if let (LeafVal::Blob(a), LeafVal::Blob(b)) = (&old_val, &new_val)
                        && a == b
                    {
                        continue;
                    }
                    let old_value = materialize_val(old_val, tree);
                    let new_value = materialize_val(new_val, tree);
                    if old_value != new_value {
                        return Some(TreeDiff::Modified {
                            key,
                            old_value,
                            new_value,
                        });
                    }
                }
            }
        }
    }
}
