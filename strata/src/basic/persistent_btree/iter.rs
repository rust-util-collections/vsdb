//! Forward iterator over entries in a persistent B+ tree.
//!
//! Uses an explicit ancestor stack — no sibling pointers needed.

use std::ops::Bound;

use super::{EMPTY_ROOT, LeafState, Node, NodeId, PersistentBTree};

/// A forward iterator over entries in a [`PersistentBTree`].
///
/// Uses an explicit ancestor stack — no sibling pointers needed.
pub struct BTreeIter<'a> {
    tree: &'a PersistentBTree,
    stack: Vec<(Node, usize)>,
    leaf: Option<LeafState>,
    hi: Bound<Vec<u8>>,
    done: bool,
}

impl<'a> BTreeIter<'a> {
    pub(crate) fn new(
        tree: &'a PersistentBTree,
        root: NodeId,
        lo: Bound<Vec<u8>>,
        hi: Bound<Vec<u8>>,
    ) -> Self {
        let mut it = Self {
            tree,
            stack: Vec::with_capacity(8),
            leaf: None,
            hi,
            done: root == EMPTY_ROOT,
        };
        if !it.done {
            it.seek(root, &lo);
        }
        it
    }

    fn seek(&mut self, id: NodeId, lo: &Bound<Vec<u8>>) {
        let mut cur = id;
        loop {
            let node = self.tree.node(cur);
            match &node {
                Node::Internal { keys, children } => {
                    let ci = match lo {
                        Bound::Unbounded => 0,
                        Bound::Included(k) | Bound::Excluded(k) => {
                            match keys.binary_search_by(|x| x.as_slice().cmp(k)) {
                                Ok(i) => i + 1,
                                Err(i) => i,
                            }
                        }
                    };
                    let child = children[ci];
                    self.stack.push((node, ci + 1));
                    cur = child;
                }
                Node::Leaf { keys, values } => {
                    let start = match lo {
                        Bound::Unbounded => 0,
                        Bound::Included(k) => keys
                            .binary_search_by(|x| x.as_slice().cmp(k))
                            .unwrap_or_else(|i| i),
                        Bound::Excluded(k) => {
                            match keys.binary_search_by(|x| x.as_slice().cmp(k)) {
                                Ok(i) => i + 1,
                                Err(i) => i,
                            }
                        }
                    };
                    if start < keys.len() {
                        self.leaf = Some((keys.clone(), values.clone(), start));
                    } else {
                        self.advance_leaf();
                    }
                    return;
                }
            }
        }
    }

    fn advance_leaf(&mut self) {
        self.leaf = None;
        while let Some((node, next_ci)) = self.stack.last_mut() {
            if let Node::Internal { children, .. } = node
                && *next_ci < children.len()
            {
                let child_id = children[*next_ci];
                *next_ci += 1;
                self.descend_leftmost(child_id);
                return;
            }
            self.stack.pop();
        }
        self.done = true;
    }

    fn descend_leftmost(&mut self, id: NodeId) {
        let mut cur = id;
        loop {
            let node = self.tree.node(cur);
            match &node {
                Node::Internal { children, .. } => {
                    let child = children[0];
                    self.stack.push((node, 1));
                    cur = child;
                }
                Node::Leaf { keys, values } => {
                    if keys.is_empty() {
                        self.advance_leaf();
                    } else {
                        self.leaf = Some((keys.clone(), values.clone(), 0));
                    }
                    return;
                }
            }
        }
    }
}

impl Iterator for BTreeIter<'_> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.done {
                return None;
            }
            if let Some((ref keys, ref values, ref mut pos)) = self.leaf {
                if *pos < keys.len() {
                    let key = &keys[*pos];
                    let within = match &self.hi {
                        Bound::Unbounded => true,
                        Bound::Included(h) => key.as_slice() <= h.as_slice(),
                        Bound::Excluded(h) => key.as_slice() < h.as_slice(),
                    };
                    if !within {
                        self.done = true;
                        return None;
                    }
                    let kv = (key.clone(), values[*pos].clone());
                    *pos += 1;
                    return Some(kv);
                }
            } else {
                self.done = true;
                return None;
            }
            // Leaf exhausted — advance.
            self.advance_leaf();
        }
    }
}
