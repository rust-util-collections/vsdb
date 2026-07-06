//! Insert path for the persistent B+ tree.
//!
//! Every insertion allocates new nodes along the path from leaf to root,
//! leaving previous versions intact (copy-on-write).

use super::{EMPTY_ROOT, InsertResult, MAX_KEYS, Node, NodeId, PersistentBTree};

impl PersistentBTree {
    /// Inserts `(key, value)`, returning the **new root**.
    ///
    /// The old root (and every version that references it) is unaffected.
    pub fn insert(&mut self, root: NodeId, key: &[u8], value: &[u8]) -> NodeId {
        let new_root = if root == EMPTY_ROOT {
            self.alloc(&Node::Leaf {
                keys: vec![key.to_vec()],
                values: vec![value.to_vec()],
            })
        } else {
            match self.insert_rec(root, key, value) {
                InsertResult::Updated(r) => r,
                InsertResult::Split { left, sep, right } => {
                    self.alloc(&Node::Internal {
                        keys: vec![sep],
                        children: vec![left, right],
                    })
                }
            }
        };
        // One engine write batch for the whole path-copy node group.
        self.flush_pending();
        new_root
    }

    fn insert_rec(&mut self, id: NodeId, key: &[u8], value: &[u8]) -> InsertResult {
        match self.node(id) {
            Node::Leaf { keys, values } => self.insert_leaf(keys, values, key, value),
            Node::Internal { keys, children } => {
                self.insert_internal(keys, children, key, value)
            }
        }
    }

    fn insert_leaf(
        &mut self,
        mut keys: Vec<Vec<u8>>,
        mut values: Vec<Vec<u8>>,
        key: &[u8],
        value: &[u8],
    ) -> InsertResult {
        match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Ok(i) => {
                values[i] = value.to_vec();
                InsertResult::Updated(self.alloc(&Node::Leaf { keys, values }))
            }
            Err(i) => {
                keys.insert(i, key.to_vec());
                values.insert(i, value.to_vec());
                if keys.len() <= MAX_KEYS {
                    InsertResult::Updated(self.alloc(&Node::Leaf { keys, values }))
                } else {
                    self.split_leaf(keys, values)
                }
            }
        }
    }

    fn split_leaf(
        &mut self,
        mut keys: Vec<Vec<u8>>,
        mut values: Vec<Vec<u8>>,
    ) -> InsertResult {
        let mid = keys.len() / 2;
        let rk = keys.split_off(mid);
        let rv = values.split_off(mid);
        let sep = rk[0].clone();
        InsertResult::Split {
            left: self.alloc(&Node::Leaf { keys, values }),
            sep,
            right: self.alloc(&Node::Leaf {
                keys: rk,
                values: rv,
            }),
        }
    }

    fn insert_internal(
        &mut self,
        mut keys: Vec<Vec<u8>>,
        mut children: Vec<NodeId>,
        key: &[u8],
        value: &[u8],
    ) -> InsertResult {
        let ci = Self::child_index(&keys, key);
        match self.insert_rec(children[ci], key, value) {
            InsertResult::Updated(nc) => {
                children[ci] = nc;
                InsertResult::Updated(self.alloc(&Node::Internal { keys, children }))
            }
            InsertResult::Split { left, sep, right } => {
                children[ci] = left;
                keys.insert(ci, sep);
                children.insert(ci + 1, right);
                if keys.len() <= MAX_KEYS {
                    InsertResult::Updated(self.alloc(&Node::Internal { keys, children }))
                } else {
                    self.split_internal(keys, children)
                }
            }
        }
    }

    fn split_internal(
        &mut self,
        mut keys: Vec<Vec<u8>>,
        mut children: Vec<NodeId>,
    ) -> InsertResult {
        let mid = keys.len() / 2;
        let rk = keys.split_off(mid + 1);
        let sep = keys.pop().unwrap();
        let rc = children.split_off(mid + 1);
        InsertResult::Split {
            left: self.alloc(&Node::Internal { keys, children }),
            sep,
            right: self.alloc(&Node::Internal {
                keys: rk,
                children: rc,
            }),
        }
    }
}
