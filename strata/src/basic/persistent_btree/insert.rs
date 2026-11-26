//! Insert path for the persistent B+ tree.
//!
//! Every insertion allocates new nodes along the path from leaf to root,
//! leaving previous versions intact (copy-on-write).

use super::{
    EMPTY_ROOT, InsertResult, LeafVal, MAX_KEYS, Node, NodeId, PersistentBTree,
};

use crate::Result;

impl PersistentBTree {
    /// Inserts `(key, value)`, returning the **new root**.
    ///
    /// The old root (and every version that references it) is unaffected.
    pub fn insert(&mut self, root: NodeId, key: &[u8], value: &[u8]) -> NodeId {
        let new_root = self
            .try_insert_buffered(root, key, value)
            .expect("B+ tree insert failed");
        // One engine write batch for the whole path-copy node group.
        self.flush_pending();
        new_root
    }

    /// [`insert`](Self::insert) without draining the write buffer: the new
    /// path stays in `pending` so a multi-step operation can discard its
    /// own intermediate versions before they ever reach the engine. The
    /// caller must [`flush_pending`](Self::flush_pending) before returning.
    pub(crate) fn try_insert_buffered(
        &mut self,
        root: NodeId,
        key: &[u8],
        value: &[u8],
    ) -> Result<NodeId> {
        Ok(if root == EMPTY_ROOT {
            self.alloc(&Node::Leaf {
                keys: vec![key.to_vec()],
                values: vec![LeafVal::Bytes(value.to_vec())],
            })
        } else {
            match self.insert_rec(root, key, value)? {
                InsertResult::Unchanged => root,
                InsertResult::Updated(r) => r,
                InsertResult::Split { left, sep, right } => {
                    self.alloc(&Node::Internal {
                        keys: vec![sep],
                        children: vec![left, right],
                    })
                }
            }
        })
    }

    fn insert_rec(
        &mut self,
        id: NodeId,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertResult> {
        match self.try_node(id)? {
            Node::Leaf { keys, values } => self.insert_leaf(keys, values, key, value),
            Node::Internal { keys, children } => {
                self.insert_internal(keys, children, key, value)
            }
        }
    }

    fn insert_leaf(
        &mut self,
        mut keys: Vec<Vec<u8>>,
        mut values: Vec<LeafVal>,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertResult> {
        Ok(match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Ok(i) if self.try_leaf_val_eq(&values[i], value)? => InsertResult::Unchanged,
            Ok(i) => {
                values[i] = LeafVal::Bytes(value.to_vec());
                InsertResult::Updated(self.alloc(&Node::Leaf { keys, values }))
            }
            Err(i) => {
                keys.insert(i, key.to_vec());
                values.insert(i, LeafVal::Bytes(value.to_vec()));
                if keys.len() <= MAX_KEYS {
                    InsertResult::Updated(self.alloc(&Node::Leaf { keys, values }))
                } else {
                    self.split_leaf(keys, values)
                }
            }
        })
    }

    pub(super) fn split_leaf(
        &mut self,
        mut keys: Vec<Vec<u8>>,
        mut values: Vec<LeafVal>,
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
    ) -> Result<InsertResult> {
        let ci = Self::child_index(&keys, key);
        Ok(match self.insert_rec(children[ci], key, value)? {
            InsertResult::Unchanged => InsertResult::Unchanged,
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
        })
    }

    pub(super) fn split_internal(
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
