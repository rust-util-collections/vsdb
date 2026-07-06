//! Remove path for the persistent B+ tree.
//!
//! Every deletion allocates new nodes along the path from leaf to root,
//! with underflow handled via borrow-left, borrow-right, and merge.
//! Previous versions are left intact (copy-on-write).

use super::{EMPTY_ROOT, MIN_KEYS, Node, NodeId, PersistentBTree, RemoveResult};

impl PersistentBTree {
    /// Removes `key`, returning the **new root**.
    ///
    /// If the key is absent the original `root` is returned (no allocation).
    pub fn remove(&mut self, root: NodeId, key: &[u8]) -> NodeId {
        if root == EMPTY_ROOT {
            return EMPTY_ROOT;
        }
        let new_root = match self.remove_rec(root, key) {
            RemoveResult::NotFound => root,
            RemoveResult::Done(r) | RemoveResult::Underflow(r) => self.shrink_root(r),
        };
        // One engine write batch for the whole path-copy node group
        // (a NotFound flush is a no-op — nothing was allocated).
        self.flush_pending();
        new_root
    }

    fn shrink_root(&mut self, root: NodeId) -> NodeId {
        match self.node(root) {
            Node::Leaf { ref keys, .. } if keys.is_empty() => {
                self.discard_node(root);
                EMPTY_ROOT
            }
            Node::Internal {
                ref keys,
                ref children,
            } if keys.is_empty() => {
                let child = children[0];
                self.discard_node(root);
                child
            }
            _ => root,
        }
    }

    /// Discards a node that is no longer reachable from any live root.
    ///
    /// Only acts when the node's `ref_count` is zero (freshly allocated
    /// and never entered a live commit).  Shared nodes (`ref_count > 0`)
    /// are left intact — they are still owned by other versions.
    ///
    /// Undoes the ref_count increments that `alloc` applied to its
    /// children and registers the node for deferred disk deletion.
    pub(crate) fn discard_node(&mut self, nid: NodeId) {
        if !self.ref_counts_ready {
            return;
        }
        // Guard: only discard truly unreferenced nodes.  In merge
        // paths the discarded NodeId may be a shared sibling from a
        // prior version (ref_count > 0) — leave those alone.
        match self.ref_counts.get(&nid) {
            Some(nr) if nr.ref_count > 0 => return,
            None => return,
            _ => {}
        }
        let nr = self.ref_counts.remove(&nid).unwrap();
        for &child in &nr.children {
            if let Some(cr) = self.ref_counts.get_mut(&child) {
                debug_assert!(
                    cr.ref_count > 0,
                    "discard_node: child {child} already at ref_count=0"
                );
                cr.ref_count = cr.ref_count.saturating_sub(1);
            }
        }
        // Intra-operation churn (split/borrow/merge intermediates) is
        // still in the write buffer: drop it there and it never reaches
        // the engine.  Nodes from earlier operations (or flushed earlier
        // in bulk_load) are on disk and go through the usual deferred
        // deletion.  NodeIds are never reused, so a lazy-delete
        // registration can never shadow a future write of the same key.
        if self.pending.remove(&nid).is_none() {
            self.nodes
                .lazy_delete_batch(vec![nid.to_le_bytes().to_vec()]);
        }
    }

    fn remove_rec(&mut self, id: NodeId, key: &[u8]) -> RemoveResult {
        match self.node(id) {
            Node::Leaf { keys, values } => self.remove_leaf(keys, values, key),
            Node::Internal { keys, children } => {
                self.remove_internal(keys, children, key)
            }
        }
    }

    fn remove_leaf(
        &mut self,
        mut keys: Vec<Vec<u8>>,
        mut values: Vec<Vec<u8>>,
        key: &[u8],
    ) -> RemoveResult {
        let idx = match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Ok(i) => i,
            Err(_) => return RemoveResult::NotFound,
        };
        keys.remove(idx);
        values.remove(idx);
        let nid = self.alloc(&Node::Leaf {
            keys: keys.clone(),
            values,
        });
        if keys.len() >= MIN_KEYS {
            RemoveResult::Done(nid)
        } else {
            RemoveResult::Underflow(nid)
        }
    }

    fn remove_internal(
        &mut self,
        keys: Vec<Vec<u8>>,
        mut children: Vec<NodeId>,
        key: &[u8],
    ) -> RemoveResult {
        let ci = Self::child_index(&keys, key);
        match self.remove_rec(children[ci], key) {
            RemoveResult::NotFound => RemoveResult::NotFound,
            RemoveResult::Done(nc) => {
                children[ci] = nc;
                let nid = self.alloc(&Node::Internal { keys, children });
                RemoveResult::Done(nid)
            }
            RemoveResult::Underflow(nc) => {
                children[ci] = nc;
                self.fix_underflow(keys, children, ci)
            }
        }
    }

    fn fix_underflow(
        &mut self,
        mut keys: Vec<Vec<u8>>,
        mut children: Vec<NodeId>,
        ci: usize,
    ) -> RemoveResult {
        // Try borrow from left sibling.
        if ci > 0 && self.node(children[ci - 1]).key_count() > MIN_KEYS {
            self.borrow_left(&mut keys, &mut children, ci);
            let nid = self.alloc(&Node::Internal { keys, children });
            return RemoveResult::Done(nid);
        }
        // Try borrow from right sibling.
        if ci + 1 < children.len() && self.node(children[ci + 1]).key_count() > MIN_KEYS
        {
            self.borrow_right(&mut keys, &mut children, ci);
            let nid = self.alloc(&Node::Internal { keys, children });
            return RemoveResult::Done(nid);
        }
        // Merge (prefer left).
        let mi = if ci > 0 { ci - 1 } else { ci };
        self.merge_children(&mut keys, &mut children, mi);
        let nid = self.alloc(&Node::Internal {
            keys: keys.clone(),
            children,
        });
        if keys.len() >= MIN_KEYS {
            RemoveResult::Done(nid)
        } else {
            RemoveResult::Underflow(nid)
        }
    }

    // ----- borrow / merge -----

    fn borrow_left(&mut self, pk: &mut [Vec<u8>], pc: &mut [NodeId], ci: usize) {
        let si = ci - 1;
        let old_si = pc[si];
        let old_ci = pc[ci];
        let left = self.node(old_si);
        let child = self.node(old_ci);
        match (left, child) {
            (
                Node::Leaf {
                    keys: mut lk,
                    values: mut lv,
                },
                Node::Leaf {
                    keys: mut ck,
                    values: mut cv,
                },
            ) => {
                ck.insert(0, lk.pop().unwrap());
                cv.insert(0, lv.pop().unwrap());
                pk[si] = ck[0].clone();
                pc[si] = self.alloc(&Node::Leaf {
                    keys: lk,
                    values: lv,
                });
                pc[ci] = self.alloc(&Node::Leaf {
                    keys: ck,
                    values: cv,
                });
            }
            (
                Node::Internal {
                    keys: mut lk,
                    children: mut lc,
                },
                Node::Internal {
                    keys: mut ck,
                    children: mut cc,
                },
            ) => {
                ck.insert(0, pk[si].clone());
                cc.insert(0, lc.pop().unwrap());
                pk[si] = lk.pop().unwrap();
                pc[si] = self.alloc(&Node::Internal {
                    keys: lk,
                    children: lc,
                });
                pc[ci] = self.alloc(&Node::Internal {
                    keys: ck,
                    children: cc,
                });
            }
            _ => unreachable!(),
        }
        self.discard_node(old_si);
        self.discard_node(old_ci);
    }

    fn borrow_right(&mut self, pk: &mut [Vec<u8>], pc: &mut [NodeId], ci: usize) {
        let ri = ci + 1;
        let old_ci = pc[ci];
        let old_ri = pc[ri];
        let child = self.node(old_ci);
        let right = self.node(old_ri);
        match (child, right) {
            (
                Node::Leaf {
                    keys: mut ck,
                    values: mut cv,
                },
                Node::Leaf {
                    keys: mut rk,
                    values: mut rv,
                },
            ) => {
                ck.push(rk.remove(0));
                cv.push(rv.remove(0));
                pk[ci] = rk[0].clone();
                pc[ci] = self.alloc(&Node::Leaf {
                    keys: ck,
                    values: cv,
                });
                pc[ri] = self.alloc(&Node::Leaf {
                    keys: rk,
                    values: rv,
                });
            }
            (
                Node::Internal {
                    keys: mut ck,
                    children: mut cc,
                },
                Node::Internal {
                    keys: mut rk,
                    children: mut rc,
                },
            ) => {
                ck.push(pk[ci].clone());
                cc.push(rc.remove(0));
                pk[ci] = rk.remove(0);
                pc[ci] = self.alloc(&Node::Internal {
                    keys: ck,
                    children: cc,
                });
                pc[ri] = self.alloc(&Node::Internal {
                    keys: rk,
                    children: rc,
                });
            }
            _ => unreachable!(),
        }
        self.discard_node(old_ci);
        self.discard_node(old_ri);
    }

    fn merge_children(
        &mut self,
        pk: &mut Vec<Vec<u8>>,
        pc: &mut Vec<NodeId>,
        idx: usize,
    ) {
        let old_idx = pc[idx];
        let left = self.node(old_idx);
        let right = self.node(pc[idx + 1]);
        let sep = pk.remove(idx);

        let merged = match (left, right) {
            (
                Node::Leaf {
                    keys: mut lk,
                    values: mut lv,
                },
                Node::Leaf {
                    keys: rk,
                    values: rv,
                },
            ) => {
                lk.extend(rk);
                lv.extend(rv);
                Node::Leaf {
                    keys: lk,
                    values: lv,
                }
            }
            (
                Node::Internal {
                    keys: mut lk,
                    children: mut lc,
                },
                Node::Internal {
                    keys: rk,
                    children: rc,
                },
            ) => {
                lk.push(sep);
                lk.extend(rk);
                lc.extend(rc);
                Node::Internal {
                    keys: lk,
                    children: lc,
                }
            }
            _ => unreachable!(),
        };
        pc[idx] = self.alloc(&merged);
        let discarded = pc.remove(idx + 1);
        self.discard_node(old_idx);
        self.discard_node(discarded);
    }
}
