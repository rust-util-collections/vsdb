//! Remove path for the persistent B+ tree.
//!
//! Every deletion allocates new nodes along the path from leaf to root,
//! with underflow handled via borrow-left, borrow-right, and merge.
//! Previous versions are left intact (copy-on-write).

use super::{
    EMPTY_ROOT, LeafVal, MIN_KEYS, Node, NodeId, PersistentBTree, RemoveResult,
};
use crate::{Result, VsdbError};
use std::collections::HashSet;

fn mismatched_siblings() -> VsdbError {
    VsdbError::Corrupt {
        detail: "B+ tree siblings have different node kinds".into(),
    }
}

impl PersistentBTree {
    /// Removes `key`, returning the **new root**.
    ///
    /// If the key is absent the original `root` is returned (no allocation).
    pub fn remove(&mut self, root: NodeId, key: &[u8]) -> NodeId {
        let new_root = self
            .try_remove_buffered(root, key)
            .expect("B+ tree removal failed");
        // One engine write batch for the whole path-copy node group
        // (a NotFound flush is a no-op — nothing was allocated).
        self.flush_pending();
        new_root
    }

    /// Build the replacement root without flushing, propagating page read errors.
    pub(crate) fn try_remove_buffered(
        &mut self,
        root: NodeId,
        key: &[u8],
    ) -> Result<NodeId> {
        if root == EMPTY_ROOT {
            return Ok(EMPTY_ROOT);
        }
        match self.remove_rec(root, key)? {
            RemoveResult::NotFound => Ok(root),
            RemoveResult::Done(r) | RemoveResult::Underflow(r) => self.shrink_root(r),
        }
    }

    pub(super) fn shrink_root(&mut self, root: NodeId) -> Result<NodeId> {
        let empty = self.with_checked_page(root, |page| {
            (page.len == 0).then(|| if page.leaf { EMPTY_ROOT } else { page.child(0) })
        })?;
        match empty {
            None => Ok(root),
            Some(EMPTY_ROOT) => {
                self.discard_node(root, &[]);
                Ok(EMPTY_ROOT)
            }
            Some(child) => {
                self.discard_node(root, &[child]);
                Ok(child)
            }
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
    pub(crate) fn discard_node(&mut self, nid: NodeId, survivors: &[NodeId]) {
        let mut refs = self.runtime.refs.lock();
        if !refs.ready {
            // Even without a reconstructed on-disk ownership map, a
            // pending entry is known to have been allocated by this
            // operation and can be dropped safely before it is flushed.
            if let Some(raw) = self.pending.remove(&nid) {
                self.pending_bytes -= 9 + raw.len();
                if nid & super::CLASS_MASK == super::BLOB_LEAF {
                    let keep: HashSet<NodeId> = survivors
                        .iter()
                        .flat_map(|&s| self.pending_closure(s))
                        .collect();
                    for blob in Node::decode_children(&raw) {
                        if !keep.contains(&blob)
                            && let Some(braw) = self.pending.remove(&blob)
                        {
                            self.pending_bytes -= 9 + braw.len();
                        }
                    }
                }
            }
            return;
        }
        // Guard: only discard truly unreferenced nodes.  In merge
        // paths the discarded NodeId may be a shared sibling from a
        // prior version (ref_count > 0) — leave those alone.
        match refs.counts.get(&nid) {
            Some(nr) if *nr > 0 => return,
            None => return,
            _ => {}
        }
        refs.counts.remove(&nid).unwrap();
        let edges = refs.edges.remove(&nid).unwrap_or_default();
        let mut dead_ids = Vec::new();
        for &child in &edges {
            if child & super::CLASS_MASK == super::BLOB {
                dead_ids.extend(refs.release(child));
                continue;
            }
            if let Some(cr) = refs.counts.get_mut(&child) {
                debug_assert!(
                    *cr > 0,
                    "discard_node: child {child} already at ref_count=0"
                );
                *cr = cr.saturating_sub(1);
            }
        }
        dead_ids.push(nid);
        // Intra-operation churn (split/borrow/merge intermediates) is
        // still in the write buffer: drop it there and it never reaches
        // the engine.  Nodes from earlier operations (or flushed earlier
        // in bulk_load) are on disk and go through the usual deferred
        // deletion.  NodeIds are never reused, so a lazy-delete
        // registration can never shadow a future write of the same key.
        let mut durable_dead = Vec::new();
        let mut dead_internal = Vec::new();
        for id in dead_ids {
            if let Some(raw) = self.pending.remove(&id) {
                self.pending_bytes -= 9 + raw.len();
            } else {
                if id & super::CLASS_MASK == 0 {
                    dead_internal.push(id);
                }
                durable_dead.push(super::object_key(id).to_vec());
            }
        }
        let immediate = refs.defer_or_return(durable_dead);
        drop(refs);
        for id in dead_internal {
            self.runtime.page_cache.remove(id);
        }
        if !immediate.is_empty() {
            self.nodes.lazy_delete_batch(immediate);
        }
    }

    fn remove_rec(&mut self, id: NodeId, key: &[u8]) -> Result<RemoveResult> {
        match self.try_node(id)? {
            Node::Leaf { keys, values } => Ok(self.remove_leaf(keys, values, key)),
            Node::Internal { keys, children } => {
                self.remove_internal(keys, children, key)
            }
        }
    }

    fn remove_leaf(
        &mut self,
        mut keys: Vec<Vec<u8>>,
        mut values: Vec<LeafVal>,
        key: &[u8],
    ) -> RemoveResult {
        let idx = match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Ok(i) => i,
            Err(_) => return RemoveResult::NotFound,
        };
        keys.remove(idx);
        values.remove(idx);
        let underflow = keys.len() < MIN_KEYS;
        let nid = self.alloc(&Node::Leaf { keys, values });
        if underflow {
            RemoveResult::Underflow(nid)
        } else {
            RemoveResult::Done(nid)
        }
    }

    fn remove_internal(
        &mut self,
        keys: Vec<Vec<u8>>,
        mut children: Vec<NodeId>,
        key: &[u8],
    ) -> Result<RemoveResult> {
        let ci = Self::child_index(&keys, key);
        match self.remove_rec(children[ci], key)? {
            RemoveResult::NotFound => Ok(RemoveResult::NotFound),
            RemoveResult::Done(nc) => {
                children[ci] = nc;
                let nid = self.alloc(&Node::Internal { keys, children });
                Ok(RemoveResult::Done(nid))
            }
            RemoveResult::Underflow(nc) => {
                children[ci] = nc;
                self.fix_underflow(keys, children, ci)
            }
        }
    }

    pub(super) fn fix_underflow(
        &mut self,
        mut keys: Vec<Vec<u8>>,
        mut children: Vec<NodeId>,
        ci: usize,
    ) -> Result<RemoveResult> {
        if ci > 0
            && let Some(left) = self.donor(children[ci - 1])?
        {
            self.borrow_left(left, &mut keys, &mut children, ci)?;
            return Ok(RemoveResult::Done(
                self.alloc(&Node::Internal { keys, children }),
            ));
        }
        if ci + 1 < children.len()
            && let Some(right) = self.donor(children[ci + 1])?
        {
            self.borrow_right(right, &mut keys, &mut children, ci)?;
            return Ok(RemoveResult::Done(
                self.alloc(&Node::Internal { keys, children }),
            ));
        }
        // Merge (prefer left).
        let mi = if ci > 0 { ci - 1 } else { ci };
        self.merge_children(&mut keys, &mut children, mi)?;
        let underflow = keys.len() < MIN_KEYS;
        let nid = self.alloc(&Node::Internal { keys, children });
        Ok(if underflow {
            RemoveResult::Underflow(nid)
        } else {
            RemoveResult::Done(nid)
        })
    }

    fn donor(&self, id: NodeId) -> Result<Option<Node>> {
        self.with_checked_page(id, |page| {
            (page.len > MIN_KEYS).then(|| page.materialize())
        })
    }

    // ----- borrow / merge -----

    fn borrow_left(
        &mut self,
        left: Node,
        pk: &mut [Vec<u8>],
        pc: &mut [NodeId],
        ci: usize,
    ) -> Result<()> {
        let si = ci - 1;
        let old_si = pc[si];
        let old_ci = pc[ci];
        let child = self.try_node(old_ci)?;
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
            _ => return Err(mismatched_siblings()),
        }
        let surv = [pc[si], pc[ci]];
        self.discard_node(old_si, &surv);
        self.discard_node(old_ci, &surv);
        Ok(())
    }

    fn borrow_right(
        &mut self,
        right: Node,
        pk: &mut [Vec<u8>],
        pc: &mut [NodeId],
        ci: usize,
    ) -> Result<()> {
        let ri = ci + 1;
        let old_ci = pc[ci];
        let old_ri = pc[ri];
        let child = self.try_node(old_ci)?;
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
            _ => return Err(mismatched_siblings()),
        }
        let surv = [pc[ci], pc[ri]];
        self.discard_node(old_ci, &surv);
        self.discard_node(old_ri, &surv);
        Ok(())
    }

    fn merge_children(
        &mut self,
        pk: &mut Vec<Vec<u8>>,
        pc: &mut Vec<NodeId>,
        idx: usize,
    ) -> Result<()> {
        let old_idx = pc[idx];
        let left = self.try_node(old_idx)?;
        let right = self.try_node(pc[idx + 1])?;
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
            _ => return Err(mismatched_siblings()),
        };
        pc[idx] = self.alloc(&merged);
        let discarded = pc.remove(idx + 1);
        let surv = [pc[idx]];
        self.discard_node(old_idx, &surv);
        self.discard_node(discarded, &surv);
        Ok(())
    }
}
