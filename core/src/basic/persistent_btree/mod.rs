//!
//! Persistent B+ Tree with copy-on-write structural sharing.
//!
//! Every mutation returns a new root [`NodeId`], leaving previous versions
//! intact. Nodes live in a flat pool backed by [`MapxRaw`], so unchanged
//! subtrees are shared across versions at the node level.
//!
//! This data structure is analogous to Git's *tree object*: a single
//! [`NodeId`] is a complete, self-contained snapshot of an ordered map.
//!
//! # Design
//!
//! * **Branching factor** — each node holds between `B` and `2B` keys
//!   (except the root which may hold fewer). The default `B = 16` gives
//!   nodes of 16..32 keys and a tree depth of ~4 for 1 million entries.
//! * **Path copying** — inserting or removing a single key allocates at
//!   most `O(depth)` new nodes (~4), sharing all others.
//! * **Garbage collection** — unreachable nodes can be reclaimed via
//!   [`PersistentBTree::gc`].
//!

#[cfg(test)]
mod test;

use crate::basic::mapx_raw::MapxRaw;
use serde::{Deserialize, Serialize};
use std::ops::Bound;

// =========================================================================
// Public types
// =========================================================================

/// Identifies a single node inside a [`PersistentBTree`].
///
/// A root `NodeId` is a complete, self-contained snapshot of a map —
/// analogous to a Git tree-object hash.
pub type NodeId = u64;

/// Sentinel: an empty tree has no root.
pub const EMPTY_ROOT: NodeId = 0;

// =========================================================================
// Constants
// =========================================================================

/// Half the maximum fan-out. Non-root nodes hold `B..=2B` keys.
const B: usize = 16;
/// Maximum keys per node.
const MAX_KEYS: usize = 2 * B;
/// Minimum keys for a non-root node.
const MIN_KEYS: usize = B;

// =========================================================================
// Node — serialised manually for zero external codec dependency
// =========================================================================

// Wire format (all multi-byte integers are big-endian):
//
//   tag: u8          0 = Leaf, 1 = Internal
//   n:   u32         number of keys
//
// Leaf   (tag=0):  for i in 0..n { key_len:u32 key:[u8] val_len:u32 val:[u8] }
// Internal(tag=1): for i in 0..n { key_len:u32 key:[u8] }
//                  for i in 0..=n { child:u64 }

const TAG_LEAF: u8 = 0;
const TAG_INTERNAL: u8 = 1;

#[derive(Clone, Debug)]
enum Node {
    Leaf {
        keys: Vec<Vec<u8>>,
        values: Vec<Vec<u8>>,
    },
    Internal {
        keys: Vec<Vec<u8>>,
        children: Vec<NodeId>,
    },
}

impl Node {
    fn key_count(&self) -> usize {
        match self {
            Node::Leaf { keys, .. } | Node::Internal { keys, .. } => keys.len(),
        }
    }

    // ---- encode ----

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);
        match self {
            Node::Leaf { keys, values } => {
                buf.push(TAG_LEAF);
                buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());
                for i in 0..keys.len() {
                    buf.extend_from_slice(&(keys[i].len() as u32).to_be_bytes());
                    buf.extend_from_slice(&keys[i]);
                    buf.extend_from_slice(&(values[i].len() as u32).to_be_bytes());
                    buf.extend_from_slice(&values[i]);
                }
            }
            Node::Internal { keys, children } => {
                buf.push(TAG_INTERNAL);
                buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());
                for k in keys {
                    buf.extend_from_slice(&(k.len() as u32).to_be_bytes());
                    buf.extend_from_slice(k);
                }
                for c in children {
                    buf.extend_from_slice(&c.to_be_bytes());
                }
            }
        }
        buf
    }

    // ---- decode ----

    fn decode(data: &[u8]) -> Self {
        let mut pos = 0;

        let tag = data[pos];
        pos += 1;

        let n = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        match tag {
            TAG_LEAF => {
                let mut keys = Vec::with_capacity(n);
                let mut values = Vec::with_capacity(n);
                for _ in 0..n {
                    let klen =
                        u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    keys.push(data[pos..pos + klen].to_vec());
                    pos += klen;
                    let vlen =
                        u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    values.push(data[pos..pos + vlen].to_vec());
                    pos += vlen;
                }
                Node::Leaf { keys, values }
            }
            TAG_INTERNAL => {
                let mut keys = Vec::with_capacity(n);
                for _ in 0..n {
                    let klen =
                        u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    keys.push(data[pos..pos + klen].to_vec());
                    pos += klen;
                }
                let mut children = Vec::with_capacity(n + 1);
                for _ in 0..=n {
                    let c = u64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    children.push(c);
                }
                Node::Internal { keys, children }
            }
            _ => panic!("PersistentBTree: corrupt node tag {tag}"),
        }
    }
}

// =========================================================================
// Insert / Remove result enums
// =========================================================================

enum InsertResult {
    Updated(NodeId),
    Split {
        left: NodeId,
        sep: Vec<u8>,
        right: NodeId,
    },
}

enum RemoveResult {
    NotFound,
    Done(NodeId),
    Underflow(NodeId),
}

// =========================================================================
// PersistentBTree
// =========================================================================

/// A persistent (copy-on-write) B+ tree backed by [`MapxRaw`].
///
/// All nodes live in a single flat pool keyed by [`NodeId`]. A "tree
/// version" is represented by a root [`NodeId`]; different versions share
/// unchanged subtrees automatically.
///
/// # Examples
///
/// ```
/// use vsdb_core::basic::persistent_btree::{PersistentBTree, EMPTY_ROOT};
/// use vsdb_core::vsdb_set_base_dir;
/// use std::fs;
///
/// let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
/// vsdb_set_base_dir(&dir).unwrap();
///
/// let mut tree = PersistentBTree::new();
///
/// // Version 1: insert two entries.
/// let v1 = tree.insert(EMPTY_ROOT, b"alice", b"100");
/// let v1 = tree.insert(v1, b"bob", b"200");
///
/// // Version 2: fork from v1, modify one entry.
/// let v2 = tree.insert(v1, b"alice", b"150");
///
/// // Both versions coexist — structural sharing keeps cost low.
/// assert_eq!(tree.get(v1, b"alice").unwrap(), b"100");
/// assert_eq!(tree.get(v2, b"alice").unwrap(), b"150");
/// assert_eq!(tree.get(v2, b"bob").unwrap(), b"200");
///
/// fs::remove_dir_all(&dir).unwrap();
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistentBTree {
    /// Flat node pool.  Key = big-endian NodeId, Value = encoded Node.
    nodes: MapxRaw,
    /// Next node ID to allocate (monotonically increasing).
    next_id: NodeId,
}

impl PersistentBTree {
    /// Creates a new, empty persistent B+ tree.
    pub fn new() -> Self {
        Self {
            nodes: MapxRaw::new(),
            next_id: 1, // 0 is EMPTY_ROOT sentinel
        }
    }

    // ----- low-level helpers -----

    fn alloc(&mut self, node: &Node) -> NodeId {
        let id = self.next_id;
        self.next_id += 1;
        self.nodes.insert(&id.to_be_bytes(), &node.encode());
        id
    }

    fn node(&self, id: NodeId) -> Node {
        let raw = self
            .nodes
            .get(&id.to_be_bytes())
            .unwrap_or_else(|| panic!("PersistentBTree: missing node {id}"));
        Node::decode(&raw)
    }

    /// Binary-search `keys` for `target`.  Returns child index to descend.
    fn child_index(keys: &[Vec<u8>], target: &[u8]) -> usize {
        match keys.binary_search_by(|k| k.as_slice().cmp(target)) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    }

    // =================================================================
    // GET
    // =================================================================

    /// Looks up `key` in the tree rooted at `root`.
    ///
    /// Returns `None` if the tree is empty or the key is absent.
    pub fn get(&self, root: NodeId, key: &[u8]) -> Option<Vec<u8>> {
        if root == EMPTY_ROOT {
            return None;
        }
        let mut cur = root;
        loop {
            match self.node(cur) {
                Node::Leaf { keys, values } => {
                    return match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                        Ok(i) => Some(values[i].clone()),
                        Err(_) => None,
                    };
                }
                Node::Internal { keys, children } => {
                    cur = children[Self::child_index(&keys, key)];
                }
            }
        }
    }

    /// Returns `true` if `key` exists in the tree rooted at `root`.
    #[inline]
    pub fn contains_key(&self, root: NodeId, key: &[u8]) -> bool {
        self.get(root, key).is_some()
    }

    // =================================================================
    // INSERT
    // =================================================================

    /// Inserts `(key, value)`, returning the **new root**.
    ///
    /// The old root (and every version that references it) is unaffected.
    pub fn insert(&mut self, root: NodeId, key: &[u8], value: &[u8]) -> NodeId {
        if root == EMPTY_ROOT {
            return self.alloc(&Node::Leaf {
                keys: vec![key.to_vec()],
                values: vec![value.to_vec()],
            });
        }
        match self.insert_rec(root, key, value) {
            InsertResult::Updated(r) => r,
            InsertResult::Split { left, sep, right } => self.alloc(&Node::Internal {
                keys: vec![sep],
                children: vec![left, right],
            }),
        }
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
            right: self.alloc(&Node::Leaf { keys: rk, values: rv }),
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

    // =================================================================
    // REMOVE
    // =================================================================

    /// Removes `key`, returning the **new root**.
    ///
    /// If the key is absent the original `root` is returned (no allocation).
    pub fn remove(&mut self, root: NodeId, key: &[u8]) -> NodeId {
        if root == EMPTY_ROOT {
            return EMPTY_ROOT;
        }
        match self.remove_rec(root, key) {
            RemoveResult::NotFound => root,
            RemoveResult::Done(r) | RemoveResult::Underflow(r) => self.shrink_root(r),
        }
    }

    fn shrink_root(&self, root: NodeId) -> NodeId {
        match self.node(root) {
            Node::Leaf { ref keys, .. } if keys.is_empty() => EMPTY_ROOT,
            Node::Internal {
                ref keys,
                ref children,
            } if keys.is_empty() => children[0],
            _ => root,
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
        if ci + 1 < children.len()
            && self.node(children[ci + 1]).key_count() > MIN_KEYS
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

    fn borrow_left(
        &mut self,
        pk: &mut Vec<Vec<u8>>,
        pc: &mut Vec<NodeId>,
        ci: usize,
    ) {
        let si = ci - 1;
        let left = self.node(pc[si]);
        let child = self.node(pc[ci]);
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
                pc[si] = self.alloc(&Node::Leaf { keys: lk, values: lv });
                pc[ci] = self.alloc(&Node::Leaf { keys: ck, values: cv });
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
                pc[si] = self.alloc(&Node::Internal { keys: lk, children: lc });
                pc[ci] = self.alloc(&Node::Internal { keys: ck, children: cc });
            }
            _ => unreachable!(),
        }
    }

    fn borrow_right(
        &mut self,
        pk: &mut Vec<Vec<u8>>,
        pc: &mut Vec<NodeId>,
        ci: usize,
    ) {
        let ri = ci + 1;
        let child = self.node(pc[ci]);
        let right = self.node(pc[ri]);
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
                pc[ci] = self.alloc(&Node::Leaf { keys: ck, values: cv });
                pc[ri] = self.alloc(&Node::Leaf { keys: rk, values: rv });
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
                pc[ci] = self.alloc(&Node::Internal { keys: ck, children: cc });
                pc[ri] = self.alloc(&Node::Internal { keys: rk, children: rc });
            }
            _ => unreachable!(),
        }
    }

    fn merge_children(
        &mut self,
        pk: &mut Vec<Vec<u8>>,
        pc: &mut Vec<NodeId>,
        idx: usize,
    ) {
        let left = self.node(pc[idx]);
        let right = self.node(pc[idx + 1]);
        let sep = pk.remove(idx);

        let merged = match (left, right) {
            (
                Node::Leaf {
                    keys: mut lk,
                    values: mut lv,
                },
                Node::Leaf { keys: rk, values: rv },
            ) => {
                lk.extend(rk);
                lv.extend(rv);
                Node::Leaf { keys: lk, values: lv }
            }
            (
                Node::Internal {
                    keys: mut lk,
                    children: mut lc,
                },
                Node::Internal { keys: rk, children: rc },
            ) => {
                lk.push(sep);
                lk.extend(rk);
                lc.extend(rc);
                Node::Internal { keys: lk, children: lc }
            }
            _ => unreachable!(),
        };
        pc[idx] = self.alloc(&merged);
        pc.remove(idx + 1);
    }

    // =================================================================
    // ITERATION
    // =================================================================

    /// Returns an iterator over **all** entries in ascending key order.
    pub fn iter(&self, root: NodeId) -> BTreeIter<'_> {
        BTreeIter::new(self, root, Bound::Unbounded, Bound::Unbounded)
    }

    /// Returns an iterator over the given key range.
    pub fn range(
        &self,
        root: NodeId,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
    ) -> BTreeIter<'_> {
        let lo = match lo {
            Bound::Included(k) => Bound::Included(k.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let hi = match hi {
            Bound::Included(k) => Bound::Included(k.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        BTreeIter::new(self, root, lo, hi)
    }

    // =================================================================
    // BULK LOAD
    // =================================================================

    /// Builds a tree from a **pre-sorted** list of `(key, value)` pairs.
    ///
    /// Much faster than inserting one-by-one, and produces an optimally
    /// packed tree.
    pub fn bulk_load(
        &mut self,
        entries: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> NodeId {
        let entries: Vec<_> = entries.into_iter().collect();
        if entries.is_empty() {
            return EMPTY_ROOT;
        }
        // 1. Pack into leaves.
        let mut leaf_ids = Vec::new();
        for chunk in entries.chunks(MAX_KEYS) {
            let keys = chunk.iter().map(|(k, _)| k.clone()).collect();
            let values = chunk.iter().map(|(_, v)| v.clone()).collect();
            leaf_ids.push(self.alloc(&Node::Leaf { keys, values }));
        }
        // 2. Build internal levels bottom-up.
        let mut level = leaf_ids;
        while level.len() > 1 {
            let mut next = Vec::new();
            for chunk in level.chunks(MAX_KEYS + 1) {
                if chunk.len() == 1 {
                    next.push(chunk[0]);
                    continue;
                }
                let mut keys = Vec::with_capacity(chunk.len() - 1);
                for &cid in &chunk[1..] {
                    keys.push(self.first_key(cid));
                }
                next.push(self.alloc(&Node::Internal {
                    keys,
                    children: chunk.to_vec(),
                }));
            }
            level = next;
        }
        level[0]
    }

    /// Returns the smallest key reachable from node `id`.
    fn first_key(&self, id: NodeId) -> Vec<u8> {
        let mut cur = id;
        loop {
            match self.node(cur) {
                Node::Leaf { keys, .. } => return keys[0].clone(),
                Node::Internal { children, .. } => cur = children[0],
            }
        }
    }

    // =================================================================
    // GARBAGE COLLECTION
    // =================================================================

    /// Deletes every node **not** reachable from any of the given roots.
    ///
    /// Call this periodically to reclaim storage from old versions that are
    /// no longer needed.
    pub fn gc(&mut self, live_roots: &[NodeId]) {
        let mut reachable = std::collections::HashSet::new();
        for &r in live_roots {
            if r != EMPTY_ROOT {
                self.mark(r, &mut reachable);
            }
        }
        let all: Vec<(Vec<u8>, Vec<u8>)> = self.nodes.iter().collect();
        for (k, _) in all {
            let id = u64::from_be_bytes(k[..8].try_into().unwrap());
            if !reachable.contains(&id) {
                self.nodes.remove(&k);
            }
        }
    }

    fn mark(&self, id: NodeId, seen: &mut std::collections::HashSet<NodeId>) {
        if !seen.insert(id) {
            return;
        }
        if let Some(raw) = self.nodes.get(&id.to_be_bytes()) {
            let node = Node::decode(&raw);
            if let Node::Internal { children, .. } = &node {
                for &c in children {
                    self.mark(c, seen);
                }
            }
        }
    }
}

impl Default for PersistentBTree {
    fn default() -> Self {
        Self::new()
    }
}

// =========================================================================
// BTreeIter
// =========================================================================

/// A forward iterator over entries in a [`PersistentBTree`].
///
/// Uses an explicit ancestor stack — no sibling pointers needed.
pub struct BTreeIter<'a> {
    tree: &'a PersistentBTree,
    stack: Vec<(Node, usize)>,
    leaf: Option<(Vec<Vec<u8>>, Vec<Vec<u8>>, usize)>,
    hi: Bound<Vec<u8>>,
    done: bool,
}

impl<'a> BTreeIter<'a> {
    fn new(
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
                                Ok(i) => i,
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
                        Bound::Included(k) => {
                            keys.binary_search_by(|x| x.as_slice().cmp(k))
                                .unwrap_or_else(|i| i)
                        }
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
            if let Node::Internal { children, .. } = node {
                if *next_ci < children.len() {
                    let child_id = children[*next_ci];
                    *next_ci += 1;
                    self.descend_leftmost(child_id);
                    return;
                }
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
