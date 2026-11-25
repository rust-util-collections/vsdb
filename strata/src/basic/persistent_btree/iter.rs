//! Forward leaf cursor over immutable, slotted pages.

use std::{marker::PhantomData, ops::Bound, sync::Arc};
use vsdb_core::basic::mapx_raw::MapxRawReader;

use super::{EMPTY_ROOT, NodeId, NodeView, PersistentBTree, RootLease, read_object};

/// A forward iterator over entries in a [`PersistentBTree`].
/// Each level retains one page. The current entry is decoded on demand.
pub struct BTreeIter<'a> {
    nodes: MapxRawReader,
    _borrow: PhantomData<&'a PersistentBTree>,
    _lease: Option<Arc<RootLease>>,
    stack: Vec<(Vec<u8>, usize)>,
    leaf: Option<(Vec<u8>, usize)>,
    hi: Bound<Vec<u8>>,
}

impl BTreeIter<'_> {
    pub(crate) fn new(
        nodes: MapxRawReader,
        root: NodeId,
        lo: Bound<Vec<u8>>,
        hi: Bound<Vec<u8>>,
        lease: Option<Arc<RootLease>>,
    ) -> Self {
        let mut it = Self {
            nodes,
            _borrow: PhantomData,
            _lease: lease,
            stack: Vec::new(),
            leaf: None,
            hi,
        };
        if root != EMPTY_ROOT {
            it.descend(root, &lo);
        }
        it
    }

    fn descend(&mut self, mut id: NodeId, lo: &Bound<Vec<u8>>) {
        loop {
            let raw = read_object(&self.nodes, id);
            let page = NodeView::open(&raw).expect("invalid B+ tree page");
            let slot = match lo {
                Bound::Included(k) => page.lower_bound(k, !page.leaf),
                Bound::Excluded(k) => page.lower_bound(k, true),
                Bound::Unbounded => 0,
            };
            if page.leaf {
                self.leaf = Some((raw, slot));
                return;
            }
            id = page.child(slot);
            self.stack.push((raw, slot + 1));
        }
    }

    fn advance(&mut self) {
        self.leaf = None;
        while let Some((raw, slot)) = self.stack.last_mut() {
            let page = NodeView::open(raw).expect("invalid B+ tree page");
            if *slot <= page.len {
                let id = page.child(*slot);
                *slot += 1;
                self.descend(id, &Bound::Unbounded);
                return;
            }
            self.stack.pop();
        }
    }
}

impl Iterator for BTreeIter<'_> {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (raw, slot) = self.leaf.as_mut()?;
            let page = NodeView::open(raw).expect("invalid B+ tree page");
            if *slot >= page.len {
                self.advance();
                continue;
            }
            let key = page.key(*slot);
            if match &self.hi {
                Bound::Included(hi) => key > hi.as_slice(),
                Bound::Excluded(hi) => key >= hi.as_slice(),
                Bound::Unbounded => false,
            } {
                self.leaf = None;
                return None;
            }
            let val = page.value(*slot, |blob| read_object(&self.nodes, blob));
            *slot += 1;
            return Some((key.to_vec(), val));
        }
    }
}
