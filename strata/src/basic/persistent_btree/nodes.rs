//! Slotted B+ tree pages. Each slot holds its key's exclusive end offset
//! followed by an inline value end offset, a blob ID or a child ID. Offsets
//! are relative to the start of the page.

use super::types::NodeId;
use crate::{Result, VsdbError};

pub(crate) const B: usize = 16;
pub(crate) const MAX_KEYS: usize = 2 * B;
pub(crate) const MIN_KEYS: usize = B;
pub(crate) const CLASS_MASK: u64 = 3 << 62;
pub(crate) const LEAF: u64 = 1 << 62;
pub(crate) const BLOB_LEAF: u64 = 2 << 62;
pub(crate) const BLOB: u64 = 3 << 62;

pub(crate) fn object_key(id: NodeId) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = if id & CLASS_MASK == BLOB { 0x12 } else { 0x10 };
    key[1..].copy_from_slice(&id.to_be_bytes());
    key
}

pub(crate) fn object_id(key: &[u8]) -> Option<NodeId> {
    if key.len() != 9 || !matches!(key[0], 0x10 | 0x12) {
        return None;
    }
    Some(NodeId::from_be_bytes(key[1..].try_into().unwrap()))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LeafVal {
    Bytes(Vec<u8>),
    Blob(NodeId),
}

#[derive(Clone, Debug)]
pub(crate) enum Node {
    Leaf {
        keys: Vec<Vec<u8>>,
        values: Vec<LeafVal>,
    },
    Internal {
        keys: Vec<Vec<u8>>,
        children: Vec<NodeId>,
    },
}

pub(super) enum LeafValueRef<'a> {
    Inline(&'a [u8]),
    Blob(NodeId),
}

#[derive(Clone, Copy)]
pub(crate) struct NodeView<'a> {
    raw: &'a [u8],
    pub(crate) len: usize,
    pub(crate) leaf: bool,
}

impl<'a> NodeView<'a> {
    pub(crate) fn open(raw: &'a [u8]) -> Result<Self> {
        let corrupt = || VsdbError::Corrupt {
            detail: "invalid slotted B+ tree page".into(),
        };
        let [tag, count, ..] = *raw else {
            return Err(corrupt());
        };
        let len = usize::from(count);
        let leaf = match tag {
            0x18 => true,
            0x19 => false,
            _ => return Err(corrupt()),
        };
        let page = Self { raw, len, leaf };
        if len > MAX_KEYS || page.payload_start() > raw.len() {
            return Err(corrupt());
        }
        Ok(page)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.validated_class().map(|_| ())
    }

    pub(super) fn check_id(&self, id: NodeId) -> Result<()> {
        if self.validated_class()? != id & CLASS_MASK
            || self.edge_ids().any(|child| {
                let serial = child & !CLASS_MASK;
                serial == 0 || serial >= id & !CLASS_MASK
            })
        {
            return Err(VsdbError::Corrupt {
                detail: format!("invalid child or class on page {id}"),
            });
        }
        Ok(())
    }

    pub(super) fn validated_class(&self) -> Result<u64> {
        let mut class = if self.leaf { LEAF } else { 0 };
        let invalid = || VsdbError::Corrupt {
            detail: "invalid B+ tree slot offset, key order or child".into(),
        };
        let mut pos = self.payload_start();
        for i in 0..self.len {
            let key_end = self.key_end(i);
            if key_end < pos || key_end > self.raw.len() {
                return Err(invalid());
            }
            let desc = self.desc(i);
            if self.leaf {
                if desc & CLASS_MASK == BLOB {
                    class = BLOB_LEAF;
                } else if u32::try_from(desc).is_err() {
                    return Err(invalid());
                }
            }
            let end = self.end(i);
            if end < key_end
                || end > self.raw.len()
                || (i > 0 && self.key(i - 1) >= self.key(i))
            {
                return Err(invalid());
            }
            pos = end;
        }
        if pos != self.raw.len()
            || (!self.leaf
                && (0..=self.len)
                    .any(|i| self.child(i) == 0 || self.child(i) & CLASS_MASK == BLOB))
        {
            return Err(invalid());
        }
        Ok(class)
    }

    fn payload_start(&self) -> usize {
        2 + 12 * self.len + if self.leaf { 0 } else { 8 }
    }
    fn key_end(&self, i: usize) -> usize {
        let p = 2 + 12 * i;
        u32::from_le_bytes(self.raw[p..p + 4].try_into().unwrap()) as usize
    }
    fn desc(&self, i: usize) -> u64 {
        let p = 6 + 12 * i;
        u64::from_le_bytes(self.raw[p..p + 8].try_into().unwrap())
    }
    fn end(&self, i: usize) -> usize {
        let desc = self.desc(i);
        if self.leaf && desc & CLASS_MASK != BLOB {
            desc as usize
        } else {
            self.key_end(i)
        }
    }
    pub(crate) fn key(&self, i: usize) -> &'a [u8] {
        let start = if i == 0 {
            self.payload_start()
        } else {
            self.end(i - 1)
        };
        &self.raw[start..self.key_end(i)]
    }
    pub(crate) fn child(&self, i: usize) -> NodeId {
        if i < self.len {
            self.desc(i)
        } else {
            let p = 2 + 12 * self.len;
            u64::from_le_bytes(self.raw[p..p + 8].try_into().unwrap())
        }
    }
    /// Returns borrowed inline bytes or the stored blob ID.
    pub(super) fn value_ref(&self, i: usize) -> LeafValueRef<'a> {
        let desc = self.desc(i);
        if desc & CLASS_MASK == BLOB {
            LeafValueRef::Blob(desc)
        } else {
            LeafValueRef::Inline(&self.raw[self.key_end(i)..self.end(i)])
        }
    }
    pub(crate) fn value(
        &self,
        i: usize,
        blob: impl FnOnce(NodeId) -> Vec<u8>,
    ) -> Vec<u8> {
        match self.value_ref(i) {
            LeafValueRef::Inline(bytes) => bytes.to_vec(),
            LeafValueRef::Blob(id) => blob(id),
        }
    }
    pub(crate) fn lower_bound(&self, key: &[u8], exclusive: bool) -> usize {
        let mut lo = 0;
        let mut hi = self.len;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let k = self.key(mid);
            if k < key || (exclusive && k == key) {
                lo = mid + 1
            } else {
                hi = mid
            }
        }
        lo
    }
    fn edge_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        (0..self.len + usize::from(!self.leaf)).filter_map(|i| {
            if self.leaf {
                let id = self.desc(i);
                (id & CLASS_MASK == BLOB).then_some(id)
            } else {
                Some(self.child(i))
            }
        })
    }
    pub(crate) fn edges(&self) -> Vec<NodeId> {
        let mut edges = Vec::with_capacity(if self.leaf { 0 } else { self.len + 1 });
        edges.extend(self.edge_ids());
        edges
    }
    pub(crate) fn leaf_val(&self, i: usize) -> LeafVal {
        let desc = self.desc(i);
        if desc & CLASS_MASK == BLOB {
            LeafVal::Blob(desc)
        } else {
            LeafVal::Bytes(self.raw[self.key_end(i)..self.end(i)].to_vec())
        }
    }
    pub(super) fn materialize(&self) -> Node {
        let keys = (0..self.len).map(|i| self.key(i).to_vec()).collect();
        if self.leaf {
            Node::Leaf {
                keys,
                values: (0..self.len).map(|i| self.leaf_val(i)).collect(),
            }
        } else {
            Node::Internal {
                keys,
                children: self.edges(),
            }
        }
    }
}

impl Node {
    pub(crate) fn key_count(&self) -> usize {
        match self {
            Self::Leaf { keys, .. } | Self::Internal { keys, .. } => keys.len(),
        }
    }

    pub(crate) fn encode_with(
        &self,
        edges: &mut Vec<NodeId>,
        mut alloc_blob: impl FnMut(&[u8]) -> NodeId,
    ) -> Vec<u8> {
        let n = self.key_count();
        let leaf = matches!(self, Self::Leaf { .. });
        let keys = match self {
            Self::Leaf { keys, .. } | Self::Internal { keys, .. } => keys,
        };
        let mut raw = vec![0; 2 + 12 * n + if leaf { 0 } else { 8 }];
        raw[0] = if leaf { 0x18 } else { 0x19 };
        raw[1] = n.try_into().expect("B+ tree page fanout overflow");
        for (i, key) in keys.iter().enumerate() {
            let p = 2 + 12 * i;
            raw.extend_from_slice(key);
            let end = u32::try_from(raw.len()).expect("B+ tree page offset overflow");
            raw[p..p + 4].copy_from_slice(&end.to_le_bytes());
            let desc = match self {
                Self::Leaf { values, .. } => match &values[i] {
                    LeafVal::Blob(id) => {
                        edges.push(*id);
                        *id
                    }
                    LeafVal::Bytes(v) if v.len() > 1024 => {
                        let blob = alloc_blob(v);
                        edges.push(blob);
                        blob
                    }
                    LeafVal::Bytes(v) => {
                        raw.extend_from_slice(v);
                        raw.len() as u64
                    }
                },
                Self::Internal { children, .. } => children[i],
            };
            raw[p + 4..p + 12].copy_from_slice(&desc.to_le_bytes());
        }
        if let Self::Internal { children, .. } = self {
            raw[2 + 12 * n..10 + 12 * n].copy_from_slice(&children[n].to_le_bytes());
            edges.extend_from_slice(children);
        }
        raw
    }

    #[cfg(test)]
    pub(crate) fn decode(raw: &[u8]) -> Self {
        let page = NodeView::open(raw).expect("invalid B+ tree page header");
        page.validate().expect("invalid B+ tree page");
        page.materialize()
    }

    #[cfg(test)]
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut edges = Vec::new();
        self.encode_with(&mut edges, |_| panic!("test page requires blob store"))
    }
    pub(crate) fn decode_children(raw: &[u8]) -> Vec<NodeId> {
        let page = NodeView::open(raw).expect("invalid B+ tree page header");
        page.validate().expect("invalid B+ tree page");
        page.edges()
    }
}
