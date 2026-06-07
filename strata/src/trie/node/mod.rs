mod codec;
pub use codec::NodeCodec;

use crate::trie::nibbles::Nibbles;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeHandle {
    /// A node that lives only in memory (not yet hashed).
    InMemory(Box<Node>),
    /// A node with its precomputed hash (after `commit`).
    Cached(Vec<u8>, Box<Node>),
}

impl Default for NodeHandle {
    fn default() -> Self {
        NodeHandle::InMemory(Box::new(Node::Null))
    }
}

impl NodeHandle {
    pub fn hash(&self) -> Option<&[u8]> {
        match self {
            NodeHandle::Cached(h, _) => Some(h),
            NodeHandle::InMemory(_) => None,
        }
    }

    /// Consumes the handle, returning the owned `Node` by move (no clone).
    pub fn into_node(self) -> Node {
        match self {
            NodeHandle::InMemory(n) | NodeHandle::Cached(_, n) => *n,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum Node {
    #[default]
    Null,
    Leaf {
        path: Nibbles,
        value: Vec<u8>,
    },
    Extension {
        path: Nibbles,
        child: NodeHandle,
    },
    Branch {
        children: Box<[Option<NodeHandle>; 16]>,
        value: Option<Vec<u8>>,
    },
}
