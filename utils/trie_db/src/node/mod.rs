mod codec;
pub use codec::NodeCodec;

use crate::nibbles::Nibbles;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeHandle {
    InMemory(Box<Node>),
    Hash(Vec<u8>),
    Cached(Vec<u8>, Box<Node>), // Hash, Node
}

impl NodeHandle {
    // pub fn as_node(&self) -> Option<&Node> {
    //     match self {
    //         NodeHandle::InMemory(n) => Some(n),
    //         NodeHandle::Cached(_, n) => Some(n),
    //         NodeHandle::Hash(_) => None,
    //     }
    // }

    pub fn hash(&self) -> Option<&[u8]> {
        match self {
            NodeHandle::Hash(h) => Some(h),
            NodeHandle::Cached(h, _) => Some(h),
            NodeHandle::InMemory(_) => None,
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
