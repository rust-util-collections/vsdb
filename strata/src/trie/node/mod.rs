mod codec;
pub use codec::NodeCodec;

use crate::trie::nibbles::Nibbles;
use std::mem;

#[derive(Debug, PartialEq, Eq)]
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
    pub fn node(&self) -> &Node {
        match self {
            Self::InMemory(node) | Self::Cached(_, node) => node,
        }
    }

    fn node_mut(&mut self) -> &mut Node {
        match self {
            Self::InMemory(node) | Self::Cached(_, node) => node,
        }
    }

    pub fn hash(&self) -> Option<&[u8]> {
        match self {
            NodeHandle::Cached(h, _) => Some(h),
            NodeHandle::InMemory(_) => None,
        }
    }

    /// Consumes the handle, returning the owned `Node` by move (no clone).
    pub fn into_node(mut self) -> Node {
        mem::take(self.node_mut())
    }
}

impl Clone for NodeHandle {
    fn clone(&self) -> Self {
        let mut pending = vec![(self, false)];
        let mut ready = Vec::new();
        while let Some((handle, visited)) = pending.pop() {
            if !visited {
                pending.push((handle, true));
                match handle.node() {
                    Node::Extension { child, .. } => pending.push((child, false)),
                    Node::Branch { children, .. } => {
                        pending
                            .extend(children.iter().rev().flatten().map(|c| (c, false)));
                    }
                    _ => {}
                }
                continue;
            }
            let node = match handle.node() {
                Node::Null => Node::Null,
                Node::Leaf { path, value } => Node::Leaf {
                    path: path.clone(),
                    value: value.clone(),
                },
                Node::Extension { path, .. } => Node::Extension {
                    path: path.clone(),
                    child: ready.pop().expect("visited child"),
                },
                Node::Branch { children, value } => {
                    let mut copied = Box::new(std::array::from_fn(|_| None));
                    for (i, child) in children.iter().enumerate().rev() {
                        if child.is_some() {
                            copied[i] = Some(ready.pop().expect("visited child"));
                        }
                    }
                    Node::Branch {
                        children: copied,
                        value: value.clone(),
                    }
                }
            };
            ready.push(match handle.hash() {
                Some(hash) => Self::Cached(hash.to_vec(), Box::new(node)),
                None => Self::InMemory(Box::new(node)),
            });
        }
        ready.pop().expect("visited root")
    }
}

impl Drop for NodeHandle {
    fn drop(&mut self) {
        // Detach descendants before dropping each box. This also covers
        // partial trees discarded by mutation or cache decoding errors.
        fn detach(node: Node, pending: &mut Vec<NodeHandle>) {
            match node {
                Node::Extension { child, .. } => pending.push(child),
                Node::Branch { children, .. } => {
                    pending.extend(children.into_iter().flatten())
                }
                _ => {}
            }
        }
        let mut pending = Vec::new();
        detach(mem::take(self.node_mut()), &mut pending);
        while let Some(mut child) = pending.pop() {
            detach(mem::take(child.node_mut()), &mut pending);
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
