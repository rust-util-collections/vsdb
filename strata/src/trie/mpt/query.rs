use crate::trie::{
    error::Result,
    nibbles::Nibbles,
    node::{Node, NodeHandle},
};

pub struct TrieRo<'a> {
    root: &'a NodeHandle,
}

impl<'a> TrieRo<'a> {
    pub fn new(root: &'a NodeHandle) -> Self {
        Self { root }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if matches!(self.root, NodeHandle::InMemory(n) if **n == Node::Null) {
            return Ok(None);
        }

        let path = Nibbles::from_raw(key);
        self.step(Self::resolve(self.root), path)
    }

    fn step(&self, node: &Node, path: Nibbles) -> Result<Option<Vec<u8>>> {
        match node {
            Node::Null => Ok(None),
            Node::Leaf {
                path: leaf_path,
                value,
            } => {
                if *leaf_path == path {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            Node::Extension {
                path: ext_path,
                child,
            } => {
                if path.starts_with(ext_path) {
                    let (_, remaining) = path.split_at(ext_path.len());
                    self.step(Self::resolve(child), remaining)
                } else {
                    Ok(None)
                }
            }
            Node::Branch { children, value } => {
                if path.is_empty() {
                    return Ok(value.clone());
                }
                let idx = path.at(0) as usize;
                if let Some(child_handle) = &children[idx] {
                    let (_, remaining) = path.split_at(1);
                    self.step(Self::resolve(child_handle), remaining)
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Borrows the node behind a handle — read-only traversal must never
    /// clone subtrees (`Node`'s derived `Clone` is recursive).
    fn resolve(handle: &NodeHandle) -> &Node {
        match handle {
            NodeHandle::InMemory(n) | NodeHandle::Cached(_, n) => n,
        }
    }
}
