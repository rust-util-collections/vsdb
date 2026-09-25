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

    fn step(&self, mut node: &Node, mut path: Nibbles) -> Result<Option<Vec<u8>>> {
        loop {
            match node {
                Node::Null => return Ok(None),
                Node::Leaf {
                    path: leaf_path,
                    value,
                } => {
                    return Ok((*leaf_path == path).then(|| value.clone()));
                }
                Node::Extension {
                    path: ext_path,
                    child,
                } => {
                    if !path.starts_with(ext_path) {
                        return Ok(None);
                    }
                    let (_, remaining) = path.split_at(ext_path.len());
                    path = remaining;
                    node = Self::resolve(child);
                }
                Node::Branch { children, value } => {
                    if path.is_empty() {
                        return Ok(value.clone());
                    }
                    let index = path.at(0) as usize;
                    let Some(child) = &children[index] else {
                        return Ok(None);
                    };
                    let (_, remaining) = path.split_at(1);
                    path = remaining;
                    node = Self::resolve(child);
                }
            }
        }
    }

    /// Borrows the node behind a handle — read-only traversal must never
    /// clone subtrees (cloning is unnecessary for reads).
    fn resolve(handle: &NodeHandle) -> &Node {
        match handle {
            NodeHandle::InMemory(n) | NodeHandle::Cached(_, n) => n,
        }
    }
}
