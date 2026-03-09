use crate::trie::error::Result;
use crate::trie::nibbles::Nibbles;
use crate::trie::node::{Node, NodeHandle};

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

        let path = Nibbles::from_raw(key, false);
        let root_node = Self::resolve(self.root);
        self.step(&root_node, path)
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
                    let child_node = Self::resolve(child);
                    self.step(&child_node, remaining)
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
                    let child_node = Self::resolve(child_handle);
                    let (_, remaining) = path.split_at(1);
                    self.step(&child_node, remaining)
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn resolve(handle: &NodeHandle) -> Node {
        match handle {
            NodeHandle::InMemory(n) | NodeHandle::Cached(_, n) => *n.clone(),
        }
    }
}
