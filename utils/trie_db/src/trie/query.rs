// use crate::config::HASH_LEN;
use crate::error::{Result, TrieError};
use crate::nibbles::Nibbles;
use crate::node::{Node, NodeCodec, NodeHandle};
use crate::storage::TrieBackend;

pub struct TrieRo<'a, B: TrieBackend + ?Sized> {
    root: Vec<u8>,
    backend: &'a B,
}

impl<'a, B: TrieBackend + ?Sized> TrieRo<'a, B> {
    pub fn new(root: Vec<u8>, backend: &'a B) -> Self {
        Self { root, backend }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.root.iter().all(|&b| b == 0) {
            return Ok(None);
        }

        let path = Nibbles::from_raw(key, false);
        let root_node = self.load_node(&self.root)?;
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
                    let child_node = self.resolve(child)?;
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
                    let child_node = self.resolve(child_handle)?;
                    let (_, remaining) = path.split_at(1);
                    self.step(&child_node, remaining)
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn resolve(&self, handle: &NodeHandle) -> Result<Node> {
        match handle {
            NodeHandle::InMemory(n) => Ok(*n.clone()),
            NodeHandle::Cached(_, n) => Ok(*n.clone()),
            NodeHandle::Hash(h) => self.load_node(h),
        }
    }

    fn load_node(&self, hash: &[u8]) -> Result<Node> {
        match self.backend.get(hash)? {
            Some(data) => NodeCodec::decode(&data),
            None => Err(TrieError::NodeNotFound(format!("Hash: {:?}", hash))),
        }
    }
}
