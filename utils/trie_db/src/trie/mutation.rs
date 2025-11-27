use crate::error::{Result, TrieError};
use crate::nibbles::Nibbles;
use crate::node::{Node, NodeCodec, NodeHandle};
use crate::storage::TrieBackend;
use sha3::{Digest, Keccak256};

pub struct TrieMut<'a, B: TrieBackend> {
    root: NodeHandle,
    backend: &'a mut B,
}

impl<'a, B: TrieBackend> TrieMut<'a, B> {
    pub fn new(root_hash: &[u8], backend: &'a mut B) -> Self {
        let root = if root_hash.iter().all(|&b| b == 0) {
            NodeHandle::InMemory(Box::new(Node::Null))
        } else {
            NodeHandle::Hash(root_hash.to_vec())
        };
        Self { root, backend }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let path = Nibbles::from_raw(key, false);
        // We need to pass backend to insert_rec.
        // self.backend is &mut B. We can pass &B.
        let new_root = Self::insert_rec(self.backend, self.root.clone(), path, value.to_vec())?;
        self.root = new_root;
        Ok(())
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        let path = Nibbles::from_raw(key, false);
        let (new_root, _) = Self::remove_rec(self.backend, self.root.clone(), path)?;
        self.root = new_root.unwrap_or(NodeHandle::InMemory(Box::new(Node::Null)));
        Ok(())
    }

    pub fn commit(self) -> Result<Vec<u8>> {
        let mut batch = Vec::new();
        // Move root out
        let root = self.root;
        // Move backend out (it's a mutable reference)
        let backend = self.backend;

        let root_handle = Self::commit_rec(root, &mut batch)?;

        backend.insert_batch(batch)?;

        match root_handle {
            NodeHandle::Hash(h) | NodeHandle::Cached(h, _) => Ok(h),
            NodeHandle::InMemory(_) => panic!("Root should be hashed after commit"),
        }
    }
    // --- Helpers (Static to avoid self borrow issues) ---

    fn resolve(backend: &B, handle: &NodeHandle) -> Result<Node> {
        match handle {
            NodeHandle::InMemory(n) => Ok(*n.clone()),
            NodeHandle::Cached(_, n) => Ok(*n.clone()),
            NodeHandle::Hash(h) => match backend.get(h)? {
                Some(data) => NodeCodec::decode(&data),
                None => Err(TrieError::NodeNotFound(format!("Hash: {:?}", h))),
            },
        }
    }

    fn insert_rec(
        backend: &B,
        node_handle: NodeHandle,
        path: Nibbles,
        value: Vec<u8>,
    ) -> Result<NodeHandle> {
        let node = Self::resolve(backend, &node_handle)?;

        match node {
            Node::Null => Ok(NodeHandle::InMemory(Box::new(Node::Leaf { path, value }))),
            Node::Leaf {
                path: leaf_path,
                value: leaf_value,
            } => {
                let common = path.common_prefix(&leaf_path);

                if common == path.len() && common == leaf_path.len() {
                    return Ok(NodeHandle::InMemory(Box::new(Node::Leaf { path, value })));
                }

                let mut children: Box<[Option<NodeHandle>; 16]> = Box::new([
                    None, None, None, None, None, None, None, None, None, None, None, None, None,
                    None, None, None,
                ]);
                let mut branch_value = None;

                if common == leaf_path.len() {
                    branch_value = Some(leaf_value);
                } else {
                    let idx = leaf_path.at(common) as usize;
                    let (_, rest) = leaf_path.split_at(common + 1);
                    children[idx] = Some(NodeHandle::InMemory(Box::new(Node::Leaf {
                        path: rest,
                        value: leaf_value,
                    })));
                }

                if common == path.len() {
                    branch_value = Some(value);
                } else {
                    let idx = path.at(common) as usize;
                    let (_, rest) = path.split_at(common + 1);
                    children[idx] = Some(NodeHandle::InMemory(Box::new(Node::Leaf {
                        path: rest,
                        value,
                    })));
                }

                let branch = NodeHandle::InMemory(Box::new(Node::Branch {
                    children,
                    value: branch_value,
                }));

                if common > 0 {
                    let (ext_path, _) = path.split_at(common);
                    Ok(NodeHandle::InMemory(Box::new(Node::Extension {
                        path: ext_path,
                        child: branch,
                    })))
                } else {
                    Ok(branch)
                }
            }
            Node::Extension {
                path: ext_path,
                child,
            } => {
                let common = path.common_prefix(&ext_path);

                if common == ext_path.len() {
                    let (_, rest) = path.split_at(common);
                    let new_child = Self::insert_rec(backend, child, rest, value)?;
                    Ok(NodeHandle::InMemory(Box::new(Node::Extension {
                        path: ext_path,
                        child: new_child,
                    })))
                } else {
                    let (common_path, _) = ext_path.split_at(common);
                    let idx_ext = ext_path.at(common) as usize;
                    let (_, rest_ext) = ext_path.split_at(common + 1);

                    let mut children: Box<[Option<NodeHandle>; 16]> = Box::new([
                        None, None, None, None, None, None, None, None, None, None, None, None,
                        None, None, None, None,
                    ]);

                    let old_branch_child = if rest_ext.is_empty() {
                        child
                    } else {
                        NodeHandle::InMemory(Box::new(Node::Extension {
                            path: rest_ext,
                            child,
                        }))
                    };
                    children[idx_ext] = Some(old_branch_child);

                    let mut branch_value = None;
                    if common == path.len() {
                        branch_value = Some(value);
                    } else {
                        let idx_new = path.at(common) as usize;
                        let (_, rest_new) = path.split_at(common + 1);
                        children[idx_new] = Some(NodeHandle::InMemory(Box::new(Node::Leaf {
                            path: rest_new,
                            value,
                        })));
                    }

                    let branch = NodeHandle::InMemory(Box::new(Node::Branch {
                        children,
                        value: branch_value,
                    }));

                    if common > 0 {
                        Ok(NodeHandle::InMemory(Box::new(Node::Extension {
                            path: common_path,
                            child: branch,
                        })))
                    } else {
                        Ok(branch)
                    }
                }
            }
            Node::Branch {
                mut children,
                value: b_value,
            } => {
                if path.is_empty() {
                    Ok(NodeHandle::InMemory(Box::new(Node::Branch {
                        children,
                        value: Some(value),
                    })))
                } else {
                    let idx = path.at(0) as usize;
                    let (_, rest) = path.split_at(1);
                    let child = children[idx]
                        .clone()
                        .unwrap_or(NodeHandle::InMemory(Box::new(Node::Null)));
                    let new_child = Self::insert_rec(backend, child, rest, value)?;
                    children[idx] = Some(new_child);
                    Ok(NodeHandle::InMemory(Box::new(Node::Branch {
                        children,
                        value: b_value,
                    })))
                }
            }
        }
    }

    fn remove_rec(
        backend: &B,
        node_handle: NodeHandle,
        path: Nibbles,
    ) -> Result<(Option<NodeHandle>, bool)> {
        let node = Self::resolve(backend, &node_handle)?;

        match node {
            Node::Null => Ok((None, false)),
            Node::Leaf {
                path: leaf_path,
                value: _,
            } => {
                if leaf_path == path {
                    Ok((None, true))
                } else {
                    Ok((Some(node_handle), false))
                }
            }
            Node::Extension {
                path: ext_path,
                child,
            } => {
                if path.starts_with(&ext_path) {
                    let (_, rest) = path.split_at(ext_path.len());
                    let (new_child, changed) = Self::remove_rec(backend, child, rest)?;
                    if !changed {
                        return Ok((Some(node_handle), false));
                    }

                    if let Some(c) = new_child {
                        Ok((
                            Some(NodeHandle::InMemory(Box::new(Node::Extension {
                                path: ext_path,
                                child: c,
                            }))),
                            true,
                        ))
                    } else {
                        Ok((None, true))
                    }
                } else {
                    Ok((Some(node_handle), false))
                }
            }
            Node::Branch {
                mut children,
                value,
            } => {
                if path.is_empty() {
                    if value.is_some() {
                        if children.iter().all(|c| c.is_none()) {
                            Ok((None, true))
                        } else {
                            Ok((
                                Some(NodeHandle::InMemory(Box::new(Node::Branch {
                                    children,
                                    value: None,
                                }))),
                                true,
                            ))
                        }
                    } else {
                        Ok((Some(node_handle), false))
                    }
                } else {
                    let idx = path.at(0) as usize;
                    let (_, rest) = path.split_at(1);
                    if let Some(child) = &children[idx] {
                        let (new_child, changed) = Self::remove_rec(backend, child.clone(), rest)?;
                        if changed {
                            children[idx] = new_child;
                            if children.iter().all(|c| c.is_none()) && value.is_none() {
                                Ok((None, true))
                            } else {
                                Ok((
                                    Some(NodeHandle::InMemory(Box::new(Node::Branch {
                                        children,
                                        value,
                                    }))),
                                    true,
                                ))
                            }
                        } else {
                            Ok((Some(node_handle), false))
                        }
                    } else {
                        Ok((Some(node_handle), false))
                    }
                }
            }
        }
    }

    fn commit_rec(handle: NodeHandle, batch: &mut Vec<(Vec<u8>, Vec<u8>)>) -> Result<NodeHandle> {
        match handle {
            NodeHandle::InMemory(mut node) => {
                match *node {
                    Node::Extension { ref mut child, .. } => {
                        *child = Self::commit_rec(child.clone(), batch)?;
                    }
                    Node::Branch {
                        ref mut children, ..
                    } => {
                        for i in 0..16 {
                            if let Some(child) = &children[i] {
                                children[i] = Some(Self::commit_rec(child.clone(), batch)?);
                            }
                        }
                    }
                    _ => {}
                }

                let encoded = NodeCodec::encode(&node);
                let hash = Keccak256::digest(&encoded).to_vec();

                batch.push((hash.clone(), encoded));
                Ok(NodeHandle::Cached(hash, node))
            }
            h => Ok(h),
        }
    }
}
