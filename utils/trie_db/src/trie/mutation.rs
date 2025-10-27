use crate::error::{Result, TrieError};
use crate::nibbles::Nibbles;
use crate::node::{Node, NodeCodec, NodeHandle};
use crate::storage::TrieBackend;
use sha3::{Digest, Keccak256};
use std::collections::HashSet;

pub struct TrieMut<'a, B: TrieBackend> {
    root: NodeHandle,
    backend: &'a mut B,
    removed_hashes: Vec<Vec<u8>>,
}

impl<'a, B: TrieBackend> TrieMut<'a, B> {
    pub fn new(root_hash: &[u8], backend: &'a mut B) -> Self {
        let root = if root_hash.iter().all(|&b| b == 0) {
            NodeHandle::InMemory(Box::new(Node::Null))
        } else {
            NodeHandle::Hash(root_hash.to_vec())
        };
        Self {
            root,
            backend,
            removed_hashes: Vec::new(),
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let path = Nibbles::from_raw(key, false);
        let new_root = Self::insert_rec(
            self.backend,
            &mut self.removed_hashes,
            self.root.clone(),
            path,
            value.to_vec(),
        )?;
        self.root = new_root;
        Ok(())
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        let path = Nibbles::from_raw(key, false);
        let (new_root, _) = Self::remove_rec(
            self.backend,
            &mut self.removed_hashes,
            self.root.clone(),
            path,
        )?;
        self.root = new_root.unwrap_or(NodeHandle::InMemory(Box::new(Node::Null)));
        Ok(())
    }

    pub fn commit(self) -> Result<Vec<u8>> {
        let root = self.root;
        let backend = self.backend;
        let removed_hashes = self.removed_hashes;

        // Special case: empty trie (root is Null)
        if matches!(&root, NodeHandle::InMemory(n) if **n == Node::Null) {
            // Still clean up any obsolete nodes from the DB
            if !removed_hashes.is_empty() {
                backend.remove_batch(&removed_hashes)?;
            }
            return Ok(vec![0u8; 32]);
        }

        let mut batch = Vec::new();
        let root_handle = Self::commit_rec(root, &mut batch)?;

        // Deduplicate batch entries (same hash can appear multiple times for identical subtrees)
        batch.sort_by(|a, b| a.0.cmp(&b.0));
        batch.dedup_by(|a, b| a.0 == b.0);

        // Deduplicate: do not remove hashes that are being (re-)inserted in this commit.
        // This prevents corruption when a node with the same content exists in multiple
        // parts of the trie, or when a modification produces a node identical to one
        // that was previously removed in the same transaction.
        let inserted_set: HashSet<&[u8]> = batch.iter().map(|(k, _)| k.as_slice()).collect();
        let mut to_remove: Vec<Vec<u8>> = removed_hashes
            .into_iter()
            .filter(|h| !inserted_set.contains(h.as_slice()))
            .collect();

        to_remove.sort();
        to_remove.dedup();

        backend.insert_batch(batch)?;
        if !to_remove.is_empty() {
            backend.remove_batch(&to_remove)?;
        }

        match root_handle {
            NodeHandle::Hash(h) | NodeHandle::Cached(h, _) => Ok(h),
            NodeHandle::InMemory(_) => Err(TrieError::InvalidState(
                "Root should be hashed after commit".into(),
            )),
        }
    }

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

    fn add_obsolete(removed_hashes: &mut Vec<Vec<u8>>, handle: &NodeHandle) {
        if let Some(h) = handle.hash() {
            removed_hashes.push(h.to_vec());
        }
    }

    fn insert_rec(
        backend: &B,
        removed_hashes: &mut Vec<Vec<u8>>,
        node_handle: NodeHandle,
        path: Nibbles,
        value: Vec<u8>,
    ) -> Result<NodeHandle> {
        let node = Self::resolve(backend, &node_handle)?;

        match node {
            Node::Null => {
                Self::add_obsolete(removed_hashes, &node_handle);
                Ok(NodeHandle::InMemory(Box::new(Node::Leaf { path, value })))
            }
            Node::Leaf {
                path: leaf_path,
                value: leaf_value,
            } => {
                Self::add_obsolete(removed_hashes, &node_handle);
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
                    let new_child = Self::insert_rec(backend, removed_hashes, child, rest, value)?;
                    Self::add_obsolete(removed_hashes, &node_handle);
                    Ok(NodeHandle::InMemory(Box::new(Node::Extension {
                        path: ext_path,
                        child: new_child,
                    })))
                } else {
                    Self::add_obsolete(removed_hashes, &node_handle);
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
                    Self::add_obsolete(removed_hashes, &node_handle);
                    Ok(NodeHandle::InMemory(Box::new(Node::Branch {
                        children,
                        value: Some(value),
                    })))
                } else {
                    Self::add_obsolete(removed_hashes, &node_handle);
                    let idx = path.at(0) as usize;
                    let (_, rest) = path.split_at(1);
                    let child = children[idx]
                        .clone()
                        .unwrap_or(NodeHandle::InMemory(Box::new(Node::Null)));
                    let new_child = Self::insert_rec(backend, removed_hashes, child, rest, value)?;
                    children[idx] = Some(new_child);
                    Ok(NodeHandle::InMemory(Box::new(Node::Branch {
                        children,
                        value: b_value,
                    })))
                }
            }
        }
    }

    fn compact(
        backend: &B,
        removed_hashes: &mut Vec<Vec<u8>>,
        node: Node,
    ) -> Result<Option<NodeHandle>> {
        match node {
            Node::Null => Ok(None),
            Node::Leaf { path, value } => Ok(Some(NodeHandle::InMemory(Box::new(Node::Leaf {
                path,
                value,
            })))),
            Node::Extension { path, child } => {
                let child_node = Self::resolve(backend, &child)?;
                match child_node {
                    Node::Extension {
                        path: ref child_path,
                        child: grand_child,
                    } => {
                        Self::add_obsolete(removed_hashes, &child);
                        let mut new_path_data = path.as_slice().to_vec();
                        new_path_data.extend_from_slice(child_path.as_slice());
                        let new_path = Nibbles::from_nibbles_unsafe(new_path_data);
                        Self::compact(
                            backend,
                            removed_hashes,
                            Node::Extension {
                                path: new_path,
                                child: grand_child,
                            },
                        )
                    }
                    Node::Leaf {
                        path: ref child_path,
                        value,
                    } => {
                        Self::add_obsolete(removed_hashes, &child);
                        let mut new_path_data = path.as_slice().to_vec();
                        new_path_data.extend_from_slice(child_path.as_slice());
                        let new_path = Nibbles::from_nibbles_unsafe(new_path_data);
                        Ok(Some(NodeHandle::InMemory(Box::new(Node::Leaf {
                            path: new_path,
                            value,
                        }))))
                    }
                    _ => Ok(Some(NodeHandle::InMemory(Box::new(Node::Extension {
                        path,
                        child,
                    })))),
                }
            }
            Node::Branch {
                mut children,
                value,
            } => {
                let mut num_children = 0;
                let mut last_idx = 0;
                for (i, c) in children.iter().enumerate() {
                    if c.is_some() {
                        num_children += 1;
                        last_idx = i;
                    }
                }

                if num_children == 0 {
                    if let Some(v) = value {
                        Ok(Some(NodeHandle::InMemory(Box::new(Node::Leaf {
                            path: Nibbles::default(),
                            value: v,
                        }))))
                    } else {
                        Ok(None)
                    }
                } else if num_children == 1 && value.is_none() {
                    let remaining_child = children[last_idx].take().unwrap();
                    let ext_path = Nibbles::from_nibbles_unsafe(vec![last_idx as u8]);
                    Self::compact(
                        backend,
                        removed_hashes,
                        Node::Extension {
                            path: ext_path,
                            child: remaining_child,
                        },
                    )
                } else {
                    Ok(Some(NodeHandle::InMemory(Box::new(Node::Branch {
                        children,
                        value,
                    }))))
                }
            }
        }
    }

    fn remove_rec(
        backend: &B,
        removed_hashes: &mut Vec<Vec<u8>>,
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
                    Self::add_obsolete(removed_hashes, &node_handle);
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
                    let (new_child, changed) =
                        Self::remove_rec(backend, removed_hashes, child, rest)?;
                    if !changed {
                        return Ok((Some(node_handle), false));
                    }

                    Self::add_obsolete(removed_hashes, &node_handle);
                    if let Some(c) = new_child {
                        let compacted = Self::compact(
                            backend,
                            removed_hashes,
                            Node::Extension {
                                path: ext_path,
                                child: c,
                            },
                        )?;
                        Ok((compacted, true))
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
                        Self::add_obsolete(removed_hashes, &node_handle);
                        let compacted = Self::compact(
                            backend,
                            removed_hashes,
                            Node::Branch {
                                children,
                                value: None,
                            },
                        )?;
                        Ok((compacted, true))
                    } else {
                        Ok((Some(node_handle), false))
                    }
                } else {
                    let idx = path.at(0) as usize;
                    let (_, rest) = path.split_at(1);
                    if let Some(child) = &children[idx] {
                        let (new_child, changed) =
                            Self::remove_rec(backend, removed_hashes, child.clone(), rest)?;
                        if changed {
                            Self::add_obsolete(removed_hashes, &node_handle);
                            children[idx] = new_child;
                            let compacted = Self::compact(
                                backend,
                                removed_hashes,
                                Node::Branch { children, value },
                            )?;
                            Ok((compacted, true))
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
