use crate::error::{Result, TrieError};
use crate::nibbles::Nibbles;
use crate::node::{Node, NodeCodec, NodeHandle};
use sha3::{Digest, Keccak256};

pub struct TrieMut {
    root: NodeHandle,
}

impl TrieMut {
    pub fn new(root: NodeHandle) -> Self {
        Self { root }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let path = Nibbles::from_raw(key, false);
        let new_root =
            Self::insert_rec(self.root.clone(), path, value.to_vec())?;
        self.root = new_root;
        Ok(())
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        let path = Nibbles::from_raw(key, false);
        let (new_root, _) =
            Self::remove_rec(self.root.clone(), path)?;
        self.root = new_root.unwrap_or_default();
        Ok(())
    }

    /// Hash the entire trie and return `(root_hash, hashed_root)`.
    pub fn commit(self) -> Result<(Vec<u8>, NodeHandle)> {
        let root = self.root;

        if matches!(&root, NodeHandle::InMemory(n) if **n == Node::Null) {
            return Ok((vec![0u8; 32], root));
        }

        let root_handle = Self::commit_rec(root)?;
        match &root_handle {
            NodeHandle::Cached(h, _) => Ok((h.clone(), root_handle)),
            NodeHandle::InMemory(_) => Err(TrieError::InvalidState(
                "Root should be hashed after commit".into(),
            )),
        }
    }

    pub fn into_root(self) -> NodeHandle {
        self.root
    }

    fn resolve(handle: &NodeHandle) -> Node {
        match handle {
            NodeHandle::InMemory(n) | NodeHandle::Cached(_, n) => *n.clone(),
        }
    }

    fn insert_rec(
        node_handle: NodeHandle,
        path: Nibbles,
        value: Vec<u8>,
    ) -> Result<NodeHandle> {
        let node = Self::resolve(&node_handle);

        match node {
            Node::Null => {
                Ok(NodeHandle::InMemory(Box::new(Node::Leaf { path, value })))
            }
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
                    let new_child = Self::insert_rec(child, rest, value)?;
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
                        .unwrap_or_default();
                    let new_child = Self::insert_rec(child, rest, value)?;
                    children[idx] = Some(new_child);
                    Ok(NodeHandle::InMemory(Box::new(Node::Branch {
                        children,
                        value: b_value,
                    })))
                }
            }
        }
    }

    fn compact(node: Node) -> Option<NodeHandle> {
        match node {
            Node::Null => None,
            Node::Leaf { path, value } => Some(NodeHandle::InMemory(Box::new(Node::Leaf {
                path,
                value,
            }))),
            Node::Extension { path, child } => {
                let child_node = Self::resolve(&child);
                match child_node {
                    Node::Extension {
                        path: ref child_path,
                        child: grand_child,
                    } => {
                        let mut new_path_data = path.as_slice().to_vec();
                        new_path_data.extend_from_slice(child_path.as_slice());
                        let new_path = Nibbles::from_nibbles_unsafe(new_path_data);
                        Self::compact(Node::Extension {
                            path: new_path,
                            child: grand_child,
                        })
                    }
                    Node::Leaf {
                        path: ref child_path,
                        value,
                    } => {
                        let mut new_path_data = path.as_slice().to_vec();
                        new_path_data.extend_from_slice(child_path.as_slice());
                        let new_path = Nibbles::from_nibbles_unsafe(new_path_data);
                        Some(NodeHandle::InMemory(Box::new(Node::Leaf {
                            path: new_path,
                            value,
                        })))
                    }
                    _ => Some(NodeHandle::InMemory(Box::new(Node::Extension {
                        path,
                        child,
                    }))),
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
                        Some(NodeHandle::InMemory(Box::new(Node::Leaf {
                            path: Nibbles::default(),
                            value: v,
                        })))
                    } else {
                        None
                    }
                } else if num_children == 1 && value.is_none() {
                    let remaining_child = children[last_idx].take().unwrap();
                    let ext_path = Nibbles::from_nibbles_unsafe(vec![last_idx as u8]);
                    Self::compact(Node::Extension {
                        path: ext_path,
                        child: remaining_child,
                    })
                } else {
                    Some(NodeHandle::InMemory(Box::new(Node::Branch {
                        children,
                        value,
                    })))
                }
            }
        }
    }

    fn remove_rec(
        node_handle: NodeHandle,
        path: Nibbles,
    ) -> Result<(Option<NodeHandle>, bool)> {
        let node = Self::resolve(&node_handle);

        match node {
            Node::Null => Ok((None, false)),
            Node::Leaf {
                path: leaf_path, ..
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
                    let (new_child, changed) =
                        Self::remove_rec(child, rest)?;
                    if !changed {
                        return Ok((Some(node_handle), false));
                    }

                    if let Some(c) = new_child {
                        let compacted = Self::compact(Node::Extension {
                            path: ext_path,
                            child: c,
                        });
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
                        let compacted = Self::compact(Node::Branch {
                            children,
                            value: None,
                        });
                        Ok((compacted, true))
                    } else {
                        Ok((Some(node_handle), false))
                    }
                } else {
                    let idx = path.at(0) as usize;
                    let (_, rest) = path.split_at(1);
                    if let Some(child) = &children[idx] {
                        let (new_child, changed) =
                            Self::remove_rec(child.clone(), rest)?;
                        if changed {
                            children[idx] = new_child;
                            let compacted = Self::compact(Node::Branch { children, value });
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

    fn commit_rec(handle: NodeHandle) -> Result<NodeHandle> {
        match handle {
            NodeHandle::InMemory(mut node) => {
                match *node {
                    Node::Extension { ref mut child, .. } => {
                        *child = Self::commit_rec(child.clone())?;
                    }
                    Node::Branch {
                        ref mut children, ..
                    } => {
                        for slot in children.iter_mut() {
                            if let Some(child) = slot {
                                *child = Self::commit_rec(child.clone())?;
                            }
                        }
                    }
                    _ => {}
                }

                let encoded = NodeCodec::encode(&node);
                let hash = Keccak256::digest(&encoded).to_vec();
                Ok(NodeHandle::Cached(hash, node))
            }
            h => Ok(h),
        }
    }
}
