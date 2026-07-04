use std::mem;

use crate::trie::error::{Result, TrieError};
use crate::trie::nibbles::Nibbles;
use crate::trie::node::{Node, NodeCodec, NodeHandle};
use sha3::{Digest, Keccak256};

pub struct TrieMut {
    root: NodeHandle,
}

/// Maximum accepted MPT key length, in bytes.
///
/// MPT traversal (insert / remove / commit / drop) recurses once per node
/// on a key's path, and path depth is proportional to the longest shared
/// key prefix — up to two nibbles' worth of nodes per key byte.  Without
/// a bound, an adversarial key set (e.g. `[0;1], [0;2], …, [0;N]`) builds
/// a chain deep enough to overflow the stack, which aborts the process.
/// Rejecting oversized keys at insertion bounds the depth of every
/// subsequent traversal to a few thousand frames, safe on default thread
/// stacks.  (The SMT is immune: its depth is hard-capped at 256 bits.)
pub const MAX_MPT_KEY_LEN: usize = 1024;

impl TrieMut {
    pub fn new(root: NodeHandle) -> Self {
        Self { root }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.len() > MAX_MPT_KEY_LEN {
            return Err(TrieError::InvalidState(format!(
                "key length {} exceeds MAX_MPT_KEY_LEN ({MAX_MPT_KEY_LEN})",
                key.len()
            )));
        }
        let path = Nibbles::from_raw(key);
        let new_root =
            Self::insert_rec(mem::take(&mut self.root), path, value.to_vec())?;
        self.root = new_root;
        Ok(())
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        let path = Nibbles::from_raw(key);
        let (new_root, _) = Self::remove_rec(mem::take(&mut self.root), path)?;
        self.root = new_root.unwrap_or_default();
        Ok(())
    }

    /// Hashes the entire trie in place and returns the 32-byte root hash.
    ///
    /// On success `self`'s root holds the freshly hashed trie, so a
    /// subsequent call without intervening mutations is essentially
    /// free.  No failure path can discard trie data: `commit_rec` is
    /// total (node encoding and hashing are infallible — its `Result`
    /// type only propagates child recursion), and the defensive
    /// root-not-hashed check below restores the root before erroring.
    pub fn commit(&mut self) -> Result<Vec<u8>> {
        let root = mem::take(&mut self.root);

        if matches!(&root, NodeHandle::InMemory(n) if **n == Node::Null) {
            self.root = root;
            return Ok(vec![0u8; 32]);
        }

        match Self::commit_rec(root) {
            Ok(root_handle) => {
                let result = match &root_handle {
                    NodeHandle::Cached(h, _) => Ok(h.clone()),
                    NodeHandle::InMemory(_) => Err(TrieError::InvalidState(
                        "Root should be hashed after commit".into(),
                    )),
                };
                self.root = root_handle;
                result
            }
            Err(e) => Err(e),
        }
    }

    pub fn into_root(self) -> NodeHandle {
        self.root
    }

    /// Re-wrap a node into a handle, preserving a precomputed hash when the
    /// node is unchanged so no-change `remove_rec` paths don't force a re-hash.
    fn rewrap(cached_hash: &Option<Vec<u8>>, node: Node) -> NodeHandle {
        match cached_hash {
            Some(h) => NodeHandle::Cached(h.clone(), Box::new(node)),
            None => NodeHandle::InMemory(Box::new(node)),
        }
    }

    fn insert_rec(
        node_handle: NodeHandle,
        path: Nibbles,
        value: Vec<u8>,
    ) -> Result<NodeHandle> {
        // insert_rec always rebuilds the node, so consume the handle by move
        // rather than deep-cloning the whole subtree via `resolve`.
        let node = node_handle.into_node();

        match node {
            Node::Null => Ok(NodeHandle::InMemory(Box::new(Node::Leaf { path, value }))),
            Node::Leaf {
                path: leaf_path,
                value: leaf_value,
            } => {
                let common = path.common_prefix(&leaf_path);

                if common == path.len() && common == leaf_path.len() {
                    return Ok(NodeHandle::InMemory(Box::new(Node::Leaf {
                        path,
                        value,
                    })));
                }

                let mut children: Box<[Option<NodeHandle>; 16]> = Box::new([
                    None, None, None, None, None, None, None, None, None, None, None,
                    None, None, None, None, None,
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
                        None, None, None, None, None, None, None, None, None, None,
                        None, None, None, None, None, None,
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
                        children[idx_new] =
                            Some(NodeHandle::InMemory(Box::new(Node::Leaf {
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
                    let child = children[idx].take().unwrap_or_default();
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
            Node::Leaf { path, value } => {
                Some(NodeHandle::InMemory(Box::new(Node::Leaf { path, value })))
            }
            Node::Extension { path, child } => {
                // Peek the cached hash so the no-merge path can rebuild
                // the original handle without re-hashing, then consume the
                // child by move — no subtree clone.
                let cached_hash = child.hash().map(|h| h.to_vec());
                match child.into_node() {
                    Node::Extension {
                        path: child_path,
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
                        path: child_path,
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
                    other => Some(NodeHandle::InMemory(Box::new(Node::Extension {
                        path,
                        child: Self::rewrap(&cached_hash, other),
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
                    value.map(|v| {
                        NodeHandle::InMemory(Box::new(Node::Leaf {
                            path: Nibbles::default(),
                            value: v,
                        }))
                    })
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
        // Peek the cached hash so no-change paths can rebuild the original
        // handle (preserving the precomputed hash) after moving the node out,
        // instead of deep-cloning the whole subtree via `resolve`.
        let cached_hash = node_handle.hash().map(|h| h.to_vec());
        let node = node_handle.into_node();

        match node {
            Node::Null => Ok((None, false)),
            Node::Leaf {
                path: leaf_path,
                value,
            } => {
                if leaf_path == path {
                    Ok((None, true))
                } else {
                    let n = Node::Leaf {
                        path: leaf_path,
                        value,
                    };
                    Ok((Some(Self::rewrap(&cached_hash, n)), false))
                }
            }
            Node::Extension {
                path: ext_path,
                child,
            } => {
                if path.starts_with(&ext_path) {
                    let (_, rest) = path.split_at(ext_path.len());
                    let (new_child, changed) = Self::remove_rec(child, rest)?;
                    if !changed {
                        let n = Node::Extension {
                            path: ext_path,
                            child: new_child.unwrap_or_default(),
                        };
                        return Ok((Some(Self::rewrap(&cached_hash, n)), false));
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
                    let n = Node::Extension {
                        path: ext_path,
                        child,
                    };
                    Ok((Some(Self::rewrap(&cached_hash, n)), false))
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
                        let n = Node::Branch { children, value };
                        Ok((Some(Self::rewrap(&cached_hash, n)), false))
                    }
                } else {
                    let idx = path.at(0) as usize;
                    let (_, rest) = path.split_at(1);
                    if let Some(child) = children[idx].take() {
                        let (new_child, changed) = Self::remove_rec(child, rest)?;
                        children[idx] = new_child;
                        if changed {
                            let compacted =
                                Self::compact(Node::Branch { children, value });
                            Ok((compacted, true))
                        } else {
                            let n = Node::Branch { children, value };
                            Ok((Some(Self::rewrap(&cached_hash, n)), false))
                        }
                    } else {
                        let n = Node::Branch { children, value };
                        Ok((Some(Self::rewrap(&cached_hash, n)), false))
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
                        *child = Self::commit_rec(mem::take(child))?;
                    }
                    Node::Branch {
                        ref mut children, ..
                    } => {
                        for child in children.iter_mut().flatten() {
                            *child = Self::commit_rec(mem::take(child))?;
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
