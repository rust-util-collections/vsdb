use std::mem;

use crate::trie::{
    error::{Result, TrieError},
    nibbles::Nibbles,
    node::{Node, NodeCodec, NodeHandle},
};
use sha3::{Digest, Keccak256};

pub struct TrieMut {
    root: NodeHandle,
}

/// Maximum accepted MPT key length, in bytes.
///
/// Each key occupies at most two path nibbles per byte. Traversal,
/// cloning, cache loading and destruction use explicit work stacks, so
/// accepted prefix-heavy trees do not consume one call frame per node.
/// This bound also limits the paths accepted by the cache decoder.
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
            Self::insert_node(mem::take(&mut self.root), path, value.to_vec())?;
        self.root = new_root;
        Ok(())
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        let path = Nibbles::from_raw(key);
        let (new_root, _) = Self::remove_node(mem::take(&mut self.root), path)?;
        self.root = new_root.unwrap_or_default();
        Ok(())
    }

    /// Hashes the entire trie in place and returns the 32-byte root hash.
    ///
    /// On success `self`'s root holds the freshly hashed trie, so a
    /// subsequent call without intervening mutations is essentially
    /// free.  No failure path can discard trie data: `commit_nodes` is
    /// total (node encoding and hashing are infallible — its `Result`
    /// type is retained for the caller), and the defensive
    /// root-not-hashed check below restores the root before erroring.
    pub fn commit(&mut self) -> Result<Vec<u8>> {
        let root = mem::take(&mut self.root);

        if matches!(&root, NodeHandle::InMemory(n) if **n == Node::Null) {
            self.root = root;
            return Ok(vec![0u8; 32]);
        }

        match Self::commit_nodes(root) {
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
    /// node is unchanged so no-change `remove_node` paths don't force a re-hash.
    fn rewrap(cached_hash: &Option<Vec<u8>>, node: Node) -> NodeHandle {
        match cached_hash {
            Some(h) => NodeHandle::Cached(h.clone(), Box::new(node)),
            None => NodeHandle::InMemory(Box::new(node)),
        }
    }

    fn insert_node(
        mut handle: NodeHandle,
        mut path: Nibbles,
        value: Vec<u8>,
    ) -> Result<NodeHandle> {
        let mut ancestors = Vec::new();
        let mut root = loop {
            let mut node = handle.into_node();
            match &mut node {
                Node::Extension {
                    path: prefix,
                    child,
                } if path.starts_with(prefix) => {
                    let (_, rest) = path.split_at(prefix.len());
                    handle = mem::take(child);
                    path = rest;
                    ancestors.push((node, 0));
                }
                Node::Branch { children, .. } if !path.is_empty() => {
                    let index = path.at(0) as usize;
                    let (_, rest) = path.split_at(1);
                    handle = children[index].take().unwrap_or_default();
                    path = rest;
                    ancestors.push((node, index));
                }
                _ => break Self::insert_terminal(node, path, value)?,
            }
        };
        for (mut node, index) in ancestors.into_iter().rev() {
            match &mut node {
                Node::Extension { child, .. } => *child = root,
                Node::Branch { children, .. } => children[index] = Some(root),
                _ => unreachable!("only ancestors are retained"),
            }
            root = NodeHandle::InMemory(Box::new(node));
        }
        Ok(root)
    }

    /// Splits/replaces the terminal node after iterative descent.
    fn insert_terminal(node: Node, path: Nibbles, value: Vec<u8>) -> Result<NodeHandle> {
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

                let (common_path, _) = ext_path.split_at(common);
                let idx_ext = ext_path.at(common) as usize;
                let (_, rest_ext) = ext_path.split_at(common + 1);

                let mut children: Box<[Option<NodeHandle>; 16]> = Box::new([
                    None, None, None, None, None, None, None, None, None, None, None,
                    None, None, None, None, None,
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
            Node::Branch { children, .. } => {
                Ok(NodeHandle::InMemory(Box::new(Node::Branch {
                    children,
                    value: Some(value),
                })))
            }
        }
    }

    fn compact(mut node: Node) -> Option<NodeHandle> {
        loop {
            return match node {
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
                            node = Node::Extension {
                                path: new_path,
                                child: grand_child,
                            };
                            continue;
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
                        let ext_path =
                            Nibbles::from_nibbles_unsafe(vec![last_idx as u8]);
                        node = Node::Extension {
                            path: ext_path,
                            child: remaining_child,
                        };
                        continue;
                    } else {
                        Some(NodeHandle::InMemory(Box::new(Node::Branch {
                            children,
                            value,
                        })))
                    }
                }
            };
        }
    }

    fn remove_node(
        mut handle: NodeHandle,
        mut path: Nibbles,
    ) -> Result<(Option<NodeHandle>, bool)> {
        let mut ancestors = Vec::new();
        let (mut root, changed) = loop {
            let cached_hash = handle.hash().map(|h| h.to_vec());
            let mut node = handle.into_node();
            match &mut node {
                Node::Extension {
                    path: prefix,
                    child,
                } if path.starts_with(prefix) => {
                    let (_, rest) = path.split_at(prefix.len());
                    handle = mem::take(child);
                    path = rest;
                    ancestors.push((node, 0, cached_hash));
                    continue;
                }
                Node::Branch { children, .. } if !path.is_empty() => {
                    let index = path.at(0) as usize;
                    if let Some(child) = children[index].take() {
                        let (_, rest) = path.split_at(1);
                        handle = child;
                        path = rest;
                        ancestors.push((node, index, cached_hash));
                        continue;
                    }
                }
                _ => {}
            }
            break match node {
                Node::Null => (None, false),
                Node::Leaf { path: ref leaf, .. } if *leaf == path => (None, true),
                Node::Branch {
                    children,
                    value: Some(_),
                } if path.is_empty() => (
                    Self::compact(Node::Branch {
                        children,
                        value: None,
                    }),
                    true,
                ),
                node => (Some(Self::rewrap(&cached_hash, node)), false),
            };
        };
        for (mut node, index, cached_hash) in ancestors.into_iter().rev() {
            match &mut node {
                Node::Extension { child, .. } => {
                    let Some(replacement) = root else { continue };
                    *child = replacement;
                }
                Node::Branch { children, .. } => children[index] = root,
                _ => unreachable!("only ancestors are retained"),
            }
            root = if changed {
                Self::compact(node)
            } else {
                Some(Self::rewrap(&cached_hash, node))
            };
        }
        Ok((root, changed))
    }

    fn commit_nodes(handle: NodeHandle) -> Result<NodeHandle> {
        enum Work {
            Visit(NodeHandle),
            Finish(Node),
        }
        let mut pending = vec![Work::Visit(handle)];
        let mut ready = Vec::new();
        while let Some(work) = pending.pop() {
            match work {
                Work::Visit(handle) if handle.hash().is_some() => ready.push(handle),
                Work::Visit(handle) => {
                    let mut node = handle.into_node();
                    let children: Vec<_> = match &mut node {
                        Node::Extension { child, .. } => vec![mem::take(child)],
                        Node::Branch { children, .. } => {
                            children.iter_mut().flatten().map(mem::take).collect()
                        }
                        _ => Vec::new(),
                    };
                    pending.push(Work::Finish(node));
                    pending.extend(children.into_iter().rev().map(Work::Visit));
                }
                Work::Finish(mut node) => {
                    match &mut node {
                        Node::Extension { child, .. } => {
                            *child = ready.pop().expect("visited child")
                        }
                        Node::Branch { children, .. } => {
                            for child in children.iter_mut().rev().flatten() {
                                *child = ready.pop().expect("visited child");
                            }
                        }
                        _ => {}
                    }
                    let hash = Keccak256::digest(NodeCodec::encode(&node)).to_vec();
                    ready.push(NodeHandle::Cached(hash, Box::new(node)));
                }
            }
        }
        Ok(ready.pop().expect("visited root"))
    }
}
