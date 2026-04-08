//!
//! SMT insert, remove, and commit (hashing).
//!

use std::mem;

use crate::trie::error::{Result, TrieError};

use super::bitpath::BitPath;
use super::codec::{hash_internal, hash_leaf, wrap_hash};
use super::{EMPTY_HASH, SmtHandle, SmtNode};

pub struct SmtMut {
    root: SmtHandle,
}

impl SmtMut {
    pub fn new(root: SmtHandle) -> Self {
        Self { root }
    }

    pub fn insert(&mut self, key_hash: &[u8; 32], value: &[u8]) -> Result<()> {
        let full_path = BitPath::from_hash(key_hash);
        let new_root = insert_rec(
            mem::take(&mut self.root),
            &full_path,
            0,
            *key_hash,
            value.to_vec(),
        )?;
        self.root = new_root;
        Ok(())
    }

    pub fn remove(&mut self, key_hash: &[u8; 32]) -> Result<()> {
        let full_path = BitPath::from_hash(key_hash);
        let new_root = remove_rec(mem::take(&mut self.root), &full_path, 0, key_hash)?;
        self.root = new_root;
        Ok(())
    }

    /// Hash the entire tree. Returns `(root_hash, hashed_root)`.
    pub fn commit(self) -> Result<(Vec<u8>, SmtHandle)> {
        let root = commit_rec(self.root)?;
        let hash = root.expect_hash()?.to_vec();
        Ok((hash, root))
    }

    pub fn into_root(self) -> SmtHandle {
        self.root
    }
}

// =========================================================================
// Insert
// =========================================================================

fn insert_rec(
    handle: SmtHandle,
    full_path: &BitPath,
    depth: usize,
    key_hash: [u8; 32],
    value: Vec<u8>,
) -> Result<SmtHandle> {
    let node = handle.into_node();

    match node {
        SmtNode::Empty => {
            // Place a leaf with the remaining bits as its path.
            let remaining = full_path.slice(depth, full_path.len());
            Ok(SmtHandle::InMemory(Box::new(SmtNode::Leaf {
                path: remaining,
                key_hash,
                value,
            })))
        }

        SmtNode::Leaf {
            path: leaf_path,
            key_hash: leaf_kh,
            value: leaf_val,
        } => {
            if leaf_kh == key_hash {
                // Same key — update value.
                return Ok(SmtHandle::InMemory(Box::new(SmtNode::Leaf {
                    path: leaf_path,
                    key_hash,
                    value,
                })));
            }

            // Different keys: find where the remaining paths diverge.
            let new_remaining = full_path.slice(depth, full_path.len());
            let common = leaf_path.common_prefix(&new_remaining);

            // Create the new leaf.
            let new_leaf_path = new_remaining.slice(common + 1, new_remaining.len());
            let new_leaf = SmtHandle::InMemory(Box::new(SmtNode::Leaf {
                path: new_leaf_path,
                key_hash,
                value,
            }));

            // Adjust the old leaf's path.
            let old_leaf_path = leaf_path.slice(common + 1, leaf_path.len());
            let old_leaf = SmtHandle::InMemory(Box::new(SmtNode::Leaf {
                path: old_leaf_path,
                key_hash: leaf_kh,
                value: leaf_val,
            }));

            // The divergence bit determines left/right.
            let new_bit = new_remaining.bit_at(common);
            let (left, right) = if new_bit == 0 {
                (new_leaf, old_leaf)
            } else {
                (old_leaf, new_leaf)
            };

            let prefix = leaf_path.slice(0, common);
            Ok(SmtHandle::InMemory(Box::new(SmtNode::Internal {
                path: prefix,
                left,
                right,
            })))
        }

        SmtNode::Internal { path, left, right } => {
            let remaining = full_path.slice(depth, full_path.len());

            let common = remaining.common_prefix(&path);

            if common < path.len() {
                // Path diverges within the compressed prefix.
                // Split the internal node.
                let diverge_bit_in_path = path.bit_at(common);
                let diverge_bit_in_key = remaining.bit_at(common);

                // New leaf for the inserted key.
                let new_leaf_path = remaining.slice(common + 1, remaining.len());
                let new_leaf = SmtHandle::InMemory(Box::new(SmtNode::Leaf {
                    path: new_leaf_path,
                    key_hash,
                    value,
                }));

                // Old internal with shortened path.
                let old_suffix = path.slice(common + 1, path.len());
                let old_internal = SmtHandle::InMemory(Box::new(SmtNode::Internal {
                    path: old_suffix,
                    left,
                    right,
                }));

                let (new_left, new_right) = if diverge_bit_in_key == 0 {
                    debug_assert_eq!(diverge_bit_in_path, 1);
                    (new_leaf, old_internal)
                } else {
                    debug_assert_eq!(diverge_bit_in_path, 0);
                    (old_internal, new_leaf)
                };

                let prefix = path.slice(0, common);
                Ok(SmtHandle::InMemory(Box::new(SmtNode::Internal {
                    path: prefix,
                    left: new_left,
                    right: new_right,
                })))
            } else {
                // Full prefix matches. Descend into the appropriate child.
                let next_depth = depth + path.len();
                if next_depth >= full_path.len() {
                    return Err(TrieError::InvalidState(
                        "SMT depth exceeded 256 bits".into(),
                    ));
                }

                let bit = full_path.bit_at(next_depth);
                let child_depth = next_depth + 1;

                let (new_left, new_right) = if bit == 0 {
                    let new_left =
                        insert_rec(left, full_path, child_depth, key_hash, value)?;
                    (new_left, right)
                } else {
                    let new_right =
                        insert_rec(right, full_path, child_depth, key_hash, value)?;
                    (left, new_right)
                };

                Ok(SmtHandle::InMemory(Box::new(SmtNode::Internal {
                    path,
                    left: new_left,
                    right: new_right,
                })))
            }
        }
    }
}

// =========================================================================
// Remove
// =========================================================================

fn remove_rec(
    handle: SmtHandle,
    full_path: &BitPath,
    depth: usize,
    key_hash: &[u8; 32],
) -> Result<SmtHandle> {
    let node = handle.into_node();

    match node {
        SmtNode::Empty => Ok(SmtHandle::default()),

        SmtNode::Leaf {
            key_hash: leaf_kh,
            path: leaf_path,
            value: leaf_val,
        } => {
            if &leaf_kh == key_hash {
                Ok(SmtHandle::default())
            } else {
                // Not our key — keep it.
                Ok(SmtHandle::InMemory(Box::new(SmtNode::Leaf {
                    path: leaf_path,
                    key_hash: leaf_kh,
                    value: leaf_val,
                })))
            }
        }

        SmtNode::Internal { path, left, right } => {
            let remaining = full_path.slice(depth, full_path.len());

            if !remaining.starts_with(&path) {
                // Path doesn't match — key not in tree.
                return Ok(SmtHandle::InMemory(Box::new(SmtNode::Internal {
                    path,
                    left,
                    right,
                })));
            }

            let next_depth = depth + path.len();
            if next_depth >= full_path.len() {
                // Shouldn't happen for valid 256-bit keys.
                return Ok(SmtHandle::InMemory(Box::new(SmtNode::Internal {
                    path,
                    left,
                    right,
                })));
            }

            let bit = full_path.bit_at(next_depth);
            let child_depth = next_depth + 1;

            let (new_left, new_right) = if bit == 0 {
                let new_left = remove_rec(left, full_path, child_depth, key_hash)?;
                (new_left, right)
            } else {
                let new_right = remove_rec(right, full_path, child_depth, key_hash)?;
                (left, new_right)
            };

            // Compact: if one child is now empty, promote the other.
            compact(path, bit, new_left, new_right)
        }
    }
}

/// Compacts an Internal node after a child becomes empty.
fn compact(
    path: BitPath,
    removed_side: u8,
    left: SmtHandle,
    right: SmtHandle,
) -> Result<SmtHandle> {
    let left_empty = left.is_empty();
    let right_empty = right.is_empty();

    if left_empty && right_empty {
        return Ok(SmtHandle::default());
    }

    if !left_empty && !right_empty {
        // Both non-empty — keep the internal node.
        return Ok(SmtHandle::InMemory(Box::new(SmtNode::Internal {
            path,
            left,
            right,
        })));
    }

    // Exactly one child is empty. Promote the other.
    let (surviving, surviving_bit) = if left_empty {
        (right, 1u8)
    } else {
        (left, 0u8)
    };

    // Only compact if the removed side is the one that became empty.
    // This prevents false compaction when the surviving child was
    // already empty before the remove.
    if surviving_bit != removed_side {
        // The non-removed side became empty somehow — this shouldn't
        // happen in normal operation but handle it gracefully.
        if surviving.is_empty() {
            return Ok(SmtHandle::default());
        }
    }

    let surviving_node = surviving.into_node();
    match surviving_node {
        SmtNode::Leaf {
            path: child_path,
            key_hash,
            value,
        } => {
            // Absorb: parent_path + bit + child_path → new leaf path.
            let bit_path = BitPath::from_bits(&[surviving_bit]);
            let new_path = path.concat(&bit_path).concat(&child_path);
            Ok(SmtHandle::InMemory(Box::new(SmtNode::Leaf {
                path: new_path,
                key_hash,
                value,
            })))
        }
        SmtNode::Internal {
            path: child_path,
            left: cl,
            right: cr,
        } => {
            // Merge paths: parent_path + bit + child_path.
            let bit_path = BitPath::from_bits(&[surviving_bit]);
            let new_path = path.concat(&bit_path).concat(&child_path);
            Ok(SmtHandle::InMemory(Box::new(SmtNode::Internal {
                path: new_path,
                left: cl,
                right: cr,
            })))
        }
        SmtNode::Empty => Ok(SmtHandle::default()),
    }
}

// =========================================================================
// Commit (hash all nodes)
// =========================================================================

fn commit_rec(handle: SmtHandle) -> Result<SmtHandle> {
    match handle {
        SmtHandle::InMemory(mut node) => {
            let hash = match *node {
                SmtNode::Empty => {
                    return Ok(SmtHandle::Cached(EMPTY_HASH.to_vec(), node));
                }
                SmtNode::Leaf {
                    ref path,
                    ref key_hash,
                    ref value,
                } => {
                    // Hash the leaf, then wrap through its compressed path.
                    let leaf_h = hash_leaf(key_hash, value);
                    wrap_hash(leaf_h, path)
                }
                SmtNode::Internal {
                    ref path,
                    ref mut left,
                    ref mut right,
                } => {
                    // Recursively commit children first.
                    *left = commit_rec(mem::take(left))?;
                    *right = commit_rec(mem::take(right))?;

                    // Combine children at the split point, then wrap
                    // through the compressed path.
                    let left_h: [u8; 32] = left
                        .expect_hash()?
                        .try_into()
                        .map_err(|_| TrieError::InvalidState("bad hash len".into()))?;
                    let right_h: [u8; 32] = right
                        .expect_hash()?
                        .try_into()
                        .map_err(|_| TrieError::InvalidState("bad hash len".into()))?;

                    let internal_h = hash_internal(&left_h, &right_h);
                    wrap_hash(internal_h, path)
                }
            };
            Ok(SmtHandle::Cached(hash.to_vec(), node))
        }
        cached => Ok(cached),
    }
}
