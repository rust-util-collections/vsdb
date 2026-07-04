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
        let (new_root, _changed) =
            remove_rec(mem::take(&mut self.root), &full_path, 0, key_hash)?;
        self.root = new_root;
        Ok(())
    }

    /// Hashes the entire tree in place and returns the 32-byte root hash.
    ///
    /// `self`'s root is always restored before returning — on success it
    /// holds the freshly hashed root; on failure (defensive-only; the
    /// SMT's 256-bit depth cap makes `commit_rec` failure unreachable in
    /// practice) it is restored to whatever `commit_rec` handed back
    /// rather than left empty, so a rejected commit never silently
    /// discards tree data.
    pub fn commit(&mut self) -> Result<Vec<u8>> {
        let root = mem::take(&mut self.root);
        match commit_rec(root) {
            Ok(root) => {
                let result = root.expect_hash().map(<[u8]>::to_vec);
                self.root = root;
                result
            }
            Err(e) => Err(e),
        }
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
            // `leaf_path` equals the full path suffix from `depth`, so
            // comparing it against `full_path[depth..]` needs no slice.
            let common = full_path.common_prefix_from(depth, &leaf_path);

            // Create the new leaf.
            let new_leaf_path = full_path.slice(depth + common + 1, full_path.len());
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
            let new_bit = full_path.bit_at(depth + common);
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
            let common = full_path.common_prefix_from(depth, &path);

            if common < path.len() {
                // Path diverges within the compressed prefix.
                // Split the internal node.
                let diverge_bit_in_path = path.bit_at(common);
                let diverge_bit_in_key = full_path.bit_at(depth + common);

                // New leaf for the inserted key.
                let new_leaf_path = full_path.slice(depth + common + 1, full_path.len());
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

/// Re-wraps a node into a handle, preserving a precomputed hash when the
/// node is unchanged — mirrors MPT's `TrieMut::rewrap` — so a no-change
/// `remove_rec` path doesn't force a re-hash of the whole ancestor chain.
fn rewrap(cached_hash: &Option<Vec<u8>>, node: SmtNode) -> SmtHandle {
    match cached_hash {
        Some(h) => SmtHandle::Cached(h.clone(), Box::new(node)),
        None => SmtHandle::InMemory(Box::new(node)),
    }
}

fn remove_rec(
    handle: SmtHandle,
    full_path: &BitPath,
    depth: usize,
    key_hash: &[u8; 32],
) -> Result<(SmtHandle, bool)> {
    // Fast-path: inspect the node without consuming the handle.
    // If the key is not in this subtree, return the original handle
    // unchanged — preserving any Cached hash.
    match handle.node() {
        SmtNode::Empty => return Ok((handle, false)),
        SmtNode::Leaf {
            key_hash: leaf_kh, ..
        } => {
            if leaf_kh != key_hash {
                return Ok((handle, false));
            }
            // Key matches — fall through to consume the handle.
        }
        SmtNode::Internal { path, .. } => {
            if !full_path.starts_with_from(depth, path) {
                return Ok((handle, false));
            }
            let next_depth = depth + path.len();
            if next_depth >= full_path.len() {
                return Ok((handle, false));
            }
            // Path matches — fall through to consume the handle.
        }
    }

    // Past here the node *may* be modified — peek its cached hash
    // first (before consuming) so the Internal case below can restore
    // it if neither child subtree actually changed: the fast-path
    // check above only guarantees the key's path is *plausible*
    // through this node, not that the key truly exists deeper down.
    let cached_hash = handle.hash().map(|h| h.to_vec());
    let node = handle.into_node();

    match node {
        SmtNode::Empty => unreachable!("handled above"),

        SmtNode::Leaf { .. } => {
            // Key matched — delete by returning empty.
            Ok((SmtHandle::default(), true))
        }

        SmtNode::Internal { path, left, right } => {
            let next_depth = depth + path.len();
            let bit = full_path.bit_at(next_depth);
            let child_depth = next_depth + 1;

            let (new_left, new_right, changed) = if bit == 0 {
                let (new_left, changed) =
                    remove_rec(left, full_path, child_depth, key_hash)?;
                (new_left, right, changed)
            } else {
                let (new_right, changed) =
                    remove_rec(right, full_path, child_depth, key_hash)?;
                (left, new_right, changed)
            };

            if !changed {
                // Neither child actually changed — restore this node's
                // Cached hash instead of unconditionally reconstructing
                // it via `compact` (which would force a re-hash of the
                // whole ancestor chain on the next `commit`).
                let n = SmtNode::Internal {
                    path,
                    left: new_left,
                    right: new_right,
                };
                return Ok((rewrap(&cached_hash, n), false));
            }

            Ok((compact(path, new_left, new_right)?, true))
        }
    }
}

/// Compacts an Internal node after a child becomes empty.
fn compact(path: BitPath, left: SmtHandle, right: SmtHandle) -> Result<SmtHandle> {
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

    // Exactly one child is empty — promote the survivor.
    let (surviving, surviving_bit) = if left_empty {
        (right, 1u8)
    } else {
        (left, 0u8)
    };

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
                    ref key_hash,
                    ref value,
                    ..
                } => {
                    // Depth-independent leaf shortcut: a lone-leaf
                    // subtree commits to the leaf hash directly; the
                    // residual path is NOT folded into the hash (the
                    // full key_hash already binds the position).
                    hash_leaf(key_hash, value)
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
