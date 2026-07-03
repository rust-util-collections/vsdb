//!
//! Read-only SMT traversal.
//!

use super::bitpath::BitPath;
use super::{SmtHandle, SmtNode};
use crate::trie::error::Result;

pub struct SmtRo<'a> {
    root: &'a SmtHandle,
}

impl<'a> SmtRo<'a> {
    pub fn new(root: &'a SmtHandle) -> Self {
        Self { root }
    }

    /// Looks up a value by its key hash.
    pub fn get(&self, key_hash: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let path = BitPath::from_hash(key_hash);
        Self::step(self.root, key_hash, &path, 0)
    }

    fn step(
        handle: &SmtHandle,
        full_key_hash: &[u8; 32],
        full_path: &BitPath,
        depth: usize,
    ) -> Result<Option<Vec<u8>>> {
        let node = handle.node();
        match node {
            SmtNode::Empty => Ok(None),
            SmtNode::Leaf {
                key_hash: leaf_key_hash,
                value,
                ..
            } => {
                // Both paths are full 256-bit `from_hash` expansions, so
                // path equality is exactly hash equality — compare the 32
                // bytes directly instead of materializing a BitPath.
                if leaf_key_hash == full_key_hash {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            SmtNode::Internal { path, left, right } => {
                // The internal node's `path` is a compressed prefix.
                // Check that the remaining key bits match this prefix
                // (offset-based compare — no slice materialization).
                if !full_path.starts_with_from(depth, path) {
                    return Ok(None);
                }

                let next_depth = depth + path.len();
                if next_depth >= full_path.len() {
                    // Exhausted all bits without reaching a leaf.
                    return Ok(None);
                }

                let bit = full_path.bit_at(next_depth);
                let child = if bit == 0 { left } else { right };
                Self::step(child, full_key_hash, full_path, next_depth + 1)
            }
        }
    }
}
