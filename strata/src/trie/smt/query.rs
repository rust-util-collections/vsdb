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
        Self::step(self.root, &path, 0)
    }

    fn step(
        handle: &SmtHandle,
        full_path: &BitPath,
        depth: usize,
    ) -> Result<Option<Vec<u8>>> {
        let node = handle.node();
        match node {
            SmtNode::Empty => Ok(None),
            SmtNode::Leaf {
                key_hash, value, ..
            } => {
                // Check if this leaf is for our key.
                let leaf_path = BitPath::from_hash(key_hash);
                if *full_path == leaf_path {
                    Ok(Some(value.clone()))
                } else {
                    Ok(None)
                }
            }
            SmtNode::Internal { path, left, right } => {
                // The internal node's `path` is a compressed prefix.
                // Check that the remaining key bits match this prefix.
                let remaining = full_path.slice(depth, full_path.len());
                if !remaining.starts_with(path) {
                    return Ok(None);
                }

                let next_depth = depth + path.len();
                if next_depth >= full_path.len() {
                    // Exhausted all bits without reaching a leaf.
                    return Ok(None);
                }

                let bit = full_path.bit_at(next_depth);
                let child = if bit == 0 { left } else { right };
                Self::step(child, full_path, next_depth + 1)
            }
        }
    }
}
