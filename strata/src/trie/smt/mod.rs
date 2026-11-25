//!
//! Sparse Merkle Tree (SMT) — a binary Merkle trie over 256-bit key
//! hashes with compressed paths and depth-independent leaf commitments
//! (the Diem/JMT construction).
//!
//! All keys are hashed to 256-bit paths via Keccak256 before insertion,
//! giving a uniform key distribution.  Compressed internal nodes
//! collapse contiguous single-child chains into a single node with a
//! multi-bit path prefix, keeping average depth at O(log N).  A subtree
//! holding exactly one leaf commits to the leaf hash directly —
//! independent of depth — so hashing the whole tree costs O(N) hash
//! operations and proofs stay O(log N).
//!

pub(crate) mod bitpath;
pub(crate) mod codec;
pub(crate) mod mutation;
pub(crate) mod proof;
pub(crate) mod query;

pub(crate) use bitpath::BitPath;
pub use proof::SmtProof;

use super::shared::{Handle, TreeNode};
use std::fmt;
/// The fixed depth of the sparse Merkle tree (256-bit key hash).
pub const TREE_DEPTH: usize = 256;

/// The hash of an empty subtree: `[0u8; 32]`.
pub const EMPTY_HASH: [u8; 32] = [0u8; 32];

// =========================================================================
// SmtNodeHandle
// =========================================================================

pub(crate) type SmtHandle = Handle<SmtNode>;
impl TreeNode for SmtNode {
    fn children<'a>(&'a self, mut visit: impl FnMut(&'a SmtHandle)) {
        if let Self::Internal { left, right, .. } = self {
            visit(left);
            visit(right)
        }
    }
    fn into_children(self, pending: &mut Vec<SmtHandle>) {
        if let Self::Internal { left, right, .. } = self {
            pending.extend([left, right]);
        }
    }
    fn digest(&self) -> [u8; 32] {
        match self {
            Self::Empty => EMPTY_HASH,
            Self::Leaf {
                key_hash, value, ..
            } => codec::hash_leaf(key_hash, value),
            Self::Internal { path, left, right } => codec::wrap_hash(
                codec::hash_internal(left.hash().unwrap(), right.hash().unwrap()),
                path,
            ),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum SmtNode {
    /// An empty subtree.
    #[default]
    Empty,

    /// A leaf node storing a key-value pair.
    ///
    /// `path` is the remaining bit-path suffix from the parent's
    /// perspective (compressed).  `key_hash` is the full 32-byte
    /// Keccak256 of the original key — needed for proof verification
    /// and non-membership proofs.
    Leaf {
        path: BitPath,
        key_hash: [u8; 32],
        value: Vec<u8>,
    },

    /// An internal node with exactly two children (left=0, right=1).
    ///
    /// `path` is the compressed bit-path prefix shared by all
    /// descendants.  May be empty (zero bits) for a standard
    /// single-bit split.
    Internal {
        path: BitPath,
        left: SmtHandle,
        right: SmtHandle,
    },
}

impl fmt::Display for SmtNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SmtNode::Empty => write!(f, "Empty"),
            SmtNode::Leaf { path, .. } => write!(f, "Leaf({}bits)", path.len()),
            SmtNode::Internal { path, .. } => {
                write!(f, "Internal({}bits)", path.len())
            }
        }
    }
}
