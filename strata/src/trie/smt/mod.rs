//!
//! Sparse Merkle Tree (SMT) — a fixed-depth binary Merkle tree with
//! compressed paths.
//!
//! All keys are hashed to 256-bit paths via Keccak256 before insertion,
//! giving a uniform key distribution and a fixed tree depth of 256.
//! Compressed internal nodes collapse contiguous single-child chains
//! into a single node with a multi-bit path prefix, keeping average
//! depth at O(log N).
//!

pub(crate) mod bitpath;
pub(crate) mod cache;
pub(crate) mod codec;
pub(crate) mod mutation;
pub(crate) mod proof;
pub(crate) mod query;

pub(crate) use bitpath::BitPath;
pub use proof::SmtProof;

use std::fmt;

/// The fixed depth of the sparse Merkle tree (256-bit key hash).
pub const TREE_DEPTH: usize = 256;

/// The hash of an empty subtree: `[0u8; 32]`.
pub const EMPTY_HASH: [u8; 32] = [0u8; 32];

// =========================================================================
// SmtNodeHandle
// =========================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SmtHandle {
    /// A node that lives only in memory (not yet hashed).
    InMemory(Box<SmtNode>),
    /// A node with its precomputed hash (after `commit`).
    Cached(Vec<u8>, Box<SmtNode>),
}

impl Default for SmtHandle {
    fn default() -> Self {
        SmtHandle::InMemory(Box::new(SmtNode::Empty))
    }
}

impl SmtHandle {
    pub fn node(&self) -> &SmtNode {
        match self {
            SmtHandle::InMemory(n) | SmtHandle::Cached(_, n) => n,
        }
    }

    pub fn into_node(self) -> SmtNode {
        match self {
            SmtHandle::InMemory(n) | SmtHandle::Cached(_, n) => *n,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.node(), SmtNode::Empty)
    }

    /// Returns the hash if cached, or `EMPTY_HASH` if the node is empty.
    /// Panics if the node is a non-empty InMemory (must commit first).
    pub(crate) fn expect_hash(&self) -> &[u8] {
        match self {
            SmtHandle::Cached(h, _) => h,
            SmtHandle::InMemory(n) if **n == SmtNode::Empty => &EMPTY_HASH,
            _ => panic!("SmtHandle::expect_hash called on unhashed non-empty node"),
        }
    }
}

// =========================================================================
// SmtNode
// =========================================================================

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
