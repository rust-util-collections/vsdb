//! Public and crate-internal type definitions for the persistent B+ tree.

// =========================================================================
// Public types
// =========================================================================

/// Identifies a single node inside a [`PersistentBTree`](super::PersistentBTree).
///
/// A root `NodeId` is a complete, self-contained snapshot of a map —
/// analogous to a Git tree-object hash.
pub type NodeId = u64;

/// Sentinel: an empty tree has no root.
pub const EMPTY_ROOT: NodeId = 0;

// =========================================================================
// Insert / Remove result enums
// =========================================================================

pub(crate) enum InsertResult {
    Unchanged,
    Updated(NodeId),
    Split {
        left: NodeId,
        sep: Vec<u8>,
        right: NodeId,
    },
}

pub(crate) enum RemoveResult {
    NotFound,
    Done(NodeId),
    Underflow(NodeId),
}

// =========================================================================
// NodeRef — in-memory reference-count metadata
// =========================================================================

/// In-memory metadata for a single B+ tree node.
#[derive(Clone, Debug)]
pub(crate) struct NodeRef {
    pub(crate) ref_count: u32,
    /// Child NodeIds (empty for leaf nodes).
    pub(crate) children: Vec<NodeId>,
}
