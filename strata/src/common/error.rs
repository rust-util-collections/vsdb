//!
//! Structured error types for the VSDB public API.
//!

use thiserror::Error;

/// Structured error type for all VSDB public API operations.
///
/// Each variant carries enough context for callers to handle errors
/// programmatically (e.g. retry on a different branch) while the
/// `String` fields preserve the full error chain from the underlying
/// `ruc` layer so that no diagnostic information is lost.
#[derive(Error, Debug)]
pub enum VsdbError {
    /// The specified branch does not exist.
    #[error("branch not found: {branch_id}")]
    BranchNotFound {
        /// The branch ID that was looked up.
        branch_id: u64,
    },
    /// The specified commit does not exist.
    #[error("commit not found: {commit_id}")]
    CommitNotFound {
        /// The commit ID that was looked up.
        commit_id: u64,
    },
    /// A branch with this name already exists.
    #[error("branch already exists: {name}")]
    BranchAlreadyExists {
        /// The duplicate branch name.
        name: String,
    },
    /// The main branch cannot be deleted.
    #[error("cannot delete the main branch")]
    CannotDeleteMainBranch,
    /// The branch has uncommitted changes that must be committed or
    /// discarded before the requested operation.
    #[error("branch {branch_id} has uncommitted changes")]
    UncommittedChanges {
        /// The branch ID that has dirty state.
        branch_id: u64,
    },
    /// Encoding or decoding failed.
    #[error("encoding error: {detail}")]
    Encoding {
        /// Detailed error chain.
        detail: String,
    },
    /// Storage-layer error (I/O, engine, compaction, etc.).
    #[error("storage error: {detail}")]
    Storage {
        /// Detailed error chain.
        detail: String,
    },
    /// Trie operation error.
    #[error("trie error: {detail}")]
    Trie {
        /// Detailed error chain.
        detail: String,
    },
    /// Catch-all for errors not covered by specific variants.
    ///
    /// The `detail` field carries the full `ruc` error chain so that
    /// no context is lost.
    #[error("{detail}")]
    Other {
        /// Detailed error chain.
        detail: String,
    },
}

/// Alias for `std::result::Result<T, VsdbError>`.
pub type Result<T> = std::result::Result<T, VsdbError>;

// ---------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------

impl From<Box<dyn ruc::err::RucError>> for VsdbError {
    fn from(e: Box<dyn ruc::err::RucError>) -> Self {
        Self::Other {
            detail: e.stringify_chain(None),
        }
    }
}

impl From<std::io::Error> for VsdbError {
    fn from(e: std::io::Error) -> Self {
        Self::Storage {
            detail: e.to_string(),
        }
    }
}

impl From<postcard::Error> for VsdbError {
    fn from(e: postcard::Error) -> Self {
        Self::Encoding {
            detail: e.to_string(),
        }
    }
}
