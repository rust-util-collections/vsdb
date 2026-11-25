//!
//! Structured error types for the VSDB public API.
//!
//! This is the **single error vocabulary** for the whole VSDB ecosystem:
//! every public API in `vsdb_core` and `vsdb` returns
//! [`Result<T>`](Result) with [`VsdbError`].
//!
use thiserror::Error;

/// Structured error type for all VSDB public API operations.
///
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum VsdbError {
    /// The specified branch does not exist.
    #[error("branch not found: {branch_id}")]
    BranchNotFound {
        /// The branch ID that was looked up.
        branch_id: super::BranchId,
    },
    /// The specified commit does not exist.
    #[error("commit not found: {commit_id}")]
    CommitNotFound {
        /// The commit ID that was looked up.
        commit_id: super::CommitId,
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
        branch_id: super::BranchId,
    },
    /// The target commit is not an ancestor of the branch head.
    #[error("commit {commit_id} is not an ancestor of branch {branch_id}")]
    NotAncestor {
        /// The requested target commit.
        commit_id: super::CommitId,
        /// The branch whose history was searched.
        branch_id: super::BranchId,
    },
    /// A branch was merged into itself.
    #[error("cannot merge branch {branch_id} into itself")]
    SelfMerge {
        /// The branch passed as both source and target.
        branch_id: super::BranchId,
    },
    /// The operation needs a branch with at least one commit.
    #[error("branch {branch_id} has no commits")]
    NoCommits {
        /// The branch without history.
        branch_id: super::BranchId,
    },
    /// A vector's length differs from the index dimension.
    #[error("dimension mismatch: expected {expected}, got {found}")]
    DimensionMismatch {
        /// The configured dimension.
        expected: usize,
        /// The length that was passed or stored.
        found: usize,
    },
    /// A configuration value is out of range.
    #[error("invalid configuration: {detail}")]
    InvalidConfig {
        /// Which value is invalid and why.
        detail: String,
    },
    /// Stored data violates a structural invariant.
    #[error("corrupt data: {detail}")]
    Corrupt {
        /// The violated invariant.
        detail: String,
    },
    /// The base directory is already frozen — the database has been
    /// initialized or a derived directory has been materialized — so it
    /// can no longer be changed.
    #[error("VSDB base directory is frozen; set it before first use")]
    BaseDirFrozen,
    /// The requested operation requires write capability, but this
    /// process opened its VSDB universe in read-only mode.
    #[error("operation {operation} is unavailable in read-only mode")]
    ReadOnly {
        /// The operation rejected before performing any side effect.
        operation: &'static str,
    },
    /// Encoding or decoding failed (postcard serialization).
    #[error("encoding error: {0}")]
    Encoding(#[from] postcard::Error),
    /// Decoding failed in a non-postcard codec (e.g. ordered-key byte
    /// decoding, instance-meta parsing, container payloads).
    #[error("decode error: {detail}")]
    Decode {
        /// Description of the malformed input.
        detail: String,
    },
    /// Storage-layer I/O error.
    #[error("storage error: {0}")]
    Io(#[from] std::io::Error),
    /// Namespace management error (registry, placement, lifecycle).
    #[error("namespace error: {detail}")]
    Namespace {
        /// Description of the failure.
        detail: String,
    },
    /// Trie operation error.
    #[error("trie error: {detail}")]
    Trie {
        /// Detailed error chain.
        detail: String,
    },
    /// Underlying storage engine failure, retaining its original source chain.
    #[error("{kind}: {source}")]
    Storage {
        kind: StorageErrorKind,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Catch-all for errors not covered by specific variants.
    #[error("{detail}")]
    Other {
        /// Detailed error chain.
        detail: String,
    },
}

/// Alias for `std::result::Result<T, VsdbError>`.
pub type Result<T> = std::result::Result<T, VsdbError>;

/// Stable classifications for engine failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageErrorKind {
    Io,
    Corruption,
    ReadOnly,
    InvalidArgument,
    DbClosed,
    Background,
    Other,
}
impl std::fmt::Display for StorageErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
pub(crate) fn from_mmdb_err(error: mmdb::Error) -> VsdbError {
    use mmdb::ErrorKind;
    let kind = match error.kind() {
        ErrorKind::Io => StorageErrorKind::Io,
        ErrorKind::Corruption => StorageErrorKind::Corruption,
        ErrorKind::ReadOnly => StorageErrorKind::ReadOnly,
        ErrorKind::InvalidArgument => StorageErrorKind::InvalidArgument,
        ErrorKind::DbClosed => StorageErrorKind::DbClosed,
        ErrorKind::Background => StorageErrorKind::Background,
        _ => StorageErrorKind::Other,
    };
    VsdbError::Storage {
        kind,
        source: Box::new(error),
    }
}
pub(crate) fn corrupt(detail: impl Into<String>) -> VsdbError {
    VsdbError::Corrupt {
        detail: detail.into(),
    }
}
