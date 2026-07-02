//!
//! Structured error types for the VSDB public API.
//!
//! This is the **single error vocabulary** for the whole VSDB ecosystem:
//! every public API in `vsdb_core` and `vsdb` returns
//! [`Result<T>`](Result) with [`VsdbError`].
//!
//! # Relationship to `ruc`
//!
//! Internally VSDB uses `ruc` for error chaining (file/line context per
//! frame).  At the public boundary those chains are converted into
//! [`VsdbError`] via [`From<Box<dyn ruc::err::RucError>>`], which
//! preserves the **complete chain** (every frame, including file/line
//! annotations) in the `detail` field — no diagnostic information is
//! lost.  Compared to exposing `ruc::Result` directly, `VsdbError`
//! additionally offers structured, matchable variants, implements
//! [`std::error::Error`] (with `source()` for wrapped I/O and codec
//! errors), and is `Send + Sync`.

use thiserror::Error;

/// Structured error type for all VSDB public API operations.
///
/// Each variant carries enough context for callers to handle errors
/// programmatically (e.g. retry on a different branch) while the
/// `String` fields preserve the full error chain from the underlying
/// `ruc` layer so that no diagnostic information is lost.
#[non_exhaustive]
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
    /// The base directory is already frozen — the database has been
    /// initialized or a derived directory has been materialized — so it
    /// can no longer be changed.
    #[error("VSDB base directory is frozen; set it before first use")]
    BaseDirFrozen,
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
    /// Converts an internal `ruc` error chain, preserving **every**
    /// frame (message + file/line context) via `stringify_chain`.
    fn from(e: Box<dyn ruc::err::RucError>) -> Self {
        Self::Other {
            detail: e.stringify_chain(None),
        }
    }
}
