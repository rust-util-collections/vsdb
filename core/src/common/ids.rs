//! Stable identifiers used by versioned collections and errors.
use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! define_id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            /// Rebuilds an id from [`raw`](Self::raw) (e.g. one persisted
            /// by the application). Lookups reject ids that do not exist.
            pub const fn from_raw(raw: u64) -> Self {
                Self(raw)
            }

            /// The numeric value, stable across restarts.
            pub const fn raw(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

    };
}

define_id! {
    /// Identifies a commit in the history DAG.
    ///
    /// A distinct type from [`BranchId`], so the two cannot be swapped;
    /// persisted exactly like the `u64` it wraps. As an ordered collection
    /// key, it uses the same big-endian encoding as `u64`.
    CommitId
}

define_id! {
    /// Identifies a branch.
    ///
    /// A distinct type from [`CommitId`], so the two cannot be swapped;
    /// persisted exactly like the `u64` it wraps. As an ordered collection
    /// key, it uses the same big-endian encoding as `u64`.
    BranchId
}
