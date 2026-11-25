//! Persistent, single-parent overlay maps. Each connected graph lives on one
//! shard. Serialize structural changes across all handles to the graph.

pub mod raw;
pub mod rawkey;
mod typed;
pub use typed::DagMap;

/// An opaque edge token local to one parent.
pub type DagMapId = [u8];
