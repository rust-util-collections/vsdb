//!
//! # Basic Data Structures
//!
//! Fundamental, unversioned data structures that wrap the raw storage
//! implementations from `vsdb_core`, offering typed APIs for keys and values.
//!
//! - [`Mapx`](crate::basic::mapx::Mapx) — `HashMap`-like, unordered.
//! - [`MapxOrd`](crate::basic::mapx_ord::MapxOrd) — `BTreeMap`-like, ordered by key.
//! - [`MapxOrdRawKey`](crate::basic::mapx_ord_rawkey::MapxOrdRawKey) — ordered map with raw-byte keys (internal building block).
//! - [`Orphan`](crate::basic::orphan::Orphan) — a single persistent value.
//! - [`PersistentBTree`](crate::basic::persistent_btree::PersistentBTree) — a persistent B+ tree with copy-on-write structural sharing.
//!

pub mod mapx;
pub mod mapx_ord;
pub mod mapx_ord_rawkey;
pub mod orphan;
pub mod persistent_btree;
