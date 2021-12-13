//!
//! # Versioned functions
//!

// use crate::{MapxRaw, MapxOC, Vecx};
//
// type BranchId = u64;
// type VersionId = u64;
//
// struct MapxRawVersioned {
//     data: MapxRaw,
//     branch: MapxRaw,
//
//     branch_to_branch_id: MapxRaw,
//     version_to_version_id: MapxRaw,
//
//     data_key_to_: MapxOC<>
//
//     // branc id  => { version id, ... }
//     branch_id_to_version_list: MapxOC<BranchIdx, Vecx<VersionIdx>>,
//
//     // version id => { data key => data value }
//     version_id_to_change_set: MapxOC<VersionIdx, MapxRaw>,
// }
