//!
//! # Versioned functions
//!

#![allow(dead_code)]
#![allow(clippy::ptr_arg)]

use crate::{MapxOC, Vecx};
use serde::{Deserialize, Serialize};

// branch ID
type BrID = u64;

// version ID
type VerID = u64;

// hash of a version
type VerHash = Vec<u8>;

type RawKey = Vec<u8>;
type RawValue = Vec<u8>;

const DEFAULT_BRANCH_ID: BrID = 0;
const DEFAULT_BRANCH_NAME: &str = "main";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ParentPoint {
    // parent branch ID of this branch
    br_id: BrID,
    // which verion of its parent branch is this branch forked from
    ver_id: VerID,
}

#[derive(Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct MapxRawVered {
    br_name_to_br_id: MapxOC<RawKey, BrID>,
    ver_name_to_ver_id: MapxOC<RawKey, VerID>,

    br_id_to_parent: MapxOC<BrID, Option<ParentPoint>>,

    // versions directly created on this branch
    br_id_to_created_vers: MapxOC<BrID, Vecx<VerID>>,

    // ever changed <key-value>s on each branch
    br_id_to_chg_set: MapxOC<BrID, RawKey>,

    // ever changed keys within each version
    ver_id_to_chg_set: MapxOC<VerID, Vecx<RawKey>>,

    ver_id_to_sig: MapxOC<VerID, VerHash>,

    kv_mapping: MapxOC<RawKey, MapxOC<BrID, MapxOC<VerID, RawValue>>>,
}

impl MapxRawVered {
    fn new() -> Self {
        let mut ret = Self::default();
        ret.br_name_to_br_id.insert(
            DEFAULT_BRANCH_NAME.to_owned().into_bytes(),
            DEFAULT_BRANCH_ID,
        );
        ret
    }

    fn get(&self, key: &[u8]) -> Option<RawValue> {
        self.get_by_br(key, DEFAULT_BRANCH_ID)
    }

    fn get_by_br(&self, key: &[u8], br_id: BrID) -> Option<RawValue> {
        if let Some(vers) = self.br_id_to_created_vers.get(&br_id) {
            if let Some(ver_id) = vers.last() {
                return self.get_by_br_ver(key, br_id, ver_id);
            }
        }
        None
    }

    fn get_by_br_ver(
        &self,
        key: &[u8],
        mut br_id: BrID,
        mut ver_id: VerID,
    ) -> Option<RawValue> {
        if let Some(brs) = self.kv_mapping._get(key) {
            'x: loop {
                if let Some(vers) = brs.get(&br_id) {
                    match vers.get_le(&ver_id) {
                        Some((_, v)) => return Some(v),
                        None => {
                            if let Some(Some(pp)) = self.br_id_to_parent.get(&br_id) {
                                br_id = pp.br_id;
                                ver_id = pp.ver_id;
                                continue 'x;
                            }
                        }
                    };
                }
                break;
            }
        }
        None
    }
}
