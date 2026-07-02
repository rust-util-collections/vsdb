//! Tier metadata for the skip-list-like acceleration structure.
//!
//! Each tier coarsens slot buckets by `tier_capacity^(1+idx)`, storing
//! `(slot_floor, count)` entries to accelerate distance computations.

use std::collections::BTreeMap;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize, de};

use crate::{MapxOrd, basic::orphan::Orphan};

use super::EntryCnt;
use super::slot_type::SlotType;

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound = "S: SlotType + Serialize + de::DeserializeOwned")]
pub(crate) struct Tier<S: SlotType> {
    pub(crate) floor_base: S,
    pub(crate) store: MapxOrd<S, EntryCnt>,
    #[serde(skip)]
    pub(crate) cache: Mutex<BTreeMap<S, EntryCnt>>,
    pub(crate) entry_count: Orphan<usize>,
    #[serde(skip)]
    pub(crate) len_cache: Option<usize>,
}

impl<S: SlotType> Tier<S> {
    pub(crate) fn new(tier_idx: u32, tier_capacity: &S) -> Self {
        let pow = 1 + tier_idx;
        Self {
            floor_base: tier_capacity
                .checked_pow(pow)
                .filter(|v| *v != S::MIN)
                .unwrap_or(S::MAX),
            store: MapxOrd::new(),
            cache: Mutex::new(BTreeMap::new()),
            entry_count: Orphan::new(0),
            len_cache: Some(0),
        }
    }

    /// Ensure cache is populated. Called lazily on first access after
    /// deserialization (cache is #[serde(skip)] so starts empty).
    /// Safe to call on &self thanks to Mutex interior mutability.
    pub(crate) fn ensure_cache(&self) {
        let mut c = self.cache.lock();
        if c.is_empty() && self.entry_count.get_value() > 0 {
            for (k, v) in self.store.iter() {
                c.insert(k, v);
            }
        }
    }

    #[inline(always)]
    pub(crate) fn len(&mut self) -> usize {
        if let Some(l) = self.len_cache {
            l
        } else {
            let l = self.entry_count.get_value();
            self.len_cache = Some(l);
            l
        }
    }

    pub(crate) fn dec_len(&mut self) {
        *self.entry_count.get_mut() -= 1;
        if let Some(l) = self.len_cache.as_mut() {
            *l -= 1;
        }
    }
}
