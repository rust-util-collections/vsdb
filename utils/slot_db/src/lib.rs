#![doc = include_str!("../README.md")]
#![deny(warnings)]
#![cfg_attr(test, warn(warnings))]

use ruc::*;
use serde::{Deserialize, Serialize, de};
use std::{
    collections::{BTreeSet, btree_set::Iter as SmallIter},
    ops::Bound,
};
use vsdb::{
    KeyEnDeOrdered, MapxOrd, basic::mapx_ord::MapxOrdIter as LargeIter,
    basic::orphan::Orphan,
};

type Slot = u64;
type SlotFloor = Slot;
type EntryCnt = u64;

// The actual slot which contains the first entry
type StartSlotActual = Slot;
type SkipNum = EntryCnt;
type TakeNum = EntryCnt;

// Declare as a signed `int`!
type Distance = i128;

type PageSize = u16;
type PageIndex = u32;

/// A `Skip List` like structure,
/// designed to support fast paged queries and indexes
#[derive(Debug, Deserialize, Serialize)]
#[serde(
    bound = "K: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned"
)]
pub struct SlotDB<K>
where
    K: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    data: MapxOrd<Slot, DataCtner<K>>,

    // How many entries in this DB
    total: Orphan<EntryCnt>,

    tiers: Vec<Tier>,

    tier_capacity: u64,

    // Switch the inner implementations of the slot direction:
    // - positive => reverse
    // - reverse => positive
    //
    // Positive query usually get better performance,
    // if most scenes are under the reverse mode,
    // then swap the low-level logic
    swap_order: bool,
}

impl<K> SlotDB<K>
where
    K: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    ///
    /// @param: `swap_order`:
    ///
    /// Switch the inner logic of the slot direction:
    /// - positive => reverse
    /// - reverse => positive
    ///
    /// Positive query usually get better performance,
    /// swap order if most cases run in the reverse mode
    pub fn new(tier_capacity: u64, swap_order: bool) -> Self {
        Self {
            data: MapxOrd::new(),
            total: Orphan::new(0),
            tiers: vec![],
            tier_capacity,
            swap_order,
        }
    }

    pub fn insert(&mut self, mut slot: Slot, k: K) -> Result<()> {
        if self.swap_order {
            slot = swap_order(slot);
        }

        if let Some(top) = self.tiers.last() {
            if top.data.len() as u64 > self.tier_capacity {
                let newtop = top.data.iter().fold(
                    Tier::new(self.tiers.len() as u32, self.tier_capacity),
                    |mut t, (slot, cnt)| {
                        let slot_floor = slot / t.floor_base * t.floor_base;
                        *t.data.entry(&slot_floor).or_insert(0) += cnt;
                        t
                    },
                );
                self.tiers.push(newtop);
            }
        } else {
            // First insertion scene, tiers' length should be 0
            let newtop = self.data.iter().fold(
                Tier::new(self.tiers.len() as u32, self.tier_capacity),
                |mut t, (slot, entries)| {
                    let slot_floor = slot / t.floor_base * t.floor_base;
                    *t.data.entry(&slot_floor).or_insert(0) +=
                        entries.len() as EntryCnt;
                    t
                },
            );
            self.tiers.push(newtop);
        };

        #[allow(clippy::unwrap_or_default)]
        if self.data.entry(&slot).or_insert(DataCtner::new()).insert(k) {
            self.tiers.iter_mut().for_each(|t| {
                let slot_floor = slot / t.floor_base * t.floor_base;
                *t.data.entry(&slot_floor).or_insert(0) += 1;
            });
            *self.total.get_mut() += 1;
        }

        Ok(())
    }

    pub fn remove(&mut self, mut slot: Slot, k: &K) {
        if self.swap_order {
            slot = swap_order(slot);
        }

        loop {
            if let Some(top_len) = self.tiers.last().map(|top| top.data.len())
            {
                if top_len < 2 {
                    self.tiers.pop();
                    continue;
                }
            }
            break;
        }

        let (exist, empty) = match self.data.get_mut(&slot) {
            Some(mut d) => (d.remove(k), d.is_empty()),
            _ => {
                return;
            }
        };

        if empty {
            self.data.remove(&slot);
        }

        if exist {
            self.tiers.iter_mut().for_each(|t| {
                let slot_floor = slot / t.floor_base * t.floor_base;
                let mut cnt = t.data.get_mut(&slot_floor).unwrap();
                if 1 == *cnt {
                    drop(cnt); // release the mut reference
                    t.data.remove(&slot_floor);
                } else {
                    *cnt -= 1;
                }
            });
            *self.total.get_mut() -= 1;
        }
    }

    pub fn clear(&mut self) {
        *self.total.get_mut() = 0;
        self.data.clear();

        self.tiers.iter_mut().for_each(|t| {
            t.data.clear();
        });

        self.tiers.clear();
    }

    /// Common usages in web services
    pub fn get_entries_by_page(
        &self,
        page_size: PageSize,
        page_index: PageIndex, // Start from 0
        reverse_order: bool,
    ) -> Vec<K> {
        self.get_entries_by_page_slot(
            None,
            None,
            page_size,
            page_index,
            reverse_order,
        )
    }

    /// Common usages in web services
    pub fn get_entries_by_page_slot(
        &self,
        slot_left_bound: Option<Slot>,  // Included
        slot_right_bound: Option<Slot>, // Included
        page_size: PageSize,
        page_index: PageIndex, // start from 0
        mut reverse_order: bool,
    ) -> Vec<K> {
        let mut slot_min = slot_left_bound.unwrap_or(Slot::MIN);
        let mut slot_max = slot_right_bound.unwrap_or(Slot::MAX);

        if self.swap_order {
            (slot_min, slot_max) =
                (swap_order(slot_max), swap_order(slot_min));
            reverse_order = !reverse_order;
        }

        if slot_max < slot_min {
            return vec![];
        }

        if 0 == page_size || 0 == self.total() {
            return vec![];
        }

        self.get_entries(
            slot_min,
            slot_max,
            page_size,
            page_index,
            reverse_order,
        )
    }

    fn slot_entry_cnt(&self, slot: Slot) -> EntryCnt {
        self.data
            .get(&slot)
            .map(|d| d.len() as EntryCnt)
            .unwrap_or(0)
    }

    // Exclude the slot itself-owned entries(whether it exists or not)
    fn distance_to_the_leftmost_slot(&self, slot: Slot) -> Distance {
        let mut left_bound = Slot::MIN;
        let mut ret = 0;
        for t in self.tiers.iter().rev() {
            let right_bound = slot / t.floor_base * t.floor_base;
            ret += t
                .data
                .range(left_bound..right_bound)
                .map(|(_, cnt)| cnt as Distance)
                .sum::<Distance>();
            left_bound = right_bound
        }
        ret += self
            .data
            .range(left_bound..slot)
            .map(|(_, d)| d.len() as Distance)
            .sum::<Distance>();
        ret
    }

    fn offsets_from_the_leftmost_slot(
        &self,
        slot_start: Slot, // Included
        slot_end: Slot,   // Included
        page_size: PageSize,
        page_index: PageIndex,
        reverse: bool,
    ) -> (SkipNum, TakeNum) {
        if slot_start > slot_end {
            return (0, 0);
        }

        if reverse {
            let mut skip_n = self.distance_to_the_leftmost_slot(slot_end)
                + self.slot_entry_cnt(slot_end) as Distance
                - (page_size as Distance) * (1 + page_index as Distance);

            let distance_of_slot_start =
                self.distance_to_the_leftmost_slot(slot_start);

            let take_n = if distance_of_slot_start <= skip_n {
                page_size
            } else {
                let back_shift = min!(
                    distance_of_slot_start.saturating_sub(skip_n),
                    PageSize::MAX as Distance
                );

                skip_n = distance_of_slot_start;

                page_size.saturating_sub(back_shift as PageSize)
            };

            (skip_n as SkipNum, take_n as TakeNum)
        } else {
            let skip_n = self.distance_to_the_leftmost_slot(slot_start)
                + (page_size as Distance) * (page_index as Distance);
            (skip_n as SkipNum, page_size as TakeNum)
        }
    }

    #[inline(always)]
    fn page_info_to_global_offsets(
        &self,
        slot_start: Slot, // Included
        slot_end: Slot,   // Included
        page_size: PageSize,
        page_index: PageIndex,
        reverse: bool,
    ) -> (SkipNum, TakeNum) {
        self.offsets_from_the_leftmost_slot(
            slot_start, slot_end, page_size, page_index, reverse,
        )
    }

    fn get_local_skip_num(
        &self,
        global_skip_num: EntryCnt,
    ) -> (Bound<StartSlotActual>, SkipNum) {
        let mut slot_start = Bound::Included(Slot::MIN);
        let mut local_idx = global_skip_num as usize;

        for t in self.tiers.iter().rev() {
            let mut hdr =
                t.data.range((slot_start, Bound::Unbounded)).peekable();
            while let Some(entry_cnt) = hdr.next().map(|(_, cnt)| cnt as usize)
            {
                if entry_cnt > local_idx {
                    break;
                } else {
                    slot_start = hdr
                        .peek()
                        .map(|(s, _)| Bound::Included(*s))
                        .unwrap_or(Bound::Excluded(Slot::MAX));
                    local_idx -= entry_cnt;
                }
            }
        }

        let mut hdr =
            self.data.range((slot_start, Bound::Unbounded)).peekable();
        while let Some(entry_cnt) =
            hdr.next().map(|(_, entries)| entries.len())
        {
            if entry_cnt > local_idx {
                break;
            } else {
                slot_start = hdr
                    .peek()
                    .map(|(s, _)| Bound::Included(*s))
                    .unwrap_or(Bound::Excluded(Slot::MAX));
                local_idx -= entry_cnt;
            }
        }

        (slot_start, local_idx as EntryCnt)
    }

    fn get_entries(
        &self,
        slot_start: Slot, // Included
        slot_end: Slot,   // Included
        page_size: PageSize,
        page_index: PageIndex,
        reverse: bool,
    ) -> Vec<K> {
        let mut ret = vec![];
        alt!(slot_end < slot_start, return ret);

        let (global_skip_n, take_n) = self.page_info_to_global_offsets(
            slot_start, slot_end, page_size, page_index, reverse,
        );

        let (slot_start_actual, local_skip_n) =
            self.get_local_skip_num(global_skip_n);

        let mut skip_n = local_skip_n as usize;
        let take_n = take_n as usize;

        for (_, entries) in self
            .data
            .range((slot_start_actual, Bound::Included(slot_end)))
        {
            entries
                .iter()
                .skip(skip_n)
                .take(take_n - ret.len())
                .for_each(|entry| ret.push(entry));
            skip_n = 0;
            if ret.len() >= take_n {
                assert_eq!(ret.len(), take_n);
                break;
            }
        }

        if reverse {
            ret = ret.into_iter().rev().collect();
        }

        ret
    }

    /// Called by the `total_by_slot`,
    /// can also be used to do some `data statistics`
    pub fn entry_cnt_within_two_slots(
        &self,
        mut slot_start: Slot,
        mut slot_end: Slot,
    ) -> EntryCnt {
        if self.swap_order {
            (slot_start, slot_end) =
                (swap_order(slot_end), swap_order(slot_start));
        }

        if slot_start > slot_end {
            0
        } else {
            let cnt = self.distance_to_the_leftmost_slot(slot_end)
                - self.distance_to_the_leftmost_slot(slot_start)
                + self.slot_entry_cnt(slot_end) as Distance;
            cnt as EntryCnt
        }
    }

    pub fn total_by_slot(
        &self,
        slot_start: Option<Slot>,
        slot_end: Option<Slot>,
    ) -> EntryCnt {
        let slot_start = slot_start.unwrap_or(Slot::MIN);
        let slot_end = slot_end.unwrap_or(Slot::MAX);

        if Slot::MIN == slot_start && Slot::MAX == slot_end {
            self.total.get_value()
        } else {
            self.entry_cnt_within_two_slots(slot_start, slot_end)
        }
    }

    pub fn total(&self) -> EntryCnt {
        self.total_by_slot(None, None)
    }
}

impl<K> Default for SlotDB<K>
where
    K: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    fn default() -> Self {
        Self::new(8, false)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(
    bound = "K: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned"
)]
enum DataCtner<K>
where
    K: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    Small(BTreeSet<K>),
    Large(MapxOrd<K, ()>),
}

impl<K> DataCtner<K>
where
    K: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    fn new() -> Self {
        Self::Small(BTreeSet::new())
    }

    fn len(&self) -> usize {
        match self {
            Self::Small(i) => i.len(),
            Self::Large(i) => i.len(),
        }
    }

    fn is_empty(&self) -> bool {
        0 == self.len()
    }

    fn insert(&mut self, k: K) -> bool {
        if let Self::Small(i) = self {
            if i.len() > 8 {
                *self = Self::Large(i.iter().fold(
                    MapxOrd::new(),
                    |mut acc, k| {
                        acc.insert(k, &());
                        acc
                    },
                ));
            }
        }

        match self {
            Self::Small(i) => i.insert(k),
            Self::Large(i) => i.insert(&k, &()).is_none(),
        }
    }

    fn remove(&mut self, target: &K) -> bool {
        match self {
            Self::Small(i) => i.remove(target),
            Self::Large(i) => i.remove(target).is_some(),
        }
    }

    fn iter(&self) -> DataCtnerIter<K> {
        match self {
            Self::Small(i) => DataCtnerIter::Small(i.iter()),
            Self::Large(i) => DataCtnerIter::Large(i.iter()),
        }
    }
}

impl<K> Default for DataCtner<K>
where
    K: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::large_enum_variant)]
enum DataCtnerIter<'a, K>
where
    K: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    Small(SmallIter<'a, K>),
    Large(LargeIter<'a, K, ()>),
}

impl<K> Iterator for DataCtnerIter<'_, K>
where
    K: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    type Item = K;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(i) => i.next().cloned(),
            Self::Large(i) => i.next().map(|j| j.0),
        }
    }
}

impl<K> DoubleEndedIterator for DataCtnerIter<'_, K>
where
    K: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(i) => i.next_back().cloned(),
            Self::Large(i) => i.next_back().map(|j| j.0),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Tier {
    floor_base: u64,
    data: MapxOrd<SlotFloor, EntryCnt>,
}

impl Tier {
    fn new(tier_idx: u32, tier_capacity: u64) -> Self {
        let pow = 1 + tier_idx;
        Self {
            floor_base: tier_capacity.pow(pow),
            data: MapxOrd::new(),
        }
    }
}

#[inline(always)]
fn swap_order(original_slot_value: Slot) -> Slot {
    !original_slot_value
}

#[cfg(test)]
mod test;
