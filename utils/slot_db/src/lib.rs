#![deny(warnings)]

use ruc::*;
use serde::{de, Deserialize, Serialize};
use std::{
    collections::{btree_set::Iter as SmallIter, BTreeSet},
    mem,
};
use vsdb::{
    basic::mapx_ord::MapxOrdIter as LargeIter, KeyEnDeOrdered, MapxOrd,
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

/// A `Skip List` like structure
#[derive(Debug, Deserialize, Serialize)]
#[serde(
    bound = "T: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned"
)]
pub struct SlotDB<T>
where
    T: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    data: MapxOrd<Slot, DataCtner<T>>,

    // How many entries in this DB
    total: EntryCnt,

    levels: Vec<Level>,

    multiple_step: u64,
}

impl<T> SlotDB<T>
where
    T: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    pub fn new(multiple_step: u64) -> Self {
        Self {
            data: MapxOrd::new(),
            total: 0,
            levels: vec![],
            multiple_step,
        }
    }

    pub fn insert(&mut self, slot: Slot, t: T) -> Result<()> {
        if let Some(top) = self.levels.last() {
            if top.data.len() as u64 > self.multiple_step {
                let newtop = top.data.iter().fold(
                    Level::new(self.levels.len() as u32, self.multiple_step),
                    |mut l, (slot, cnt)| {
                        let slot_floor = slot / l.floor_base * l.floor_base;
                        *l.data.entry(&slot_floor).or_insert(0) += cnt;
                        l
                    },
                );
                self.levels.push(newtop);
            }
        } else {
            let newtop = self.data.iter().fold(
                Level::new(self.levels.len() as u32, self.multiple_step),
                |mut l, (slot, entries)| {
                    let slot_floor = slot / l.floor_base * l.floor_base;
                    *l.data.entry(&slot_floor).or_insert(0) +=
                        entries.len() as EntryCnt;
                    l
                },
            );
            self.levels.push(newtop);
        };

        if self
            .data
            .entry(&slot)
            .or_insert(DataCtner::default())
            .insert(t)
        {
            self.levels.iter_mut().for_each(|l| {
                let slot_floor = slot / l.floor_base * l.floor_base;
                *l.data.entry(&slot_floor).or_insert(0) += 1;
            });
            self.total += 1;
        }

        Ok(())
    }

    pub fn remove(&mut self, slot: Slot, t: &T) {
        loop {
            if let Some(top_len) = self.levels.last().map(|top| top.data.len())
            {
                if top_len < 2 {
                    self.levels.pop();
                    continue;
                }
            }
            break;
        }

        let (exist, empty) = if let Some(mut d) = self.data.get_mut(&slot) {
            (d.remove(t), d.is_empty())
        } else {
            return;
        };

        if empty {
            self.data.remove(&slot);
        }

        if exist {
            self.levels.iter_mut().for_each(|l| {
                let slot_floor = slot / l.floor_base * l.floor_base;
                let mut cnt = l.data.get_mut(&slot_floor).unwrap();
                if 1 == *cnt {
                    mem::forget(cnt); // for performance
                    l.data.remove(&slot_floor);
                } else {
                    *cnt -= 1;
                }
            });
            self.total -= 1;
        }
    }

    pub fn clear(&mut self) {
        self.total = 0;
        self.data.clear();

        self.levels.iter_mut().for_each(|l| {
            l.data.clear();
        });

        self.levels.clear();
    }

    /// Common usages in web services
    pub fn get_entries_by_page(
        &self,
        page_size: PageSize,
        page_index: PageIndex, // start from 0
        reverse_order: bool,
    ) -> Vec<T> {
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
        reverse_order: bool,
    ) -> Vec<T> {
        if 0 == page_size || 0 == self.total() {
            return vec![];
        }

        let slot_min = slot_left_bound.unwrap_or(Slot::MIN);
        let slot_max = slot_right_bound.unwrap_or(Slot::MAX);

        if slot_max < slot_min {
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
        for l in self.levels.iter().rev() {
            let right_bound = slot / l.floor_base * l.floor_base;
            ret += l
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
                page_size.saturating_sub(back_shift as PageSize)
            };

            if 0 > skip_n {
                skip_n = 0;
            }

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
    ) -> (StartSlotActual, SkipNum) {
        let mut slot_start = Slot::MIN;
        let mut local_idx = global_skip_num as usize;

        for l in self.levels.iter().rev() {
            let mut hdr = l.data.range(slot_start..).peekable();
            while let Some(entry_cnt) = hdr.next().map(|(_, cnt)| cnt as usize)
            {
                if entry_cnt > local_idx {
                    break;
                } else {
                    slot_start =
                        hdr.peek().map(|(s, _)| *s).unwrap_or(u64::MAX);
                    local_idx -= entry_cnt;
                }
            }
        }

        let mut hdr = self.data.range(slot_start..).peekable();
        while let Some(entry_cnt) =
            hdr.next().map(|(_, entries)| entries.len())
        {
            if entry_cnt > local_idx {
                break;
            } else {
                slot_start = hdr.peek().map(|(s, _)| *s).unwrap_or(u64::MAX);
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
    ) -> Vec<T> {
        let mut ret = vec![];
        alt!(slot_end < slot_start, return ret);

        let (global_skip_n, take_n) = self.page_info_to_global_offsets(
            slot_start, slot_end, page_size, page_index, reverse,
        );

        let (slot_start_actual, local_skip_n) =
            self.get_local_skip_num(global_skip_n);

        let mut skip_n = local_skip_n as usize;
        let take_n = take_n as usize;

        for (_, entries) in self.data.range(slot_start_actual..=slot_end) {
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

    // Can also be used to do some `data statistics`
    fn entry_cnt_within_two_slots(
        &self,
        slot_start: Slot,
        slot_end: Slot,
    ) -> EntryCnt {
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
            self.total
        } else {
            self.entry_cnt_within_two_slots(slot_start, slot_end)
        }
    }

    pub fn total(&self) -> EntryCnt {
        self.total_by_slot(None, None)
    }
}

impl<T> Default for SlotDB<T>
where
    T: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    fn default() -> Self {
        Self::new(8)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(
    bound = "T: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned"
)]
enum DataCtner<T>
where
    T: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    Small(BTreeSet<T>),
    Large(MapxOrd<T, ()>),
}

impl<T> DataCtner<T>
where
    T: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    fn len(&self) -> usize {
        match self {
            Self::Small(i) => i.len(),
            Self::Large(i) => i.len(),
        }
    }

    fn is_empty(&self) -> bool {
        0 == self.len()
    }

    fn insert(&mut self, t: T) -> bool {
        if let Self::Small(i) = self {
            if i.len() > 8 {
                *self = Self::Large(i.iter().fold(
                    MapxOrd::new(),
                    |mut acc, t| {
                        acc.insert(t, &());
                        acc
                    },
                ));
            }
        }

        match self {
            Self::Small(i) => i.insert(t),
            Self::Large(i) => i.insert(&t, &()).is_none(),
        }
    }

    fn remove(&mut self, target: &T) -> bool {
        match self {
            Self::Small(i) => i.remove(target),
            Self::Large(i) => i.remove(target).is_some(),
        }
    }

    fn iter(&self) -> DataCtnerIter<T> {
        match self {
            Self::Small(i) => DataCtnerIter::Small(i.iter()),
            Self::Large(i) => DataCtnerIter::Large(i.iter()),
        }
    }
}

impl<T> Default for DataCtner<T>
where
    T: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    fn default() -> Self {
        Self::Small(BTreeSet::new())
    }
}

enum DataCtnerIter<'a, T>
where
    T: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    Small(SmallIter<'a, T>),
    Large(LargeIter<'a, T, ()>),
}

impl<'a, T> Iterator for DataCtnerIter<'a, T>
where
    T: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(i) => i.next().cloned(),
            Self::Large(i) => i.next().map(|j| j.0),
        }
    }
}

impl<'a, T> DoubleEndedIterator for DataCtnerIter<'a, T>
where
    T: Clone + Ord + KeyEnDeOrdered + Serialize + de::DeserializeOwned,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(i) => i.next_back().cloned(),
            Self::Large(i) => i.next_back().map(|j| j.0),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Level {
    floor_base: u64,
    data: MapxOrd<SlotFloor, EntryCnt>,
}

impl Level {
    fn new(level_idx: u32, multiple_step: u64) -> Self {
        let pow = 1 + level_idx;
        Self {
            floor_base: multiple_step.pow(pow),
            data: MapxOrd::new(),
        }
    }
}

#[cfg(test)]
mod test;
