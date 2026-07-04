//! Slot-based index for efficient, timestamp-based paged queries.
//!
//! [`SlotDex`] is a skip-list-like data structure ideal for indexing and
//! querying large datasets where entries are associated with a slot
//! (e.g., a timestamp or sequence number).

mod container;
mod slot_type;
mod tier;

pub(crate) use container::DataCtner;
pub use slot_type::SlotType;
pub(crate) use tier::Tier;

use crate::{
    KeyEnDeOrdered, MapxOrd,
    basic::orphan::Orphan,
    common::{dirty_count as dc, error::Result},
};
use serde::{Deserialize, Serialize};
use std::{ops::Bound, result::Result as StdResult};

pub(crate) type EntryCnt = u64;
type SkipNum = EntryCnt;
type TakeNum = EntryCnt;

// Declare as a signed `int`!
type Distance = i128;

type PageSize = u16;
type PageIndex = u32;

/// A skip-list-like data structure for fast, timestamp-based paged queries.
///
/// `SlotDex` organizes data into "slots" (e.g., timestamps or sequence numbers),
/// which are then grouped into tiers. This hierarchical structure allows for
/// rapid seeking and counting, making it highly efficient for pagination and
/// range queries over large datasets.
///
/// The slot type `S` must implement [`SlotType`]; built-in support covers
/// `u32`, `u64`, and `u128`.
#[derive(Debug)]
pub struct SlotDex<S, K>
where
    S: SlotType,
    K: Clone + Ord + KeyEnDeOrdered,
{
    data: MapxOrd<S, DataCtner<K>>,
    total: Orphan<EntryCnt>,
    tiers: Vec<Tier<S>>,
    tier_capacity: S,
    swap_order: bool,
}

impl<S, K> Serialize for SlotDex<S, K>
where
    S: SlotType,
    K: Clone + Ord + KeyEnDeOrdered,
{
    fn serialize<Ser>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error>
    where
        Ser: serde::Serializer,
    {
        crate::common::serialize_typed_handle_meta::<Self, Ser>(
            &(
                &self.data,
                &self.total,
                &self.tiers,
                &self.tier_capacity,
                &self.swap_order,
            ),
            serializer,
        )
    }
}

impl<'de, S, K> Deserialize<'de> for SlotDex<S, K>
where
    S: SlotType,
    K: Clone + Ord + KeyEnDeOrdered,
{
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        type Payload<S, K> = (
            MapxOrd<S, DataCtner<K>>,
            Orphan<EntryCnt>,
            Vec<Tier<S>>,
            S,
            bool,
        );
        let (data, total, tiers, tier_capacity, swap_order) =
            crate::common::deserialize_typed_handle_meta::<Self, Payload<S, K>, D>(
                deserializer,
            )?;
        let mut me = SlotDex {
            data,
            total,
            tiers,
            tier_capacity,
            swap_order,
        };
        me.ensure_count();
        Ok(me)
    }
}

impl<S, K> SlotDex<S, K>
where
    S: SlotType,
    K: Clone + Ord + KeyEnDeOrdered,
{
    /// Creates a new `SlotDex`.
    ///
    /// # Arguments
    ///
    /// * `tier_capacity` - The capacity of each tier, controlling the granularity
    ///   of the index.  Must be at least 2: each tier coarsens slot buckets by
    ///   this factor, so a capacity of 1 would never terminate tier growth.
    /// * `swap_order` - If `true`, reverses the internal slot order. This can improve
    ///   performance for applications that primarily query in reverse chronological order.
    pub fn new(tier_capacity: S, swap_order: bool) -> Self {
        // Each tier's floor_base is tier_capacity^(1+idx); growth only
        // terminates when every new tier strictly coarsens the previous
        // one, which requires a capacity of at least 2.
        assert!(
            tier_capacity.as_i128() >= 2,
            "SlotDex: tier_capacity must be >= 2"
        );

        Self {
            data: MapxOrd::new(),
            total: Orphan::new(dc::set_dirty(0)),
            tiers: vec![],
            tier_capacity,
            swap_order,
        }
    }

    /// Returns the unique instance ID of this `SlotDex`.
    #[inline(always)]
    pub fn instance_id(&self) -> u64 {
        self.data.instance_id()
    }

    /// Persists this instance's metadata to disk so that it can be
    /// recovered later via [`from_meta`](Self::from_meta).
    ///
    /// Marks a clean shutdown so that the next [`from_meta`](Self::from_meta)
    /// call can skip the count rebuild.
    pub fn save_meta(&mut self) -> Result<u64> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;

        // Clear dirty only after the latest metadata was persisted.  If a
        // crash happens before that write, the previous meta still points at
        // this dirty total and recovery rebuilds derived tier state.
        let raw = self.total.get_value();
        self.total.set_value(&dc::clear_dirty(raw));
        Ok(id)
    }

    /// Recovers a `SlotDex` instance from previously saved metadata.
    ///
    /// If the previous session did not call [`save_meta`](Self::save_meta)
    /// (unclean shutdown), the total count is automatically rebuilt from
    /// the live data.
    pub fn from_meta(instance_id: u64) -> Result<Self> {
        crate::common::load_instance_meta(instance_id)
    }

    /// If the dirty bit is set, rebuild the count from live data.
    /// Then set the dirty bit for the current process lifetime.
    /// Called automatically during deserialization.
    fn ensure_count(&mut self) {
        let raw = self.total.get_value();
        if dc::is_dirty(raw) || self.has_invalid_empty_tier() {
            // Unclean shutdown.  insert()/remove() update several
            // independent structures (Large ctner maps, ctner records,
            // tier floor counts, the grand total) without batch
            // atomicity, so everything derived must be rebuilt from the
            // backing maps — not just the total.
            //
            // 1. Repair each Large ctner's cached `len` from its backing
            //    map (map writes land before the record write), dropping
            //    records that turn out to be empty.
            let mut total: EntryCnt = 0;
            let mut rewrites: Vec<(S, DataCtner<K>)> = vec![];
            let mut removals: Vec<S> = vec![];
            for (slot, mut d) in self.data.iter() {
                if let DataCtner::Large { map, len } = &mut d {
                    let actual = map.iter().count();
                    if actual != *len {
                        *len = actual;
                        if actual == 0 {
                            removals.push(slot);
                        } else {
                            total += actual as EntryCnt;
                            rewrites.push((slot, d));
                        }
                        continue;
                    }
                }

                total += d.len() as EntryCnt;
            }
            for slot in removals {
                self.data.remove(&slot);
            }
            for (slot, d) in rewrites {
                self.data.insert(&slot, &d);
            }

            // 2. Tier floor counts may be skewed by the same crash
            //    window and would permanently corrupt pagination
            //    offsets.  Discard the whole tier stack, then eagerly
            //    rebuild it right here from a full data scan — leaving
            //    the tier-less state in place until "the next insert"
            //    (as before) would silently degrade every pagination
            //    query to an O(N) raw scan (instead of the intended
            //    O(tiers * tier_capacity) walk) for however long the
            //    process stays idle or read-only after an unclean
            //    shutdown.
            self.tiers.iter_mut().for_each(|t| {
                t.store.clear();
                *t.entry_count.get_mut() = 0;
            });
            self.tiers.clear();
            self.rebuild_tier_stack();

            self.total.set_value(&dc::set_dirty(total));
        } else {
            self.total.set_value(&dc::set_dirty(raw));
        }
    }

    /// Rebuilds the full tier-acceleration stack from a full scan of
    /// `self.data`, mirroring the depth the stack would have reached via
    /// ordinary incremental inserts.
    ///
    /// Used after crash recovery clears the stack (see [`Self::ensure_count`]),
    /// so pagination doesn't silently regress to an O(N) raw scan until
    /// some future write happens to trigger `ensure_tier_capacity`.
    /// Requires `self.tiers` to already be empty; a no-op on an empty
    /// `self.data` (leaving `self.tiers` empty, matching the state of a
    /// freshly created, never-written-to `SlotDex` rather than
    /// introducing a spurious zero-entry tier that `has_invalid_empty_tier`
    /// would then flag on the next reload).
    fn rebuild_tier_stack(&mut self) {
        debug_assert!(self.tiers.is_empty());
        if self.data.iter().next().is_none() {
            return;
        }
        loop {
            self.ensure_tier_capacity();
            // `ensure_tier_capacity` pushes at most one tier per call:
            // the first call always pushes (empty-stack branch), and
            // each subsequent call pushes again only if the current top
            // tier still exceeds capacity — so this converges in the
            // same number of iterations the stack would have reached
            // organically.
            let top = self
                .tiers
                .last_mut()
                .expect("just pushed by ensure_tier_capacity");
            if (top.len() as i128) <= self.tier_capacity.as_i128() {
                break;
            }
        }
    }

    fn has_invalid_empty_tier(&self) -> bool {
        self.tiers
            .iter()
            .any(|t| t.entry_count.get_value() == 0 || t.store.iter().next().is_none())
    }

    /// Inserts a key into a specified slot.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot to insert the key into (e.g., a timestamp).
    /// * `k` - The key to insert.
    pub fn insert(&mut self, slot: S, k: K) -> Result<()> {
        let slot = self.to_storage_slot(slot);

        self.mark_dirty();
        self.ensure_tier_capacity();

        let mut ctner = self.data.get(&slot).unwrap_or_default();
        if ctner.insert(k) {
            self.data.insert(&slot, &ctner);
            self.tiers.iter_mut().for_each(|t| {
                t.ensure_cache();
                let slot_floor = slot.floor_align(&t.floor_base);
                let c = t.cache.get_mut();
                let mut v = c.get(&slot_floor).copied().unwrap_or(0);
                if 0 == v {
                    *t.entry_count.get_mut() += 1;
                    if let Some(l) = t.len_cache.as_mut() {
                        *l += 1;
                    }
                }
                v += 1;
                c.insert(slot_floor.clone(), v);
                t.store.insert(&slot_floor, &v);
            });
            let t = self.total.get_value();
            self.total.set_value(&dc::inc(t));
        }

        Ok(())
    }

    /// Removes a key from a specified slot.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot to remove the key from.
    /// * `k` - The key to remove.
    pub fn remove(&mut self, slot: S, k: &K) {
        let slot = self.to_storage_slot(slot);

        let mut d = match self.data.get(&slot) {
            Some(d) => d,
            _ => return,
        };

        self.mark_dirty();
        let exist = d.remove(k);
        let empty = d.is_empty();
        if empty {
            self.data.remove(&slot);
        } else if exist {
            self.data.insert(&slot, &d);
        }

        if exist {
            // Shrink degenerate top tiers (structural maintenance).
            loop {
                let dominated = self.tiers.last_mut().is_some_and(|top| {
                    if top.len() < 2 {
                        top.store.clear();
                        *top.entry_count.get_mut() = 0;
                        top.cache.get_mut().clear();
                        true
                    } else {
                        false
                    }
                });
                if dominated {
                    self.tiers.pop();
                } else {
                    break;
                }
            }

            self.tiers.iter_mut().for_each(|t| {
                t.ensure_cache();
                let slot_floor = slot.floor_align(&t.floor_base);
                let c = t.cache.get_mut();
                let cnt = match c.get(&slot_floor).copied() {
                    Some(n) => n,
                    None => return,
                };
                if 1 == cnt {
                    c.remove(&slot_floor);
                    t.store.remove(&slot_floor);
                    t.dec_len();
                } else {
                    let new_cnt = cnt - 1;
                    c.insert(slot_floor.clone(), new_cnt);
                    t.store.insert(&slot_floor, &new_cnt);
                }
            });
            let t = self.total.get_value();
            self.total.set_value(&dc::dec(t));
        }
    }

    /// Clears the `SlotDex`, removing all entries and tiers.
    pub fn clear(&mut self) {
        self.mark_dirty();
        for mut ctner in self.data.values_mut() {
            ctner.clear_storage();
        }
        self.total.set_value(&dc::zero(self.total.get_value()));
        self.data.clear();

        self.tiers.iter_mut().for_each(|t| {
            t.store.clear();
            *t.entry_count.get_mut() = 0;
            t.cache.get_mut().clear();
        });

        self.tiers.clear();
    }

    /// Retrieves entries by page, a common use case for web services.
    ///
    /// # Arguments
    ///
    /// * `page_size` - The number of entries per page.
    /// * `page_index` - The zero-based index of the page to retrieve.
    /// * `reverse_order` - If `true`, returns entries in reverse order.
    ///
    /// # Returns
    ///
    /// A `Vec<K>` containing the entries for the specified page.
    ///
    /// # Note
    ///
    /// This is **offset-based** pagination (`page_size` × `page_index`),
    /// like SQL `LIMIT`/`OFFSET`: each call reflects the dataset as it is
    /// at that moment. If entries are inserted or removed between page
    /// requests, later pages may skip or repeat entries. Take a snapshot
    /// (or avoid concurrent mutation) when a stable full scan is required.
    pub fn get_entries_by_page(
        &self,
        page_size: PageSize,
        page_index: PageIndex, // Start from 0
        reverse_order: bool,
    ) -> Vec<K> {
        self.get_entries_by_page_slot(None, None, page_size, page_index, reverse_order)
    }

    /// Retrieves entries by page within a specified slot range.
    ///
    /// # Arguments
    ///
    /// * `slot_left_bound` - The inclusive left bound of the slot range.
    /// * `slot_right_bound` - The inclusive right bound of the slot range.
    /// * `page_size` - The number of entries per page.
    /// * `page_index` - The zero-based index of the page to retrieve.
    /// * `reverse_order` - If `true`, returns entries in reverse order.
    ///
    /// # Returns
    ///
    /// A `Vec<K>` containing the entries for the specified page and slot range.
    ///
    /// # Note
    ///
    /// Pagination is **offset-based** (see [`get_entries_by_page`]): pages
    /// are not stable across concurrent inserts/removes between requests.
    pub fn get_entries_by_page_slot(
        &self,
        slot_left_bound: Option<S>,  // Included
        slot_right_bound: Option<S>, // Included
        page_size: PageSize,
        page_index: PageIndex, // start from 0
        reverse_order: bool,
    ) -> Vec<K> {
        let (slot_min, slot_max, storage_is_reversed) =
            self.transform_range(slot_left_bound, slot_right_bound);

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
            reverse_order ^ storage_is_reversed,
        )
    }

    fn slot_entry_cnt(&self, slot: &S) -> EntryCnt {
        self.data
            .get(slot)
            .map(|d| d.len() as EntryCnt)
            .unwrap_or(0)
    }

    // Number of entries stored in slots strictly greater than `slot`
    // (whether the slot itself exists or not).
    fn distance_to_the_rightmost_slot(&self, slot: &S) -> Distance {
        if *slot == S::MAX {
            return 0;
        }
        self.total() as Distance
            - self.distance_to_the_leftmost_slot(slot)
            - self.slot_entry_cnt(slot) as Distance
    }

    // Exclude the slot itself-owned entries (whether it exists or not)
    fn distance_to_the_leftmost_slot(&self, slot: &S) -> Distance {
        if *slot == S::MIN {
            return 0;
        }
        let mut left_bound = S::MIN;
        let mut ret = 0;
        for t in self.tiers.iter().rev() {
            t.ensure_cache();
            let right_bound = slot.floor_align(&t.floor_base);
            ret += t
                .cache
                .lock()
                .range(left_bound.clone()..right_bound.clone())
                .map(|(_, cnt)| *cnt as Distance)
                .sum::<Distance>();
            left_bound = right_bound
        }
        ret += self
            .data
            .range(left_bound..slot.clone())
            .map(|(_, d)| d.len() as Distance)
            .sum::<Distance>();
        ret
    }

    fn offsets_from_the_leftmost_slot(
        &self,
        slot_start: &S, // Included
        page_size: PageSize,
        page_index: PageIndex,
    ) -> (SkipNum, TakeNum) {
        let skip_n = self.distance_to_the_leftmost_slot(slot_start)
            + (page_size as Distance) * (page_index as Distance);
        (skip_n as SkipNum, page_size as TakeNum)
    }

    /// Single-pass page location using in-memory tier caches.
    fn locate_page_start(&self, global_skip_n: EntryCnt) -> (Bound<S>, SkipNum) {
        let mut slot_start = Bound::Included(S::MIN);
        let mut remaining: u64 = global_skip_n;

        for t in self.tiers.iter().rev() {
            t.ensure_cache();
            let c = t.cache.lock();
            let mut hdr = c.range((slot_start.clone(), Bound::Unbounded)).peekable();
            while let Some(entry_cnt) = hdr.next().map(|(_, cnt)| *cnt) {
                if entry_cnt > remaining {
                    break;
                } else {
                    slot_start = hdr
                        .peek()
                        .map(|(s, _)| Bound::Included((*s).clone()))
                        .unwrap_or(Bound::Excluded(S::MAX));
                    remaining -= entry_cnt;
                }
            }
        }

        let mut hdr = self
            .data
            .range((slot_start.clone(), Bound::Unbounded))
            .peekable();
        while let Some(entry_cnt) = hdr.next().map(|(_, entries)| entries.len() as u64) {
            if entry_cnt > remaining {
                break;
            } else {
                slot_start = hdr
                    .peek()
                    .map(|(s, _)| Bound::Included((*s).clone()))
                    .unwrap_or(Bound::Excluded(S::MAX));
                remaining -= entry_cnt;
            }
        }

        (slot_start, remaining)
    }

    /// Single-pass reverse page location using in-memory tier caches.
    ///
    /// Mirror of [`locate_page_start`](Self::locate_page_start):
    /// `global_skip_n` counts entries to skip walking from the greatest
    /// storage slot downward. Returns the upper bound at which the reverse
    /// data walk must resume, plus the number of entries still to skip
    /// inside that boundary slot.
    ///
    /// Consumed units are cut off with `Bound::Excluded(floor)`: bucket
    /// floors are left-aligned, so excluding a consumed floor also drops
    /// every finer-grained bucket (and data slot) belonging to it.
    fn locate_page_rstart(&self, global_skip_n: EntryCnt) -> (Bound<S>, SkipNum) {
        let mut slot_end: Bound<S> = Bound::Unbounded;
        let mut remaining: u64 = global_skip_n;

        for t in self.tiers.iter().rev() {
            t.ensure_cache();
            let c = t.cache.lock();
            for (floor, entry_cnt) in c.range((Bound::Unbounded, slot_end.clone())).rev()
            {
                if *entry_cnt > remaining {
                    break;
                }
                slot_end = Bound::Excluded(floor.clone());
                remaining -= *entry_cnt;
            }
        }

        for (slot, entries) in
            self.data.range((Bound::Unbounded, slot_end.clone())).rev()
        {
            let entry_cnt = entries.len() as u64;
            if entry_cnt > remaining {
                break;
            }
            slot_end = Bound::Excluded(slot);
            remaining -= entry_cnt;
        }

        (slot_end, remaining)
    }

    fn get_entries(
        &self,
        slot_start: S, // Included
        slot_end: S,   // Included
        page_size: PageSize,
        page_index: PageIndex,
        reverse: bool,
    ) -> Vec<K> {
        if slot_end < slot_start {
            return vec![];
        }

        if reverse {
            return self
                .get_entries_reverse(slot_start, slot_end, page_size, page_index);
        }

        let (global_skip_n, take_n) =
            self.offsets_from_the_leftmost_slot(&slot_start, page_size, page_index);

        let mut ret = Vec::with_capacity(take_n as usize);

        let (slot_start_actual, local_skip_n) = self.locate_page_start(global_skip_n);

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

        ret
    }

    /// Reverse-order paging: walk slots from `slot_end` down to `slot_start`
    /// in descending storage order while keeping each slot's entries in their
    /// natural ascending key order.
    ///
    /// Only the slot order is reversed, not the within-slot order: a slot is a
    /// set of keys, so its members stay ascending in every view. Reversing the
    /// whole result vector instead would corrupt within-slot order and shift
    /// page membership across slot boundaries when a slot holds >1 entry.
    ///
    /// The page start is located through
    /// [`locate_page_rstart`](Self::locate_page_rstart) — the
    /// tier-accelerated mirror of the forward path — so the raw data walk
    /// below only touches the slots that actually contribute to the
    /// returned page.
    fn get_entries_reverse(
        &self,
        slot_start: S, // Included
        slot_end: S,   // Included
        page_size: PageSize,
        page_index: PageIndex,
    ) -> Vec<K> {
        // Skip counted from the greatest storage slot downward; entries in
        // slots above `slot_end` are prepended to the skip so the locate
        // walk can start from the global right end and consume whole tier
        // buckets without range-boundary bookkeeping.
        let global_skip_n = self.distance_to_the_rightmost_slot(&slot_end)
            + (page_size as Distance) * (page_index as Distance);
        let global_skip_n = u64::try_from(global_skip_n).unwrap_or(u64::MAX);

        let (slot_end_actual, local_skip_n) = self.locate_page_rstart(global_skip_n);

        let take_n = page_size as usize;
        let mut to_skip = local_skip_n as usize;
        let mut ret = Vec::with_capacity(take_n);

        for (_, entries) in self
            .data
            .range((Bound::Included(slot_start), slot_end_actual))
            .rev()
        {
            let n = entries.len();
            if to_skip >= n {
                to_skip -= n;
                continue;
            }
            for entry in entries.iter().skip(to_skip) {
                ret.push(entry);
                if ret.len() == take_n {
                    return ret;
                }
            }
            to_skip = 0;
        }

        ret
    }

    /// Calculates the number of entries within a given slot range.
    ///
    /// This method can be used for data statistics and is called by `total_by_slot`.
    ///
    /// # Arguments
    ///
    /// * `slot_start` - The starting slot of the range.
    /// * `slot_end` - The ending slot of the range.
    ///
    /// # Returns
    ///
    /// The total number of entries (`EntryCnt`) within the specified range.
    pub fn entry_cnt_within_two_slots(&self, slot_start: S, slot_end: S) -> EntryCnt {
        let (slot_min, slot_max, _) =
            self.transform_range(Some(slot_start), Some(slot_end));

        if slot_min > slot_max {
            0
        } else {
            let cnt = self.distance_to_the_leftmost_slot(&slot_max)
                - self.distance_to_the_leftmost_slot(&slot_min)
                + self.slot_entry_cnt(&slot_max) as Distance;
            cnt as EntryCnt
        }
    }

    /// Returns the total number of entries within a specified slot range.
    ///
    /// # Arguments
    ///
    /// * `slot_start` - An `Option<S>` for the starting slot. If `None`, `S::MIN` is used.
    /// * `slot_end` - An `Option<S>` for the ending slot. If `None`, `S::MAX` is used.
    ///
    /// # Returns
    ///
    /// The total number of entries (`EntryCnt`) in the given range.
    pub fn total_by_slot(&self, slot_start: Option<S>, slot_end: Option<S>) -> EntryCnt {
        let slot_start = slot_start.unwrap_or(S::MIN);
        let slot_end = slot_end.unwrap_or(S::MAX);

        if S::MIN == slot_start && S::MAX == slot_end {
            dc::count(self.total.get_value())
        } else {
            self.entry_cnt_within_two_slots(slot_start, slot_end)
        }
    }

    /// Returns the total number of entries in the `SlotDex`.
    ///
    /// Automatically rebuilt from disk on recovery after an unclean
    /// shutdown (see [`from_meta`](Self::from_meta)).
    pub fn total(&self) -> EntryCnt {
        self.total_by_slot(None, None)
    }

    // --- Private Helper Methods ---

    fn mark_dirty(&mut self) {
        let raw = self.total.get_value();
        self.total.set_value(&dc::set_dirty(raw));
    }

    // Ensure there is enough tier capacity to cover the new slot.
    fn ensure_tier_capacity(&mut self) {
        let tiers_len = self.tiers.len();
        if let Some(top) = self.tiers.last_mut() {
            if (top.len() as i128) <= self.tier_capacity.as_i128() {
                return;
            }
            top.ensure_cache();
            let entries: Vec<(S, EntryCnt)> = top
                .cache
                .get_mut()
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect();
            let mut newtop = Tier::new(tiers_len as u32, &self.tier_capacity);
            for (slot, cnt) in entries {
                let slot_floor = slot.floor_align(&newtop.floor_base);
                let c = newtop.cache.get_mut();
                let v = c.get(&slot_floor).copied().unwrap_or(0);
                if 0 == v {
                    *newtop.entry_count.get_mut() += 1;
                    if let Some(l) = newtop.len_cache.as_mut() {
                        *l += 1;
                    }
                }
                let new_v = v + cnt;
                c.insert(slot_floor.clone(), new_v);
                newtop.store.insert(&slot_floor, &new_v);
            }
            self.tiers.push(newtop);
        } else {
            let mut newtop = Tier::new(tiers_len as u32, &self.tier_capacity);
            for (slot, entries) in self.data.iter() {
                let slot_floor = slot.floor_align(&newtop.floor_base);
                let c = newtop.cache.get_mut();
                let v = c.get(&slot_floor).copied().unwrap_or(0);
                if 0 == v {
                    *newtop.entry_count.get_mut() += 1;
                    if let Some(l) = newtop.len_cache.as_mut() {
                        *l += 1;
                    }
                }
                let new_v = v + entries.len() as EntryCnt;
                c.insert(slot_floor.clone(), new_v);
                newtop.store.insert(&slot_floor, &new_v);
            }
            self.tiers.push(newtop);
        }
    }

    // Convert a logical slot (user perspective) to a storage slot (internal key).
    #[inline(always)]
    fn to_storage_slot(&self, logical_slot: S) -> S {
        if self.swap_order {
            !logical_slot
        } else {
            logical_slot
        }
    }

    // Transform a logical range [min, max] into a storage range and direction flag.
    // Returns (storage_min, storage_max, storage_is_reversed_relative_to_logical)
    fn transform_range(
        &self,
        logical_min: Option<S>,
        logical_max: Option<S>,
    ) -> (S, S, bool) {
        let min = logical_min.unwrap_or(S::MIN);
        let max = logical_max.unwrap_or(S::MAX);

        if self.swap_order {
            // If storage is reversed:
            // logical [10, 20] -> storage [!20, !10]
            // And the storage order is reversed relative to logical order.
            (self.to_storage_slot(max), self.to_storage_slot(min), true)
        } else {
            (min, max, false)
        }
    }
}

/// Convenience alias for `SlotDex<u32, K>`.
pub type SlotDex32<K> = SlotDex<u32, K>;
/// Convenience alias for `SlotDex<u64, K>`.
pub type SlotDex64<K> = SlotDex<u64, K>;
/// Convenience alias for `SlotDex<u128, K>`.
pub type SlotDex128<K> = SlotDex<u128, K>;

// Compile-time proof that SlotDex is Send + Sync.
fn _assert_send_sync() {
    fn require<T: Send + Sync>() {}
    require::<SlotDex<u64, u64>>();
}

#[cfg(test)]
mod test;
